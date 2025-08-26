#!/usr/bin/env python3
# gh_task_viewer: Terminal GitHub Projects (v2) task viewer with live progress
#
# Hotkeys
#   u  refresh (runs in background and updates a progress bar)
#   t  show tasks with date == today
#   a  show all cached tasks
#   q  quit
#
# Config highlights
# - Accepts either:
#     date_field_regex: "^Start( date)?$"
#   or
#     date_field_names: ["Start date", "Due date", "Target date"]
# - For projects you can put explicit numbers or:
#     numbers: all               # auto-discover open Projects v2 for that owner
#
# Notes
# - Assigned-to-me = Issue/PR assignee OR People-field contains your login.
# - Filters Project DATE fields whose *name matches* your config (regex/names).
# - Skips NOT_FOUND/closed/inaccessible project numbers gracefully.
#
# Environment
# - GITHUB_TOKEN (scopes: repo, project, read:org)
# - MOCK_FETCH=1 (optional offline demo)

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import os
import re
import sqlite3
import sys
import string
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Iterable

import requests
import yaml
from prompt_toolkit import Application
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style
from prompt_toolkit.buffer import Buffer
from prompt_toolkit.layout.controls import BufferControl


# -----------------------------
# Config models
# -----------------------------
@dataclass
class ProjectSpec:
    owner_type: str            # "org" or "user"
    owner: str                 # login
    numbers: Optional[List[int]]  # None => auto-discover


@dataclass
class Config:
    user: str
    date_field_regex: str
    projects: List[ProjectSpec]


def _compile_date_regex(raw: dict) -> str:
    """Support date_field_regex (string) OR date_field_names (list[str])."""
    names = raw.get("date_field_names")
    if names and isinstance(names, list) and names:
        parts = [f"^{re.escape(n)}$" for n in names]
        return "|".join(parts)
    return raw.get("date_field_regex") or "start"


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    user = raw.get("user") or ""
    if not user:
        raise ValueError("Config: 'user' is required.")
    dfr = _compile_date_regex(raw)
    prjs: List[ProjectSpec] = []
    for item in raw.get("projects", []):
        nums_raw = item.get("numbers", None)
        if nums_raw is None or (isinstance(nums_raw, str) and str(nums_raw).lower() == "all"):
            nums = None
        else:
            nums = list(map(int, nums_raw))
        if "org" in item:
            prjs.append(ProjectSpec("org", item["org"], nums))
        elif "user" in item:
            prjs.append(ProjectSpec("user", item["user"], nums))
        else:
            raise ValueError(f"Project entry needs 'org' or 'user': {item}")
    return Config(user=user, date_field_regex=dfr, projects=prjs)


# -----------------------------
# DB
# -----------------------------
@dataclass
class TaskRow:
    owner_type: str
    owner: str
    project_number: int
    project_title: str
    start_field: str
    start_date: str
    title: str
    repo: Optional[str]
    url: str
    updated_at: str
    status: Optional[str] = None  # textual status (eg. In Progress, Done)
    is_done: int = 0              # 1 if done / completed


class TaskDB:
    SCHEMA_COLUMNS = [
        "owner_type","owner","project_number","project_title","start_field",
        "start_date","title","repo","url","updated_at","status","is_done"
    ]
    CREATE_TABLE_SQL = """      CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_type TEXT NOT NULL,
        owner TEXT NOT NULL,
        project_number INTEGER NOT NULL,
        project_title TEXT NOT NULL,
        start_field TEXT NOT NULL,
        start_date TEXT NOT NULL,
        title TEXT NOT NULL,
        repo TEXT,
        url TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        status TEXT,
        is_done INTEGER DEFAULT 0,
        UNIQUE(owner_type, owner, project_number, title, url, start_field, start_date)
      )
    """
    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._migrate_if_needed()

    def _cols(self) -> List[str]:
        cur = self.conn.cursor()
        try:
            cur.execute("PRAGMA table_info(tasks)")
            return [r[1] for r in cur.fetchall()]
        except sqlite3.OperationalError:
            return []

    def _idx(self):
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_date ON tasks(start_date)")
        self.conn.commit()

    def _migrate_if_needed(self):
        cols = self._cols()
        if not cols:
            self.conn.execute(self.CREATE_TABLE_SQL)
            self._idx()
            return
        missing = [c for c in self.SCHEMA_COLUMNS if c not in cols]
        if not missing:
            self._idx()
            return
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE tasks RENAME TO tasks_old")
        cur.execute(self.CREATE_TABLE_SQL)
        defaults = {
            "owner_type":"''","owner":"''","project_number":"0","project_title":"''",
            "start_field":"''","start_date":"''","title":"''","repo":"NULL","url":"''",
            "updated_at":"datetime('now')","status":"NULL","is_done":"0",
        }
        sel = ", ".join([c if c in cols else defaults[c] for c in self.SCHEMA_COLUMNS])
        cur.execute(
            f"INSERT OR IGNORE INTO tasks ({', '.join(self.SCHEMA_COLUMNS)}) "
            f"SELECT {sel} FROM tasks_old"
        )
        cur.execute("DROP TABLE tasks_old")
        self.conn.commit()
        self._idx()

    def upsert_many(self, rows: List[TaskRow]):
        if not rows:
            return
        cur = self.conn.cursor()
        cur.executemany(
            """            INSERT INTO tasks (
              owner_type, owner, project_number, project_title,
              start_field, start_date, title, repo, url, updated_at, status, is_done
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_type, owner, project_number, title, url, start_field, start_date)
            DO UPDATE SET project_title=excluded.project_title,
                          repo=excluded.repo,
                          updated_at=excluded.updated_at,
                          status=excluded.status,
                          is_done=excluded.is_done
            """,
            [
                (
                    r.owner_type,
                    r.owner,
                    r.project_number,
                    r.project_title,
                    r.start_field,
                    r.start_date,
                    r.title,
                    r.repo,
                    r.url,
                    r.updated_at,
                    r.status,
                    r.is_done,
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def load(self, today_only=False, today: Optional[str]=None) -> List[TaskRow]:
        cur = self.conn.cursor()
        if today_only:
            today = today or dt.date.today().isoformat()
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,title,repo,url,updated_at,status,is_done
                FROM tasks WHERE start_date = ?
                ORDER BY project_title, start_date, repo, title
                """,
                (today,),
            )
        else:
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,title,repo,url,updated_at,status,is_done
                FROM tasks
                ORDER BY project_title, start_date, repo, title
                """
            )
        return [TaskRow(*r) for r in cur.fetchall()]


# -----------------------------
# GitHub GraphQL
# -----------------------------
GQL_LIST_ORG_PROJECTS = """query($login:String!) {
  organization(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { number title url closed }
    }
  }
}
"""
GQL_LIST_USER_PROJECTS = """query($login:String!) {
  user(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { number title url closed }
    }
  }
}
"""
GQL_SCAN_ORG = """query($org:String!, $number:Int!, $after:String) {
  organization(login:$org){
    projectV2(number:$number){
      items(first:100, after:$after){
        pageInfo{ hasNextPage endCursor }
        nodes{
          content{
            __typename
            ... on DraftIssue { title }
            ... on Issue {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
            ... on PullRequest {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
          }
                    fieldValues(first:50){
                        nodes{
                            __typename
                            ... on ProjectV2ItemFieldDateValue {
                                date
                                field { ... on ProjectV2FieldCommon { name } }
                            }
                            ... on ProjectV2ItemFieldUserValue {
                                users(first:50){ nodes{ login } }
                                field { ... on ProjectV2FieldCommon { name } }
                            }
                            ... on ProjectV2ItemFieldSingleSelectValue {
                                name
                                field { ... on ProjectV2FieldCommon { name } }
                            }
                        }
                    }
          project{ title url }
        }
      }
    }
  }
}
"""
GQL_SCAN_USER = """query($login:String!, $number:Int!, $after:String) {
  user(login:$login){
    projectV2(number:$number){
      items(first:100, after:$after){
        pageInfo{ hasNextPage endCursor }
        nodes{
          content{
            __typename
            ... on DraftIssue { title }
            ... on Issue {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
            ... on PullRequest {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
          }
                    fieldValues(first:50){
                        nodes{
                            __typename
                            ... on ProjectV2ItemFieldDateValue {
                                date
                                field { ... on ProjectV2FieldCommon { name } }
                            }
                            ... on ProjectV2ItemFieldUserValue {
                                users(first:50){ nodes{ login } }
                                field { ... on ProjectV2FieldCommon { name } }
                            }
                            ... on ProjectV2ItemFieldSingleSelectValue {
                                name
                                field { ... on ProjectV2FieldCommon { name } }
                            }
                        }
                    }
          project{ title url }
        }
      }
    }
  }
}
"""

def _session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["Accept"] = "application/vnd.github+json"
    return s

def _graphql_raw(session: requests.Session, query: str, variables: Dict[str, object]) -> Dict:
    r = session.post("https://api.github.com/graphql", json={"query": query, "variables": variables}, timeout=60)
    r.raise_for_status()
    return r.json()

def discover_open_projects(session: requests.Session, owner_type: str, owner: str) -> List[Dict]:
    if owner_type == "org":
        data = _graphql_raw(session, GQL_LIST_ORG_PROJECTS, {"login": owner})
        nodes = (((data.get("data") or {}).get("organization") or {}).get("projectsV2") or {}).get("nodes") or []
    else:
        data = _graphql_raw(session, GQL_LIST_USER_PROJECTS, {"login": owner})
        nodes = (((data.get("data") or {}).get("user") or {}).get("projectsV2") or {}).get("nodes") or []
    return [n for n in nodes if not n.get("closed")]


# -----------------------------
# Fetch with progress callback
# -----------------------------
ProgressCB = Callable[[int, int, str], None]  # (done, total, status_line)

def _ascii_bar(done:int, total:int, width:int=40)->str:
    pct = 0 if total<=0 else int(done*100/total)
    fill = int(width*pct/100)
    return f"[{'#'*fill}{'.'*(width-fill)}] {pct:3d}%"

def fetch_tasks_github(
    token: str,
    cfg: Config,
    date_cutoff: dt.date,
    include_unassigned: bool = False,
    progress: Optional[ProgressCB] = None,
) -> List[TaskRow]:
    session = _session(token)
    regex = re.compile(cfg.date_field_regex, re.IGNORECASE)
    me = cfg.user
    iso_now = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
    out: List[TaskRow] = []

    targets: List[Tuple[str,str,int,str]] = []
    for spec in cfg.projects:
        if spec.numbers is None:
            projs = discover_open_projects(session, spec.owner_type, spec.owner)
            for n in projs:
                num_val = n.get("number")
                try:
                    num_int = int(num_val) if num_val is not None else -1
                except (TypeError, ValueError):
                    continue
                targets.append((spec.owner_type, spec.owner, num_int, n.get("title") or ""))
        else:
            for num in spec.numbers:
                targets.append((spec.owner_type, spec.owner, int(num), ""))

    total = len(targets)
    done = 0

    def tick(msg: str):
        nonlocal done
        if progress:
            progress(done, total, f"{_ascii_bar(done,total)}  {msg}")

    for owner_type, owner, number, ptitle in targets:
        tick(f"Scanning {owner_type}:{owner} #{number} {('— '+ptitle) if ptitle else ''}")
        after = None
        while True:
            variables = (
                {"org": owner, "number": number, "after": after}
                if owner_type == "org"
                else {"login": owner, "number": number, "after": after}
            )
            query = GQL_SCAN_ORG if owner_type == "org" else GQL_SCAN_USER
            resp = _graphql_raw(session, query, variables)

            errs = resp.get("errors") or []
            if errs:
                nf = any((e.get("type") == "NOT_FOUND") and ("projectV2" in (e.get("path") or [])) for e in errs)
                if nf:
                    break  # skip invalid/inaccessible project number
                raise RuntimeError(f"GraphQL errors: {errs}")

            proj_node = (
                (((resp.get("data") or {}).get("organization") or {}).get("projectV2"))
                if owner_type == "org"
                else (((resp.get("data") or {}).get("user") or {}).get("projectV2"))
            )
            if not proj_node:
                break

            items = (proj_node.get("items") or {}).get("nodes") or []
            for it in items:
                content = it.get("content") or {}
                ctype = content.get("__typename")
                title = content.get("title") or "(Draft item)"
                url = content.get("url") or it.get("project", {}).get("url") or ""
                repo = None
                if ctype in ("Issue","PullRequest"):
                    rep = content.get("repository") or {}
                    repo = rep.get("nameWithOwner")

                assignees = []
                if ctype in ("Issue","PullRequest"):
                    assignees = [n["login"] for n in (content.get("assignees") or {}).get("nodes") or [] if n and "login" in n]
                people_logins: List[str] = []
                status_text: Optional[str] = None
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldUserValue":
                        people_logins.extend([n["login"] for n in (fv.get("users") or {}).get("nodes") or [] if n and "login" in n])
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldSingleSelectValue":
                        fname_sel = ((fv.get("field") or {}).get("name") or "").lower()
                        if fname_sel in ("status","state","progress"):
                            status_text = (fv.get("name") or "").strip()
                assigned_to_me = (me in assignees) or (me in people_logins)
                if (not assigned_to_me) and (not include_unassigned):
                    continue

                found_date = False
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldDateValue":
                        fname = ((fv.get("field") or {}).get("name")) or ""
                        fdate = fv.get("date")
                        if not fdate or not regex.search(fname):
                            continue
                        try:
                            dt.date.fromisoformat(fdate)  # validate
                        except ValueError:
                            continue
                        # Store all tasks regardless of whether date is past/future so UI can filter.
                        done_flag = 0
                        if status_text:
                            low = status_text.lower()
                            if any(k in low for k in ("done","complete","closed","merged","finished","✅","✔")):
                                done_flag = 1
                        out.append(
                            TaskRow(
                                owner_type=owner_type, owner=owner, project_number=number,
                                project_title=(it.get("project") or {}).get("title") or "",
                                start_field=fname, start_date=fdate,
                                title=title, repo=repo, url=url, updated_at=iso_now,
                                status=status_text, is_done=done_flag
                            )
                        )
                        found_date = True
                # If no matching date field was found, still include the item so the project shows up.
                if not found_date:
                    done_flag = 0
                    if status_text:
                        low = status_text.lower()
                        if any(k in low for k in ("done","complete","closed","merged","finished","✅","✔")):
                            done_flag = 1
                    out.append(
                        TaskRow(
                            owner_type=owner_type, owner=owner, project_number=number,
                            project_title=(it.get("project") or {}).get("title") or "",
                            start_field="(no date)", start_date="",  # empty date -> neutral grey
                            title=title + (" (unassigned)" if not assigned_to_me else ""), repo=repo, url=url, updated_at=iso_now,
                            status=status_text, is_done=done_flag
                        )
                    )

            page = (proj_node.get("items") or {}).get("pageInfo") or {}
            if page.get("hasNextPage"):
                after = page.get("endCursor")
                tick(f"Scanning {owner_type}:{owner} #{number} (next page)")
            else:
                break

        done += 1
        tick(f"Finished {owner_type}:{owner} #{number}")

    if progress:
        progress(total, total, f"{_ascii_bar(total,total)}  Done")
    # Ensure each project appears at least once
    existing = {(r.owner_type, r.owner, r.project_number) for r in out}
    for owner_type, owner, number, ptitle in targets:
        key = (owner_type, owner, number)
        if key not in existing:
            out.append(
                TaskRow(
                    owner_type=owner_type, owner=owner, project_number=number,
                    project_title=ptitle or "(project)", start_field="(none)", start_date="",
                    title="(no assigned items) - press Shift+U to include unassigned", repo=None, url="", updated_at=iso_now,
                    status=None, is_done=0
                )
            )
    return out


# -----------------------------
# UI helpers (fragments only)
# -----------------------------
def color_for_date(d: str, today: dt.date) -> str:
    try:
        dd = dt.date.fromisoformat(d)
    except Exception:
        return "ansigray"
    if dd == today:
        return "ansired bold"
    if dd < today:
        return "ansiyellow"
    return "ansigreen"

def _truncate(s: str, maxlen: int) -> str:
    s = (s or "").replace("\n", " ").replace("\r", " ")
    return s if len(s) <= maxlen else s[: maxlen - 1] + "…"

def build_fragments(tasks: List[TaskRow], today: dt.date) -> List[Tuple[str, str]]:
    """Return a list of (style, text) tuples for FormattedTextControl."""
    frags: List[Tuple[str, str]] = []
    if not tasks:
        return [("bold", "Nothing to show."), ("", " Press "), ("bold", "u"), ("", " to fetch.")]

    current: Optional[str] = None
    header = "DATE         FIELD                STATUS      TITLE                                     REPO                 URL"
    for t in tasks:
        if t.project_title != current:
            current = t.project_title
            if frags:
                frags.append(("", "\n"))
            frags.append(("bold", f"## {current}"))
            frags.append(("", "\n"))
            frags.append(("bold", header))
            frags.append(("", "\n"))

        col = color_for_date(t.start_date, today)
        title = _truncate(t.title, 61)
        repo  = _truncate(t.repo or "-", 20)
        url   = _truncate(t.url, 40)
        field = _truncate(t.start_field, 20)
        status = _truncate(t.status or "-", 10)
        frags.append((col, f"{t.start_date:<12}"))
        frags.append(("",  "  "))
        frags.append(("", f"{field:<20}  {status:<10}  {title:<41}  {repo:<20}  {url}"))
        frags.append(("", "\n"))

    if frags and frags[-1] == ("", "\n"):
        frags.pop()
    return frags


# -----------------------------
# TUI
# -----------------------------
def run_ui(db: TaskDB, cfg: Config, token: Optional[str]) -> None:
    """Full-screen, non-editable, vim-like browser with:
    - j/k, arrows: move selection
    - g g / G: top / bottom
    - h/l, arrows: horizontal scroll
    - Enter: toggle detail popup
    - / start incremental search (inline); type to build query, Enter confirm, Esc cancel
    - p cycle project filter; d toggle done-only; t today-only; a all dates; u update cache
    - q quit (or close detail)
    """
    today_date = dt.date.today()
    show_today_only = False
    # Hide-done toggle: start showing everything; 'd' hides completed tasks.
    hide_done = False
    show_unassigned = False
    project_cycle: Optional[str] = None
    search_term: Optional[str] = None
    in_search = False
    search_buffer = ""
    current_index = 0
    v_offset = 0  # top row index currently displayed
    h_offset = 0
    detail_mode = False
    status_line = ""

    def load_all():
        return db.load(today_only=show_today_only, today=today_date.isoformat())

    all_rows = load_all()

    def apply_filters(rows: List[TaskRow]) -> List[TaskRow]:
        out = rows
        if hide_done:
            out = [r for r in out if not r.is_done]
        if project_cycle:
            out = [r for r in out if r.project_title == project_cycle]
        if search_term:
            needle = search_term.lower()
            out = [r for r in out if needle in (r.title or '').lower() or
                                   needle in (r.repo or '').lower() or
                                   needle in (r.status or '').lower() or
                                   needle in (r.project_title or '').lower()]
        return out

    def projects_list(rows: Iterable[TaskRow]) -> List[str]:
        seen = []
        for r in rows:
            if r.project_title not in seen:
                seen.append(r.project_title)
        return seen

    def filtered_rows() -> List[TaskRow]:
        return apply_filters(all_rows)

    def build_table_fragments() -> List[Tuple[str,str]]:
        rows = filtered_rows()
        nonlocal current_index
        nonlocal v_offset
        if current_index >= len(rows):
            current_index = max(0, len(rows)-1)
        # Determine available vertical space (rough estimate: terminal rows - status bar - maybe 0 extra)
        try:
            from prompt_toolkit.application.current import get_app
            total_rows = get_app().output.get_size().rows
        except Exception:
            total_rows = 40
        # Reserve 1 row for status bar. Header consumes 2 lines (header + blank after).
        visible_rows = max(1, total_rows - 3)
        # Adjust v_offset to ensure current_index visible
        if current_index < v_offset:
            v_offset = current_index
        elif current_index >= v_offset + visible_rows:
            v_offset = current_index - visible_rows + 1
        frags: List[Tuple[str,str]] = []
        header = "DATE        FIELD                STATUS      TITLE                                     REPO                 URL"
        frags.append(("bold", header[h_offset:]))
        frags.append(("", "\n"))
        if not rows:
            frags.append(("italic", "(no tasks match filters)"))
            return frags
        today = today_date
        display_slice = rows[v_offset:v_offset+visible_rows]
        for rel_idx, t in enumerate(display_slice):
            idx = v_offset + rel_idx
            is_sel = (idx == current_index)
            style_row = "reverse" if is_sel else ""
            col = color_for_date(t.start_date, today)
            base_style = (col + " bold") if is_sel else col
            title = _truncate(t.title, 41)
            repo = _truncate(t.repo or '-', 20)
            url = _truncate(t.url, 40)
            field = _truncate(t.start_field, 20)
            status_txt = _truncate(t.status or '-', 10)
            line = f"{t.start_date:<12}  {field:<20}  {status_txt:<10}  {title:<41}  {repo:<20}  {url}"
            line = line[h_offset:]
            # highlight search term occurrences
            if search_term and not is_sel:
                low = line.lower()
                needle = search_term.lower()
                start = 0
                while True:
                    i = low.find(needle, start)
                    if i == -1:
                        break
                    if i>start:
                        frags.append((base_style, line[start:i]))
                    frags.append((base_style + ' underline', line[i:i+len(needle)]))
                    start = i+len(needle)
                if start < len(line):
                    frags.append((base_style, line[start:]))
            else:
                frags.append((base_style if not is_sel else style_row, line))
            frags.append(("", "\n"))
        if frags and frags[-1][1] == "\n":
            frags.pop()
        return frags

    def summarize() -> str:
        rows = filtered_rows()
        if not rows:
            return "No tasks"
        per_status: Dict[str,int] = {}
        for r in rows:
            per_status[r.status or '(none)'] = per_status.get(r.status or '(none)',0)+1
        total = len(rows)
        done_ct = sum(1 for r in rows if r.is_done)
        lines = [f"User: {cfg.user}", f"Total: {total}", f"Done: {done_ct}"]
        # projects
        by_proj: Dict[str, Tuple[int,int]] = {}
        for r in rows:
            d,t = by_proj.get(r.project_title,(0,0))
            by_proj[r.project_title]=(d+(1 if r.is_done else 0), t+1)
        lines.append("")
        lines.append("Proj:")
        for p,(d,t) in sorted(by_proj.items()):
            pct = 0 if t==0 else int(d*100/t)
            lines.append(f"{_truncate(p,12):<12}{d:>2}/{t:<2} {pct:>3}%")
        lines.append("")
        lines.append("Filters:")
        lines.append(f"HideDone:{'Y' if hide_done else 'N'} Unassigned:{'Y' if show_unassigned else 'N'}")
        lines.append(f"Proj:{_truncate(project_cycle or 'All',10)}")
        lines.append(f"Today:{'Y' if show_today_only else 'N'}")
        lines.append(f"Search:{_truncate(search_term or '-',12)}")
        return "\n".join(lines)

    table_control = FormattedTextControl(text=lambda: build_table_fragments())
    table_window = Window(content=table_control, wrap_lines=False, always_hide_cursor=True)
    stats_control = FormattedTextControl(text=lambda: summarize())
    stats_window = Window(width=32, content=stats_control, wrap_lines=False, always_hide_cursor=True)

    detail_control = FormattedTextControl(text=lambda: build_detail_text())
    detail_window = Window(width=80, height=20, content=detail_control, wrap_lines=True, always_hide_cursor=True, style="bg:#202020 #ffffff")

    def build_detail_text() -> List[Tuple[str,str]]:
        if not detail_mode:
            return []
        rows = filtered_rows()
        if not rows:
            return [("", "No selection")] 
        t = rows[current_index]
        lines = [
            f"Project: {t.project_title}",
            f"Title:   {t.title}",
            f"Repo:    {t.repo}",
            f"URL:     {t.url}",
            f"Date:    {t.start_date} ({t.start_field})",
            f"Status:  {t.status}",
            f"Done:    {'Yes' if t.is_done else 'No'}",
            "",
            "Press Enter / q / Esc to close"
        ]
        return [("bold", "Task Detail"), ("", "\n"+"\n".join(lines))]

    status_control = FormattedTextControl(text=lambda: [("reverse", build_status_bar())])
    status_window = Window(height=1, content=status_control)

    show_help = False

    def build_status_bar() -> str:
        mode = "SEARCH" if in_search else ("DETAIL" if detail_mode else ("HELP" if show_help else "BROWSE"))
        base = f" {mode} u:update U:unassigned j/k:nav h/l:←/→ /:search Enter:detail p:project d:hide-done t:today a:all ?:help q:quit "
        if search_term:
            base += f"| filter='{search_term}' "
        if hide_done:
            base += "[HideDone] "
        if project_cycle:
            base += f"[Proj:{_truncate(project_cycle,10)}] "
        return base + status_line

    from prompt_toolkit.layout.containers import Float, FloatContainer
    floats = []
    root_body = VSplit([table_window, Window(width=1, char='│'), stats_window])
    container = FloatContainer(content=HSplit([root_body, status_window]), floats=floats)

    kb = KeyBindings()

    def invalidate():
        table_control.text = lambda: build_table_fragments()  # ensure recalculated
        stats_control.text = lambda: summarize()
        app.invalidate()

    @kb.add('q')
    def _(event):
        nonlocal detail_mode, in_search, search_buffer
        if detail_mode:
            detail_mode = False
            if floats:
                floats.clear()
            invalidate()
            return
        if in_search:
            in_search = False
            search_buffer = ""
            invalidate()
            return
        event.app.exit()

    @kb.add('enter')
    def _(event):
        nonlocal detail_mode
        if in_search:
            finalize_search()
            return
        detail_mode = not detail_mode
        floats.clear()
        if detail_mode:
            floats.append(Float(content=detail_window, top=2, left=4))
        invalidate()

    def move(delta:int):
        nonlocal current_index, v_offset
        rows = filtered_rows()
        if not rows:
            current_index = 0
            return
        current_index = max(0, min(len(rows)-1, current_index+delta))
        # Adjust vertical offset (reuse logic from build but simpler here)
        try:
            from prompt_toolkit.application.current import get_app
            total_rows = get_app().output.get_size().rows
        except Exception:
            total_rows = 40
        visible_rows = max(1, total_rows - 3)
        if current_index < v_offset:
            v_offset = current_index
        elif current_index >= v_offset + visible_rows:
            v_offset = current_index - visible_rows + 1

    @kb.add('j')
    @kb.add('down')
    def _(event):
        if detail_mode or in_search:
            return
        move(1); invalidate()

    @kb.add('k')
    @kb.add('up')
    def _(event):
        if detail_mode or in_search:
            return
        move(-1); invalidate()

    # horizontal scroll
    @kb.add('h')
    @kb.add('left')
    def _(event):
        nonlocal h_offset
        if detail_mode or in_search:
            return
        h_offset = max(0, h_offset-4); invalidate()

    @kb.add('l')
    @kb.add('right')
    def _(event):
        nonlocal h_offset
        if detail_mode or in_search:
            return
        h_offset += 4; invalidate()

    # top/bottom
    gg_state = {'g': False}
    @kb.add('g')
    def _(event):
        if detail_mode or in_search:
            return
        if gg_state['g']:
            gg_state['g'] = False
            nonlocal current_index
            current_index = 0
            invalidate()
        else:
            gg_state['g'] = True

    @kb.add('G')
    def _(event):
        if detail_mode or in_search:
            return
        rows = filtered_rows();
        if rows:
            nonlocal current_index
            current_index = len(rows)-1
            invalidate()

    # filters
    @kb.add('d')
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal hide_done, current_index
        hide_done = not hide_done
        current_index = 0
        invalidate()

    @kb.add('t')
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal show_today_only, all_rows, current_index
        show_today_only = True
        all_rows = load_all(); current_index = 0
        invalidate()

    @kb.add('a')
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal show_today_only, all_rows, current_index
        show_today_only = False
        all_rows = load_all(); current_index = 0
        invalidate()

    @kb.add('p')
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal project_cycle
        projs = projects_list(load_all())
        if not projs:
            project_cycle = None
        else:
            if project_cycle is None:
                project_cycle = projs[0]
            else:
                try:
                    i = projs.index(project_cycle)
                    project_cycle = projs[(i+1)%len(projs)]
                except ValueError:
                    project_cycle = projs[0]
        invalidate()

    # search mode
    @kb.add('/')
    def _(event):
        if detail_mode:
            return
        nonlocal in_search, search_buffer
        in_search = True
        search_buffer = ""
        invalidate()

    def finalize_search():
        nonlocal in_search, search_term, search_buffer, current_index
        search_term = search_buffer or None
        in_search = False
        rows = filtered_rows()
        current_index = 0 if rows else 0
        invalidate()

    @kb.add('escape')
    def _(event):
        nonlocal in_search, detail_mode, search_buffer
        if in_search:
            in_search = False; search_buffer = ""; invalidate(); return
        if detail_mode:
            detail_mode = False; floats.clear(); invalidate(); return

    @kb.add('backspace')
    def _(event):
        nonlocal search_buffer
        if in_search and search_buffer:
            search_buffer = search_buffer[:-1]
            invalidate()

    # character input for search (exclude reserved keys used for commands)
    reserved = set("jkgGhHlLdDtTaApPuUqQ/?:")
    search_chars = [c for c in (string.ascii_letters + string.digits + " -_./") if c not in reserved]
    for ch in search_chars:
        @kb.add(ch)
        def _(event, _ch=ch):
            nonlocal search_buffer, status_line
            if in_search:
                search_buffer += _ch
                status_line = f"Search: {search_buffer}"
                invalidate()

    @kb.add('u')
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal status_line, all_rows, current_index
        status_line = "Updating..."; invalidate()

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        def progress(done:int,total:int,line:str):
            nonlocal status_line
            status_line = line
            invalidate()

        async def worker():
            try:
                def do_fetch():
                    if os.environ.get('MOCK_FETCH')=='1':
                        rows = generate_mock_tasks(cfg)
                        progress(1,1,'[########################################] 100% Done')
                        return rows
                    if not token:
                        raise RuntimeError('TOKEN not set')
                    return fetch_tasks_github(token, cfg, date_cutoff=today_date, progress=progress, include_unassigned=show_unassigned)
                fut_rows = await asyncio.get_running_loop().run_in_executor(None, do_fetch)
                db.upsert_many(fut_rows)
                all_rows = load_all(); current_index = 0
                progress(1,1,'Updated')
            except Exception as e:
                status_line = f"Error: {e}"; invalidate()
        asyncio.create_task(worker())

    def update_search_status():
        nonlocal status_line
        if in_search:
            status_line = f"Search: {search_buffer}"
        elif not detail_mode and not status_line:
            status_line = ''

    @kb.add('?')
    def _(event):
        nonlocal show_help, detail_mode, in_search
        if in_search:
            return
        detail_mode = False
        in_search = False
        show_help = not show_help
        floats.clear()
        if show_help:
            help_lines = [
                "Hotkeys:",
                "  j/k or arrows  Move selection",
                "  gg / G         Top / Bottom",
                "  h/l or arrows  Horizontal scroll",
                "  Enter          Toggle detail pane",
                "  /              Start search (type, Enter to apply, Esc cancel)",
                "  p              Cycle project filter",
                "  U              Toggle include unassigned (then press u to refetch)",
                "  d              Toggle done-only filter",
                "  t / a          Today-only / All dates",
                "  u              Update (fetch GitHub)",
                "  ?              Toggle help",
                "  q / Esc        Quit / Close",
                f"  Current tasks: {len(filtered_rows())}",
                "",
                "Press ? to close help."
            ]
            hl_control = FormattedTextControl(text="\n".join(help_lines))
            floats.append(Float(content=Window(width=84, height=24, content=hl_control, style="bg:#202020 #ffffff", wrap_lines=True), top=1, left=2))
        invalidate()

    # refresh loop timer to update status bar (search typing etc.)
    style = Style.from_dict({})
    app = Application(layout=Layout(container), key_bindings=kb, full_screen=True, mouse_support=True, style=style, editing_mode=EditingMode.VI)
    app.run()
    return


# -----------------------------
# Utilities / Mock
# -----------------------------
def generate_mock_tasks(cfg: Config) -> List[TaskRow]:
    """Generate synthetic tasks for offline demo & testing."""
    today = dt.date.today()
    rows: List[TaskRow] = []
    iso_now = dt.datetime.now().isoformat(timespec="seconds")
    projects = ["Alpha", "Beta", "Gamma"]
    statuses = ["Todo", "In Progress", "Done", "Blocked"]
    for i, proj in enumerate(projects, start=1):
        for d_off in range(-2, 5):
            date_str = (today + dt.timedelta(days=d_off)).isoformat()
            status = statuses[(i + d_off) % len(statuses)]
            rows.append(TaskRow(
                owner_type="org", owner="example", project_number=i, project_title=proj,
                start_field="Start date", start_date=date_str, title=f"Task {i}-{d_off}", repo="demo/repo",
                url=f"https://example.com/{i}-{d_off}", updated_at=iso_now, status=status,
                is_done=1 if status.lower()=="done" else 0
            ))
    return rows


def load_dotenv_token() -> Optional[str]:
    """Load TOKEN or GITHUB_TOKEN from a .env file (current dir or script dir) if present."""
    candidates = [os.getcwd(), os.path.dirname(os.path.abspath(__file__))]
    for base in candidates:
        path = os.path.join(base, ".env")
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or '=' not in line:
                        continue
                    k,v = line.split('=',1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k in ("TOKEN","GITHUB_TOKEN") and v:
                        # set env for child libs as well
                        os.environ.setdefault("GITHUB_TOKEN", v)
                        return v
        except Exception:
            continue
    return None


# -----------------------------
# CLI
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="GitHub Project tasks viewer")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--db", default=os.path.expanduser("~/.gh_tasks.db"), help="Path to sqlite DB")
    ap.add_argument("--discover", action="store_true", help="List open Projects v2 for each owner and exit")
    ap.add_argument("--no-ui", action="store_true", help="Run a non-interactive summary (for testing)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    # Load token precedence: env var, .env TOKEN/GITHUB_TOKEN
    token = os.environ.get("GITHUB_TOKEN") or load_dotenv_token()

    if args.discover:
        if not token:
            print("GITHUB_TOKEN is not set (needed for --discover).", file=sys.stderr)
            sys.exit(1)
        s = _session(token)
        for spec in cfg.projects:
            print(f"{spec.owner_type}:{spec.owner}")
            projs = discover_open_projects(s, spec.owner_type, spec.owner)
            if not projs:
                print("  (no open projects or insufficient access)")
                continue
            for n in projs:
                print(f"  #{n['number']}: {n['title']}")
        return

    db = TaskDB(args.db)

    if not db.load():
        if os.environ.get("MOCK_FETCH") == "1":
            db.upsert_many(generate_mock_tasks(cfg))
        else:
            if not token:
                print("TOKEN/GITHUB_TOKEN not set. Create .env with TOKEN=... or export variable (or use MOCK_FETCH=1).", file=sys.stderr)
                sys.exit(1)
            db.upsert_many(fetch_tasks_github(token, cfg, date_cutoff=dt.date.today()))

    if args.no_ui:
        rows = db.load()
        done_ct = sum(1 for r in rows if r.is_done)
        print(f"Tasks: {len(rows)} (done {done_ct})")
        projects = sorted({r.project_title for r in rows})
        print("Projects:", ", ".join(projects))
        return

    run_ui(db, cfg, token)


if __name__ == "__main__":
    main()
