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
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

import requests
import yaml
from prompt_toolkit import Application
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style


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


class TaskDB:
    SCHEMA_COLUMNS = [
        "owner_type","owner","project_number","project_title","start_field",
        "start_date","title","repo","url","updated_at"
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
            "updated_at":"datetime('now')",
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
              start_field, start_date, title, repo, url, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_type, owner, project_number, title, url, start_field, start_date)
            DO UPDATE SET project_title=excluded.project_title,
                          repo=excluded.repo,
                          updated_at=excluded.updated_at
            """,            [(r.owner_type,r.owner,r.project_number,r.project_title,r.start_field,
              r.start_date,r.title,r.repo,r.url,r.updated_at) for r in rows]
        )
        self.conn.commit()

    def load(self, today_only=False, today: Optional[str]=None) -> List[TaskRow]:
        cur = self.conn.cursor()
        if today_only:
            today = today or dt.date.today().isoformat()
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,title,repo,url,updated_at
                FROM tasks WHERE start_date = ?
                ORDER BY project_title, start_date, repo, title
                """, (today,)
            )
        else:
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,title,repo,url,updated_at
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
                targets.append((spec.owner_type, spec.owner, int(n.get("number")), n.get("title") or ""))
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
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldUserValue":
                        people_logins.extend([n["login"] for n in (fv.get("users") or {}).get("nodes") or [] if n and "login" in n])
                if (me not in assignees) and (me not in people_logins):
                    continue

                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldDateValue":
                        fname = ((fv.get("field") or {}).get("name")) or ""
                        fdate = fv.get("date")
                        if not fdate or not regex.search(fname):
                            continue
                        try:
                            d = dt.date.fromisoformat(fdate)
                        except ValueError:
                            continue
                        if d <= date_cutoff:
                            out.append(
                                TaskRow(
                                    owner_type=owner_type, owner=owner, project_number=number,
                                    project_title=(it.get("project") or {}).get("title") or "",
                                    start_field=fname, start_date=fdate,
                                    title=title, repo=repo, url=url, updated_at=iso_now
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
    header = "DATE         FIELD                TITLE                                     REPO                 URL"
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
        frags.append((col, f"{t.start_date:<12}"))
        frags.append(("",  "  "))
        frags.append(("", f"{field:<20}  {title:<41}  {repo:<20}  {url}"))
        frags.append(("", "\n"))

    if frags and frags[-1] == ("", "\n"):
        frags.pop()
    return frags


# -----------------------------
# TUI
# -----------------------------
def run_ui(db: TaskDB, cfg: Config, token: Optional[str]) -> None:
    today_date = dt.date.today()
    show_today_only = False

    def load_view() -> List[TaskRow]:
        return db.load(today_only=show_today_only, today=today_date.isoformat())

    tasks = load_view()
    text_control = FormattedTextControl(
        text=build_fragments(tasks, today_date),
        focusable=True,
        show_cursor=False,
    )
    window = Window(content=text_control, wrap_lines=False)

    status = FormattedTextControl(
        text=lambda: [("reverse", f"  u:update  t:today  a:all  q:quit   • {cfg.user} • {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ")]
    )
    root = HSplit([window, Window(height=1, content=status)])
    kb = KeyBindings()

    def refresh_view():
        tks = load_view()
        text_control.text = build_fragments(tks, today_date)

    @kb.add("q")
    def _(event):
        event.app.exit()

    @kb.add("a")
    def _(event):
        nonlocal show_today_only
        show_today_only = False
        refresh_view()

    @kb.add("t")
    def _(event):
        nonlocal show_today_only
        show_today_only = True
        refresh_view()

    @kb.add("u")
    def _(event):
        app = event.app
        text_control.text = [("bold", "Updating cache…  "), ("", "[................................]   0%  Starting")]
        app.invalidate()

        def progress(done: int, total: int, line: str):
            def _update():
                text_control.text = [("bold", "Updating cache…  "), ("", line)]
                app.invalidate()
            app.call_from_executor(_update)

        async def worker():
            try:
                loop = asyncio.get_running_loop()

                def _do_fetch():
                    if os.environ.get("MOCK_FETCH") == "1":
                        rows = generate_mock_tasks(cfg)
                        progress(1, 1, "[########################################] 100%  Done")
                        return rows
                    if not token:
                        raise RuntimeError("GITHUB_TOKEN is not set")
                    return fetch_tasks_github(token, cfg, date_cutoff=today_date, progress=progress)

                rows = await loop.run_in_executor(None, _do_fetch)
                db.upsert_many(rows)

                def _finish():
                    refresh_view()
                    app.invalidate()
                app.call_from_executor(_finish)

            except Exception as e:
                def _error():
                    text_control.text = [("ansired", "Error: "), ("", str(e))]
                    app.invalidate()
                app.call_from_executor(_error)

        app.create_background_task(worker())

    style = Style.from_dict({"statusbar": "reverse"})
    app = Application(layout=Layout(root), key_bindings=kb, full_screen=True, mouse_support=True, style=style)
    app.run()


# -----------------------------
# CLI
# -----------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="GitHub Project tasks viewer")
    ap.add_argument("--config", required=True, help="Path to YAML config")
    ap.add_argument("--db", default=os.path.expanduser("~/.gh_tasks.db"), help="Path to sqlite DB")
    ap.add_argument("--discover", action="store_true", help="List open Projects v2 for each owner and exit")
    args = ap.parse_args()

    cfg = load_config(args.config)
    token = os.environ.get("GITHUB_TOKEN")

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
                print("GITHUB_TOKEN is not set. Set it or run with MOCK_FETCH=1.", file=sys.stderr)
                sys.exit(1)
            db.upsert_many(fetch_tasks_github(token, cfg, date_cutoff=dt.date.today()))

    run_ui(db, cfg, token)


if __name__ == "__main__":
    main()
