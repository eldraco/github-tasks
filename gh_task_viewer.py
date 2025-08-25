#!/usr/bin/env python3
"""
gh_task_viewer: Terminal GitHub Projects (v2) task viewer

Hotkeys:  u (update cache from GitHub) • t (today only) • a (all) • q (quit)

New in this version
- Graceful handling of NOT_FOUND project numbers (skip & continue; no crash)
- --discover flag to list open Projects v2 numbers for each owner in config
- numbers: all (or omit numbers) to auto-discover open Projects v2 per owner
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
import yaml
from prompt_toolkit import Application
from prompt_toolkit.formatted_text import HTML, to_formatted_text
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style


# -----------------------------
# Config & Data models
# -----------------------------
@dataclass
class ProjectSpec:
    owner_type: str            # "org" or "user"
    owner: str                 # login
    numbers: Optional[List[int]]  # None == auto-discover all open projects


@dataclass
class Config:
    user: str                    # GitHub login for assignment checks
    date_field_regex: str        # regex to match DATE field names
    projects: List[ProjectSpec]  # owners/projects to scan


def load_config(path: str) -> "Config":
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    user = raw.get("user") or ""
    if not user:
        raise ValueError("Config: 'user' is required.")
    dfr = raw.get("date_field_regex") or "start"
    prjs: List[ProjectSpec] = []
    for item in raw.get("projects", []):
        # numbers can be omitted or set to "all" to auto-discover
        nums_val = item.get("numbers", None)
        nums: Optional[List[int]]
        if nums_val is None or (isinstance(nums_val, str) and nums_val.lower() == "all"):
            nums = None
        else:
            nums = list(map(int, nums_val))
        if "org" in item:
            prjs.append(ProjectSpec("org", item["org"], nums))
        elif "user" in item:
            prjs.append(ProjectSpec("user", item["user"], nums))
        else:
            raise ValueError(f"Project entry needs 'org' or 'user': {item}")
    return Config(user=user, date_field_regex=dfr, projects=prjs)


@dataclass
class TaskRow:
    owner_type: str
    owner: str
    project_number: int
    project_title: str
    start_field: str
    start_date: str  # YYYY-MM-DD
    title: str
    repo: Optional[str]
    url: str
    updated_at: str  # ISO timestamp


# -----------------------------
# SQLite cache with migration
# -----------------------------
class TaskDB:
    SCHEMA_COLUMNS = [
        "owner_type",
        "owner",
        "project_number",
        "project_title",
        "start_field",
        "start_date",
        "title",
        "repo",
        "url",
        "updated_at",
    ]

    CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS tasks (
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
        self.path = path
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init()

    def _existing_columns(self) -> List[str]:
        cur = self.conn.cursor()
        try:
            cur.execute("PRAGMA table_info(tasks)")
            return [r[1] for r in cur.fetchall()]
        except sqlite3.OperationalError:
            return []

    def _create_indexes(self) -> None:
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_date ON tasks(start_date)")
        self.conn.commit()

    def _migrate_if_needed(self) -> None:
        cols = self._existing_columns()
        if not cols:
            self.conn.execute(self.CREATE_TABLE_SQL)
            self._create_indexes()
            return
        missing = [c for c in self.SCHEMA_COLUMNS if c not in cols]
        if not missing:
            self._create_indexes()
            return
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE tasks RENAME TO tasks_old")
        cur.execute(self.CREATE_TABLE_SQL)

        def sel(col: str) -> str:
            if col in cols:
                return col
            defaults = {
                "owner_type": "''",
                "owner": "''",
                "project_number": "0",
                "project_title": "''",
                "start_field": "''",
                "start_date": "''",
                "title": "''",
                "repo": "NULL",
                "url": "''",
                "updated_at": "datetime('now')",
            }
            return defaults[col]

        select_exprs = ", ".join(sel(c) for c in self.SCHEMA_COLUMNS)
        insert_cols = ", ".join(self.SCHEMA_COLUMNS)
        cur.execute(
            f"INSERT OR IGNORE INTO tasks ({insert_cols}) "
            f"SELECT {select_exprs} FROM tasks_old"
        )
        cur.execute("DROP TABLE tasks_old")
        self.conn.commit()
        self._create_indexes()

    def _init(self) -> None:
        self._migrate_if_needed()

    def upsert_many(self, rows: List[TaskRow]) -> None:
        if not rows:
            return
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO tasks (
                owner_type, owner, project_number, project_title,
                start_field, start_date, title, repo, url, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_type, owner, project_number, title, url, start_field, start_date)
            DO UPDATE SET
                project_title = excluded.project_title,
                repo = excluded.repo,
                updated_at = excluded.updated_at
            """,
            [
                (
                    r.owner_type, r.owner, r.project_number, r.project_title,
                    r.start_field, r.start_date, r.title, r.repo, r.url, r.updated_at
                )
                for r in rows
            ],
        )
        self.conn.commit()

    def load(self, today_only: bool = False, today: Optional[str] = None) -> List[TaskRow]:
        cur = self.conn.cursor()
        if today_only:
            if not today:
                today = dt.date.today().isoformat()
            cur.execute(
                """
                SELECT owner_type, owner, project_number, project_title, start_field,
                       start_date, title, repo, url, updated_at
                FROM tasks WHERE start_date = ?
                ORDER BY project_title, start_date, repo, title
                """,
                (today,),
            )
        else:
            cur.execute(
                """
                SELECT owner_type, owner, project_number, project_title, start_field,
                       start_date, title, repo, url, updated_at
                FROM tasks
                ORDER BY project_title, start_date, repo, title
                """
            )
        return [
            TaskRow(
                owner_type=r[0], owner=r[1], project_number=r[2], project_title=r[3],
                start_field=r[4], start_date=r[5], title=r[6], repo=r[7], url=r[8], updated_at=r[9]
            )
            for r in cur.fetchall()
        ]


# -----------------------------
# GitHub GraphQL queries
# -----------------------------
GQL_LIST_ORG_PROJECTS = """
query($login:String!) {
  organization(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { number title url closed }
    }
  }
}
"""

GQL_LIST_USER_PROJECTS = """
query($login:String!) {
  user(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { number title url closed }
    }
  }
}
"""

GQL_SCAN_ORG = """
query($org:String!, $number:Int!, $after:String) {
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

GQL_SCAN_USER = """
query($login:String!, $number:Int!, $after:String) {
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


def fetch_tasks_github(token: str, cfg: Config, date_cutoff: dt.date) -> List[TaskRow]:
    session = _session(token)
    regex = re.compile(cfg.date_field_regex, re.IGNORECASE)
    me = cfg.user
    iso_now = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
    out: List[TaskRow] = []

    for spec in cfg.projects:
        # Decide which numbers to scan
        numbers: List[int]
        if spec.numbers is None:
            # auto-discover open projects for this owner
            discovered = discover_open_projects(session, spec.owner_type, spec.owner)
            numbers = [int(n.get("number")) for n in discovered]
        else:
            numbers = list(spec.numbers)

        for number in numbers:
            after = None
            while True:
                variables = (
                    {"org": spec.owner, "number": int(number), "after": after}
                    if spec.owner_type == "org"
                    else {"login": spec.owner, "number": int(number), "after": after}
                )
                query = GQL_SCAN_ORG if spec.owner_type == "org" else GQL_SCAN_USER
                resp = _graphql_raw(session, query, variables)

                # If GraphQL returns NOT_FOUND for this project number, skip it
                errs = resp.get("errors") or []
                if errs:
                    nf = any(
                        (e.get("type") == "NOT_FOUND") and ("projectV2" in (e.get("path") or []))
                        for e in errs
                    )
                    if nf:
                        # silently skip bad project number (wrong/archived/private)
                        break
                    else:
                        # other errors: raise
                        raise RuntimeError(f"GraphQL errors: {errs}")

                # Navigate to project node
                project_node = (
                    (((resp.get("data") or {}).get("organization") or {}).get("projectV2"))
                    if spec.owner_type == "org"
                    else (((resp.get("data") or {}).get("user") or {}).get("projectV2"))
                )
                if not project_node:
                    break

                items = (project_node.get("items") or {}).get("nodes") or []
                for it in items:
                    content = it.get("content") or {}
                    ctype = content.get("__typename")
                    title = content.get("title") or "(Draft item)"
                    url = content.get("url") or it.get("project", {}).get("url") or ""
                    repo = None
                    if ctype in ("Issue", "PullRequest"):
                        rep = content.get("repository") or {}
                        repo = rep.get("nameWithOwner")

                    # assigned? assignees OR People field
                    assignees = []
                    if ctype in ("Issue", "PullRequest"):
                        assignees = [n["login"] for n in (content.get("assignees") or {}).get("nodes") or [] if n and "login" in n]
                    people_field_logins: List[str] = []
                    for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                        if fv and fv.get("__typename") == "ProjectV2ItemFieldUserValue":
                            people_field_logins.extend(
                                [n["login"] for n in (fv.get("users") or {}).get("nodes") or [] if n and "login" in n]
                            )
                    if (me not in assignees) and (me not in people_field_logins):
                        continue

                    # DATE fields ≤ cutoff and matching regex
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
                                        owner_type=spec.owner_type,
                                        owner=spec.owner,
                                        project_number=int(number),
                                        project_title=(it.get("project") or {}).get("title") or "",
                                        start_field=fname,
                                        start_date=fdate,
                                        title=title,
                                        repo=repo,
                                        url=url,
                                        updated_at=iso_now,
                                    )
                                )

                page = (project_node.get("items") or {}).get("pageInfo") or {}
                if page.get("hasNextPage"):
                    after = page.get("endCursor")
                else:
                    break
    return out


# -----------------------------
# Mock data (optional)
# -----------------------------
def generate_mock_tasks(cfg: Config) -> List[TaskRow]:
    today = dt.date.today()
    iso_now = dt.datetime.now().isoformat(timespec="seconds")
    rows: List[TaskRow] = []
    for spec in cfg.projects:
        # demo picks: if numbers unspecified, pretend 1..2
        nums = spec.numbers or [1, 2]
        for num in nums:
            rows.append(
                TaskRow(
                    owner_type=spec.owner_type, owner=spec.owner, project_number=int(num),
                    project_title=f"Project {num}", start_field="Start date",
                    start_date=today.isoformat(), title=f"Demo task for {spec.owner}/{num}",
                    repo=None, url="https://github.com/orgs/demo/projects/1", updated_at=iso_now
                )
            )
            rows.append(
                TaskRow(
                    owner_type=spec.owner_type, owner=spec.owner, project_number=int(num),
                    project_title=f"Project {num}", start_field="Start date",
                    start_date=(today - dt.timedelta(days=2)).isoformat(),
                    title=f"Older task for {spec.owner}/{num}", repo="demo/repo",
                    url="https://github.com/demo/repo/issues/1", updated_at=iso_now
                )
            )
    return rows


# -----------------------------
# TUI helpers
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


def build_task_lines(tasks: List[TaskRow], today: dt.date) -> List[HTML]:
    if not tasks:
        return [HTML("<b>Nothing to show.</b> Press <b>u</b> to fetch.")]
    lines: List[HTML] = []
    current_project: Optional[str] = None
    for t in tasks:
        if t.project_title != current_project:
            current_project = t.project_title
            lines.append(HTML(f"<b>## {current_project}</b>"))
            lines.append(HTML("<b>DATE         FIELD                TITLE                                     REPO                 URL</b>"))
        col = color_for_date(t.start_date, today)
        title = (t.title[:60] + "…") if len(t.title) > 61 else t.title
        repo = (t.repo or "-")
        if len(repo) > 20:
            repo = repo[:19] + "…"
        url = t.url
        if len(url) > 40:
            url = url[:39] + "…"
        field = t.start_field
        if len(field) > 20:
            field = field[:19] + "…"
        lines.append(
            HTML(
                f'<{col}>{t.start_date:<12}</{col}>  '
                f'{field:<20}  '
                f'{title:<41}  '
                f'{repo:<20}  '
                f'{url}'
            )
        )
    return lines


def html_lines_to_fragments(lines: List[HTML]):
    frags: List[Tuple[str, str]] = []
    first = True
    for h in lines:
        if not first:
            frags.append(("", "\n"))
        frags.extend(to_formatted_text(h))
        first = False
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
    fragments = html_lines_to_fragments(build_task_lines(tasks, today_date))
    text_control = FormattedTextControl(text=fragments, focusable=True, show_cursor=False)
    window = Window(content=text_control, wrap_lines=False)

    status = FormattedTextControl(
        text=lambda: [("reverse", f"  u:update  t:today  a:all  q:quit   • {cfg.user} • {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  ")]
    )
    root = HSplit([window, Window(height=1, content=status)])
    kb = KeyBindings()

    def refresh_view():
        tks = load_view()
        text_control.text = html_lines_to_fragments(build_task_lines(tks, today_date))

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
        text_control.text = html_lines_to_fragments([HTML("<b>Refreshing…</b>")])
        event.app.invalidate()
        try:
            if os.environ.get("MOCK_FETCH") == "1":
                rows = generate_mock_tasks(cfg)
            else:
                if not token:
                    raise RuntimeError("GITHUB_TOKEN is not set")
                rows = fetch_tasks_github(token, cfg, date_cutoff=today_date)
            db.upsert_many(rows)
        except Exception as e:
            text_control.text = html_lines_to_fragments([HTML(f"<ansired><b>Error:</b> {HTML.escape(str(e))}</ansired>")])
            event.app.invalidate()
            return
        refresh_view()
        event.app.invalidate()

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
    ap.add_argument("--discover", action="store_true", help="List open Projects v2 for each owner from config and exit")
    args = ap.parse_args()

    cfg = load_config(args.config)
    token = os.environ.get("GITHUB_TOKEN")

    if args.discover:
        if not token:
            print("GITHUB_TOKEN is not set (needed for --discover).", file=sys.stderr)
            sys.exit(1)
        s = _session(token)
        for spec in cfg.projects:
            projs = discover_open_projects(s, spec.owner_type, spec.owner)
            who = f"{spec.owner_type}:{spec.owner}"
            print(f"{who}")
            if not projs:
                print("  (no open projects or insufficient access)")
                continue
            for n in projs:
                print(f"  #{n['number']}: {n['title']}")
        return

    db = TaskDB(args.db)

    # If first run (empty cache), populate.
    if not db.load():
        if os.environ.get("MOCK_FETCH") == "1":
            rows = generate_mock_tasks(cfg)
            db.upsert_many(rows)
        else:
            if not token:
                print("GITHUB_TOKEN is not set. Set it or run with MOCK_FETCH=1.", file=sys.stderr)
                sys.exit(1)
            # This call now skips any NOT_FOUND projects (won’t crash)
            rows = fetch_tasks_github(token, cfg, date_cutoff=dt.date.today())
            db.upsert_many(rows)

    run_ui(db, cfg, token)


if __name__ == "__main__":
    main()

