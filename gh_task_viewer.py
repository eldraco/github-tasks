#!/usr/bin/env python3
# gh_task_viewer: Terminal GitHub Projects (v2) task viewer with live progress
#
# Hotkeys
#   u  refresh (runs in background and updates a progress bar)
#   t  show tasks with date == today
#   a  show all cached tasks
#   P  clear project filter (show all projects again)
#   N  toggle hide tasks with no date
#   F  set/clear a max date filter (Date <= YYYY-MM-DD); empty to clear
#   W  toggle work timer for the selected task (multiple tasks can run)
#   R  open timer report (daily/weekly/monthly aggregates)
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
import json
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Iterable, Set

import requests
import yaml
import time
from prompt_toolkit import Application
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style
# Buffer-based input removed; keep core controls only
from prompt_toolkit.keys import Keys
from prompt_toolkit.filters import Condition
import logging
from logging.handlers import RotatingFileHandler


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
    focus_field: str
    focus_date: str
    title: str
    repo: Optional[str]
    url: str
    updated_at: str
    status: Optional[str] = None  # textual status (eg. In Progress, Done)
    is_done: int = 0              # 1 if done / completed


class TaskDB:
    SCHEMA_COLUMNS = [
        "owner_type","owner","project_number","project_title",
        "start_field","start_date",
        "focus_field","focus_date",
        "title","repo","url","updated_at","status","is_done"
    ]
    CREATE_TABLE_SQL = """      CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_type TEXT NOT NULL,
        owner TEXT NOT NULL,
        project_number INTEGER NOT NULL,
        project_title TEXT NOT NULL,
        start_field TEXT NOT NULL,
        start_date TEXT NOT NULL,
        focus_field TEXT NOT NULL,
        focus_date TEXT NOT NULL,
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
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_focus_date ON tasks(focus_date)")
        self.conn.commit()

    def _migrate_if_needed(self):
        cols = self._cols()
        if not cols:
            self.conn.execute(self.CREATE_TABLE_SQL)
            self._idx()
            # also ensure timer tables
            self._ensure_timer_tables()
            return
        missing = [c for c in self.SCHEMA_COLUMNS if c not in cols]
        if not missing:
            self._idx()
            # still ensure timer tables exist
            self._ensure_timer_tables()
            return
        cur = self.conn.cursor()
        cur.execute("ALTER TABLE tasks RENAME TO tasks_old")
        cur.execute(self.CREATE_TABLE_SQL)
        defaults = {
            "owner_type":"''","owner":"''","project_number":"0","project_title":"''",
            "start_field":"''","start_date":"''",
            "focus_field":"''","focus_date":"''",
            "title":"''","repo":"NULL","url":"''",
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
        self._ensure_timer_tables()

    # --- Work session timer tables and helpers ---
    def _ensure_timer_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS work_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_url TEXT NOT NULL,
                project_title TEXT,
                started_at TEXT NOT NULL,
                ended_at TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ws_task ON work_sessions(task_url)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ws_open ON work_sessions(ended_at)")
        self.conn.commit()

    def start_session(self, task_url: str, project_title: Optional[str] = None) -> None:
        if not task_url:
            return
        # Avoid duplicate open sessions for same task
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM work_sessions WHERE task_url=? AND ended_at IS NULL LIMIT 1", (task_url,))
        if cur.fetchone():
            return
        now = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        cur.execute(
            "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at) VALUES (?,?,?,NULL)",
            (task_url, project_title, now),
        )
        self.conn.commit()

    def stop_session(self, task_url: str) -> None:
        if not task_url:
            return
        now = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE work_sessions SET ended_at=? WHERE task_url=? AND ended_at IS NULL",
            (now, task_url),
        )
        self.conn.commit()

    def active_task_urls(self) -> Set[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT DISTINCT task_url FROM work_sessions WHERE ended_at IS NULL")
        return {r[0] for r in cur.fetchall()}

    def _parse_iso(self, s: str) -> Optional[dt.datetime]:
        if not s:
            return None
        try:
            return dt.datetime.fromisoformat(s)
        except Exception:
            try:
                return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

    def _sum_rows_seconds(self, rows: List[Tuple[str, Optional[str]]]) -> int:
        total = 0
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        for started_at, ended_at in rows:
            st = self._parse_iso(started_at)
            en = self._parse_iso(ended_at) if ended_at else None
            if st is None:
                continue
            if en is None:
                en = now
            delta = en - st
            total += int(delta.total_seconds())
        return max(0, total)

    def task_total_seconds(self, task_url: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT started_at, ended_at FROM work_sessions WHERE task_url=?",
            (task_url,),
        )
        return self._sum_rows_seconds(cur.fetchall())

    def project_total_seconds(self, project_title: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT started_at, ended_at FROM work_sessions WHERE project_title=?",
            (project_title,),
        )
        return self._sum_rows_seconds(cur.fetchall())

    def task_current_elapsed_seconds(self, task_url: str) -> int:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT started_at, ended_at FROM work_sessions WHERE task_url=? AND ended_at IS NULL ORDER BY id DESC LIMIT 1",
            (task_url,),
        )
        row = cur.fetchone()
        if not row:
            return 0
        st = self._parse_iso(row[0])
        if not st:
            return 0
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        return max(0, int((now - st).total_seconds()))

    # ---- Aggregations for reports ----
    def _period_key(self, d: dt.datetime, granularity: str) -> str:
        if granularity == 'day':
            return d.date().isoformat()
        if granularity == 'week':
            iso_year, iso_week, _ = d.isocalendar()
            return f"{iso_year}-W{iso_week:02d}"
        if granularity == 'month':
            return f"{d.year}-{d.month:02d}"
        raise ValueError("granularity must be 'day' | 'week' | 'month'")

    def _next_boundary(self, d: dt.datetime, granularity: str) -> dt.datetime:
        if granularity == 'day':
            base = d.replace(hour=0, minute=0, second=0, microsecond=0)
            return base + dt.timedelta(days=1)
        if granularity == 'week':
            # ISO week: Monday start
            start_of_week = d - dt.timedelta(days=d.weekday())
            start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
            return start_of_week + dt.timedelta(days=7)
        if granularity == 'month':
            first = d.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if first.month == 12:
                return first.replace(year=first.year+1, month=1)
            else:
                return first.replace(month=first.month+1)
        raise ValueError("granularity must be 'day' | 'week' | 'month'")

    def _clip_range(self, start: dt.datetime, end: dt.datetime, since: Optional[dt.datetime]) -> Tuple[dt.datetime, dt.datetime, bool]:
        if since is None:
            return start, end, True
        if end <= since:
            return start, end, False
        start2 = max(start, since)
        return start2, end, True

    def _load_sessions(self, project_title: Optional[str] = None, task_url: Optional[str] = None) -> List[Tuple[str, Optional[str], Optional[str]]]:
        # Returns list of (project_title, started_at, ended_at)
        cur = self.conn.cursor()
        if task_url:
            cur.execute("SELECT project_title, started_at, ended_at FROM work_sessions WHERE task_url=?", (task_url,))
        elif project_title:
            cur.execute("SELECT project_title, started_at, ended_at FROM work_sessions WHERE project_title=?", (project_title,))
        else:
            cur.execute("SELECT project_title, started_at, ended_at FROM work_sessions")
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]

    def aggregate_period_totals(self, granularity: str, since_days: Optional[int] = None,
                                 project_title: Optional[str] = None, task_url: Optional[str] = None) -> Dict[str, int]:
        rows = self._load_sessions(project_title, task_url)
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        since_dt = (now - dt.timedelta(days=since_days)) if since_days else None
        out: Dict[str, int] = {}
        for proj, st_s, en_s in rows:
            st = self._parse_iso(st_s)
            en = self._parse_iso(en_s) if en_s else None
            if not st:
                continue
            if en is None:
                en = now
            # Clip to since window
            st, en, keep = self._clip_range(st, en, since_dt)
            if not keep or st >= en:
                continue
            cur = st
            while cur < en:
                boundary = self._next_boundary(cur, granularity)
                seg_end = min(boundary, en)
                key = self._period_key(cur, granularity)
                out[key] = out.get(key, 0) + int((seg_end - cur).total_seconds())
                cur = seg_end
        return out

    def aggregate_project_totals(self, since_days: Optional[int] = None) -> Dict[str, int]:
        cur = self.conn.cursor()
        cur.execute("SELECT project_title, started_at, ended_at FROM work_sessions")
        rows = cur.fetchall()
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        since_dt = (now - dt.timedelta(days=since_days)) if since_days else None
        out: Dict[str, int] = {}
        for proj, st_s, en_s in rows:
            proj = proj or ''
            st = self._parse_iso(st_s)
            en = self._parse_iso(en_s) if en_s else None
            if not st:
                continue
            if en is None:
                en = now
            st, en, keep = self._clip_range(st, en, since_dt)
            if not keep or st >= en:
                continue
            out[proj] = out.get(proj, 0) + int((en - st).total_seconds())
        return out

    def upsert_many(self, rows: List[TaskRow]):
        if not rows:
            return
        cur = self.conn.cursor()
        cur.executemany(
            """            INSERT INTO tasks (
              owner_type, owner, project_number, project_title,
              start_field, start_date,
              focus_field, focus_date,
              title, repo, url, updated_at, status, is_done
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    r.focus_field,
                    r.focus_date,
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

    def replace_all(self, rows: List[TaskRow]):
        """Replace all existing tasks with new list (ensures deletions reflected)."""
        cur = self.conn.cursor()
        cur.execute("DELETE FROM tasks")
        self.conn.commit()
        self.upsert_many(rows)

    def load(self, today_only=False, today: Optional[str]=None) -> List[TaskRow]:
        cur = self.conn.cursor()
        if today_only:
            today = today or dt.date.today().isoformat()
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,focus_field,focus_date,title,repo,url,updated_at,status,is_done
                FROM tasks WHERE focus_date = ?
                ORDER BY project_title, focus_date, repo, title
                """,
                (today,),
            )
        else:
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,focus_field,focus_date,title,repo,url,updated_at,status,is_done
                FROM tasks
                ORDER BY project_title, focus_date, repo, title
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
    try:
        r = session.post("https://api.github.com/graphql", json={"query": query, "variables": variables}, timeout=60)
        r.raise_for_status()
        return r.json()
    except Exception:
        try:
            logging.getLogger('gh_task_viewer').exception("GraphQL request failed")
        except Exception:
            pass
        raise

def _retry_sleep(seconds: float, on_wait: Optional[Callable[[str], None]] = None) -> None:
    try:
        msg = f"Rate limited; waiting {int(seconds)}s…"
        if on_wait:
            on_wait(msg)
        else:
            try:
                logging.getLogger('gh_task_viewer').info(msg)
            except Exception:
                pass
        time.sleep(max(0.0, seconds))
    except Exception:
        time.sleep(max(0.0, seconds))

def _parse_retry_after_seconds(resp: Optional[requests.Response]) -> Optional[int]:
    if not resp:
        return None
    # Prefer Retry-After header (secondary rate limits)
    ra = resp.headers.get('Retry-After') if resp.headers is not None else None
    if ra:
        try:
            return int(float(ra))
        except Exception:
            pass
    # Next, X-RateLimit-Reset (epoch seconds)
    try:
        xrlr = resp.headers.get('X-RateLimit-Reset') if resp.headers is not None else None
        if xrlr:
            reset_at = int(xrlr)
            now = int(time.time())
            return max(1, reset_at - now)
    except Exception:
        pass
    return None

def _graphql_with_backoff(
    session: requests.Session,
    query: str,
    variables: Dict[str, object],
    on_wait: Optional[Callable[[str], None]] = None,
    max_total_wait: int = 900,
) -> Dict:
    """Call GraphQL with handling for rate limits and transient failures.

    - Retries RATE_LIMITED GraphQL errors with exponential backoff.
    - Retries HTTP 403/429/502 with Retry-After or exponential backoff.
    """
    backoff = 10
    total_wait = 0
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = _graphql_raw(session, query, variables)
        except requests.exceptions.HTTPError as e:
            # Handle HTTP-level rate limits/abuse and transient errors
            status = e.response.status_code if e.response is not None else None
            if status in (403, 429, 502, 503, 504):
                wait_s = _parse_retry_after_seconds(e.response)
                if wait_s is None:
                    wait_s = min(300, backoff)
                    backoff = min(300, backoff * 2)
                if total_wait + wait_s > max_total_wait:
                    raise
                _retry_sleep(wait_s, on_wait)
                total_wait += wait_s
                continue
            raise
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            wait_s = min(60, backoff)
            backoff = min(300, backoff * 2)
            if total_wait + wait_s > max_total_wait:
                raise
            _retry_sleep(wait_s, on_wait)
            total_wait += wait_s
            continue

        # GraphQL-level errors may include RATE_LIMITED with HTTP 200
        errs = resp.get("errors") or []
        if errs and any((e.get("type") == "RATE_LIMITED") for e in errs):
            wait_s = min(300, backoff)
            backoff = min(300, backoff * 2)
            if total_wait + wait_s > max_total_wait:
                # Let caller handle after exceeding budget
                return resp
            _retry_sleep(wait_s, on_wait)
            total_wait += wait_s
            continue
        return resp

def discover_open_projects(session: requests.Session, owner_type: str, owner: str) -> List[Dict]:
    if owner_type == "org":
        data = _graphql_with_backoff(session, GQL_LIST_ORG_PROJECTS, {"login": owner})
        nodes = (((data.get("data") or {}).get("organization") or {}).get("projectsV2") or {}).get("nodes") or []
    else:
        data = _graphql_with_backoff(session, GQL_LIST_USER_PROJECTS, {"login": owner})
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
    try:
        logging.getLogger('gh_task_viewer').info("Fetching from %d project targets", total)
    except Exception:
        pass
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
            resp = _graphql_with_backoff(session, query, variables, on_wait=lambda m: tick(m))

            errs = resp.get("errors") or []
            if errs:
                nf = any((e.get("type") == "NOT_FOUND") and ("projectV2" in (e.get("path") or [])) for e in errs)
                if nf:
                    try:
                        logging.getLogger('gh_task_viewer').warning("Project not found or inaccessible: %s:%s #%s", owner_type, owner, number)
                    except Exception:
                        pass
                    break  # skip invalid/inaccessible project number
                # Handle RATE_LIMITED gracefully: keep partial results and return
                rate_limited = any((e.get("type") == "RATE_LIMITED") for e in errs)
                if rate_limited:
                    if progress:
                        progress(done, total, f"{_ascii_bar(done,total)}  Rate limited; partial results")
                    try:
                        logging.getLogger('gh_task_viewer').warning("Rate limited; returning partial results")
                    except Exception:
                        pass
                    return out
                # Other errors are considered fatal
                try:
                    logging.getLogger('gh_task_viewer').error("GraphQL errors: %s", errs)
                except Exception:
                    pass
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

                # Extract optional Focus Day for coloring and Today filter
                focus_fname: str = ""
                focus_fdate: str = ""
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldDateValue":
                        fname_fd = ((fv.get("field") or {}).get("name") or "")
                        if fname_fd.strip().lower() == "focus day":
                            fdate_fd = fv.get("date")
                            if fdate_fd:
                                try:
                                    dt.date.fromisoformat(fdate_fd)
                                    focus_fname, focus_fdate = fname_fd, fdate_fd
                                except ValueError:
                                    pass

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
                                focus_field=focus_fname or "",
                                focus_date=focus_fdate or "",
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
                            focus_field=focus_fname or "",
                            focus_date=focus_fdate or "",
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
                    focus_field="", focus_date="",
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
    header = "Focus Day   Start Date   STATUS      TITLE                                     REPO                 URL"
    for t in tasks:
        if t.project_title != current:
            current = t.project_title
            if frags:
                frags.append(("", "\n"))
            frags.append(("bold", f"## {current}"))
            frags.append(("", "\n"))
            frags.append(("bold", header))
            frags.append(("", "\n"))
        col = color_for_date(t.focus_date, today)
        title = _truncate(t.title, 45)
        repo  = _truncate(t.repo or "-", 20)
        url   = _truncate(t.url, 40)
        status = _truncate(t.status or "-", 10)
        frags.append((col, f"{t.focus_date or '-':<11}  {t.start_date:<12}"))
        frags.append(("",  "  "))
        frags.append(("", f"{status:<10}  {title:<45}  {repo:<20}  {url}"))
        frags.append(("", "\n"))

    if frags and frags[-1] == ("", "\n"):
        frags.pop()
    return frags


# -----------------------------
# TUI
# -----------------------------
def run_ui(db: TaskDB, cfg: Config, token: Optional[str], state_path: Optional[str] = None, log_level: str = 'ERROR') -> None:
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
    hide_no_date = False  # new toggle to hide tasks without any date
    show_unassigned = False
    project_cycle: Optional[str] = None
    search_term: Optional[str] = None
    in_search = False
    search_buffer = ""
    in_date_filter = False   # when True, we're typing a date filter (<= date_max)
    date_buffer = ""         # buffer for date filter input
    date_max: Optional[str] = None
    # Sort mode: 'project' (default) or 'date'
    sort_mode: str = 'project'
    current_index = 0
    v_offset = 0  # top row index currently displayed
    h_offset = 0
    detail_mode = False
    status_line = ""
    # Inline search buffer (used when in_search == True)

    if state_path is None:
        state_path = os.path.expanduser("~/.gh_tasks.ui.json")

    # Setup file logger for diagnostics; default level is ERROR unless CLI overrides.
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gh_task_viewer.log')
    logger = logging.getLogger('gh_task_viewer')
    # Always reset handlers so CLI --log-level reliably controls file output.
    for h in list(logger.handlers):
        try:
            logger.removeHandler(h)
        except Exception:
            pass
    # root logger: keep DEBUG level to allow verbose logs internally; handler filters by level
    logger.setLevel(logging.DEBUG)
    fh = RotatingFileHandler(log_path, maxBytes=2000000, backupCount=2, encoding='utf-8')
    # handler level honors CLI option (default ERROR)
    try:
        lvl = getattr(logging, log_level.upper(), logging.ERROR)
    except Exception:
        lvl = logging.ERROR
    fh.setLevel(lvl)
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(fh)

    def _load_state() -> dict:
        try:
            with open(state_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_state():
        data = {
            'show_today_only': show_today_only,
            'hide_done': hide_done,
            'hide_no_date': hide_no_date,
            'show_unassigned': show_unassigned,
            'project_cycle': project_cycle,
            'search_term': search_term,
            'date_max': date_max,
            'sort_mode': sort_mode,
            'current_index': current_index,
            'v_offset': v_offset,
            'h_offset': h_offset,
        }
        try:
            d = os.path.dirname(state_path)
            if d and not os.path.isdir(d):
                os.makedirs(d, exist_ok=True)
            with open(state_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # apply any saved UI state before loading rows
    _st = _load_state()
    show_today_only = bool(_st.get('show_today_only', show_today_only))
    hide_done = bool(_st.get('hide_done', hide_done))
    hide_no_date = bool(_st.get('hide_no_date', hide_no_date))
    show_unassigned = bool(_st.get('show_unassigned', show_unassigned))
    project_cycle = _st.get('project_cycle', project_cycle)
    search_term = _st.get('search_term', search_term)
    date_max = _st.get('date_max', date_max)
    sort_mode = _st.get('sort_mode', sort_mode) if _st.get('sort_mode') in ('project','date') else 'project'
    current_index = int(_st.get('current_index', current_index) or 0)
    v_offset = int(_st.get('v_offset', v_offset) or 0)
    h_offset = int(_st.get('h_offset', h_offset) or 0)

    def load_all():
        return db.load(today_only=show_today_only, today=today_date.isoformat())

    all_rows = load_all()

    def _safe_date(s: str) -> Optional[dt.date]:
        try:
            return dt.date.fromisoformat(s)
        except Exception:
            return None

    def apply_filters(rows: List[TaskRow]) -> List[TaskRow]:
        out = rows
        try:
            logger.debug("apply_filters start: hide_done=%s hide_no_date=%s project_cycle=%r in_search=%s search_term=%r search_buffer=%r", hide_done, hide_no_date, project_cycle, in_search, search_term, search_buffer)
        except Exception:
            pass
        if hide_done:
            out = [r for r in out if not r.is_done]
        if hide_no_date:
            out = [r for r in out if r.focus_date]
        if project_cycle:
            out = [r for r in out if r.project_title == project_cycle]
        active_search = search_buffer if in_search else search_term
        if active_search:
            needle = active_search.lower()
            out = [r for r in out if needle in (r.title or '').lower() or
                                   needle in (r.repo or '').lower() or
                                   needle in (r.status or '').lower() or
                                   needle in (r.project_title or '').lower()]
        if date_max:
            dm = _safe_date(date_max)
            if dm:
                tmp: List[TaskRow] = []
                for r in out:
                    if not r.focus_date:
                        continue
                    rsd = _safe_date(r.focus_date)
                    if rsd and rsd <= dm:
                        tmp.append(r)
                out = tmp
        # apply sorting last
        if sort_mode == 'date':
            def _date_key(r: TaskRow):
                dd = _safe_date(r.focus_date)
                return (dd is None, dd or dt.date.max, r.project_title or '', r.title or '')
            out = sorted(out, key=_date_key)
        else:  # 'project'
            def _proj_key(r: TaskRow):
                dd = _safe_date(r.focus_date) or dt.date.max
                return (r.project_title or '', dd, r.repo or '', r.title or '')
            out = sorted(out, key=_proj_key)
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
            size = get_app().output.get_size()
            total_rows = size.rows
            total_cols = size.columns
        except Exception:
            total_rows = 40
            total_cols = 120
        # Reserve 1 row for status bar. Header consumes 2 lines (header + blank after).
        visible_rows = max(1, total_rows - 3)
        # Adjust v_offset to ensure current_index visible
        if current_index < v_offset:
            v_offset = current_index
        elif current_index >= v_offset + visible_rows:
            v_offset = current_index - visible_rows + 1
        frags: List[Tuple[str,str]] = []
        # Determine dynamic column widths to fully use available space
        # Layout: marker(2) + Focus(11) + 2 + Start(12) + 2 + Status(10) + 2 + Time(12) + 2 + Title(VAR) + 2 + Project(VAR)
        # Right side stats window width is fixed at 32 plus a 1-char separator in root_body
        right_panel_width = 32 + 1
        avail_cols = max(40, total_cols - right_panel_width)
        time_w = 12  # "mm:ss|HH:MM" (right aligned)
        fixed = 2 + 11 + 2 + 12 + 2 + 10 + 2 + time_w + 2  # = 55
        dyn = max(1, avail_cols - fixed)
        proj_min = 12
        title_min = 20
        # split dyn ~70%/30% between title/project
        title_w = max(title_min, int(dyn * 0.7))
        proj_w = max(proj_min, dyn - title_w)
        # Rebalance if rounding starves project
        if proj_w < proj_min:
            delta = proj_min - proj_w
            title_w = max(title_min, title_w - delta)
            proj_w = proj_min
        header = f"  {'Focus Day':<11}  {'Start Date':<12}  {'STATUS':<10}  {'TIME':>{time_w}}  {'TITLE':<{title_w}}  {'PROJECT':<{proj_w}}"
        frags.append(("bold", header[h_offset:]))
        frags.append(("", "\n"))
        if not rows:
            frags.append(("italic", "(no tasks match filters)"))
            return frags
        today = today_date
        active_urls = db.active_task_urls()
        display_slice = rows[v_offset:v_offset+visible_rows]
        for rel_idx, t in enumerate(display_slice):
            idx = v_offset + rel_idx
            is_sel = (idx == current_index)
            style_row = "reverse" if is_sel else ""
            col = color_for_date(t.focus_date, today)
            running = bool(t.url and (t.url in active_urls))
            # For running tasks, override foreground color to cyan (plus bold)
            if is_sel:
                base_style = col + " bold"
            elif running:
                base_style = "ansicyan bold"
            else:
                base_style = col
            title = _truncate(t.title, title_w)
            project = _truncate(t.project_title, proj_w)
            status_txt = _truncate(t.status or '-', 10)
            marker = '⏱ ' if running else '  '
            # Time column: current run (mm:ss) and total (H:MM)
            cur_s = db.task_current_elapsed_seconds(t.url) if (t.url and running) else 0
            tot_s = db.task_total_seconds(t.url) if t.url else 0
            mm, ss = divmod(int(max(0, cur_s)), 60)
            # total in H:MM (no leading zeros on hours)
            th, rem = divmod(int(max(0, tot_s)), 3600)
            tm, _ = divmod(rem, 60)
            time_cell = f"{mm:02d}:{ss:02d}|{th:d}:{tm:02d}" if tot_s else (f"{mm:02d}:{ss:02d}|0:00")
            time_cell = f"{time_cell:>{time_w}}"
            line = f"{marker}{(t.focus_date or '-'):<11}  {t.start_date:<12}  {status_txt:<10}  {time_cell}  {title:<{title_w}}  {project:<{proj_w}}"
            line = line[h_offset:]
            # highlight search term occurrences (live search buffer if active)
            active_search = search_buffer if in_search else search_term
            if active_search and not is_sel:
                low = line.lower()
                needle = active_search.lower()
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
        total = len(rows)
        done_ct = sum(1 for r in rows if r.is_done)
        def _fmt_hm(total_seconds: int) -> str:
            s = int(max(0, total_seconds))
            h, r = divmod(s, 3600)
            m, _ = divmod(r, 60)
            return f"{h:d}:{m:02d}"
        lines: List[str] = [f"User: {cfg.user}", f"Total: {total}", f"Done: {done_ct}"]
        # per-project stats
        by_proj: Dict[str, Tuple[int,int]] = {}
        for r in rows:
            d, t = by_proj.get(r.project_title, (0,0))
            by_proj[r.project_title] = (d + (1 if r.is_done else 0), t + 1)
        # total time by project (all time)
        proj_time = db.aggregate_project_totals(since_days=None)
        lines.append("")
        lines.append("Proj:")
        for p,(d,t) in sorted(by_proj.items()):
            pct = 0 if t==0 else int(d*100/t)
            secs = proj_time.get(p or '', 0)
            lines.append(f"{_truncate(p,12):<12}{d:>2}/{t:<2} {pct:>3}% {_fmt_hm(secs):>6}")
        lines.append("")
        lines.append("Filters:")
        active_search = search_buffer if in_search else search_term
        lines.append(f"HideDone:{'Y' if hide_done else 'N'} HideNoDate:{'Y' if hide_no_date else 'N'} Unassigned:{'Y' if show_unassigned else 'N'}")
        lines.append(f"Proj:{_truncate(project_cycle or 'All',10)}")
        lines.append(f"Today:{'Y' if show_today_only else 'N'}")
        lines.append(f"Date<=:{date_max or '-'}")
        lines.append(f"Search:{_truncate(active_search or '-',12)}")
        return "\n".join(lines)

    table_control = FormattedTextControl(text=lambda: build_table_fragments())
    table_window = Window(content=table_control, wrap_lines=False, always_hide_cursor=True)
    # Top status bar: shows date, current project, total tasks shown, and active search filter
    def build_top_status() -> List[Tuple[str,str]]:
        rows = filtered_rows()
        total = len(rows)
        active_proj = project_cycle or 'All'
        active_search = search_buffer if in_search else search_term or '-'
        # Timer summary for current selection at start for visibility
        now_s = task_s = proj_s = 0
        active_count = 0
        if rows:
            t = rows[current_index]
            if t.url:
                now_s = db.task_current_elapsed_seconds(t.url)
                task_s = db.task_total_seconds(t.url)
            if t.project_title:
                proj_s = db.project_total_seconds(t.project_title)
        try:
            active_count = len(db.active_task_urls())
        except Exception:
            active_count = 0
        def _fmt_hms(s:int)->str:
            s = int(max(0, s))
            h, r = divmod(s, 3600)
            m, s = divmod(r, 60)
            return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"
        timers = f" Now:{_fmt_hms(now_s)} Task:{_fmt_hms(task_s)} Proj:{_fmt_hms(proj_s)} Act:{active_count} "
        txt = f"{timers}| Date: {today_date.isoformat()}  | Project: {_truncate(active_proj,30)}  | Shown: {total}  | Search: {_truncate(active_search,30)} "
        return [("reverse", txt)]
    top_status_control = FormattedTextControl(text=lambda: build_top_status())
    top_status_window = Window(height=1, content=top_status_control)
    stats_control = FormattedTextControl(text=lambda: summarize())
    stats_window = Window(width=32, content=stats_control, wrap_lines=False, always_hide_cursor=True)

    detail_control = FormattedTextControl(text=lambda: build_detail_text())
    detail_window = Window(width=80, height=20, content=detail_control, wrap_lines=True, always_hide_cursor=True, style="bg:#202020 #ffffff")

    # Report overlay
    show_report = False
    report_granularity = 'day'  # one of: day, week, month

    def _fmt_hms_full(s:int) -> str:
        s = int(max(0, s))
        h, r = divmod(s, 3600)
        m, s = divmod(r, 60)
        return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

    def build_report_text() -> List[Tuple[str,str]]:
        lines: List[str] = []
        # current selection snapshot
        rows = filtered_rows()
        cur_proj = rows[current_index].project_title if rows else None
        cur_url = rows[current_index].url if rows else None
        now_s = db.task_current_elapsed_seconds(cur_url) if cur_url else 0
        task_s = db.task_total_seconds(cur_url) if cur_url else 0
        proj_s = db.project_total_seconds(cur_proj) if cur_proj else 0
        hdr = f"Timer Report — granularity: {report_granularity.upper()}  (d/w/m to switch, Enter/Esc to close)"
        lines.append(hdr)
        lines.append("")
        lines.append(f"Now: {_fmt_hms_full(now_s)}  Task: {_fmt_hms_full(task_s)}  Proj: {_fmt_hms_full(proj_s)}  Active: {len(db.active_task_urls())}")
        lines.append("")
        # Choose lookback window
        if report_granularity == 'day':
            since_days = 30
            limit = 14
        elif report_granularity == 'week':
            since_days = 7*26
            limit = 12
        else:
            since_days = 365*2
            limit = 12
        # Overall
        lines.append("Overall:")
        totals = db.aggregate_period_totals(report_granularity, since_days=since_days)
        keys = sorted(totals.keys(), reverse=True)[:limit]
        if not keys:
            lines.append("  (no data)")
        else:
            maxv = max(totals[k] for k in keys) or 1
            for k in keys:
                v = totals[k]
                bar = '█' * max(1, int(30 * v / maxv))
                lines.append(f"  {k:<10} {_fmt_hms_full(v):>10}  {bar}")
        lines.append("")
        # Current selection project/task
        lines.append(f"Project: {cur_proj or '-'}")
        p_tot = db.aggregate_period_totals(report_granularity, since_days=since_days, project_title=cur_proj) if cur_proj else {}
        p_keys = sorted(p_tot.keys(), reverse=True)[:limit]
        if not p_keys:
            lines.append("  (no data)")
        else:
            maxv = max(p_tot[k] for k in p_keys) or 1
            for k in p_keys:
                v = p_tot[k]
                bar = '█' * max(1, int(30 * v / maxv))
                lines.append(f"  {k:<10} {_fmt_hms_full(v):>10}  {bar}")
        lines.append("")
        lines.append(f"Task: {rows[current_index].title if rows else '-'}")
        t_tot = db.aggregate_period_totals(report_granularity, since_days=since_days, task_url=cur_url) if cur_url else {}
        t_keys = sorted(t_tot.keys(), reverse=True)[:limit]
        if not t_keys:
            lines.append("  (no data)")
        else:
            maxv = max(t_tot[k] for k in t_keys) or 1
            for k in t_keys:
                v = t_tot[k]
                bar = '█' * max(1, int(30 * v / maxv))
                lines.append(f"  {k:<10} {_fmt_hms_full(v):>10}  {bar}")
        lines.append("")
        lines.append("Top projects (window):")
        proj_totals = db.aggregate_project_totals(since_days=since_days)
        tops = sorted(proj_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        if not tops:
            lines.append("  (no data)")
        else:
            maxv = max(v for _,v in tops) or 1
            for name, secs in tops:
                nm = (name or '-')
                bar = '█' * max(1, int(30 * secs / maxv))
                lines.append(f"  {_truncate(nm,20):<20} {_fmt_hms_full(secs):>10}  {bar}")
        # Quick multi-granularity snapshot (recent sums)
        lines.append("")
        lines.append("Quick view (recent sums):")
        def _sum_recent(gran: str, days: int, filt_proj=None, filt_task=None) -> int:
            m = db.aggregate_period_totals(gran, since_days=days, project_title=filt_proj, task_url=filt_task)
            return sum(m.values())
        lines.append(f"Overall  D:{_fmt_hms_full(_sum_recent('day', 14))}  W:{_fmt_hms_full(_sum_recent('week', 7*12))}  M:{_fmt_hms_full(_sum_recent('month', 365))}")
        if cur_proj:
            lines.append(f"Project  D:{_fmt_hms_full(_sum_recent('day', 14, filt_proj=cur_proj))}  W:{_fmt_hms_full(_sum_recent('week', 7*12, filt_proj=cur_proj))}  M:{_fmt_hms_full(_sum_recent('month', 365, filt_proj=cur_proj))}")
        if cur_url:
            lines.append(f"Task     D:{_fmt_hms_full(_sum_recent('day', 14, filt_task=cur_url))}  W:{_fmt_hms_full(_sum_recent('week', 7*12, filt_task=cur_url))}  M:{_fmt_hms_full(_sum_recent('month', 365, filt_task=cur_url))}")
        return [("bold", lines[0])] + [("", "\n" + "\n".join(lines[1:]))]

    report_control = FormattedTextControl(text=lambda: build_report_text())
    report_window = Window(width=100, height=28, content=report_control, wrap_lines=True, always_hide_cursor=True, style="bg:#202020 #ffffff")

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
            f"Start:   {t.start_date} ({t.start_field})",
            f"Focus:   {t.focus_date or '-'} ({t.focus_field or '-'})",
            f"Status:  {t.status}",
            f"Done:    {'Yes' if t.is_done else 'No'}",
            "",
            "Press Enter / q / Esc to close"
        ]
        return [("bold", "Task Detail"), ("", "\n"+"\n".join(lines))]

    status_control = FormattedTextControl(text=lambda: [("reverse", build_status_bar())])
    status_window = Window(height=1, content=status_control)

    show_help = False

    def _fmt_hms(total_seconds: int) -> str:
        s = int(max(0, total_seconds))
        h, r = divmod(s, 3600)
        m, s = divmod(r, 60)
        if h:
            return f"{h:d}:{m:02d}:{s:02d}"
        return f"{m:02d}:{s:02d}"

    def build_status_bar() -> str:
        mode = (
            "DATE" if in_date_filter else (
            "SEARCH" if in_search else (
            "DETAIL" if detail_mode else (
            "REPORT" if show_report else (
            "HELP" if show_help else "BROWSE"))))
        )
        base = f" {mode} W:timer R:report u:update U:unassigned j/k:nav h/l:←/→ /:search F:date<= Enter:detail p:project P:clear N:hide-no-date d:hide-done t:today a:all ?:help q:quit "
    # Keep bottom bar compact; top bar shows Project/Search to avoid overflow
        base += f"[Sort:{'Date' if sort_mode=='date' else 'Project'}] "
        if hide_done:
            base += "[HideDone] "
        if hide_no_date:
            base += "[HideNoDate] "
    # project and search are shown on the top status bar
        if date_max:
            base += f"[<= {date_max}] "
        return base + status_line

    from prompt_toolkit.layout.containers import Float, FloatContainer
    floats = []
    root_body = VSplit([table_window, Window(width=1, char='│'), stats_window])
    container = FloatContainer(content=HSplit([top_status_window, root_body, status_window]), floats=floats)

    kb = KeyBindings()
    # Mode filters to enable/disable keybindings contextually
    is_search = Condition(lambda: in_search)
    is_date = Condition(lambda: in_date_filter)
    is_detail = Condition(lambda: detail_mode)
    is_input_mode = Condition(lambda: in_search or in_date_filter or detail_mode or show_report)
    is_normal = Condition(lambda: not (in_search or in_date_filter or detail_mode or show_report))

    def invalidate():
        table_control.text = lambda: build_table_fragments()  # ensure recalculated
        stats_control.text = lambda: summarize()
        app.invalidate()

    @kb.add('q')
    def _(event):
        nonlocal detail_mode, in_search, search_buffer, show_report
        if detail_mode:
            detail_mode = False
            if floats:
                floats.clear()
            invalidate()
            return
        if show_report:
            show_report = False
            if floats:
                floats.clear()
            invalidate(); return
        if in_search:
            in_search = False
            search_buffer = ""
            invalidate()
            return
        # Save UI state before exiting
        _save_state()
        event.app.exit()

    @kb.add('enter')
    def _(event):
        nonlocal detail_mode, show_report
        if in_search:
            finalize_search(); return
        if in_date_filter:
            finalize_date(); return
        if show_report:
            show_report = False
            floats.clear()
        else:
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

    @kb.add('j', filter=is_normal)
    @kb.add('down', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        move(1); invalidate()

    @kb.add('k', filter=is_normal)
    @kb.add('up', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        move(-1); invalidate()

    # horizontal scroll
    @kb.add('h', filter=is_normal)
    @kb.add('left', filter=is_normal)
    def _(event):
        nonlocal h_offset
        if detail_mode or in_search:
            return
        h_offset = max(0, h_offset-4); invalidate()

    @kb.add('l', filter=is_normal)
    @kb.add('right', filter=is_normal)
    def _(event):
        nonlocal h_offset
        if detail_mode or in_search:
            return
        h_offset += 4; invalidate()

    # top/bottom
    gg_state = {'g': False}
    @kb.add('g', filter=is_normal)
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

    @kb.add('G', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        rows = filtered_rows();
        if rows:
            nonlocal current_index
            current_index = len(rows)-1
            invalidate()

    # filters
    @kb.add('d', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal hide_done, current_index
        hide_done = not hide_done
        current_index = 0
        invalidate()

    @kb.add('t', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal show_today_only, all_rows, current_index
        show_today_only = True
        all_rows = load_all(); current_index = 0
        invalidate()

    @kb.add('a', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        nonlocal show_today_only, all_rows, current_index
        show_today_only = False
        all_rows = load_all(); current_index = 0
        invalidate()

    @kb.add('p', filter=is_normal)
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

    @kb.add('P', filter=is_normal)
    def _(event):
        # Clear project filter
        if detail_mode or in_search:
            return
        nonlocal project_cycle
        project_cycle = None
        invalidate()

    @kb.add('N', filter=is_normal)
    def _(event):
        # Toggle hide no-date tasks
        if detail_mode or in_search:
            return
        nonlocal hide_no_date, current_index
        hide_no_date = not hide_no_date
        current_index = 0
        invalidate()

    # search mode
    @kb.add('/')
    def _(event):
        if detail_mode:
            return
        nonlocal in_search, search_buffer, in_date_filter, status_line
        in_date_filter = False
        in_search = True
        search_buffer = ""
        status_line = "Search: "
        invalidate()

    def finalize_search():
        nonlocal in_search, search_term, search_buffer, current_index, status_line
        search_term = search_buffer or None
        in_search = False
        status_line = ''
        try:
            logger.debug("finalize_search committed='%s'", search_term)
        except Exception:
            pass
        rows = filtered_rows()
        current_index = 0 if rows else 0
        invalidate()

    @kb.add('escape')
    def _(event):
        nonlocal in_search, detail_mode, search_buffer, in_date_filter, date_buffer, status_line, show_report
        if in_search:
            in_search = False
            search_buffer = ""
            status_line = ''
            invalidate(); return
        if in_date_filter:
            in_date_filter = False; date_buffer = ""; invalidate(); return
        if detail_mode:
            detail_mode = False; floats.clear(); invalidate(); return
        if show_report:
            show_report = False; floats.clear(); invalidate(); return

    @kb.add('backspace')
    def _(event):
        nonlocal search_buffer, date_buffer, status_line
        if in_search and search_buffer:
            search_buffer = search_buffer[:-1]
            status_line = f"Search: {search_buffer}"
            invalidate()
        elif in_date_filter and date_buffer:
            date_buffer = date_buffer[:-1]
            status_line = f"Date<= {date_buffer}"
            invalidate()

    # Catch-all printable character input for live search and date filter typing
    @kb.add(Keys.Any, filter=Condition(lambda: in_search or in_date_filter))
    def _(event):
        nonlocal search_buffer, status_line, date_buffer
        ch = event.data or ""
        if not ch:
            return
        if in_search:
            # Accept any printable char; special keys (enter/esc/backspace) have empty event.data
            search_buffer += ch
            status_line = f"Search: {search_buffer}"
            invalidate()
        elif in_date_filter and ch in (string.digits + "-"):
            if len(date_buffer) < 10:  # YYYY-MM-DD
                date_buffer += ch
                status_line = f"Date<= {date_buffer}"
                invalidate()

    @kb.add('u', filter=is_normal)
    def _(event):
        if detail_mode or in_search or in_date_filter:
            return
        nonlocal status_line, all_rows, current_index
        status_line = "Updating..."; invalidate()
        try:
            logger.info("Update triggered via 'u' (include_unassigned=%s, show_today_only=%s)", show_unassigned, show_today_only)
        except Exception:
            pass

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
                nonlocal all_rows, current_index, status_line, today_date
                # Refresh today's date at start of update so colors/filters use current day
                try:
                    today_date = dt.date.today()
                    logger.debug("today_date refreshed at start of update: %s", today_date)
                except Exception:
                    pass
                def do_fetch():
                    if os.environ.get('MOCK_FETCH')=='1':
                        try:
                            logger.info("MOCK_FETCH enabled; generating mock tasks")
                        except Exception:
                            pass
                        rows = generate_mock_tasks(cfg)
                        progress(1,1,'[########################################] 100% Done')
                        return rows
                    if not token:
                        raise RuntimeError('TOKEN not set')
                    try:
                        logger.info("Fetching tasks from GitHub… (cutoff=%s, include_unassigned=%s)", today_date, show_unassigned)
                    except Exception:
                        pass
                    return fetch_tasks_github(token, cfg, date_cutoff=today_date, progress=progress, include_unassigned=show_unassigned)
                fut_rows = await asyncio.get_running_loop().run_in_executor(None, do_fetch)
                try:
                    logger.info("Fetched %d tasks; replacing DB rows", len(fut_rows))
                except Exception:
                    pass
                db.replace_all(fut_rows)
                # After replacing DB, re-evaluate today's date again (midnight rollovers)
                try:
                    today_date = dt.date.today()
                    logger.debug("today_date refreshed after update: %s", today_date)
                except Exception:
                    pass
                all_rows = load_all(); current_index = 0
                progress(1,1,'Updated')
                try:
                    logger.info("Update finished successfully. Cached rows: %d", len(all_rows))
                except Exception:
                    pass
            except Exception as e:
                status_line = f"Error: {e}"; invalidate()
                try:
                    logger.exception("Update failed")
                except Exception:
                    pass
        asyncio.create_task(worker())

    # Timer toggle
    @kb.add('W', filter=is_normal)
    def _(event):
        rows = filtered_rows()
        if not rows:
            return
        t = rows[current_index]
        if not t.url:
            return
        # Toggle: if running -> stop, else start
        if t.url in db.active_task_urls():
            db.stop_session(t.url)
            try:
                logger.info("Stopped timer for %s", t.url)
            except Exception:
                pass
        else:
            db.start_session(t.url, t.project_title)
            try:
                logger.info("Started timer for %s", t.url)
            except Exception:
                pass
        invalidate()
    # Sort toggle
    @kb.add('s', filter=is_normal)
    def _(event):
        nonlocal sort_mode, current_index, v_offset
        sort_mode = 'date' if sort_mode == 'project' else 'project'
        current_index = 0
        v_offset = 0
        invalidate()
    # Date <= filter input
    @kb.add('F')
    def _(event):
        if detail_mode:
            return
        nonlocal in_date_filter, in_search, date_buffer, status_line
        in_search = False
        in_date_filter = True
        date_buffer = date_max or ""
        status_line = f"Date<= {date_buffer or 'YYYY-MM-DD'}"
        invalidate()

    def finalize_date():
        nonlocal in_date_filter, date_max, date_buffer, status_line
        val = date_buffer.strip()
        if not val:
            date_max = None
        else:
            # basic validation
            try:
                dt.date.fromisoformat(val)
                date_max = val
            except Exception:
                status_line = f"Bad date '{val}' (use YYYY-MM-DD)"; invalidate(); return
        in_date_filter = False
        date_buffer = ""
        status_line = ""
        invalidate()

    # (Removed duplicate enter handler)

    def update_search_status():
        nonlocal status_line
        if in_search:
            status_line = f"Search: {search_buffer}"
        elif not detail_mode and not status_line:
            status_line = ''

    @kb.add('?', filter=is_normal)
    def _(event):
        nonlocal show_help, detail_mode, in_search, show_report
        if in_search:
            return
        detail_mode = False
        show_report = False
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
                "  W              Toggle work timer for selected task",
                "  R              Open timer report (day/week/month)",
                "  /              Start search (type, Enter to apply, Esc cancel)",
                "  s              Toggle sort (Project/Date)",
                "  p              Cycle project filter",
                "  P              Clear project filter (show all)",
                "  U              Toggle include unassigned (then press u to refetch)",
                "  d              Toggle done-only filter",
                "  N              Toggle hide tasks without a date",
                "  t / a          Today-only / All dates",
                "  u              Update (fetch GitHub)",
                "  ?              Toggle help",
                "  q / Esc        Quit / Close",
                f"  Current tasks: {len(filtered_rows())}",
                "",
                "Visual cues:",
                "  ⏱ + cyan row   Task timer running",
                "",
                "Press ? to close help."
            ]
            hl_control = FormattedTextControl(text="\n".join(help_lines))
            floats.append(Float(content=Window(width=84, height=24, content=hl_control, style="bg:#202020 #ffffff", wrap_lines=True), top=1, left=2))
        invalidate()

    # Report bindings
    @kb.add('R', filter=is_normal)
    def _(event):
        nonlocal show_report
        show_report = True
        floats.clear()
        floats.append(Float(content=report_window, top=1, left=2))
        invalidate()

    @kb.add('d', filter=Condition(lambda: show_report))
    def _(event):
        nonlocal report_granularity
        report_granularity = 'day'; invalidate()

    @kb.add('w', filter=Condition(lambda: show_report))
    def _(event):
        nonlocal report_granularity
        report_granularity = 'week'; invalidate()

    @kb.add('m', filter=Condition(lambda: show_report))
    def _(event):
        nonlocal report_granularity
        report_granularity = 'month'; invalidate()

    # refresh loop timer to update status bar (search typing etc.)
    style = Style.from_dict({})
    app = Application(layout=Layout(container), key_bindings=kb, full_screen=True, mouse_support=True, style=style, editing_mode=EditingMode.VI)

    # Background ticker to refresh timers & status once per second
    async def _ticker():
        while True:
            try:
                await asyncio.sleep(1)
                update_search_status()
                app.invalidate()
            except Exception:
                # don't crash on background exceptions
                await asyncio.sleep(1)
                continue
    try:
        app.create_background_task(_ticker())
    except Exception:
        # Fallback: start via asyncio if available
        try:
            asyncio.create_task(_ticker())
        except Exception:
            pass
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
                start_field="Start date", start_date=date_str,
                focus_field="Focus Day", focus_date=date_str,
                title=f"Task {i}-{d_off}", repo="demo/repo",
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
    ap.add_argument("--log-level", default="ERROR", help="File log level (DEBUG, INFO, WARNING, ERROR)")
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

    # Do not auto-update on start; leave DB empty unless MOCK_FETCH is requested.
    # Users update manually with the 'u' hotkey in the UI.
    if not db.load():
        if os.environ.get("MOCK_FETCH") == "1":
            db.upsert_many(generate_mock_tasks(cfg))
        else:
            # Start with empty cache. The UI will show a hint to press 'u' to fetch.
            pass

    if args.no_ui:
        rows = db.load()
        done_ct = sum(1 for r in rows if r.is_done)
        print(f"Tasks: {len(rows)} (done {done_ct})")
        projects = sorted({r.project_title for r in rows})
        print("Projects:", ", ".join(projects))
        return

    run_ui(db, cfg, token, log_level=args.log_level)


if __name__ == "__main__":
    main()
