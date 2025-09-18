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
#   X  export a JSON report (quick export)
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
import unicodedata
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
try:
    from prompt_toolkit.utils import get_cwidth as _pt_get_cwidth
except ImportError:  # pragma: no cover - fallback when prompt_toolkit changes API
    _pt_get_cwidth = None
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
    iteration_field_regex: Optional[str] = None


def _compile_date_regex(raw: dict) -> str:
    """Support date_field_regex (string) OR date_field_names (list[str])."""
    names = raw.get("date_field_names")
    if names and isinstance(names, list) and names:
        parts = [f"^{re.escape(n)}$" for n in names]
        return "|".join(parts)
    return raw.get("date_field_regex") or "start"


def _compile_iteration_regex(raw: dict) -> Optional[str]:
    names = raw.get("iteration_field_names")
    if names and isinstance(names, list) and names:
        parts = [f"^{re.escape(n)}$" for n in names]
        return "|".join(parts)
    it_regex = raw.get("iteration_field_regex")
    return it_regex or None


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    user = raw.get("user") or ""
    if not user:
        raise ValueError("Config: 'user' is required.")
    dfr = _compile_date_regex(raw)
    ifr = _compile_iteration_regex(raw)
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
    return Config(user=user, date_field_regex=dfr, projects=prjs, iteration_field_regex=ifr)


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
    iteration_field: str = ""
    iteration_title: str = ""
    iteration_start: str = ""
    iteration_duration: int = 0
    title: str = ""
    repo: Optional[str] = None
    url: str = ""
    updated_at: str = ""
    status: Optional[str] = None  # textual status (eg. In Progress, Done)
    is_done: int = 0              # 1 if done / completed
    assigned_to_me: int = 0       # 1 if explicitly assigned
    created_by_me: int = 0        # 1 if authored/created by me


class TaskDB:
    SCHEMA_COLUMNS = [
        "owner_type","owner","project_number","project_title",
        "start_field","start_date",
        "focus_field","focus_date",
        "iteration_field","iteration_title","iteration_start","iteration_duration",
        "title","repo","url","updated_at","status","is_done","assigned_to_me","created_by_me"
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
        iteration_field TEXT,
        iteration_title TEXT,
        iteration_start TEXT,
        iteration_duration INTEGER DEFAULT 0,
        title TEXT NOT NULL,
        repo TEXT,
        url TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        status TEXT,
        is_done INTEGER DEFAULT 0,
        assigned_to_me INTEGER DEFAULT 0,
        created_by_me INTEGER DEFAULT 0,
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
            "iteration_field":"''","iteration_title":"''","iteration_start":"''","iteration_duration":"0",
            "title":"''","repo":"NULL","url":"''",
            "updated_at":"datetime('now')","status":"NULL","is_done":"0",
            "assigned_to_me":"0","created_by_me":"0",
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

    def aggregate_task_totals(self, since_days: Optional[int] = None) -> Dict[str, int]:
        cur = self.conn.cursor()
        cur.execute("SELECT task_url, started_at, ended_at FROM work_sessions")
        rows = cur.fetchall()
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        since_dt = (now - dt.timedelta(days=since_days)) if since_days else None
        out: Dict[str, int] = {}
        for url, st_s, en_s in rows:
            url = url or ''
            st = self._parse_iso(st_s)
            en = self._parse_iso(en_s) if en_s else None
            if not st:
                continue
            if en is None:
                en = now
            st, en, keep = self._clip_range(st, en, since_dt)
            if not keep or st >= en:
                continue
            out[url] = out.get(url, 0) + int((en - st).total_seconds())
        return out

    def task_titles(self) -> Dict[str, str]:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT url, MAX(updated_at) as u, title FROM tasks GROUP BY url")
            rows = cur.fetchall()
            return {url: (title or url) for url, _, title in rows if url}
        except Exception:
            return {}

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

    def aggregate_project_period_totals(self, granularity: str, since_days: Optional[int] = None) -> Dict[str, Dict[str, int]]:
        cur = self.conn.cursor()
        cur.execute("SELECT project_title, started_at, ended_at FROM work_sessions")
        rows = cur.fetchall()
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        since_dt = (now - dt.timedelta(days=since_days)) if since_days else None
        out: Dict[str, Dict[str, int]] = {}
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
            cur_dt = st
            while cur_dt < en:
                boundary = self._next_boundary(cur_dt, granularity)
                seg_end = min(boundary, en)
                key = self._period_key(cur_dt, granularity)
                bucket = out.setdefault(proj, {})
                bucket[key] = bucket.get(key, 0) + int((seg_end - cur_dt).total_seconds())
                cur_dt = seg_end
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
              iteration_field, iteration_title, iteration_start, iteration_duration,
              title, repo, url, updated_at, status, is_done, assigned_to_me, created_by_me
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(owner_type, owner, project_number, title, url, start_field, start_date)
            DO UPDATE SET project_title=excluded.project_title,
                          repo=excluded.repo,
                          updated_at=excluded.updated_at,
                          status=excluded.status,
                          is_done=excluded.is_done,
                          iteration_field=excluded.iteration_field,
                          iteration_title=excluded.iteration_title,
                          iteration_start=excluded.iteration_start,
                          iteration_duration=excluded.iteration_duration,
                          assigned_to_me=excluded.assigned_to_me,
                          created_by_me=excluded.created_by_me
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
                    r.iteration_field,
                    r.iteration_title,
                    r.iteration_start,
                    r.iteration_duration,
                    r.title,
                    r.repo,
                    r.url,
                    r.updated_at,
                    r.status,
                    r.is_done,
                    r.assigned_to_me,
                    r.created_by_me,
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
                       start_date,focus_field,focus_date,
                       iteration_field,iteration_title,iteration_start,iteration_duration,
                       title,repo,url,updated_at,status,is_done,assigned_to_me,created_by_me
                FROM tasks WHERE focus_date = ?
                ORDER BY project_title, focus_date, repo, title
                """,
                (today,),
            )
        else:
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,focus_field,focus_date,
                       iteration_field,iteration_title,iteration_start,iteration_duration,
                       title,repo,url,updated_at,status,is_done,assigned_to_me,created_by_me
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
            ... on DraftIssue {
              title
              creator { login }
            }
            ... on Issue {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
              author { login }
            }
            ... on PullRequest {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
              author { login }
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
                            ... on ProjectV2ItemFieldIterationValue {
                                title
                                startDate
                                duration
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
            ... on DraftIssue {
              title
              creator { login }
            }
            ... on Issue {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
              author { login }
            }
            ... on PullRequest {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
              author { login }
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
                            ... on ProjectV2ItemFieldIterationValue {
                                title
                                startDate
                                duration
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
    return [n for n in nodes if n is not None and isinstance(n, dict) and not n.get("closed")]


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
    iter_regex = re.compile(cfg.iteration_field_regex, re.IGNORECASE) if cfg.iteration_field_regex else None
    me = cfg.user
    me_login = me.strip().lower()
    def _norm_login(login: Optional[str]) -> Optional[str]:
        if isinstance(login, str):
            return login.strip().lower()
        return None
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

                assignees_norm: List[str] = []
                if ctype in ("Issue","PullRequest"):
                    for node in (content.get("assignees") or {}).get("nodes") or []:
                        login_norm = _norm_login((node or {}).get("login"))
                        if login_norm:
                            assignees_norm.append(login_norm)
                people_logins: List[str] = []
                status_text: Optional[str] = None
                author_login_norm: Optional[str] = None
                iteration_field: str = ""
                iteration_title: str = ""
                iteration_start: str = ""
                iteration_duration: int = 0
                iteration_captured = False
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldUserValue":
                        for node in (fv.get("users") or {}).get("nodes") or []:
                            login_norm = _norm_login((node or {}).get("login"))
                            if login_norm:
                                people_logins.append(login_norm)
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldSingleSelectValue":
                        fname_sel = ((fv.get("field") or {}).get("name") or "").lower()
                        if fname_sel in ("status","state","progress"):
                            status_text = (fv.get("name") or "").strip()
                    if (not iteration_captured) and fv and fv.get("__typename") == "ProjectV2ItemFieldIterationValue":
                        fname_iter = ((fv.get("field") or {}).get("name") or "")
                        if (iter_regex is None) or iter_regex.search(fname_iter):
                            iteration_field = fname_iter
                            iteration_title = (fv.get("title") or "")
                            iteration_start = fv.get("startDate") or ""
                            try:
                                iteration_duration = int(fv.get("duration") or 0)
                            except (TypeError, ValueError):
                                iteration_duration = 0
                            iteration_captured = True
                if ctype == "DraftIssue":
                    author_login_norm = _norm_login(((content.get("creator") or {})).get("login"))
                elif ctype in ("Issue","PullRequest"):
                    author_login_norm = _norm_login(((content.get("author") or {})).get("login"))
                assigned_to_me = (me_login in assignees_norm) or (me_login in people_logins)
                created_by_me = author_login_norm == me_login if author_login_norm else False
                if (not assigned_to_me) and (not created_by_me) and (not include_unassigned):
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
                                iteration_field=iteration_field,
                                iteration_title=iteration_title,
                                iteration_start=iteration_start,
                                iteration_duration=iteration_duration,
                                title=title, repo=repo, url=url, updated_at=iso_now,
                                status=status_text, is_done=done_flag,
                                assigned_to_me=int(assigned_to_me),
                                created_by_me=int(created_by_me)
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
                            iteration_field=iteration_field,
                            iteration_title=iteration_title,
                            iteration_start=iteration_start,
                            iteration_duration=iteration_duration,
                            title=title + (" (unassigned)" if not assigned_to_me else ""), repo=repo, url=url, updated_at=iso_now,
                            status=status_text, is_done=done_flag,
                            assigned_to_me=int(assigned_to_me),
                            created_by_me=int(created_by_me)
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

def _char_width(ch: str) -> int:
    """Return printable cell width for a single character."""
    if _pt_get_cwidth is not None:
        try:
            return _pt_get_cwidth(ch)
        except Exception:
            pass
    if unicodedata.combining(ch):
        return 0
    if unicodedata.category(ch) == "Cf":  # zero-width formatting chars
        return 0
    return 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1


def _display_width(text: str) -> int:
    return sum(_char_width(ch) for ch in text)


def _sanitize_cell_text(s: Optional[str]) -> str:
    return (s or "").replace("\n", " ").replace("\r", " ")


def _truncate(s: str, maxlen: int) -> str:
    """Truncate string to a maximum display width, preserving whole glyphs."""
    s = _sanitize_cell_text(s)
    if maxlen <= 0:
        return ""
    if _display_width(s) <= maxlen:
        return s
    ellipsis = "…"
    ell_w = _display_width(ellipsis)
    out: List[str] = []
    width = 0
    for ch in s:
        ch_w = _char_width(ch)
        if width + ch_w + ell_w >= maxlen:
            break
        out.append(ch)
        width += ch_w
    # Ensure ellipsis fits; if not, drop last chars until it does.
    while out and width + ell_w > maxlen:
        removed = out.pop()
        width -= _char_width(removed)
    if out:
        return "".join(out) + ellipsis
    return ellipsis if maxlen >= ell_w else ""


def _pad_display(text: Optional[str], width: int, align: str = "left") -> str:
    """Pad/truncate text to an exact display width using spaces."""
    align = align.lower()
    raw = _truncate(_sanitize_cell_text(text), width)
    pad = max(0, width - _display_width(raw))
    if align == "right":
        return " " * pad + raw
    if align == "center":
        left = pad // 2
        right = pad - left
        return (" " * left) + raw + (" " * right)
    return raw + (" " * pad)

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
        focus_cell = _pad_display(t.focus_date or '-', 11)
        start_cell = _pad_display(t.start_date, 12)
        status_cell = _pad_display(t.status or '-', 10)
        title_cell = _pad_display(t.title, 45)
        repo_cell = _pad_display(t.repo or '-', 20)
        url_cell = _pad_display(t.url, 40)
        frags.append((col, focus_cell))
        frags.append(("",  "  "))
        frags.append(("", f"{start_cell}  {status_cell}  {title_cell}  {repo_cell}  {url_cell}"))
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
    include_created = True
    use_iteration = False
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
            'include_created': include_created,
            'use_iteration': use_iteration,
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
    include_created = bool(_st.get('include_created', include_created))
    use_iteration = bool(_st.get('use_iteration', use_iteration))
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
            if use_iteration:
                out = [r for r in out if r.iteration_title or r.iteration_start]
            else:
                out = [r for r in out if r.focus_date]
        if not include_created:
            out = [r for r in out if not (r.created_by_me and not r.assigned_to_me)]
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
        # Determine column widths; right panel width fixed at 32 + separator.
        right_panel_width = 32 + 1
        avail_cols = max(40, total_cols - right_panel_width)
        time_w = 12  # "mm:ss|HH:MM" (right aligned)
        if use_iteration:
            iter_min = 15
            title_min = 20
            proj_min = 12
            sum_min = iter_min + title_min + proj_min
            base_fixed = 2 + 2 + 10 + 2 + time_w + 2 + 2  # marker + spaces + status + time separators
            dyn_total = max(sum_min, avail_cols - base_fixed)
            extra = dyn_total - sum_min
            iter_w = iter_min + extra // 4
            title_w = title_min + extra // 2
            proj_w = proj_min + extra - (extra // 4) - (extra // 2)
            header = (
                "  " + _pad_display("Iteration", iter_w) +
                "  " + _pad_display("STATUS", 10) +
                "  " + _pad_display("TIME", time_w, align='right') +
                "  " + _pad_display("TITLE", title_w) +
                "  " + _pad_display("PROJECT", proj_w)
            )
        else:
            proj_min = 12
            title_min = 20
            fixed = 2 + 11 + 2 + 12 + 2 + 10 + 2 + time_w + 2  # marker + focus + start + status + time + spaces
            dyn = max(title_min + proj_min, avail_cols - fixed)
            extra = dyn - (title_min + proj_min)
            title_w = title_min + extra // 2
            proj_w = proj_min + extra - (extra // 2)
            header = (
                "  " + _pad_display("Focus Day", 11) +
                "  " + _pad_display("Start Date", 12) +
                "  " + _pad_display("STATUS", 10) +
                "  " + _pad_display("TIME", time_w, align='right') +
                "  " + _pad_display("TITLE", title_w) +
                "  " + _pad_display("PROJECT", proj_w)
            )
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
            marker = '⏱ ' if running else '  '
            # Time column: current run (mm:ss) and total (H:MM)
            cur_s = db.task_current_elapsed_seconds(t.url) if (t.url and running) else 0
            tot_s = db.task_total_seconds(t.url) if t.url else 0
            mm, ss = divmod(int(max(0, cur_s)), 60)
            # total in H:MM (no leading zeros on hours)
            th, rem = divmod(int(max(0, tot_s)), 3600)
            tm, _ = divmod(rem, 60)
            time_text = f"{mm:02d}:{ss:02d}|{th:d}:{tm:02d}" if tot_s else f"{mm:02d}:{ss:02d}|0:00"
            status_cell = _pad_display(t.status or '-', 10)
            time_cell = _pad_display(time_text, time_w, align='right')
            title_cell = _pad_display(t.title, title_w)
            project_cell = _pad_display(t.project_title, proj_w)
            if use_iteration:
                iter_label = t.iteration_title or t.iteration_start or '-'
                if t.iteration_title and t.iteration_start:
                    iter_label = f"{t.iteration_title} ({t.iteration_start})"
                iteration_cell = _pad_display(iter_label or '-', iter_w)
                line = f"{marker}{iteration_cell}  {status_cell}  {time_cell}  {title_cell}  {project_cell}"
            else:
                focus_cell = _pad_display(t.focus_date or '-', 11)
                start_cell = _pad_display(t.start_date, 12)
                line = f"{marker}{focus_cell}  {start_cell}  {status_cell}  {time_cell}  {title_cell}  {project_cell}"
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
            proj_cell = _pad_display(p, 12)
            lines.append(f"{proj_cell}{d:>2}/{t:<2} {pct:>3}% {_fmt_hm(secs):>6}")
        lines.append("")
        lines.append("Filters:")
        active_search = search_buffer if in_search else search_term
        lines.append(f"HideDone:{'Y' if hide_done else 'N'} HideNoDate:{'Y' if hide_no_date else 'N'} Unassigned:{'Y' if show_unassigned else 'N'} Created:{'Y' if include_created else 'N'}")
        lines.append(f"Proj:{_truncate(project_cycle or 'All',10)}")
        lines.append(f"Today:{'Y' if show_today_only else 'N'}")
        lines.append(f"Date<=:{date_max or '-'}")
        lines.append(f"Search:{_truncate(active_search or '-',12)}")
        lines.append(f"View:{'Iteration' if use_iteration else 'Dates'}")
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
        view_label = 'Iteration' if use_iteration else 'Dates'
        txt = f"{timers}| Date: {today_date.isoformat()}  | Project: {_truncate(active_proj,30)}  | View: {view_label}  | Shown: {total}  | Search: {_truncate(active_search,30)} "
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
        iter_parts = []
        if t.iteration_title:
            iter_parts.append(t.iteration_title)
        if t.iteration_start:
            iter_parts.append(t.iteration_start)
        iter_display = " | ".join(iter_parts) if iter_parts else "-"
        iter_suffix = []
        if t.iteration_field:
            iter_suffix.append(t.iteration_field)
        if t.iteration_duration:
            iter_suffix.append(f"{t.iteration_duration}d")
        iter_meta = f" ({', '.join(iter_suffix)})" if iter_suffix else ""
        lines = [
            f"Project: {t.project_title}",
            f"Title:   {t.title}",
            f"Repo:    {t.repo}",
            f"URL:     {t.url}",
            f"Start:   {t.start_date} ({t.start_field})",
            f"Focus:   {t.focus_date or '-'} ({t.focus_field or '-'})",
            f"Iter:    {iter_display}{iter_meta}",
            f"Status:  {t.status}",
            f"Done:    {'Yes' if t.is_done else 'No'}",
            f"Assigned:{'Yes' if t.assigned_to_me else 'No'}",
            f"Created: {'Yes' if t.created_by_me else 'No'}",
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
        base = f" {mode} W:timer R:report X:export Z:pdf u:update U:unassigned C:created j/k:nav h/l:←/→ /:search F:date<= Enter:detail p:project P:clear N:hide-no-date d:hide-done t:today a:all I:iteration ?:help q:quit "
    # Keep bottom bar compact; top bar shows Project/Search to avoid overflow
        base += f"[Sort:{'Date' if sort_mode=='date' else 'Project'}] "
        if hide_done:
            base += "[HideDone] "
        if hide_no_date:
            base += "[HideNoDate] "
        if not include_created:
            base += "[NoCreated] "
        if use_iteration:
            base += "[Iteration] "
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

    @kb.add('I', filter=is_normal)
    def _(event):
        # Toggle date vs iteration layout
        if detail_mode or in_search:
            return
        nonlocal use_iteration, h_offset, status_line
        use_iteration = not use_iteration
        h_offset = 0
        status_line = 'Iteration view ON' if use_iteration else 'Iteration view OFF'
        invalidate()

    @kb.add('C', filter=is_normal)
    def _(event):
        # Toggle inclusion of tasks created-by-me but not assigned
        if detail_mode or in_search:
            return
        nonlocal include_created, status_line, current_index
        include_created = not include_created
        current_index = 0
        status_line = 'Including created tasks' if include_created else 'Hiding created-only tasks'
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

    # Quick export from UI: writes a JSON report next to DB with timestamp
    @kb.add('X', filter=is_normal)
    def _(event):
        nonlocal status_line
        try:
            ts = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
            out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'report-{ts}.json')
            gran_opts = ['day','week','month']
            since_days = 90
            payload: Dict[str, object] = {
                'meta': {
                    'generated_at': dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec='seconds'),
                    'user': cfg.user,
                    'since_days': since_days,
                    'granularity': 'all',
                    'scope': 'all',
                }
            }
            overall: Dict[str, Dict[str, int]] = {}
            for g in gran_opts:
                overall[g] = db.aggregate_period_totals(g, since_days=since_days)
            payload['overall'] = overall
            proj_totals_window = db.aggregate_project_totals(since_days=since_days)
            payload['projects_total_window'] = proj_totals_window
            projects_periods: Dict[str, Dict[str, Dict[str,int]]] = {}
            for g in gran_opts:
                projects_periods[g] = db.aggregate_project_period_totals(g, since_days=since_days)
            payload['projects_periods'] = projects_periods
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=2)
            status_line = f"Exported {out_path}"
        except Exception as e:
            status_line = f"Export failed: {e}"
        invalidate()

    # Quick PDF export from UI
    @kb.add('Z', filter=is_normal)
    def _(event):
        nonlocal status_line
        try:
            # Build payload (same as JSON export) and render with portrait summary
            since_days = 90
            payload = {
                'meta': {
                    'generated_at': dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec='seconds'),
                    'user': cfg.user,
                    'since_days': since_days,
                    'granularity': 'all',
                    'scope': 'all',
                },
                'overall': {g: db.aggregate_period_totals(g, since_days=since_days) for g in ['day','week','month']},
                'projects_total_window': db.aggregate_project_totals(since_days=since_days),
                'tasks_total_window': db.aggregate_task_totals(since_days=since_days),
                'task_titles': db.task_titles(),
            }
            # Try importing reportlab here to check availability
            try:
                from reportlab.lib.pagesizes import A4  # noqa: F401
            except Exception:
                status_line = "PDF export needs reportlab (pip install reportlab)"; invalidate(); return
            ts = dt.datetime.now().strftime('%Y%m%d-%H%M%S')
            out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), f'report-{ts}.pdf')
            # Use the same portrait renderer as CLI export
            # Implemented within CLI block; re-implement minimal version here for consistency
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
            from reportlab.lib import colors
            from reportlab.lib.units import mm
            def fmt_hm(s:int)->str:
                s = int(max(0, s))
                h, r = divmod(s, 3600)
                m, _ = divmod(r, 60)
                return f"{h:d}:{m:02d}"
            def draw_header(c):
                generated = dt.datetime.now().strftime('%Y-%m-%d %H:%M')
                c.setFillColor(colors.HexColor('#111111'))
                c.setFont('Helvetica-Bold', 16)
                c.drawString(20*mm, 285*mm, 'Work Timers Summary')
                c.setFont('Helvetica', 10)
                c.setFillColor(colors.HexColor('#555555'))
                c.drawString(20*mm, 278*mm, f"User: {cfg.user}  •  Generated: {generated}")
            def table(c, x, y, cols, rows):
                c.setFont('Helvetica', 9)
                line_h = 9
                c.setFillColor(colors.HexColor('#f0f3ff'))
                c.rect(x, y, sum(w for _,w in cols), line_h+2, stroke=0, fill=True)
                c.setFillColor(colors.HexColor('#222222'))
                cx = x
                for (name, w) in cols:
                    c.setFont('Helvetica-Bold', 9)
                    c.drawString(cx+2, y+2, name)
                    cx += w
                yy = y - line_h
                for i, r in enumerate(rows):
                    if i % 2 == 0:
                        c.setFillColor(colors.HexColor('#fafafa'))
                        c.rect(x, yy-1, sum(w for _,w in cols), line_h+1, stroke=0, fill=True)
                    cx = x
                    for j, val in enumerate(r):
                        c.setFillColor(colors.HexColor('#000000'))
                        c.setFont('Helvetica', 9)
                        c.drawString(cx+2, yy+1, str(val))
                        cx += cols[j][1]
                    yy -= line_h
                return yy
            def pie_chart(c, cx, cy, r, items, palette, center_label):
                total = sum(v for _,v in items) or 1
                start = 90
                for i,(lab,val) in enumerate(items):
                    extent = 360.0 * (val/total)
                    c.setFillColor(palette[i % len(palette)])
                    c.wedge(cx-r, cy-r, cx+r, cy+r, start, extent, stroke=0, fill=1)
                    start += extent
                c.setFillColor(colors.white)
                c.circle(cx, cy, r*0.4, stroke=0, fill=1)
                c.setFillColor(colors.HexColor('#333333'))
                c.setFont('Helvetica-Bold', 10)
                c.drawCentredString(cx, cy-3, center_label)
            def pie_legend(c, x, y, items, palette, max_lines=4):
                total = sum(v for _,v in items) or 1
                lines = items[:max_lines]
                for i,(lab,val) in enumerate(lines):
                    pct = int(round(100*val/total))
                    c.setFillColor(palette[i % len(palette)])
                    c.rect(x, y-4, 6, 6, stroke=0, fill=1)
                    c.setFillColor(colors.HexColor('#333333'))
                    c.setFont('Helvetica', 8)
                    c.drawString(x+8, y-3, f"{_truncate(lab,12)} {pct}%")
                    y -= 8
            # Build rows using current DB
            since_map = {'D':1, 'W':7, 'M':30, 'Y':365}
            proj_totals = {k: db.aggregate_project_totals(v) for k,v in since_map.items()}
            task_totals = {k: db.aggregate_task_totals(v) for k,v in since_map.items()}
            task_titles = db.task_titles()
            proj_names = set().union(*[set(d.keys()) for d in proj_totals.values()])
            proj_rows_all = []
            for name in proj_names:
                d = proj_totals['D'].get(name,0); wv = proj_totals['W'].get(name,0); m = proj_totals['M'].get(name,0); yv = proj_totals['Y'].get(name,0)
                proj_rows_all.append((name or '-', yv, [name or '-', fmt_hm(d), fmt_hm(wv), fmt_hm(m), fmt_hm(yv)]))
            proj_rows_all.sort(key=lambda t: t[1], reverse=True)
            task_urls = set().union(*[set(d.keys()) for d in task_totals.values()])
            task_rows_all = []
            for url in task_urls:
                nm = task_titles.get(url, url)
                d = task_totals['D'].get(url,0); wv = task_totals['W'].get(url,0); m = task_totals['M'].get(url,0); yv = task_totals['Y'].get(url,0)
                task_rows_all.append((nm, yv, [nm[:48] + ('…' if len(nm)>48 else ''), fmt_hm(d), fmt_hm(wv), fmt_hm(m), fmt_hm(yv)]))
            task_rows_all.sort(key=lambda t: t[1], reverse=True)
            c = canvas.Canvas(out_path, pagesize=A4)
            draw_header(c)
            # pies row
            pal = [colors.HexColor(h) for h in ['#5B8FF9','#61DDAA','#65789B','#F6BD16','#7262FD','#78D3F8','#9661BC','#F6903D','#E86452','#6DC8EC']]
            def top_items(d):
                items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
                top = items[:5]
                other = sum(v for _,v in items[5:])
                if other>0:
                    top.append(("Other", other))
                return top
            c.setFillColor(colors.HexColor('#222222')); c.setFont('Helvetica-Bold', 12)
            c.drawString(20*mm, 250*mm, 'Distribution by Period (project share)')
            pies_y = 235*mm
            x_positions = [40*mm, 90*mm, 140*mm, 190*mm]
            for i,key in enumerate(['D','W','M','Y']):
                data = proj_totals.get(key,{}) or {}
                items = top_items(data)
                if items:
                    cx = x_positions[i]
                    pie_chart(c, cx, pies_y, 14*mm, items, pal, key)
                    pie_legend(c, cx - 14*mm, pies_y - 14*mm - 6*mm, items, pal, max_lines=3)
            cols_proj = [("Project", 70*mm), ("D", 15*mm), ("W", 15*mm), ("M", 15*mm), ("Y", 20*mm)]
            cols_task = [("Task", 100*mm), ("D", 15*mm), ("W", 15*mm), ("M", 15*mm), ("Y", 20*mm)]
            y = 195*mm
            c.setFont('Helvetica-Bold', 12); c.setFillColor(colors.HexColor('#222222'))
            c.drawString(20*mm, y, 'Per Project')
            y -= 6*mm
            y = table(c, 20*mm, y, cols_proj, [r[2] for r in proj_rows_all[:16]]) - 4
            c.setStrokeColor(colors.HexColor('#dddddd'))
            c.line(20*mm, y, 190*mm, y)
            y -= 6*mm
            c.setFont('Helvetica-Bold', 12); c.setFillColor(colors.HexColor('#222222'))
            c.drawString(20*mm, y, 'Per Task')
            y -= 6*mm
            max_rows_task = max(10, int((y - 20*mm)/9) - 2)
            table(c, 20*mm, y, cols_task, [r[2] for r in task_rows_all[:max_rows_task]])
            c.setFont('Helvetica', 8); c.setFillColor(colors.HexColor('#888888'))
            c.drawRightString(200*mm, 12*mm, 'Times are H:MM over last D=1/W=7/M=30/Y=365 days. Top rows shown.')
            c.showPage(); c.save()
            status_line = f"Exported {out_path}"
        except Exception as e:
            status_line = f"PDF export failed: {e}"
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
                "  X              Export a JSON report (quick)",
                "  Z              Export a PDF report (quick)",
                "  /              Start search (type, Enter to apply, Esc cancel)",
                "  s              Toggle sort (Project/Date)",
                "  p              Cycle project filter",
                "  P              Clear project filter (show all)",
                "  U              Toggle include unassigned (then press u to refetch)",
                "  d              Toggle done-only filter",
                "  N              Toggle hide tasks without a date",
                "  C              Toggle showing created-but-unassigned tasks",
                "  I              Toggle iteration/date view",
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
                is_done=1 if status.lower()=="done" else 0,
                assigned_to_me=1 if (i + d_off) % 2 == 0 else 0,
                created_by_me=1 if (i + d_off) % 3 == 0 else 0
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
    # Report export options
    ap.add_argument("--export-report", metavar="PATH", help="Write timer report to PATH (JSON)")
    ap.add_argument("--export-granularity", default="all", choices=["day","week","month","all"], help="Granularity for export (or 'all')")
    ap.add_argument("--export-since-days", type=int, default=90, help="How many days back to include (default 90)")
    ap.add_argument("--export-scope", default="all", choices=["overall","project","task","all"], help="Data scope to include")
    ap.add_argument("--export-project", help="Limit export to a project title (with --export-scope project/task/all)")
    ap.add_argument("--export-task-url", help="Limit export to a task URL (with --export-scope task/all)")
    ap.add_argument("--export-pdf", metavar="PATH", help="Write a 1-page PDF report to PATH")
    ap.add_argument("--pdf-from-json", metavar="JSON", help="Render PDF from an existing JSON report payload")
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

    def _seconds_hms(s:int) -> str:
        s = int(max(0, s))
        h, r = divmod(s, 3600)
        m, s = divmod(r, 60)
        return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"

    def _build_report_payload(db: TaskDB, cfg: Config, since_days: int, granularity: str, scope: str, proj: Optional[str], task_url: Optional[str]) -> Dict[str, object]:
        gran_opts = ([granularity] if granularity != 'all' else ['day','week','month'])
        payload: Dict[str, object] = {
            "meta": {
                "generated_at": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds"),
                "user": cfg.user,
                "since_days": since_days,
                "granularity": granularity,
                "scope": scope,
                "project": proj,
                "task_url": task_url,
            }
        }
        overall: Dict[str, Dict[str, int]] = {}
        for g in gran_opts:
            overall[g] = db.aggregate_period_totals(g, since_days=since_days)
        payload["overall"] = overall
        # Per-project totals and per-period totals
        proj_totals_window = db.aggregate_project_totals(since_days=since_days)
        payload["projects_total_window"] = proj_totals_window
        # Per-task totals (window) + titles map for labeling
        task_totals_window = db.aggregate_task_totals(since_days=since_days)
        payload["tasks_total_window"] = task_totals_window
        payload["task_titles"] = db.task_titles()
        projects_periods: Dict[str, Dict[str, Dict[str,int]]] = {}
        for g in gran_opts:
            projects_periods[g] = db.aggregate_project_period_totals(g, since_days=since_days)
        payload["projects_periods"] = projects_periods
        # Optional: single project filter summary
        if proj:
            proj_section: Dict[str, Dict[str,int]] = {}
            for g in gran_opts:
                proj_section[g] = db.aggregate_period_totals(g, since_days=since_days, project_title=proj)
            payload["project"] = {"title": proj, "periods": proj_section}
        # Optional: single task filter summary
        if task_url:
            task_section: Dict[str, Dict[str,int]] = {}
            for g in gran_opts:
                task_section[g] = db.aggregate_period_totals(g, since_days=since_days, task_url=task_url)
            payload["task"] = {"url": task_url, "periods": task_section}
        return payload

    if args.export_report and not args.export_pdfs if False else False:
        pass

    if args.export_report:
        # Build a structured JSON payload for external tooling/PDF generation
        payload = _build_report_payload(db, cfg, args.export_since_days, args.export_granularity, args.export_scope, args.export_project, args.export_task_url)
        # Write JSON
        out_path = args.export_report
        try:
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(payload, f, indent=2)
            print(f"Wrote report JSON to {out_path}")
        except Exception as e:
            print(f"Failed to write report: {e}", file=sys.stderr)
            sys.exit(2)
        return

    if args.export_pdf:
        # Build payload from DB or load from given JSON
        if args.pdf_from_json:
            try:
                with open(args.pdf_from_json, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
            except Exception as e:
                print(f"Failed to read JSON payload: {e}", file=sys.stderr)
                sys.exit(2)
        else:
            payload = _build_report_payload(db, cfg, args.export_since_days, args.export_granularity, args.export_scope, args.export_project, args.export_task_url)

        # Render a portrait A4 summary table with D/W/M/Y totals per project and per task
        try:
            from reportlab.lib.pagesizes import A4
            from reportlab.pdfgen import canvas
            from reportlab.lib import colors
            from reportlab.lib.units import mm
        except Exception:
            print("ReportLab not installed. Try: pip install reportlab", file=sys.stderr)
            sys.exit(2)

        def fmt_hm(s:int)->str:
            s = int(max(0, s))
            h, r = divmod(s, 3600)
            m, _ = divmod(r, 60)
            return f"{h:d}:{m:02d}"

        def draw_header(c):
            meta = payload.get('meta',{})
            user = meta.get('user','')
            generated = dt.datetime.now().strftime('%Y-%m-%d %H:%M')
            c.setFillColor(colors.HexColor('#111111'))
            c.setFont('Helvetica-Bold', 16)
            c.drawString(20*mm, 285*mm, 'Work Timers Summary')
            c.setFont('Helvetica', 10)
            c.setFillColor(colors.HexColor('#555555'))
            c.drawString(20*mm, 278*mm, f"User: {user}  •  Generated: {generated}")

        def table(c, x, y, cols, rows, col_colors=None, zebra=True):
            # rows: list of lists (strings)
            c.setFont('Helvetica', 9)
            line_h = 9
            # header
            c.setFillColor(colors.HexColor('#f0f3ff'))
            c.rect(x, y, sum(w for _,w in cols), line_h+2, stroke=0, fill=True)
            c.setFillColor(colors.HexColor('#222222'))
            cx = x
            for (name, w) in cols:
                c.setFont('Helvetica-Bold', 9)
                c.drawString(cx+2, y+2, name)
                cx += w
            yy = y - line_h
            for i, r in enumerate(rows):
                if zebra and (i % 2 == 0):
                    c.setFillColor(colors.HexColor('#fafafa'))
                    c.rect(x, yy-1, sum(w for _,w in cols), line_h+1, stroke=0, fill=True)
                cx = x
                for j, (val) in enumerate(r):
                    if col_colors and col_colors.get(j):
                        c.setFillColor(col_colors[j])
                    else:
                        c.setFillColor(colors.HexColor('#000000'))
                    c.setFont('Helvetica', 9)
                    c.drawString(cx+2, yy+1, str(val))
                    cx += cols[j][1]
                yy -= line_h
            return yy

        def pie_chart(c, cx, cy, r, items, palette, center_label):
            # items: list[(label,value)]
            total = sum(v for _,v in items) or 1
            start = 90  # start at top
            bbox = (cx-r, cy-r, cx+r, cy+r)
            for i,(lab,val) in enumerate(items):
                extent = 360.0 * (val/total)
                c.setFillColor(palette[i % len(palette)])
                c.wedge(bbox[0], bbox[1], bbox[2], bbox[3], start, extent, stroke=0, fill=1)
                start += extent
            # center label
            c.setFillColor(colors.white)
            c.circle(cx, cy, r*0.4, stroke=0, fill=1)
            c.setFillColor(colors.HexColor('#333333'))
            c.setFont('Helvetica-Bold', 10)
            c.drawCentredString(cx, cy-3, center_label)

        def pie_legend(c, x, y, items, palette, max_lines=4):
            total = sum(v for _,v in items) or 1
            lines = items[:max_lines]
            for i,(lab,val) in enumerate(lines):
                pct = int(round(100*val/total))
                c.setFillColor(palette[i % len(palette)])
                c.rect(x, y-4, 6, 6, stroke=0, fill=1)
                c.setFillColor(colors.HexColor('#333333'))
                c.setFont('Helvetica', 8)
                c.drawString(x+8, y-3, f"{_truncate(lab,12)} {pct}%")
                y -= 8

        def palette():
            return [colors.HexColor(h) for h in ['#5B8FF9','#61DDAA','#65789B','#F6BD16','#7262FD','#78D3F8','#9661BC','#F6903D','#E86452','#6DC8EC']]

        # Collect totals for D/W/M/Y windows
        since_map = {'D':1, 'W':7, 'M':30, 'Y':365}
        proj_totals = {k: db.aggregate_project_totals(v) for k,v in since_map.items()}
        task_totals = {k: db.aggregate_task_totals(v) for k,v in since_map.items()}
        task_titles = payload.get('task_titles', {})

        # Build project rows sorted by yearly desc
        proj_names = set()
        for d in proj_totals.values():
            proj_names.update(d.keys())
        proj_rows_all = []
        for name in proj_names:
            d = proj_totals['D'].get(name,0)
            w = proj_totals['W'].get(name,0)
            m = proj_totals['M'].get(name,0)
            y = proj_totals['Y'].get(name,0)
            proj_rows_all.append((name or '-', y, [name or '-', fmt_hm(d), fmt_hm(w), fmt_hm(m), fmt_hm(y)]))
        proj_rows_all.sort(key=lambda t: t[1], reverse=True)

        # Build task rows sorted by yearly desc
        task_urls = set()
        for d in task_totals.values():
            task_urls.update(d.keys())
        task_rows_all = []
        for url in task_urls:
            name = task_titles.get(url, url)
            d = task_totals['D'].get(url,0)
            w = task_totals['W'].get(url,0)
            m = task_totals['M'].get(url,0)
            y = task_totals['Y'].get(url,0)
            task_rows_all.append((name, y, [name and (name[:48]+('…' if len(name)>48 else '')) or '-', fmt_hm(d), fmt_hm(w), fmt_hm(m), fmt_hm(y)]))
        task_rows_all.sort(key=lambda t: t[1], reverse=True)

        # Compose page
        c = canvas.Canvas(args.export_pdf, pagesize=A4)
        draw_header(c)

        # Row of 4 pies: distribution by period (per project share)
        pal = palette()
        pie_r = 14*mm
        # compute items for each period: top5 projects + other
        def top_items(d: Dict[str,int]):
            items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)
            top = items[:5]
            other = sum(v for _,v in items[5:])
            if other > 0:
                top.append(("Other", other))
            return top
        c.setFillColor(colors.HexColor('#222222')); c.setFont('Helvetica-Bold', 12)
        c.drawString(20*mm, 250*mm, 'Distribution by Period (project share)')
        pies_y = 235*mm
        x_positions = [40*mm, 90*mm, 140*mm, 190*mm]
        labels = ['D','W','M','Y']
        for i,key in enumerate(labels):
            data = proj_totals.get(key,{}) or {}
            items = top_items(data)
            if items:
                cx = x_positions[i]
                pie_chart(c, cx, pies_y, pie_r, items, pal, key)
                # legend beneath each pie using period-specific percentages (limit 3 lines for compactness)
                pie_legend(c, cx - pie_r, pies_y - pie_r - 6*mm, items, pal, max_lines=3)
        cols_proj = [("Project", 70*mm), ("D", 15*mm), ("W", 15*mm), ("M", 15*mm), ("Y", 20*mm)]
        cols_task = [("Task", 100*mm), ("D", 15*mm), ("W", 15*mm), ("M", 15*mm), ("Y", 20*mm)]

        # Start positions
        y = 195*mm
        c.setFont('Helvetica-Bold', 12); c.setFillColor(colors.HexColor('#222222'))
        c.drawString(20*mm, y, 'Per Project')
        y -= 6*mm
        max_rows_proj = 16
        y = table(c, 20*mm, y, cols_proj, [r[2] for r in proj_rows_all[:max_rows_proj]]) - 4
        # Separator
        c.setStrokeColor(colors.HexColor('#dddddd'))
        c.line(20*mm, y, 190*mm, y)
        y -= 6*mm
        c.setFont('Helvetica-Bold', 12); c.setFillColor(colors.HexColor('#222222'))
        c.drawString(20*mm, y, 'Per Task')
        y -= 6*mm
        max_rows_task = max(10, int((y - 20*mm)/9) - 2)
        table(c, 20*mm, y, cols_task, [r[2] for r in task_rows_all[:max_rows_task]])

        # Footer note
        c.setFont('Helvetica', 8); c.setFillColor(colors.HexColor('#888888'))
        c.drawRightString(200*mm, 12*mm, 'Times are H:MM over last D=1/W=7/M=30/Y=365 days. Top rows shown.')

        c.showPage()
        c.save()
        print(f"Wrote PDF to {args.export_pdf}")
        return

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
