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
#   E  edit work sessions for the selected task
#   ]/[ cycle priority forward/backward for selected task
#   O  open task field editor (start/focus date, priority)
#   s/S cycle sort presets forward/backward
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
import calendar
import datetime as dt
import os
from pathlib import Path
import re
import sqlite3
import sys
import string
import json
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple, Iterable, Set

import requests
import yaml
import time
from prompt_toolkit import Application
from prompt_toolkit.enums import EditingMode
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, VSplit, Layout, Window
from prompt_toolkit.layout.dimension import Dimension
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.styles import Style
from prompt_toolkit.widgets import Frame
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
# Themes
# -----------------------------


@dataclass
class ThemePreset:
    name: str
    style: Dict[str, str]
    layout: str = "vertical"
    description: Optional[str] = None


BASE_THEME_STYLE: Dict[str, str] = {
    'editor.frame': 'bg:#1c1c1c #f0f0f0',
    'editor.frame.border': '#5f5f5f',
    'editor.frame.title': 'bold #ffd75f',
    'editor.body': 'bg:#1c1c1c #f0f0f0',
    'editor.header': 'bold #ffd75f',
    'editor.meta': '#87d7ff',
    'editor.text': '#f0f0f0',
    'editor.field': '#d7d7d7',
    'editor.field.cursor': 'bold #ffffff bg:#444444',
    'editor.instructions': '#5fd7af',
    'editor.priority': '#f0f0f0',
    'editor.priority.cursor': 'bold #ffffff bg:#875f00',
    'editor.label': '#d0d0d0',
    'editor.label.selected': 'bold #87ff5f',
    'editor.label.cursor': 'reverse #ffffaf',
    'editor.label.cursor.selected': 'reverse bold #ffffff',
    'editor.message': '#ffd787',
    'editor.warning': 'bold #ff8787',
    'editor.calendar': '#87afff',
    'editor.entry': '#ffffff bg:#303030',
    'table.header': 'bold #ffd75f',
    'table.status.todo': '#87d7ff',
    'table.status.in_progress': '#ffd75f',
    'table.status.done': '#87ff5f',
    'table.status.blocked': '#ff8787',
    'table.status.other': '#d0d0d0',
    'table.date.today': 'ansired bold',
    'table.date.past': 'ansiyellow',
    'table.date.future': 'ansigreen',
    'table.date.unknown': 'ansigray',
    'summary.panel': 'bg:#1c1c1c #f0f0f0',
    'summary.title': 'bold #ffd75f',
    'summary.label': '#ffd787',
    'summary.value': '#f0f0f0',
    'summary.accent': '#87d7ff',
}

DEFAULT_THEME_LAYOUT = "vertical"
SHIFTED_DIGIT_KEYS = ['!', '@', '#', '$', '%', '^', '&', '*', '(', ')']


def _load_theme_presets(theme_dir: Path) -> List[ThemePreset]:
    presets: List[ThemePreset] = [ThemePreset(name="Default", style=dict(BASE_THEME_STYLE), layout=DEFAULT_THEME_LAYOUT)]
    seen = {presets[0].name.lower()}
    if not theme_dir.is_dir():
        return presets
    candidates = sorted(theme_dir.glob("*.yml")) + sorted(theme_dir.glob("*.yaml"))
    for path in candidates:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            logging.getLogger('gh_task_viewer').warning("Failed to load theme file %s", path, exc_info=True)
            continue
        if not isinstance(data, dict):
            continue
        raw_name = str(data.get("name") or path.stem).strip()
        name = raw_name or path.stem
        layout = str(data.get("layout") or DEFAULT_THEME_LAYOUT).strip().lower()
        if layout not in {"vertical", "horizontal"}:
            layout = DEFAULT_THEME_LAYOUT
        overrides = data.get("style") if isinstance(data.get("style"), dict) else {}
        style_dict = dict(BASE_THEME_STYLE)
        if isinstance(overrides, dict):
            for key, value in overrides.items():
                if isinstance(key, str) and isinstance(value, str):
                    style_dict[key] = value
        preset = ThemePreset(name=name, style=style_dict, layout=layout, description=data.get("description"))
        lowered = name.lower()
        if lowered == "default":
            presets[0] = preset
            continue
        if lowered in seen:
            continue
        presets.append(preset)
        seen.add(lowered)
    return presets


# -----------------------------
# Project discovery cache
# -----------------------------

TARGET_CACHE_PATH = os.path.expanduser("~/.gh_tasks.targets.json")
USER_ID_CACHE: Dict[str, str] = {}
STATUS_OPTION_CACHE: Dict[str, List[Dict[str, object]]] = {}
_UNSET = object()


def _load_target_cache() -> Dict[str, List[Dict[str, object]]]:
    try:
        with open(TARGET_CACHE_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def _save_target_cache(data: Dict[str, List[Dict[str, object]]]) -> None:
    try:
        directory = os.path.dirname(TARGET_CACHE_PATH)
        if directory and not os.path.isdir(directory):
            os.makedirs(directory, exist_ok=True)
        with open(TARGET_CACHE_PATH, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    except Exception:
        try:
            logging.getLogger('gh_task_viewer').warning("Unable to write project cache", exc_info=True)
        except Exception:
            pass


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
    focus_field_id: str = ""
    iteration_field: str = ""
    iteration_title: str = ""
    iteration_start: str = ""
    iteration_duration: int = 0
    title: str = ""
    repo_id: str = ""
    repo: Optional[str] = None
    labels: str = "[]"
    priority: Optional[str] = None
    priority_field_id: str = ""
    priority_option_id: str = ""
    priority_options: str = "[]"
    priority_dirty: int = 0
    priority_pending_option_id: str = ""
    url: str = ""
    updated_at: str = ""
    status: Optional[str] = None  # textual status (eg. In Progress, Done)
    is_done: int = 0              # 1 if done / completed
    assigned_to_me: int = 0       # 1 if explicitly assigned
    created_by_me: int = 0        # 1 if authored/created by me
    item_id: str = ""
    project_id: str = ""
    status_field_id: str = ""
    status_option_id: str = ""
    status_options: str = "[]"
    status_dirty: int = 0
    status_pending_option_id: str = ""
    start_field_id: str = ""
    iteration_field_id: str = ""
    iteration_options: str = "[]"
    assignee_field_id: str = ""
    assignee_user_ids: str = "[]"
    assignee_logins: str = "[]"
    content_node_id: str = ""


class TaskDB:
    SCHEMA_COLUMNS = [
        "owner_type","owner","project_number","project_title",
        "start_field","start_date",
        "focus_field","focus_date","focus_field_id",
        "iteration_field","iteration_title","iteration_start","iteration_duration",
        "title","repo_id","repo","labels","priority","priority_field_id","priority_option_id","priority_options","priority_dirty","priority_pending_option_id","url","updated_at","status","is_done","assigned_to_me","created_by_me",
        "item_id","project_id","status_field_id","status_option_id","status_options","status_dirty","status_pending_option_id",
        "start_field_id","iteration_field_id","iteration_options","assignee_field_id","assignee_user_ids","assignee_logins","content_node_id"
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
        focus_field_id TEXT,
        iteration_field TEXT,
        iteration_title TEXT,
        iteration_start TEXT,
        iteration_duration INTEGER DEFAULT 0,
        title TEXT NOT NULL,
        repo_id TEXT,
        repo TEXT,
        labels TEXT,
        priority TEXT,
        priority_field_id TEXT,
        priority_option_id TEXT,
        priority_options TEXT,
        priority_dirty INTEGER DEFAULT 0,
        priority_pending_option_id TEXT,
        url TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        status TEXT,
        is_done INTEGER DEFAULT 0,
        assigned_to_me INTEGER DEFAULT 0,
        created_by_me INTEGER DEFAULT 0,
        item_id TEXT,
        project_id TEXT,
        status_field_id TEXT,
        status_option_id TEXT,
        status_options TEXT,
        status_dirty INTEGER DEFAULT 0,
        status_pending_option_id TEXT,
        start_field_id TEXT,
        iteration_field_id TEXT,
        iteration_options TEXT,
        assignee_field_id TEXT,
        assignee_user_ids TEXT,
        assignee_logins TEXT,
        content_node_id TEXT,
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
            "focus_field":"''","focus_date":"''","focus_field_id":"''",
            "iteration_field":"''","iteration_title":"''","iteration_start":"''","iteration_duration":"0",
            "title":"''","repo_id":"''","repo":"NULL","labels":"'[]'","priority":"NULL","priority_field_id":"''","priority_option_id":"''","priority_options":"'[]'","priority_dirty":"0","priority_pending_option_id":"''","url":"''",
            "updated_at":"datetime('now')","status":"NULL","is_done":"0",
            "assigned_to_me":"0","created_by_me":"0",
            "item_id":"''","project_id":"''","status_field_id":"''","status_option_id":"''",
            "status_options":"'[]'","status_dirty":"0","status_pending_option_id":"''",
            "start_field_id":"''","iteration_field_id":"''","iteration_options":"'[]'",
            "assignee_field_id":"''","assignee_user_ids":"'[]'","assignee_logins":"'[]'","content_node_id":"''",
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
                ended_at TEXT,
                labels TEXT
            )
            """
        )
        try:
            cur.execute("ALTER TABLE work_sessions ADD COLUMN labels TEXT")
        except sqlite3.OperationalError:
            pass
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ws_task ON work_sessions(task_url)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ws_open ON work_sessions(ended_at)")
        # Detailed timer events log for later forensics/reports
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS timer_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_url TEXT NOT NULL,
                project_title TEXT,
                repo TEXT,
                labels TEXT,
                action TEXT NOT NULL, -- 'start' | 'stop'
                at TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_te_task_at ON timer_events(task_url, at)")
        self.conn.commit()

    def start_session(self, task_url: str, project_title: Optional[str] = None, repo: Optional[str] = None, labels_json: Optional[str] = None) -> None:
        if not task_url:
            return
        # Avoid duplicate open sessions for same task
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM work_sessions WHERE task_url=? AND ended_at IS NULL LIMIT 1", (task_url,))
        if cur.fetchone():
            return
        now = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        cur.execute(
            "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
            (task_url, project_title, now, None, labels_json or "[]"),
        )
        cur.execute(
            "INSERT INTO timer_events(task_url, project_title, repo, labels, action, at) VALUES (?,?,?,?,?,?)",
            (task_url, project_title, repo, labels_json or "[]", 'start', now),
        )
        self.conn.commit()

    def stop_session(self, task_url: str, project_title: Optional[str] = None, repo: Optional[str] = None, labels_json: Optional[str] = None) -> None:
        if not task_url:
            return
        now = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE work_sessions SET ended_at=?, labels=? WHERE task_url=? AND ended_at IS NULL",
            (now, labels_json or "[]", task_url),
        )
        cur.execute(
            "INSERT INTO timer_events(task_url, project_title, repo, labels, action, at) VALUES (?,?,?,?,?,?)",
            (task_url, project_title, repo, labels_json or "[]", 'stop', now),
        )
        self.conn.commit()

    def log_timer_event(self, task_url: str, project_title: Optional[str], repo: Optional[str], labels_json: Optional[str], action: str, at_ts: Optional[str] = None) -> None:
        at_ts = at_ts or dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO timer_events(task_url, project_title, repo, labels, action, at) VALUES (?,?,?,?,?,?)",
            (task_url, project_title, repo, labels_json or "[]", action, at_ts),
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

    def get_sessions_for_task(self, task_url: str) -> List[Dict[str, object]]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT id, project_title, started_at, ended_at, labels
            FROM work_sessions
            WHERE task_url=?
            ORDER BY started_at DESC, id DESC
            """,
            (task_url,),
        )
        rows = []
        for sid, proj, started_at, ended_at, labels in cur.fetchall():
            rows.append({
                'id': sid,
                'project_title': proj,
                'started_at': started_at,
                'ended_at': ended_at,
                'labels': labels,
            })
        return rows

    def update_session_times(self, session_id: int, *, started_at: object = _UNSET, ended_at: object = _UNSET) -> None:
        if started_at is _UNSET and ended_at is _UNSET:
            return
        cur = self.conn.cursor()
        fields: List[str] = []
        params: List[object] = []
        if started_at is not _UNSET:
            fields.append("started_at=?")
            params.append(started_at)
        if ended_at is not _UNSET:
            fields.append("ended_at=?")
            params.append(ended_at)
        params.append(session_id)
        cur.execute(f"UPDATE work_sessions SET {', '.join(fields)} WHERE id=?", params)
        cur.execute("SELECT task_url, project_title FROM work_sessions WHERE id=?", (session_id,))
        row = cur.fetchone()
        if row:
            task_url, project_title = row
        else:
            task_url, project_title = '', None
        self.conn.commit()
        if task_url:
            try:
                self.log_timer_event(task_url, project_title, None, "[]", 'edit')
            except Exception:
                pass

    def delete_session(self, session_id: int) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT task_url, project_title FROM work_sessions WHERE id=?", (session_id,))
        row = cur.fetchone()
        cur.execute("DELETE FROM work_sessions WHERE id=?", (session_id,))
        self.conn.commit()
        if row:
            task_url, project_title = row
            try:
                self.log_timer_event(task_url, project_title, None, "[]", 'delete')
            except Exception:
                pass

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

    def task_duration_snapshot(self, task_urls: Iterable[str]) -> Dict[str, Dict[str, int]]:
        ordered: List[str] = []
        seen: Set[str] = set()
        for url in task_urls:
            if not url:
                continue
            if url in seen:
                continue
            seen.add(url)
            ordered.append(url)
        if not ordered:
            return {}
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        totals: Dict[str, int] = {url: 0 for url in ordered}
        running: Dict[str, Tuple[int, dt.datetime]] = {}
        cur = self.conn.cursor()
        chunk_size = 200
        for idx in range(0, len(ordered), chunk_size):
            subset = ordered[idx:idx + chunk_size]
            placeholders = ",".join(["?"] * len(subset))
            cur.execute(
                f"SELECT id, task_url, started_at, ended_at "
                f"FROM work_sessions WHERE task_url IN ({placeholders}) ORDER BY id",
                subset,
            )
            for sid, url, started_at, ended_at in cur.fetchall():
                if not url:
                    continue
                st = self._parse_iso(started_at)
                if not st:
                    continue
                en = self._parse_iso(ended_at) if ended_at else None
                if en is None:
                    en = now
                    prev = running.get(url)
                    if (prev is None) or (sid > prev[0]):
                        running[url] = (sid, st)
                totals[url] = totals.get(url, 0) + max(0, int((en - st).total_seconds()))
        result: Dict[str, Dict[str, int]] = {}
        for url in ordered:
            cur_entry = running.get(url)
            current_secs = 0
            if cur_entry:
                current_secs = max(0, int((now - cur_entry[1]).total_seconds()))
            result[url] = {
                'current': current_secs,
                'total': totals.get(url, 0),
            }
        return result

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

    def recent_repositories(self, limit: int = 20) -> List[Tuple[str, str]]:
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                SELECT repo, repo_id, MAX(updated_at) as u
                FROM tasks
                WHERE repo IS NOT NULL AND repo<>'' AND repo_id IS NOT NULL AND repo_id<>''
                GROUP BY repo, repo_id
                ORDER BY u DESC
                LIMIT ?
                """,
                (limit,)
            )
            return [(row[0], row[1]) for row in cur.fetchall() if row[0] and row[1]]
        except Exception:
            return []

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

    def aggregate_label_totals(self, since_days: Optional[int] = None,
                               project_title: Optional[str] = None,
                               task_url: Optional[str] = None) -> Dict[str, int]:
        cur = self.conn.cursor()
        query = "SELECT task_url, project_title, labels, started_at, ended_at FROM work_sessions"
        params: List[object] = []
        conditions: List[str] = []
        if project_title:
            conditions.append("project_title=?")
            params.append(project_title)
        if task_url:
            conditions.append("task_url=?")
            params.append(task_url)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        cur.execute(query, params)
        rows = cur.fetchall()
        now = dt.datetime.now(dt.timezone.utc).astimezone()
        since_dt = (now - dt.timedelta(days=since_days)) if since_days else None
        out: Dict[str, int] = {}
        for _, _, labels_json, st_s, en_s in rows:
            st = self._parse_iso(st_s)
            en = self._parse_iso(en_s) if en_s else None
            if not st:
                continue
            if en is None:
                en = now
            st, en, keep = self._clip_range(st, en, since_dt)
            if not keep or st >= en:
                continue
            duration = int((en - st).total_seconds())
            try:
                data = json.loads(labels_json or "[]")
                names = [str(x) for x in data if isinstance(x, str) and x]
            except Exception:
                names = []
            if not names:
                names = ['(no label)']
            for name in set(names):
                out[name] = out.get(name, 0) + duration
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

    def mark_status_pending(self, url: str, status_text: str, option_id: str, is_done: int) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET status=?, is_done=?, status_dirty=1, status_pending_option_id=?, status_option_id=? WHERE url=?",
            (status_text, int(is_done), option_id or '', option_id or '', url),
        )
        self.conn.commit()

    def mark_status_synced(self, url: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET status_dirty=0, status_pending_option_id='' WHERE url=?",
            (url,),
        )
        self.conn.commit()

    def reset_status(self, url: str, status_text: str, option_id: str, is_done: int) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET status=?, is_done=?, status_option_id=?, status_dirty=0, status_pending_option_id='' WHERE url=?",
            (status_text, int(is_done), option_id or '', url),
        )
        self.conn.commit()

    def update_status_options(self, url: str, options: List[Dict[str, object]]) -> None:
        try:
            payload = json.dumps(options or [], ensure_ascii=False)
        except Exception:
            payload = "[]"
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET status_options=? WHERE url=?",
            (payload, url),
        )
        self.conn.commit()

    def update_status_options_by_field(self, field_id: str, options: List[Dict[str, object]]) -> None:
        if not field_id:
            return
        try:
            payload = json.dumps(options or [], ensure_ascii=False)
        except Exception:
            payload = "[]"
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET status_options=? WHERE status_field_id=?",
            (payload, field_id),
        )
        self.conn.commit()

    def update_start_date(self, url: str, start_date: str, field_id: Optional[str] = None) -> None:
        cur = self.conn.cursor()
        if field_id is not None and field_id:
            cur.execute(
                "UPDATE tasks SET start_date=?, start_field_id=? WHERE url=?",
                (start_date, field_id, url),
            )
        else:
            cur.execute(
                "UPDATE tasks SET start_date=? WHERE url=?",
                (start_date, url),
            )
        self.conn.commit()

    def update_focus_date(self, url: str, focus_date: str, field_id: Optional[str] = None) -> None:
        cur = self.conn.cursor()
        if field_id is not None and field_id:
            cur.execute(
                "UPDATE tasks SET focus_date=?, focus_field_id=? WHERE url=?",
                (focus_date, field_id, url),
            )
        else:
            cur.execute(
                "UPDATE tasks SET focus_date=? WHERE url=?",
                (focus_date, url),
            )
        self.conn.commit()

    def update_assignees(self, url: str, user_ids: List[str], logins: List[str]) -> None:
        try:
            user_ids_json = json.dumps(user_ids or [], ensure_ascii=False)
        except Exception:
            user_ids_json = "[]"
        try:
            logins_json = json.dumps(logins or [], ensure_ascii=False)
        except Exception:
            logins_json = "[]"
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET assignee_user_ids=?, assignee_logins=? WHERE url=?",
            (user_ids_json, logins_json, url),
        )
        self.conn.commit()

    def update_labels(self, url: str, labels: List[str]) -> None:
        try:
            payload = json.dumps(labels or [], ensure_ascii=False)
        except Exception:
            payload = "[]"
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET labels=? WHERE url=?",
            (payload, url),
        )
        self.conn.commit()

    def mark_priority_pending(self, url: str, priority_text: str, option_id: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET priority=?, priority_dirty=1, priority_pending_option_id=?, priority_option_id=? WHERE url=?",
            (priority_text, option_id or '', option_id or '', url),
        )
        self.conn.commit()

    def mark_priority_synced(self, url: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET priority_dirty=0, priority_pending_option_id='' WHERE url=?",
            (url,),
        )
        self.conn.commit()

    def reset_priority(self, url: str, priority_text: str, option_id: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET priority=?, priority_option_id=?, priority_dirty=0, priority_pending_option_id='' WHERE url=?",
            (priority_text, option_id or '', url),
        )
        self.conn.commit()

    def update_priority_options(self, url: str, options: List[Dict[str, object]]) -> None:
        try:
            payload = json.dumps(options or [], ensure_ascii=False)
        except Exception:
            payload = "[]"
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET priority_options=? WHERE url=?",
            (payload, url),
        )
        self.conn.commit()

    def update_priority_options_by_field(self, field_id: str, options: List[Dict[str, object]]) -> None:
        if not field_id:
            return
        try:
            payload = json.dumps(options or [], ensure_ascii=False)
        except Exception:
            payload = "[]"
        cur = self.conn.cursor()
        cur.execute(
            "UPDATE tasks SET priority_options=? WHERE priority_field_id=?",
            (payload, field_id),
        )
        self.conn.commit()

    def upsert_many(self, rows: List[TaskRow], *, commit: bool = True):
        if not rows:
            return
        cur = self.conn.cursor()
        cur.executemany(
            """            INSERT INTO tasks (
              owner_type, owner, project_number, project_title,
              start_field, start_date,
              focus_field, focus_date, focus_field_id,
              iteration_field, iteration_title, iteration_start, iteration_duration,
              title, repo_id, repo, labels, priority, priority_field_id, priority_option_id, priority_options, priority_dirty, priority_pending_option_id,
              url, updated_at, status, is_done, assigned_to_me, created_by_me,
              item_id, project_id, status_field_id, status_option_id, status_options, status_dirty, status_pending_option_id,
              start_field_id, iteration_field_id, iteration_options, assignee_field_id, assignee_user_ids, assignee_logins, content_node_id
            ) VALUES (
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
              ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(owner_type, owner, project_number, title, url, start_field, start_date)
            DO UPDATE SET project_title=excluded.project_title,
                          repo=excluded.repo,
                          updated_at=excluded.updated_at,
                          focus_field_id=excluded.focus_field_id,
                          priority=excluded.priority,
                          priority_field_id=excluded.priority_field_id,
                          priority_option_id=excluded.priority_option_id,
                          priority_options=excluded.priority_options,
                          priority_dirty=excluded.priority_dirty,
                          priority_pending_option_id=excluded.priority_pending_option_id,
                          status=excluded.status,
                          is_done=excluded.is_done,
                          iteration_field=excluded.iteration_field,
                          iteration_title=excluded.iteration_title,
                          iteration_start=excluded.iteration_start,
                          iteration_duration=excluded.iteration_duration,
                          labels=excluded.labels,
                          assigned_to_me=excluded.assigned_to_me,
                          created_by_me=excluded.created_by_me,
                          item_id=excluded.item_id,
                          project_id=excluded.project_id,
                          status_field_id=excluded.status_field_id,
                          status_option_id=excluded.status_option_id,
                          status_options=excluded.status_options,
                          status_dirty=excluded.status_dirty,
                          status_pending_option_id=excluded.status_pending_option_id,
                          start_field_id=excluded.start_field_id,
                          iteration_field_id=excluded.iteration_field_id,
                          iteration_options=excluded.iteration_options,
                          assignee_field_id=excluded.assignee_field_id,
                          assignee_user_ids=excluded.assignee_user_ids,
                          assignee_logins=excluded.assignee_logins,
                          content_node_id=excluded.content_node_id
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
                    r.focus_field_id,
                    r.iteration_field,
                    r.iteration_title,
                    r.iteration_start,
                    r.iteration_duration,
                    r.title,
                    r.repo_id,
                    r.repo,
                    r.labels,
                    r.priority,
                    r.priority_field_id,
                    r.priority_option_id,
                    r.priority_options,
                    int(getattr(r, 'priority_dirty', 0)),
                    getattr(r, 'priority_pending_option_id', ''),
                    r.url,
                    r.updated_at,
                    r.status,
                    r.is_done,
                    r.assigned_to_me,
                    r.created_by_me,
                    r.item_id,
                    r.project_id,
                    r.status_field_id,
                    r.status_option_id,
                    r.status_options,
                    int(r.status_dirty),
                    r.status_pending_option_id,
                    r.start_field_id,
                    r.iteration_field_id,
                    r.iteration_options,
                    r.assignee_field_id,
                    r.assignee_user_ids,
                    r.assignee_logins,
                    r.content_node_id,
                )
                for r in rows
            ],
        )
        if commit:
            self.conn.commit()

    def replace_all(self, rows: List[TaskRow]):
        """Replace all existing tasks with new list (ensures deletions reflected)."""
        cur = self.conn.cursor()
        try:
            cur.execute("BEGIN")
            cur.execute("DELETE FROM tasks")
            if rows:
                self.upsert_many(rows, commit=False)
            cur.execute("COMMIT")
        except Exception:
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            raise

    def load(self, today_only=False, today: Optional[str]=None) -> List[TaskRow]:
        cur = self.conn.cursor()
        if today_only:
            today = today or dt.date.today().isoformat()
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,focus_field,focus_date,focus_field_id,
                       iteration_field,iteration_title,iteration_start,iteration_duration,
                       title,repo_id,repo,labels,priority,priority_field_id,priority_option_id,priority_options,priority_dirty,priority_pending_option_id,
                       url,updated_at,status,is_done,assigned_to_me,created_by_me,
                       item_id,project_id,status_field_id,status_option_id,status_options,status_dirty,status_pending_option_id,
                       start_field_id,iteration_field_id,iteration_options,assignee_field_id,assignee_user_ids,assignee_logins,content_node_id
                FROM tasks WHERE focus_date = ?
                ORDER BY project_title, focus_date, repo, title
                """,
                (today,),
            )
        else:
            cur.execute(
                """                SELECT owner_type,owner,project_number,project_title,start_field,
                       start_date,focus_field,focus_date,focus_field_id,
                       iteration_field,iteration_title,iteration_start,iteration_duration,
                       title,repo_id,repo,labels,priority,priority_field_id,priority_option_id,priority_options,priority_dirty,priority_pending_option_id,
                       url,updated_at,status,is_done,assigned_to_me,created_by_me,
                       item_id,project_id,status_field_id,status_option_id,status_options,status_dirty,status_pending_option_id,
                       start_field_id,iteration_field_id,iteration_options,assignee_field_id,assignee_user_ids,assignee_logins,content_node_id
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
      nodes { id number title url closed }
    }
  }
}
"""
GQL_LIST_USER_PROJECTS = """query($login:String!) {
  user(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { id number title url closed }
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
          id
          content{
            __typename
            ... on DraftIssue {
              title
              creator { login }
            }
            ... on Issue {
              title url repository{ id nameWithOwner }
              assignees(first:50){ nodes{ id login } }
              author { login }
              labels(first:50){ nodes{ name color } }
            }
            ... on PullRequest {
              title url repository{ id nameWithOwner }
              assignees(first:50){ nodes{ id login } }
              author { login }
              labels(first:50){ nodes{ name color } }
            }
          }
                    fieldValues(first:50){
                        nodes{
                            __typename
                            ... on ProjectV2ItemFieldDateValue {
                                date
                                field { ... on ProjectV2FieldCommon { id name } }
                            }
                            ... on ProjectV2ItemFieldUserValue {
                                users(first:50){ nodes{ id login } }
                                field { ... on ProjectV2FieldCommon { id name } }
                            }
                            ... on ProjectV2ItemFieldSingleSelectValue {
                                name
                                optionId
                                field {
                                  ... on ProjectV2FieldCommon { id name }
                                  ... on ProjectV2SingleSelectField {
                                    id
                                    name
                                    options { id name }
                                  }
                                }
                            }
                            ... on ProjectV2ItemFieldIterationValue {
                                title
                                startDate
                                duration
                                iterationId
                                field {
                                  ... on ProjectV2FieldCommon { id name }
                                  ... on ProjectV2IterationField {
                                    id
                                    name
                                    configuration { iterations { id title startDate duration } }
                                  }
                                }
                            }
                        }
                    }
          project{ title url id }
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
          id
          content{
            __typename
            ... on DraftIssue {
              title
              creator { login }
            }
            ... on Issue {
              title url repository{ id nameWithOwner }
              assignees(first:50){ nodes{ id login } }
              author { login }
              labels(first:50){ nodes{ name color } }
            }
            ... on PullRequest {
              title url repository{ id nameWithOwner }
              assignees(first:50){ nodes{ id login } }
              author { login }
              labels(first:50){ nodes{ name color } }
            }
          }
                    fieldValues(first:50){
                        nodes{
                            __typename
                            ... on ProjectV2ItemFieldDateValue {
                                date
                                field { ... on ProjectV2FieldCommon { id name } }
                            }
                            ... on ProjectV2ItemFieldUserValue {
                                users(first:50){ nodes{ id login } }
                                field { ... on ProjectV2FieldCommon { id name } }
                            }
                            ... on ProjectV2ItemFieldSingleSelectValue {
                                name
                                optionId
                                field {
                                  ... on ProjectV2FieldCommon { id name }
                                  ... on ProjectV2SingleSelectField {
                                    id
                                    name
                                    options { id name }
                                  }
                                }
                            }
                            ... on ProjectV2ItemFieldIterationValue {
                                title
                                startDate
                                duration
                                iterationId
                                field {
                                  ... on ProjectV2FieldCommon { id name }
                                  ... on ProjectV2IterationField {
                                    id
                                    name
                                    configuration { iterations { id title startDate duration } }
                                  }
                                }
                            }
                        }
                    }
          project{ title url id }
        }
      }
    }
  }
}
"""

GQL_MUTATION_SET_STATUS = """mutation($projectId:ID!, $itemId:ID!, $fieldId:ID!, $optionId:String!) {
  updateProjectV2ItemFieldValue(
    input:{
      projectId:$projectId,
      itemId:$itemId,
      fieldId:$fieldId,
      value:{singleSelectOptionId:$optionId}
    }
  ){
    projectV2Item{ id }
  }
}
"""

GQL_MUTATION_CREATE_DRAFT = """mutation($projectId:ID!, $title:String!, $body:String) {
  addProjectV2DraftIssue(input:{projectId:$projectId, title:$title, body:$body}){
    projectItem{ id }
  }
}
"""

GQL_MUTATION_SET_DATE = """mutation($projectId:ID!, $itemId:ID!, $fieldId:ID!, $date:Date!) {
  updateProjectV2ItemFieldValue(
    input:{
      projectId:$projectId,
      itemId:$itemId,
      fieldId:$fieldId,
      value:{date:$date}
    }
  ){
    projectV2Item{ id }
  }
}
"""

GQL_MUTATION_SET_ITERATION = """mutation($projectId:ID!, $itemId:ID!, $fieldId:ID!, $iterationId:ID!) {
  updateProjectV2ItemFieldValue(
    input:{
      projectId:$projectId,
      itemId:$itemId,
      fieldId:$fieldId,
      value:{iterationId:$iterationId}
    }
  ){
    projectV2Item{ id }
  }
}
"""

GQL_MUTATION_SET_USERS_USERIDS = """mutation($projectId:ID!, $itemId:ID!, $fieldId:ID!, $userIds:[ID!]!) {
  updateProjectV2ItemFieldValue(
    input:{
      projectId:$projectId,
      itemId:$itemId,
      fieldId:$fieldId,
      value:{userIds:$userIds}
    }
  ){
    projectV2Item{ id }
  }
}
"""

GQL_MUTATION_SET_USERS_TEMPLATE = """mutation($projectId:ID!, $itemId:ID!, $fieldId:ID!) {
  updateProjectV2ItemFieldValue(
    input:{
      projectId:$projectId,
      itemId:$itemId,
      fieldId:$fieldId,
      value:{users:[__NODES__]}
    }
  ){
    projectV2Item{ id }
  }
}
"""

GQL_QUERY_USER_ID = """query($login:String!){ user(login:$login){ id login } }"""

GQL_MUTATION_CREATE_ISSUE = """mutation($repositoryId:ID!, $title:String!, $body:String, $assigneeIds:[ID!]) {
  createIssue(input:{repositoryId:$repositoryId, title:$title, body:$body, assigneeIds:$assigneeIds}){
    issue{ id url }
  }
}
"""

GQL_MUTATION_ADD_PROJECT_ITEM = """mutation($projectId:ID!, $contentId:ID!) {
  addProjectV2ItemById(input:{projectId:$projectId, contentId:$contentId}){
    item{ id }
  }
}
"""

GQL_QUERY_REPO = """query($owner:String!, $name:String!){ repository(owner:$owner, name:$name){ id nameWithOwner } }"""

GQL_PROJECT_FIELDS = """query($id:ID!){
  node(id:$id){
    ... on ProjectV2{
      fields(first:100){
        nodes{ ... on ProjectV2FieldCommon { id name } }
      }
    }
  }
}
"""

GQL_FIELD_OPTIONS = """query($id:ID!){
  node(id:$id){
    ... on ProjectV2SingleSelectField{
      name
      options { id name }
    }
  }
}
"""

def _session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {token}"
    s.headers["Accept"] = "application/vnd.github+json"
    return s


def _parse_issue_url(url: str) -> Optional[tuple[str,str,int]]:
    try:
        m = re.match(r"https?://github\.com/([^/]+)/([^/]+)/(issues|pull)/([0-9]+)", url or "")
        if not m:
            return None
        owner, name, _, num = m.groups()
        return owner, name, int(num)
    except Exception:
        return None

def fetch_labels_for_url(token: Optional[str], url: str) -> List[str]:
    if not token:
        return []
    parts = _parse_issue_url(url)
    if not parts:
        return []
    owner, name, number = parts
    try:
        headers = { 'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json' }
        import requests as _rq
        r = _rq.get(f'https://api.github.com/repos/{owner}/{name}/issues/{number}', headers=headers, timeout=20)
        if r.status_code != 200:
            return []
        data = r.json() or {}
        labels = data.get('labels') or []
        out = []
        for lab in labels:
            if isinstance(lab, dict):
                nm = lab.get('name')
                if nm:
                    out.append(str(nm))
        return out
    except Exception:
        try:
            logging.getLogger('gh_task_viewer').exception('fetch_labels_for_url failed')
        except Exception:
            pass
        return []


def list_repo_labels(token: Optional[str], full_name: str, max_pages: int = 5) -> List[Dict[str, str]]:
    if not token or not full_name or '/' not in full_name:
        return []
    owner, name = full_name.split('/', 1)
    session = _session(token)
    url = f"https://api.github.com/repos/{owner}/{name}/labels"
    params = {"per_page": 100}
    labels: List[Dict[str, str]] = []
    pages = 0
    while url and pages < max_pages:
        try:
            resp = session.get(url, params=params if pages == 0 else None, timeout=30)
        except Exception as exc:
            try:
                logging.getLogger('gh_task_viewer').warning('list_repo_labels request failed: %s', exc)
            except Exception:
                pass
            break
        if resp.status_code >= 300:
            try:
                logging.getLogger('gh_task_viewer').warning('list_repo_labels HTTP %s: %s', resp.status_code, resp.text[:200])
            except Exception:
                pass
            break
        data = resp.json() or []
        for item in data:
            if not isinstance(item, dict):
                continue
            name_val = (item.get('name') or '').strip()
            if not name_val:
                continue
            labels.append({
                'name': name_val,
                'description': item.get('description') or '',
                'color': item.get('color') or '',
            })
        next_link = (resp.links or {}).get('next', {})
        url = next_link.get('url')
        pages += 1
    return labels


def list_repo_assignees(token: Optional[str], full_name: str, max_pages: int = 5) -> List[Dict[str, str]]:
    if not token or not full_name or '/' not in full_name:
        return []
    owner, name = full_name.split('/', 1)
    session = _session(token)
    url = f"https://api.github.com/repos/{owner}/{name}/assignees"
    params = {"per_page": 100}
    users: List[Dict[str, str]] = []
    pages = 0
    while url and pages < max_pages:
        try:
            resp = session.get(url, params=params if pages == 0 else None, timeout=30)
        except Exception as exc:
            try:
                logging.getLogger('gh_task_viewer').warning('list_repo_assignees request failed: %s', exc)
            except Exception:
                pass
            break
        if resp.status_code >= 300:
            try:
                logging.getLogger('gh_task_viewer').warning('list_repo_assignees HTTP %s: %s', resp.status_code, resp.text[:200])
            except Exception:
                pass
            break
        data = resp.json() or []
        for item in data:
            if not isinstance(item, dict):
                continue
            login_val = (item.get('login') or '').strip()
            if not login_val:
                continue
            users.append({
                'login': login_val,
                'name': item.get('name') or '',
                'id': item.get('id'),
            })
        next_link = (resp.links or {}).get('next', {})
        url = next_link.get('url')
        pages += 1
    return users

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


def set_project_status(token: str, project_id: str, item_id: str, field_id: str, option_id: str) -> None:
    if not token:
        raise RuntimeError("Cannot update status without GITHUB_TOKEN")
    if not (project_id and item_id and field_id and option_id):
        raise RuntimeError("Status update missing required identifiers")
    session = _session(token)
    variables = {
        "projectId": project_id,
        "itemId": item_id,
        "fieldId": field_id,
        "optionId": option_id,
    }
    resp = _graphql_with_backoff(session, GQL_MUTATION_SET_STATUS, variables)
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Status update failed: " + "; ".join(e.get("message", str(e)) for e in errs))


def set_project_priority(token: str, project_id: str, item_id: str, field_id: str, option_id: str) -> None:
    if not token:
        raise RuntimeError("Cannot update priority without GITHUB_TOKEN")
    if not (project_id and item_id and field_id and option_id):
        raise RuntimeError("Priority update missing required identifiers")
    session = _session(token)
    variables = {
        "projectId": project_id,
        "itemId": item_id,
        "fieldId": field_id,
        "optionId": option_id,
    }
    resp = _graphql_with_backoff(session, GQL_MUTATION_SET_STATUS, variables)
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Priority update failed: " + "; ".join(e.get("message", str(e)) for e in errs))


def create_project_draft(token: str, project_id: str, title: str, body: str = "") -> str:
    if not token:
        raise RuntimeError("Cannot create task without GITHUB_TOKEN")
    if not (project_id and title.strip()):
        raise RuntimeError("Project ID and title are required to create a task")
    session = _session(token)
    variables = {"projectId": project_id, "title": title.strip(), "body": body or ""}
    resp = _graphql_with_backoff(session, GQL_MUTATION_CREATE_DRAFT, variables)
    errs = resp.get("errors") or []
    if errs:
        import logging
        try:
            logging.getLogger('gh_task_viewer').error("create_project_draft error: %s", errs)
        except Exception:
            pass
        raise RuntimeError("Create task failed: " + "; ".join(e.get("message", str(e)) for e in errs))
    try:
        return ((resp.get("data") or {}).get("addProjectV2DraftIssue") or {}).get("projectItem", {}).get("id") or ""
    except Exception:
        return ""


def set_project_date(token: str, project_id: str, item_id: str, field_id: str, date_val: str) -> None:
    if not (project_id and item_id and field_id and date_val):
        return
    session = _session(token)
    variables = {
        "projectId": project_id,
        "itemId": item_id,
        "fieldId": field_id,
        "date": date_val,
    }
    resp = _graphql_with_backoff(session, GQL_MUTATION_SET_DATE, variables)
    errs = resp.get("errors") or []
    if errs:
        import logging
        try:
            logging.getLogger('gh_task_viewer').error("set_project_date error: %s", errs)
        except Exception:
            pass
        raise RuntimeError("Setting date failed: " + "; ".join(e.get("message", str(e)) for e in errs))


def set_project_iteration(token: str, project_id: str, item_id: str, field_id: str, iteration_id: str) -> None:
    if not (project_id and item_id and field_id and iteration_id):
        return
    session = _session(token)
    variables = {
        "projectId": project_id,
        "itemId": item_id,
        "fieldId": field_id,
        "iterationId": iteration_id,
    }
    resp = _graphql_with_backoff(session, GQL_MUTATION_SET_ITERATION, variables)
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Setting iteration failed: " + "; ".join(e.get("message", str(e)) for e in errs))


def set_project_users(token: str, project_id: str, item_id: str, field_id: str, user_ids: List[str]) -> None:
    if user_ids is None:
        user_ids = []
    session = _session(token)
    attempts: List[Tuple[str, Dict[str, object]]] = [
        (GQL_MUTATION_SET_USERS_USERIDS, {"userIds": [uid for uid in user_ids if uid]})
    ]

    filtered_ids = [uid for uid in user_ids if uid]
    if filtered_ids:
        def _escape(uid: str) -> str:
            return uid.replace('"', '\"')

        node_payload = ", ".join(f'{{userId:"{_escape(uid)}"}}' for uid in filtered_ids)
        if node_payload:
            query_nodes = GQL_MUTATION_SET_USERS_TEMPLATE.replace("__NODES__", node_payload)
            attempts.append((query_nodes, {}))

    last_error = ""
    for query, extra in attempts:
        variables = {
            "projectId": project_id,
            "itemId": item_id,
            "fieldId": field_id,
        }
        variables.update(extra)
        try:
            resp = _graphql_with_backoff(session, query, variables)
        except Exception as exc:
            last_error = str(exc)
            continue
        errs = resp.get("errors") or []
        if not errs:
            return
        last_error = "; ".join(e.get("message", str(e)) for e in errs)
    raise RuntimeError("Setting assignees failed: " + (last_error or "unknown error"))


def set_issue_labels(token: str, issue_url: str, labels: List[str]) -> None:
    parts = _parse_issue_url(issue_url)
    if not parts or token is None:
        raise RuntimeError("Issue URL required for labels")
    owner, repo, number = parts
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
    }
    import requests as _rq
    r = _rq.patch(
        f'https://api.github.com/repos/{owner}/{repo}/issues/{number}',
        headers=headers,
        json={'labels': labels},
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Label update failed ({r.status_code}): {r.text}")


def set_issue_assignees(token: str, issue_url: str, assignees: List[str]) -> None:
    parts = _parse_issue_url(issue_url)
    if not parts or token is None:
        raise RuntimeError("Issue URL required for assignees")
    owner, repo, number = parts
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
    }
    import requests as _rq
    r = _rq.patch(
        f'https://api.github.com/repos/{owner}/{repo}/issues/{number}',
        headers=headers,
        json={'assignees': assignees},
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Assignee update failed ({r.status_code}): {r.text}")


def add_issue_comment(token: str, issue_url: str, body: str) -> None:
    parts = _parse_issue_url(issue_url)
    if not parts or token is None:
        raise RuntimeError("Issue URL required for comment")
    owner, repo, number = parts
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github+json',
    }
    import requests as _rq
    r = _rq.post(
        f'https://api.github.com/repos/{owner}/{repo}/issues/{number}/comments',
        headers=headers,
        json={'body': body},
        timeout=30,
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Comment failed ({r.status_code}): {r.text}")


def get_user_node_id(token: str, login: str) -> str:
    login_key = (login or '').lower()
    if not login_key:
        return ""
    cached = USER_ID_CACHE.get(login_key)
    if cached:
        return cached
    session = _session(token)
    resp = _graphql_with_backoff(session, GQL_QUERY_USER_ID, {"login": login})
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Lookup user id failed: " + "; ".join(e.get("message", str(e)) for e in errs))
    try:
        user_id = ((resp.get("data") or {}).get("user") or {}).get("id") or ""
    except Exception:
        user_id = ""
    USER_ID_CACHE[login_key] = user_id
    return user_id


def create_issue(token: str, repository_id: str, title: str, body: str, assignee_ids: List[str]) -> Dict[str, str]:
    session = _session(token)
    variables = {
        "repositoryId": repository_id,
        "title": title,
        "body": body or "",
        "assigneeIds": assignee_ids or [],
    }
    resp = _graphql_with_backoff(session, GQL_MUTATION_CREATE_ISSUE, variables)
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Create issue failed: " + "; ".join(e.get("message", str(e)) for e in errs))
    issue = ((resp.get("data") or {}).get("createIssue") or {}).get("issue") or {}
    issue_id = issue.get("id")
    if not issue_id:
        raise RuntimeError('Create issue succeeded but returned no id')
    return {"issue_id": issue_id, "url": issue.get("url")}


def add_project_item(token: str, project_id: str, content_id: str) -> str:
    if not content_id:
        raise RuntimeError('Issue creation did not return id')
    if not token:
        raise RuntimeError('GITHUB_TOKEN required')
    session = _session(token)
    variables = {"projectId": project_id, "contentId": content_id}
    resp = _graphql_with_backoff(session, GQL_MUTATION_ADD_PROJECT_ITEM, variables)
    errs = resp.get("errors") or []
    if errs:
        try:
            logging.getLogger('gh_task_viewer').error("add_project_item error: %s", errs)
        except Exception:
            pass
        raise RuntimeError("Add issue to project failed: " + "; ".join(e.get("message", str(e)) for e in errs))
    item = ((resp.get("data") or {}).get("addProjectV2ItemById") or {}).get("item") or {}
    return item.get("id") or ""


def get_repo_id(token: str, full_name: str) -> Dict[str, str]:
    if '/' not in (full_name or ''):
        raise RuntimeError('Repository must be in owner/name format')
    owner, name = full_name.split('/', 1)
    session = _session(token)
    variables = {"owner": owner.strip(), "name": name.strip()}
    resp = _graphql_with_backoff(session, GQL_QUERY_REPO, variables)
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Lookup repository failed: " + "; ".join(e.get("message", str(e)) for e in errs))
    repo = ((resp.get("data") or {}).get("repository")) or {}
    repo_id = repo.get("id")
    if not repo_id:
        raise RuntimeError('Repository not found')
    return {"repo_id": repo_id, "repo": repo.get("nameWithOwner") or full_name.strip()}

def discover_open_projects(session: requests.Session, owner_type: str, owner: str) -> List[Dict]:
    if owner_type == "org":
        resp = _graphql_with_backoff(session, GQL_LIST_ORG_PROJECTS, {"login": owner})
        errs = resp.get("errors") or []
        if errs:
            raise RuntimeError(
                f"Project discovery failed for org:{owner}: " + "; ".join(e.get("message", str(e)) for e in errs)
            )
        nodes = (((resp.get("data") or {}).get("organization") or {}).get("projectsV2") or {}).get("nodes") or []
    else:
        resp = _graphql_with_backoff(session, GQL_LIST_USER_PROJECTS, {"login": owner})
        errs = resp.get("errors") or []
        if errs:
            raise RuntimeError(
                f"Project discovery failed for user:{owner}: " + "; ".join(e.get("message", str(e)) for e in errs)
            )
        nodes = (((resp.get("data") or {}).get("user") or {}).get("projectsV2") or {}).get("nodes") or []
    return [n for n in nodes if n is not None and isinstance(n, dict) and not n.get("closed")]

def get_project_field_id_by_name(token: str, project_id: str, name_lower: str) -> Optional[str]:
    if not project_id:
        return None
    session = _session(token)
    resp = _graphql_with_backoff(session, GQL_PROJECT_FIELDS, {"id": project_id})
    errs = resp.get("errors") or []
    if errs:
        return None
    node = (resp.get("data") or {}).get("node") or {}
    fields = ((node.get("fields") or {}).get("nodes")) or []
    target = (name_lower or '').strip().lower()
    for f in fields:
        nm = (f.get("name") or '').strip().lower()
        if nm == target:
            return f.get("id") or None
    return None


def get_project_field_options(token: str, field_id: str) -> List[Dict[str, str]]:
    if not (token and field_id):
        return []
    if field_id in STATUS_OPTION_CACHE:
        cached = STATUS_OPTION_CACHE.get(field_id) or []
        return [opt for opt in cached if isinstance(opt, dict)]
    session = _session(token)
    resp = _graphql_with_backoff(session, GQL_FIELD_OPTIONS, {"id": field_id})
    errs = resp.get("errors") or []
    if errs:
        raise RuntimeError("Fetch status options failed: " + "; ".join(e.get("message", str(e)) for e in errs))
    node = (resp.get("data") or {}).get("node") or {}
    options_raw = node.get("options") or []
    out: List[Dict[str, str]] = []
    for opt in options_raw:
        if not isinstance(opt, dict):
            continue
        opt_id = (opt.get("id") or "").strip()
        opt_name = (opt.get("name") or "").strip()
        if opt_id and opt_name:
            out.append({"id": opt_id, "name": opt_name})
    STATUS_OPTION_CACHE[field_id] = out
    return out


# -----------------------------
# Fetch with progress callback
# -----------------------------
ProgressCB = Callable[[int, int, str], None]  # (done, total, status_line)

def _ascii_bar(done:int, total:int, width:int=40)->str:
    pct = 0 if total<=0 else int(done*100/total)
    fill = int(width*pct/100)
    return f"[{'#'*fill}{'.'*(width-fill)}] {pct:3d}%"


STATUS_FIELD_HINTS = ("status", "state", "progress", "stage", "column")
PRIORITY_FIELD_HINTS = ("priority", "prio")


def _looks_like_status_field(name: Optional[str]) -> bool:
    if not name:
        return False
    norm = name.strip().lower()
    return bool(norm) and any(hint in norm for hint in STATUS_FIELD_HINTS)


def _looks_like_priority_field(name: Optional[str]) -> bool:
    if not name:
        return False
    norm = name.strip().lower()
    return bool(norm) and any(hint in norm for hint in PRIORITY_FIELD_HINTS)


def _status_field_priority(name: Optional[str]) -> int:
    norm = (name or "").strip().lower()
    if 'status' in norm:
        return 0
    if 'state' in norm:
        return 1
    if 'progress' in norm:
        return 2
    if 'stage' in norm:
        return 3
    if 'column' in norm:
        return 4
    return 10


class _ParallelProgress:
    """Thread-safe progress reporter shared across project fetch workers."""

    def __init__(self, total: int, progress_cb: Optional[ProgressCB]):
        self._total = max(0, total)
        self._cb = progress_cb
        self._lock = threading.Lock()
        self._done = 0
        self._message = ""

    def set_message(self, message: str) -> None:
        if not self._cb:
            return
        with self._lock:
            self._message = message or ""
            self._emit()

    def advance(self, message: Optional[str] = None) -> None:
        if not self._cb:
            return
        with self._lock:
            self._done = min(self._total, self._done + 1)
            if message:
                self._message = message
            self._emit()

    def complete(self, message: str) -> None:
        if not self._cb:
            return
        with self._lock:
            self._done = self._total
            self._message = message
            self._emit()

    def _emit(self) -> None:
        status = f"{_ascii_bar(self._done, self._total)}  {self._message}".rstrip()
        try:
            self._cb(self._done, self._total, status)
        except Exception:
            try:
                logging.getLogger('gh_task_viewer').debug('Progress callback failed', exc_info=True)
            except Exception:
                pass


@dataclass
class _ProjectFetchResult:
    rows: List[TaskRow]
    label: str
    rate_limited: bool = False
    message: str = ""


@dataclass
class FetchTasksResult:
    rows: List[TaskRow]
    partial: bool = False
    message: str = ""

def fetch_tasks_github(
    token: str,
    cfg: Config,
    date_cutoff: dt.date,
    include_unassigned: bool = False,
    progress: Optional[ProgressCB] = None,
) -> FetchTasksResult:
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

    target_cache = _load_target_cache()
    cache_updated = False
    targets: List[Tuple[str,str,int,str]] = []
    for spec in cfg.projects:
        cache_key = f"{spec.owner_type}:{spec.owner}"
        if spec.numbers is None:
            discovered: List[Dict[str, object]] = []
            projs: List[Dict[str, object]] = []
            try:
                projs = discover_open_projects(session, spec.owner_type, spec.owner)
            except Exception as err:
                try:
                    logging.getLogger('gh_task_viewer').warning("Project discovery failed: %s", err)
                except Exception:
                    pass
                projs = []
            for n in projs:
                num_val = n.get("number")
                try:
                    num_int = int(num_val) if num_val is not None else -1
                except (TypeError, ValueError):
                    continue
                title = n.get("title") or ""
                project_id_val = n.get("id") or ""
                targets.append((spec.owner_type, spec.owner, num_int, title))
                discovered.append({"number": num_int, "title": title, "project_id": project_id_val})
            if discovered:
                target_cache[cache_key] = discovered
                cache_updated = True
            elif cache_key in target_cache:
                cached_entries = target_cache.get(cache_key) or []
                if cached_entries:
                    try:
                        logging.getLogger('gh_task_viewer').warning(
                            "Project discovery empty for %s; using cached project list", cache_key)
                    except Exception:
                        pass
                    for entry in cached_entries:
                        try:
                            num_int = int(entry.get("number"))
                        except Exception:
                            continue
                        targets.append((spec.owner_type, spec.owner, num_int, entry.get("title") or ""))
                else:
                    target_cache.pop(cache_key, None)
                    cache_updated = True
        else:
            for num in spec.numbers:
                targets.append((spec.owner_type, spec.owner, int(num), ""))

    if cache_updated:
        _save_target_cache(target_cache)

    if not targets:
        raise RuntimeError("No project targets resolved; check config or specify project numbers")

    total = len(targets)
    tracker = _ParallelProgress(total, progress)
    try:
        logging.getLogger('gh_task_viewer').info("Fetching from %d project targets", total)
    except Exception:
        pass
    if total == 0:
        return []

    tracker.set_message("Queued project fetch")

    wait_cb: Optional[Callable[[str], None]] = tracker.set_message if progress else None

    def _scan_project(owner_type: str, owner: str, number: int, ptitle: str) -> _ProjectFetchResult:
        label = f"{owner_type}:{owner} #{number}"
        if ptitle:
            label = f"{label} — {ptitle}"
        local_rows: List[TaskRow] = []
        session_local = _session(token)
        after: Optional[str] = None
        tracker.set_message(f"Scanning {label}")
        while True:
            variables = (
                {"org": owner, "number": number, "after": after}
                if owner_type == "org"
                else {"login": owner, "number": number, "after": after}
            )
            query = GQL_SCAN_ORG if owner_type == "org" else GQL_SCAN_USER
            resp = _graphql_with_backoff(session_local, query, variables, on_wait=wait_cb)

            errs = resp.get("errors") or []
            if errs:
                nf = any((e.get("type") == "NOT_FOUND") and ("projectV2" in (e.get("path") or [])) for e in errs)
                if nf:
                    try:
                        logging.getLogger('gh_task_viewer').warning(
                            "Project not found or inaccessible: %s:%s #%s", owner_type, owner, number
                        )
                    except Exception:
                        pass
                    tracker.set_message(f"Project not found: {label}")
                    break
                rate_limited = any((e.get("type") == "RATE_LIMITED") for e in errs)
                if rate_limited:
                    msg = f"{label}: Rate limited; partial results"
                    tracker.set_message(msg)
                    try:
                        logging.getLogger('gh_task_viewer').warning("Rate limited during fetch; returning partial results")
                    except Exception:
                        pass
                    return _ProjectFetchResult(rows=local_rows, label=label, rate_limited=True, message=msg)
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
                item_id = it.get("id") or ""
                content = it.get("content") or {}
                ctype = content.get("__typename")
                title = content.get("title") or "(Draft item)"
                url = content.get("url") or it.get("project", {}).get("url") or ""
                project_info = it.get("project") or {}
                project_title = project_info.get("title") or ""
                project_id = project_info.get("id") or ""
                repo = None
                repo_id = ""
                if ctype in ("Issue", "PullRequest"):
                    rep = content.get("repository") or {}
                    repo = rep.get("nameWithOwner")
                    repo_id = rep.get("id") or ""

                label_names: List[str] = []
                if ctype in ("Issue", "PullRequest"):
                    for node in (content.get("labels") or {}).get("nodes") or []:
                        nm = (node or {}).get("name")
                        if nm:
                            label_names.append(str(nm))

                assignees_norm: List[str] = []
                if ctype in ("Issue", "PullRequest"):
                    for node in (content.get("assignees") or {}).get("nodes") or []:
                        login_norm = _norm_login((node or {}).get("login"))
                        if login_norm:
                            assignees_norm.append(login_norm)
                people_logins: List[str] = []
                assignee_field_id: str = ""
                assignee_user_ids: List[str] = []
                status_text: Optional[str] = None
                status_field_id: str = ""
                status_field_priority = 999
                status_option_id: str = ""
                status_options_list: List[Dict[str, str]] = []
                priority_text: Optional[str] = None
                priority_field_id: str = ""
                priority_option_id: str = ""
                priority_options_list: List[Dict[str, str]] = []
                author_login_norm: Optional[str] = None
                iteration_field: str = ""
                iteration_title: str = ""
                iteration_start: str = ""
                iteration_duration: int = 0
                iteration_captured = False
                iteration_field_id: str = ""
                iteration_options_list: List[Dict[str, object]] = []
                start_field_id: str = ""
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldUserValue":
                        field_data = fv.get("field") or {}
                        assignee_field_id = field_data.get("id") or assignee_field_id
                        for node in (fv.get("users") or {}).get("nodes") or []:
                            login_norm = _norm_login((node or {}).get("login"))
                            if login_norm:
                                people_logins.append(login_norm)
                            node_id = (node or {}).get("id")
                            if node_id:
                                assignee_user_ids.append(node_id)
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldSingleSelectValue":
                        field_data = fv.get("field") or {}
                        raw_name = field_data.get("name") or ""
                        fname_sel = raw_name.strip().lower()
                        option_id_val = fv.get("optionId") or ""
                        options_raw = field_data.get("options") or []
                        is_status_field = _looks_like_status_field(raw_name)
                        is_priority_field = _looks_like_priority_field(raw_name)
                        if is_status_field and options_raw:
                            new_opts = [
                                {"id": opt.get("id"), "name": opt.get("name")}
                                for opt in options_raw if opt and opt.get("id")
                            ]
                            if status_options_list:
                                seen_ids = {opt.get("id") for opt in status_options_list if isinstance(opt, dict)}
                                for opt in new_opts:
                                    if opt.get("id") not in seen_ids:
                                        status_options_list.append(opt)
                            else:
                                status_options_list = new_opts
                        if is_status_field:
                            priority = _status_field_priority(raw_name)
                            field_id_candidate = field_data.get("id") or ""
                            if (
                                priority < status_field_priority
                                or (field_id_candidate and field_id_candidate == status_field_id)
                            ):
                                status_field_id = field_id_candidate or status_field_id
                                status_field_priority = priority
                                status_text = (fv.get("name") or "").strip()
                                status_option_id = option_id_val
                        if is_priority_field and options_raw:
                            new_priority_opts = [
                                {"id": opt.get("id"), "name": opt.get("name")}
                                for opt in options_raw if opt and opt.get("id")
                            ]
                            if priority_options_list:
                                seen_ids = {opt.get("id") for opt in priority_options_list if isinstance(opt, dict)}
                                for opt in new_priority_opts:
                                    if opt.get("id") not in seen_ids:
                                        priority_options_list.append(opt)
                            else:
                                priority_options_list = new_priority_opts
                        if is_priority_field:
                            priority_field_id = field_data.get("id") or priority_field_id
                            priority_text = (fv.get("name") or "").strip()
                            priority_option_id = option_id_val
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldDateValue":
                        field_info = fv.get("field") or {}
                        start_field_id = field_info.get("id") or start_field_id
                    if (not iteration_captured) and fv and fv.get("__typename") == "ProjectV2ItemFieldIterationValue":
                        field_info = fv.get("field") or {}
                        fname_iter = (field_info.get("name") or "")
                        if (iter_regex is None) or iter_regex.search(fname_iter):
                            iteration_field = fname_iter
                            iteration_title = (fv.get("title") or "")
                            iteration_start = fv.get("startDate") or ""
                            iteration_field_id = field_info.get("id") or iteration_field_id
                            config = (field_info.get("configuration") or {}).get("iterations") or []
                            if config:
                                iteration_options_list = [
                                    {
                                        "id": it_conf.get("id"),
                                        "title": it_conf.get("title"),
                                        "startDate": it_conf.get("startDate"),
                                        "duration": it_conf.get("duration"),
                                    }
                                    for it_conf in config if it_conf and it_conf.get("id")
                                ]
                            try:
                                iteration_duration = int(fv.get("duration") or 0)
                            except (TypeError, ValueError):
                                iteration_duration = 0
                            iteration_captured = True
                if ctype == "DraftIssue":
                    author_login_norm = _norm_login(((content.get("creator") or {})).get("login"))
                elif ctype in ("Issue", "PullRequest"):
                    author_login_norm = _norm_login(((content.get("author") or {})).get("login"))
                assigned_to_me = (me_login in assignees_norm) or (me_login in people_logins)
                created_by_me = author_login_norm == me_login if author_login_norm else False
                if (not assigned_to_me) and (not created_by_me) and (not include_unassigned):
                    continue

                focus_fname: str = ""
                focus_fdate: str = ""
                focus_field_id_local: str = ""
                for fv in (it.get("fieldValues") or {}).get("nodes") or []:
                    if fv and fv.get("__typename") == "ProjectV2ItemFieldDateValue":
                        field_fd = fv.get("field") or {}
                        fname_fd = (field_fd.get("name") or "")
                        if fname_fd.strip().lower() == "focus day":
                            fdate_fd = fv.get("date")
                            if fdate_fd:
                                try:
                                    dt.date.fromisoformat(fdate_fd)
                                    focus_fname, focus_fdate = fname_fd, fdate_fd
                                    focus_field_id_local = field_fd.get("id") or focus_field_id_local
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
                            dt.date.fromisoformat(fdate)
                        except ValueError:
                            continue
                        done_flag = 0
                        if status_text:
                            low = status_text.lower()
                            if any(k in low for k in ("done", "complete", "closed", "merged", "finished", "✅", "✔")):
                                done_flag = 1
                        local_rows.append(
                            TaskRow(
                                owner_type=owner_type, owner=owner, project_number=number,
                                project_title=project_title,
                                start_field=fname, start_date=fdate,
                                focus_field=focus_fname or "",
                                focus_date=focus_fdate or "",
                                iteration_field=iteration_field,
                                iteration_title=iteration_title,
                                iteration_start=iteration_start,
                                iteration_duration=iteration_duration,
                                title=title, repo=repo,
                                labels=json.dumps(label_names, ensure_ascii=False),
                                priority=priority_text,
                                priority_field_id=priority_field_id,
                                priority_option_id=priority_option_id,
                                priority_options=json.dumps(priority_options_list, ensure_ascii=False),
                                url=url, updated_at=iso_now,
                                status=status_text, is_done=done_flag,
                                repo_id=repo_id,
                                assigned_to_me=int(assigned_to_me),
                                created_by_me=int(created_by_me),
                                item_id=item_id,
                                project_id=project_id,
                                status_field_id=status_field_id,
                                status_option_id=status_option_id,
                                status_options=json.dumps(status_options_list, ensure_ascii=False),
                                status_dirty=0,
                                status_pending_option_id="",
                                start_field_id=start_field_id,
                                focus_field_id=focus_field_id_local,
                                iteration_field_id=iteration_field_id,
                                iteration_options=json.dumps(iteration_options_list, ensure_ascii=False),
                                assignee_field_id=assignee_field_id,
                                assignee_user_ids=json.dumps(assignee_user_ids, ensure_ascii=False),
                            )
                        )
                        found_date = True
                if not found_date:
                    done_flag = 0
                    if status_text:
                        low = status_text.lower()
                        if any(k in low for k in ("done", "complete", "closed", "merged", "finished", "✅", "✔")):
                            done_flag = 1
                    local_rows.append(
                        TaskRow(
                            owner_type=owner_type, owner=owner, project_number=number,
                            project_title=project_title,
                            start_field="(no date)", start_date="",
                            focus_field=focus_fname or "",
                            focus_date=focus_fdate or "",
                            iteration_field=iteration_field,
                            iteration_title=iteration_title,
                            iteration_start=iteration_start,
                            iteration_duration=iteration_duration,
                            title=title + (" (unassigned)" if not assigned_to_me else ""),
                            repo=repo,
                            labels=json.dumps(label_names, ensure_ascii=False),
                            priority=priority_text,
                            priority_field_id=priority_field_id,
                            priority_option_id=priority_option_id,
                            priority_options=json.dumps(priority_options_list, ensure_ascii=False),
                            url=url, updated_at=iso_now,
                            status=status_text, is_done=done_flag,
                            repo_id=repo_id,
                            assigned_to_me=int(assigned_to_me),
                            created_by_me=int(created_by_me),
                            item_id=item_id,
                            project_id=project_id,
                            status_field_id=status_field_id,
                            status_option_id=status_option_id,
                            status_options=json.dumps(status_options_list, ensure_ascii=False),
                            status_dirty=0,
                            status_pending_option_id="",
                            start_field_id=start_field_id,
                            focus_field_id=focus_field_id_local,
                            iteration_field_id=iteration_field_id,
                            iteration_options=json.dumps(iteration_options_list, ensure_ascii=False),
                            assignee_field_id=assignee_field_id,
                            assignee_user_ids=json.dumps(assignee_user_ids, ensure_ascii=False),
                        )
                    )

            page = (proj_node.get("items") or {}).get("pageInfo") or {}
            if page.get("hasNextPage"):
                after = page.get("endCursor")
                tracker.set_message(f"Scanning {label} (next page)")
            else:
                break

        return _ProjectFetchResult(rows=local_rows, label=label)

    results_by_idx: Dict[int, List[TaskRow]] = {}
    rate_limited_triggered = False
    partial_message: str = ""

    workers = min(4, max(1, total))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(_scan_project, owner_type, owner, number, ptitle): idx
            for idx, (owner_type, owner, number, ptitle) in enumerate(targets)
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                tracker.set_message(f"Error: {exc}")
                raise
            results_by_idx[idx] = result.rows
            if result.rate_limited:
                rate_limited_triggered = True
                if not partial_message:
                    partial_message = result.message or "Rate limited; fetching incomplete"
                for other_future, other_idx in future_map.items():
                    if other_future is future:
                        continue
                    if other_future.done():
                        try:
                            other_result = other_future.result()
                        except Exception:
                            continue
                        results_by_idx[other_idx] = other_result.rows
                break
            tracker.advance(f"Finished {result.label}")

    out: List[TaskRow] = []
    for idx in range(len(targets)):
        rows = results_by_idx.get(idx)
        if rows:
            out.extend(rows)

    if not rate_limited_triggered and progress:
        tracker.complete("Done")

    if rate_limited_triggered and not partial_message:
        partial_message = "Rate limited; keeping existing cache"

    return FetchTasksResult(rows=out, partial=rate_limited_triggered, message=partial_message)


# -----------------------------
# UI helpers (fragments only)
# -----------------------------
def color_for_date(d: Optional[str], today: dt.date, palette: Optional[Dict[str, str]] = None) -> str:
    try:
        dd = dt.date.fromisoformat(d) if d else None
    except Exception:
        dd = None
    if dd is None:
        if palette:
            return palette.get('unknown', 'ansigray')
        return "ansigray"
    if dd == today:
        if palette:
            return palette.get('today', 'ansired bold')
        return "ansired bold"
    if dd < today:
        if palette:
            return palette.get('past', 'ansiyellow')
        return "ansiyellow"
    if palette:
        return palette.get('future', 'ansigreen')
    return "ansigreen"

def _char_width(ch: str) -> int:
    """Return printable cell width for a single character."""
    # Heuristic fallback based on Unicode metadata; keeps wide glyphs wide even if
    # prompt_toolkit is stubbed in tests.
    if unicodedata.combining(ch) or unicodedata.category(ch) == "Cf":
        fallback = 0
    else:
        fallback = 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1

    if _pt_get_cwidth is not None:
        try:
            width = int(_pt_get_cwidth(ch))
        except Exception:
            width = None
        else:
            if width <= 0:
                return fallback
            if fallback == 0:
                return 0
            return width if width > fallback else fallback

    return fallback


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
    if pad and raw and _char_width(raw[-1]) == 2:
        # Keep wide glyphs flush with the right edge so they don't trail spaces.
        return (" " * pad) + raw
    return raw + (" " * pad)

def build_fragments(tasks: List[TaskRow], today: dt.date) -> List[Tuple[str, str]]:
    """Return a list of (style, text) tuples for FormattedTextControl."""
    frags: List[Tuple[str, str]] = []
    if not tasks:
        return [("bold", "Nothing to show."), ("", " Press "), ("bold", "u"), ("", " to fetch.")]

    current: Optional[str] = None
    header = "Focus Day   Start Date   Status   Priority   Title                                     Repo                 URL"
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
        priority_cell = _pad_display((t.priority or '-') + ('*' if getattr(t, 'priority_dirty', 0) else ''), 10)
        title_cell = _pad_display(t.title, 45)
        repo_cell = _pad_display(t.repo or '-', 20)
        url_cell = _pad_display(t.url, 40)
        frags.append((col, focus_cell))
        frags.append(("",  "  "))
        frags.append(("", f"{start_cell}  {status_cell}  {priority_cell}  {title_cell}  {repo_cell}  {url_cell}"))
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
    # Sort preset index
    sort_index: int = 0
    current_index = 0
    v_offset = 0  # top row index currently displayed
    h_offset = 0
    detail_mode = False
    status_line = ""
    pending_status_urls: Set[str] = set()
    pending_priority_urls: Set[str] = set()
    edit_task_mode = False
    task_edit_state: Dict[str, object] = {}
    task_edit_float: Optional[Float] = None
    add_mode = False
    add_state: Dict[str, object] = {}
    add_float: Optional[Float] = None
    edit_sessions_mode = False
    session_state: Dict[str, object] = {}
    session_float: Optional[Float] = None
    update_in_progress = False
    task_duration_cache: Dict[str, Dict[str, int]] = {}
    SUMMARY_CACHE_TTL = 5.0  # seconds; avoid recomputing heavy aggregates on every repaint
    summary_cache: Dict[str, Dict[str, object]] = {
        'project': {'ts': 0.0, 'data': {}, 'tops': []},
        'label': {'ts': 0.0, 'data': {}, 'tops': []},
    }

    theme_dir = Path(__file__).resolve().parent / "themes"
    theme_presets = _load_theme_presets(theme_dir)
    if not theme_presets:
        theme_presets = [ThemePreset(name="Default", style=dict(BASE_THEME_STYLE), layout=DEFAULT_THEME_LAYOUT)]
    current_theme_index = 0
    current_layout_name = theme_presets[current_theme_index].layout or DEFAULT_THEME_LAYOUT
    style = Style.from_dict(theme_presets[current_theme_index].style)
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
            'sort_index': sort_index,
            'current_index': current_index,
            'v_offset': v_offset,
            'h_offset': h_offset,
            'theme_index': current_theme_index,
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
    saved_theme_idx = int(_st.get('theme_index', current_theme_index) or current_theme_index)
    if 0 <= saved_theme_idx < len(theme_presets):
        current_theme_index = saved_theme_idx
        current_layout_name = theme_presets[current_theme_index].layout or DEFAULT_THEME_LAYOUT
        style = Style.from_dict(theme_presets[current_theme_index].style)
    show_today_only = bool(_st.get('show_today_only', show_today_only))
    hide_done = bool(_st.get('hide_done', hide_done))
    hide_no_date = bool(_st.get('hide_no_date', hide_no_date))
    show_unassigned = bool(_st.get('show_unassigned', show_unassigned))
    include_created = bool(_st.get('include_created', include_created))
    use_iteration = bool(_st.get('use_iteration', use_iteration))
    project_cycle = _st.get('project_cycle', project_cycle)
    search_term = _st.get('search_term', search_term)
    date_max = _st.get('date_max', date_max)
    sort_index = int(_st.get('sort_index', sort_index) or 0)
    current_index = int(_st.get('current_index', current_index) or 0)
    v_offset = int(_st.get('v_offset', v_offset) or 0)
    h_offset = int(_st.get('h_offset', h_offset) or 0)

    def _style_value(name: str, default: str = '') -> str:
        style_map = theme_presets[current_theme_index].style
        if name in style_map:
            return style_map[name]
        if name in BASE_THEME_STYLE:
            return BASE_THEME_STYLE[name]
        return default

    def _style_class(name: str, fallback: Optional[str] = None) -> str:
        key = name if name in theme_presets[current_theme_index].style else fallback
        if key is None and name in BASE_THEME_STYLE:
            key = name
        if key is None:
            return ''
        return f"class:{key}"

    def load_all():
        return db.load(today_only=show_today_only, today=today_date.isoformat())

    all_rows = load_all()

    def _json_list(raw: str) -> List[Dict[str, object]]:
        if not raw:
            return []
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return data
        except Exception:
            pass
        return []

    def build_project_choices() -> List[Dict[str, object]]:
        meta: Dict[Tuple[str, str, int], Dict[str, object]] = {}
        for row in all_rows:
            key = (row.owner_type, row.owner, row.project_number)
            entry = meta.setdefault(key, {
                'owner_type': row.owner_type,
                'owner': row.owner,
                'project_number': row.project_number,
                'project_title': row.project_title or "",
                'project_id': row.project_id or "",
                'start_field_id': row.start_field_id or "",
                'start_field_name': row.start_field or "",
                'focus_field_id': row.focus_field_id or "",
                'focus_field_name': row.focus_field or "",
                'iteration_field_id': row.iteration_field_id or "",
                'iteration_options': _json_list(row.iteration_options),
                'priority_field_id': row.priority_field_id or "",
                'priority_options': _json_list(row.priority_options),
                'assignee_field_id': row.assignee_field_id or "",
                'repos': {}
            })
            if not entry['project_title'] and row.project_title:
                entry['project_title'] = row.project_title
            if not entry['project_id'] and row.project_id:
                entry['project_id'] = row.project_id
            if not entry['start_field_id'] and row.start_field_id:
                entry['start_field_id'] = row.start_field_id
                entry['start_field_name'] = row.start_field
            if not entry['focus_field_id'] and row.focus_field_id:
                entry['focus_field_id'] = row.focus_field_id
                entry['focus_field_name'] = row.focus_field or entry.get('focus_field_name', '')
            if (not entry['iteration_field_id']) and row.iteration_field_id:
                entry['iteration_field_id'] = row.iteration_field_id
            if not entry['iteration_options'] and row.iteration_options:
                entry['iteration_options'] = _json_list(row.iteration_options)
            if not entry['priority_field_id'] and row.priority_field_id:
                entry['priority_field_id'] = row.priority_field_id
            if not entry['priority_options'] and row.priority_options:
                entry['priority_options'] = _json_list(row.priority_options)
            if not entry['assignee_field_id'] and row.assignee_field_id:
                entry['assignee_field_id'] = row.assignee_field_id
            repo_key = (row.repo or '').strip()
            if repo_key:
                entry['repos'].setdefault(repo_key, row.repo_id or '')

        cache = _load_target_cache()
        for spec in cfg.projects:
            cache_entries = cache.get(f"{spec.owner_type}:{spec.owner}", []) or []
            for entry in cache_entries:
                try:
                    num_int = int(entry.get("number"))
                except Exception:
                    continue
                key = (spec.owner_type, spec.owner, num_int)
                existing = meta.setdefault(key, {
                    'owner_type': spec.owner_type,
                    'owner': spec.owner,
                    'project_number': num_int,
                    'project_title': entry.get("title") or "",
                    'project_id': entry.get("project_id") or "",
                    'start_field_id': "",
                    'start_field_name': "",
                    'focus_field_id': "",
                    'focus_field_name': "",
                    'iteration_field_id': "",
                    'iteration_options': [],
                    'priority_field_id': "",
                    'priority_options': [],
                    'assignee_field_id': entry.get('assignee_field_id') or "",
                    'repos': {}
                })
                if not existing['project_title'] and entry.get("title"):
                    existing['project_title'] = entry.get("title")
                if not existing['project_id'] and entry.get("project_id"):
                    existing['project_id'] = entry.get("project_id")

        choices = []
        for key, entry in meta.items():
            if not entry.get('project_id'):
                continue
            title = entry.get('project_title') or f"{entry['owner']}/#{entry['project_number']}"
            entry['project_title'] = title
            entry['iteration_options'] = entry.get('iteration_options') or []
            entry['repos'] = entry.get('repos') or {}
            choices.append(entry)
        choices.sort(key=lambda e: e.get('project_title', '').lower())
        return choices

    def _set_project_choices_for_mode(mode: str) -> None:
        all_choices = add_state.get('project_choices_all') or []
        if mode == 'issue':
            filtered = [c for c in all_choices if c.get('repos')]
            if not filtered:
                filtered = list(all_choices)
        else:
            filtered = list(all_choices)
        add_state['project_choices'] = filtered
        add_state['project_index'] = 0
        project = filtered[0] if filtered else None
        add_state['repo_choices'] = _build_repo_choices(project) if project else []
        add_state['repo_index'] = 0
        add_state['iteration_choices'] = _build_iteration_choices(project) if project else []
        add_state['iteration_index'] = 0
        add_state['repo_manual'] = ''
        add_state['priority_options'] = (project.get('priority_options') if project else []) or []
        _reset_repo_metadata_state()

    def _current_add_project() -> Optional[Dict[str, object]]:
        if not add_state.get('project_choices'):
            return None
        idx = add_state.get('project_index', 0)
        choices = add_state['project_choices']
        if not choices:
            return None
        return choices[max(0, min(idx, len(choices)-1))]

    def _build_iteration_choices(project: Dict[str, object]) -> List[Dict[str, object]]:
        opts = project.get('iteration_options') or []
        if not opts:
            return []
        return [{'id': '', 'title': '(None)'}] + opts

    def _build_repo_choices(project: Optional[Dict[str, object]]) -> List[Dict[str, str]]:
        repo_map = (project or {}).get('repos') or {}
        items = sorted(repo_map.items())
        choices = [{'repo': name, 'repo_id': rid or ''} for name, rid in items if name]
        if not choices:
            recent = db.recent_repositories(limit=20)
            choices = [{'repo': name, 'repo_id': rid or ''} for name, rid in recent if name]
        return choices

    def _reset_repo_metadata_state() -> None:
        add_state['label_choices'] = []
        add_state['label_index'] = 0
        add_state['labels_selected'] = set()
        add_state['priority_choices'] = []
        add_state['priority_index'] = 0
        add_state['priority_label'] = ''
        add_state['assignee_choices'] = []
        add_state['assignee_index'] = 0
        add_state['assignees_selected'] = set()
        add_state['metadata_error'] = ''
        add_state['loading_repo_metadata'] = False
        add_state['repo_metadata_source'] = ''
        add_state['repo_metadata_task'] = None

    async def _fetch_repo_metadata(repo_full_name: str) -> None:
        nonlocal add_state, status_line
        if not token:
            add_state['metadata_error'] = 'GITHUB_TOKEN required to load labels'
            add_state['loading_repo_metadata'] = False
            add_state['repo_metadata_task'] = None
            invalidate()
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            add_state['metadata_error'] = 'Unable to access event loop'
            add_state['loading_repo_metadata'] = False
            add_state['repo_metadata_task'] = None
            invalidate()
            return

        def _do_fetch():
            return (
                list_repo_labels(token, repo_full_name),
                list_repo_assignees(token, repo_full_name),
            )

        try:
            labels_raw, assignees_raw = await loop.run_in_executor(None, _do_fetch)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            add_state['metadata_error'] = f'Metadata error: {exc}'
            add_state['label_choices'] = []
            add_state['priority_choices'] = []
            add_state['assignee_choices'] = []
        else:
            label_names: List[str] = []
            seen_labels: Set[str] = set()
            for item in labels_raw:
                name_val = (item.get('name') if isinstance(item, dict) else '') or ''
                name_clean = name_val.strip()
                if not name_clean:
                    continue
                key = name_clean.lower()
                if key in seen_labels:
                    continue
                seen_labels.add(key)
                label_names.append(name_clean)
            add_state['label_choices'] = label_names
            add_state['label_index'] = 0 if label_names else 0
            add_state['labels_selected'] = set()

            priority_options_local = add_state.get('priority_options') or []
            priority_names: List[str] = []
            seen_priority: Set[str] = set()
            for opt in priority_options_local:
                if not isinstance(opt, dict):
                    continue
                name_val = (opt.get('name') or '').strip()
                if not name_val:
                    continue
                key = name_val.lower()
                if key in seen_priority:
                    continue
                seen_priority.add(key)
                priority_names.append(name_val)
            if not priority_names:
                priority_names = [nm for nm in label_names if 'priority' in nm.lower()]
            add_state['priority_choices'] = priority_names
            add_state['priority_index'] = 0 if priority_names else 0
            # Do not auto-select priority; let the user choose explicitly
            add_state['priority_label'] = ''

            assignee_entries: List[Dict[str, str]] = []
            seen_users: Set[str] = set()
            for item in assignees_raw:
                if not isinstance(item, dict):
                    continue
                login_val = (item.get('login') or '').strip()
                if not login_val:
                    continue
                key = login_val.lower()
                if key in seen_users:
                    continue
                seen_users.add(key)
                real_name = (item.get('name') or '').strip()
                if real_name and real_name.lower() != key:
                    display = f"{login_val} ({real_name})"
                else:
                    display = login_val
                assignee_entries.append({'login': login_val, 'display': display})
            assignee_entries.sort(key=lambda x: x['display'].lower())
            add_state['assignee_choices'] = assignee_entries
            add_state['assignee_index'] = 0 if assignee_entries else 0
            default_login = (cfg.user or '').strip().lower()
            selected_assignees: Set[str] = set()
            if default_login:
                for entry in assignee_entries:
                    if entry['login'].strip().lower() == default_login:
                        selected_assignees.add(entry['login'])
                        add_state['assignee_index'] = assignee_entries.index(entry)
                        break
            add_state['assignees_selected'] = selected_assignees
            add_state['metadata_error'] = '' if label_names else 'No labels found for repository'
        finally:
            add_state['loading_repo_metadata'] = False
            add_state['repo_metadata_task'] = None
            invalidate()

    def _start_repo_metadata_fetch(repo_full_name: str) -> None:
        if not repo_full_name:
            return
        if not token:
            add_state['metadata_error'] = 'GITHUB_TOKEN required to load labels'
            add_state['loading_repo_metadata'] = False
            return
        # Avoid duplicate fetch if already loaded
        if add_state.get('repo_metadata_source') == repo_full_name and add_state.get('label_choices'):
            return
        task = add_state.get('repo_metadata_task')
        if isinstance(task, asyncio.Task):
            task.cancel()
        add_state['repo_metadata_source'] = repo_full_name
        add_state['loading_repo_metadata'] = True
        add_state['metadata_error'] = ''
        add_state['label_choices'] = []
        add_state['label_index'] = 0
        add_state['labels_selected'] = set()
        add_state['priority_choices'] = []
        add_state['priority_index'] = 0
        add_state['priority_label'] = ''
        add_state['assignee_choices'] = []
        add_state['assignee_index'] = 0
        add_state['assignees_selected'] = set()
        invalidate()
        add_state['repo_metadata_task'] = asyncio.create_task(_fetch_repo_metadata(repo_full_name))

    def build_add_overlay() -> List[Tuple[str, str]]:
        if not add_mode:
            return []
        step = add_state.get('step', 'project')
        project = _current_add_project()
        mode_label = 'Issue' if add_state.get('mode', 'issue') == 'issue' else 'Project Task'
        repo_full = (add_state.get('repo_full_name') or '').strip()
        metadata_error = add_state.get('metadata_error', '')
        loading_metadata = bool(add_state.get('loading_repo_metadata'))

        headers = {
            'mode': '🧷 Choose Type',
            'project': '📁 Select Project',
            'repo': '📦 Select Repository',
            'title': '📝 Enter Title',
            'start': '📅 Start Date (YYYY-MM-DD)',
            'end': '📆 End Date (optional)',
            'focus': '🎯 Focus Day (YYYY-MM-DD)',
            'iteration': '🔁 Select Iteration',
            'labels': '🏷️ Select Labels',
            'priority': '⚡ Select Priority',
            'assignee': '👥 Choose Assignees',
            'comment': '💬 Initial Comment',
            'confirm': '✅ Confirm',
        }
        title_head = headers.get(step, '➕ Add Item')
        body: List[str] = []

        if step == 'mode':
            body.append("Use j/k to move, Enter to choose, Esc to cancel")
            choices = add_state.get('mode_choices') or []
            for idx, label in enumerate(choices):
                prefix = "➤" if idx == add_state.get('mode_index', 0) else " "
                body.append(f" {prefix} {label}")
        elif step == 'project':
            body.append("Use j/k to move, Enter to choose, Esc to cancel")
            choices = add_state.get('project_choices') or []
            if not choices:
                body.append("  (no projects available)")
            for idx, proj in enumerate(choices):
                prefix = "➤" if idx == add_state.get('project_index', 0) else " "
                body.append(f" {prefix} {proj.get('project_title')} (#{proj.get('project_number')})")
        elif step == 'repo':
            choices = add_state.get('repo_choices') or []
            if choices:
                body.append("Use j/k to move, Enter to choose, Esc to cancel")
                for idx, repo_entry in enumerate(choices):
                    prefix = "➤" if idx == add_state.get('repo_index', 0) else " "
                    body.append(f" {prefix} {repo_entry.get('repo')}")
                if loading_metadata:
                    body.append("")
                    body.append(" Loading metadata…")
            else:
                body.append("Type owner/name, Enter to confirm, Esc to cancel")
                r = add_state.get('repo_manual', '')
                cur = max(0, min(len(r), add_state.get('repo_cursor', len(r))))
                body.append(r[:cur] + "_" + r[cur:])
                if loading_metadata:
                    body.append("")
                    body.append(" Loading metadata…")
                elif metadata_error:
                    body.append(f" ⚠️ {metadata_error}")
        elif step == 'title':
            body.append("Type a concise title, Enter to continue, Esc cancel")
            text = add_state.get('title', '')
            cur = max(0, min(len(text), add_state.get('title_cursor', len(text))))
            body.append(text[:cur] + "_" + text[cur:])
        elif step in ('start', 'end', 'focus'):
            prompts = {
                'start': 'Enter Start Date (YYYY-MM-DD)',
                'end': 'Enter End Date (optional, YYYY-MM-DD)',
                'focus': 'Enter Focus Day (YYYY-MM-DD)',
            }
            field_map = {
                'start': ('start_date', 'start_cursor'),
                'end': ('end_date', 'end_cursor'),
                'focus': ('focus_date', 'focus_cursor'),
            }
            prompt = prompts.get(step, 'Enter Date')
            body.append(f"{prompt}, Enter to continue, Esc cancel")
            field_key, cursor_key = field_map[step]
            val = add_state.get(field_key, '')
            cur = max(0, min(len(val), add_state.get(cursor_key, len(val))))
            body.append(val[:cur] + "_" + val[cur:])
        elif step == 'iteration':
            body.append("Use j/k to move, Enter to choose, Esc cancel")
            choices = add_state.get('iteration_choices') or []
            if not choices:
                body.append("  (no iterations configured)")
            for idx, opt in enumerate(choices):
                prefix = "➤" if idx == add_state.get('iteration_index', 0) else " "
                label = opt.get('title') or '(None)'
                body.append(f" {prefix} {label}")
        elif step == 'labels':
            labels = add_state.get('label_choices') or []
            selected = add_state.get('labels_selected') or set()
            if not isinstance(selected, set):
                selected = set(selected)
            idx = max(0, min(len(labels)-1, add_state.get('label_index', 0))) if labels else 0
            if loading_metadata:
                body.append(f"Loading labels for {repo_full or '(repo pending)'}…")
            elif metadata_error:
                body.append(f"⚠️ {metadata_error}")
            body.append("Use j/k to move, Space to toggle, Enter to continue, Esc cancel")
            if not labels:
                body.append("  (no labels available)")
            for i, name in enumerate(labels):
                prefix = "➤" if i == idx else " "
                marker = '✔' if name in selected else ' '
                body.append(f" {prefix} [{marker}] {name}")
        elif step == 'priority':
            priorities = add_state.get('priority_choices') or []
            selected_label = (add_state.get('priority_label') or '').strip()
            idx = max(0, min(len(priorities)-1, add_state.get('priority_index', 0))) if priorities else 0
            if loading_metadata:
                body.append(f"Loading labels for {repo_full or '(repo pending)'}…")
            elif metadata_error and not priorities:
                body.append(f"⚠️ {metadata_error}")
            body.append("Use j/k to move, Space to select, Enter to continue, Esc cancel")
            if not priorities:
                body.append("  (no priority candidates; leave blank if not needed)")
            for i, name in enumerate(priorities):
                prefix = "➤" if i == idx else " "
                marker = '●' if name == selected_label else '○'
                body.append(f" {prefix} {marker} {name}")
        elif step == 'assignee':
            assignees = add_state.get('assignee_choices') or []
            selected = add_state.get('assignees_selected') or set()
            if not isinstance(selected, set):
                selected = set(selected)
            idx = max(0, min(len(assignees)-1, add_state.get('assignee_index', 0))) if assignees else 0
            if loading_metadata:
                body.append(f"Loading assignees for {repo_full or '(repo pending)'}…")
            elif metadata_error and not assignees:
                body.append(f"⚠️ {metadata_error}")
            body.append("Use j/k to move, Space to toggle, Enter to continue, Esc cancel")
            if not assignees:
                body.append("  (no assignable users found)")
            for i, entry in enumerate(assignees):
                prefix = "➤" if i == idx else " "
                marker = '✔' if entry.get('login') in selected else ' '
                body.append(f" {prefix} [{marker}] {entry.get('display') or entry.get('login')}")
        elif step == 'comment':
            body.append("Type an optional comment, Enter to continue, Esc cancel")
            comment = add_state.get('comment', '')
            cur = max(0, min(len(comment), add_state.get('comment_cursor', len(comment))))
            body.append(comment[:cur] + "_" + comment[cur:])
        elif step == 'confirm':
            title_val = add_state.get('title', '').strip()
            start_val = (add_state.get('start_date') or '').strip() or '(auto)'
            end_val = (add_state.get('end_date') or '').strip() or '(none)'
            focus_val = (add_state.get('focus_date') or '').strip() or '(none)'
            iteration_choices = add_state.get('iteration_choices') or []
            iteration_idx = add_state.get('iteration_index', 0)
            iter_label = '(none)'
            if iteration_choices:
                opt = iteration_choices[max(0, min(iteration_idx, len(iteration_choices)-1))]
                iter_label = opt.get('title') or '(none)'
            repo_label = repo_full or '(n/a)'
            labels_selected = add_state.get('labels_selected') or set()
            if not isinstance(labels_selected, set):
                labels_selected = set(labels_selected)
            label_choices = add_state.get('label_choices') or []
            label_list = [name for name in label_choices if name in labels_selected]
            priority_label = (add_state.get('priority_label') or '').strip() or '(none)'
            assignees_selected = add_state.get('assignees_selected') or set()
            if not isinstance(assignees_selected, set):
                assignees_selected = set(assignees_selected)
            assignee_entries = add_state.get('assignee_choices') or []
            assignee_display = []
            for entry in assignee_entries:
                login = entry.get('login')
                if login in assignees_selected:
                    assignee_display.append(entry.get('display') or login)
            assignee_display.extend(sorted({login for login in assignees_selected if login not in {e.get('login') for e in assignee_entries}}))
            comment_val = add_state.get('comment', '').strip() or '(none)'

            body.append("Review and press Enter to create, Esc cancel")
            body.append("")
            body.append(f"📁 Project  : {project.get('project_title') if project else '(unknown)'}")
            body.append(f"🧷 Type     : {mode_label}")
            if add_state.get('mode', 'issue') == 'issue':
                body.append(f"📦 Repo     : {repo_label}")
            body.append(f"📝 Title    : {title_val or '(missing)'}")
            body.append(f"🗓️ Start    : {start_val}")
            body.append(f"📆 End      : {end_val}")
            body.append(f"🎯 Focus    : {focus_val}")
            body.append(f"🔁 Iteration: {iter_label}")
            if add_state.get('mode', 'issue') == 'issue':
                body.append(f"🏷️ Labels   : {', '.join(label_list) if label_list else '(none)'}")
                body.append(f"⚡ Priority : {priority_label}")
                body.append(f"👥 Assignees: {', '.join(assignee_display) if assignee_display else '(none)'}")
                body.append(f"💬 Comment  : {comment_val}")

        def boxed(title: str, lines: List[str], width: int = 92) -> str:
            inner = width - 2
            out = ["╭" + ("─" * (width-2)) + "╮"]
            t = f" {title.strip()} "
            t = t[: max(0, inner-2)]
            pad = max(0, (inner-2) - len(t))
            left = pad // 2
            right = pad - left
            out.append("│ " + (" "*left) + t + (" "*right) + " │")
            out.append("├" + ("─" * (width-2)) + "┤")
            for ln in lines:
                ln = ln.rstrip()
                if len(ln) > inner-2:
                    ln = ln[:inner-5] + "…"
                out.append("│ " + ln.ljust(inner-2) + " │")
            out.append("╰" + ("─" * (width-2)) + "╯")
            return "\n".join(out)

        content = boxed(title_head, body, width=92)
        return [("", content)]

    STATUS_KEYWORDS: Dict[str, List[str]] = {
        'done': ["done", "complete", "completed", "finished", "closed", "resolved", "merged", "✅", "✔"],
        'in_progress': ["in progress", "progress", "doing", "active", "working"]
    }

    def _status_options_map(row: TaskRow) -> Dict[str, Tuple[str, str]]:
        try:
            data = json.loads(row.status_options or "[]")
        except Exception:
            data = []
        out: Dict[str, Tuple[str, str]] = {}
        if isinstance(data, list):
            for opt in data:
                if not isinstance(opt, dict):
                    continue
                name = (opt.get("name") or "").strip()
                opt_id = (opt.get("id") or "").strip()
                if name and opt_id:
                    out[name.lower()] = (opt_id, name)
        if row.status and row.status_option_id:
            low = row.status.strip().lower()
            out.setdefault(low, (row.status_option_id, row.status))
        return out

    def _match_status_option(row: TaskRow, target: str) -> Tuple[str, str]:
        options = _status_options_map(row)
        keywords = STATUS_KEYWORDS.get(target, [])
        for kw in keywords:
            opt = options.get(kw.lower())
            if opt:
                return opt
        for key, opt in options.items():
            if any(kw in key for kw in keywords):
                return opt
        return ("", "")

    def _is_done_name(name: str) -> int:
        low = (name or "").lower()
        return int(any(kw in low for kw in STATUS_KEYWORDS['done']))

    async def _apply_status_change(target: str):
        nonlocal all_rows, status_line, current_index
        if not token:
            status_line = "GITHUB_TOKEN required for status updates"
            invalidate(); return
        rows = filtered_rows()
        if not rows:
            status_line = "No task selected"
            invalidate(); return
        current_index = max(0, min(len(rows)-1, current_index))
        row = rows[current_index]
        selected_url = row.url
        if row.url in pending_status_urls:
            status_line = "Status update already in progress"
            invalidate(); return
        if not (row.project_id and row.item_id and row.status_field_id):
            status_line = "Task missing status metadata"
            invalidate(); return
        option_id, display_name = _match_status_option(row, target)
        if not option_id and token and row.status_field_id:
            try:
                fetched_opts = get_project_field_options(token, row.status_field_id)
            except Exception as exc:
                fetched_opts = []
                try:
                    logger.warning("Unable to fetch status options for %s: %s", row.status_field_id, exc)
                except Exception:
                    pass
            if fetched_opts:
                try:
                    db.update_status_options_by_field(row.status_field_id, fetched_opts)
                except Exception:
                    try:
                        db.update_status_options(selected_url, fetched_opts)
                    except Exception:
                        pass
                all_rows = load_all()
                rows = filtered_rows()
                if rows:
                    for idx, candidate in enumerate(rows):
                        if candidate.url == selected_url:
                            current_index = idx
                            row = candidate
                            break
                    else:
                        current_index = max(0, min(len(rows)-1, current_index))
                        row = rows[current_index]
                option_id, display_name = _match_status_option(row, target)
        if not option_id:
            status_line = f"No status option matches '{target}'"
            invalidate(); return
        if (row.status_option_id == option_id) and not row.status_dirty:
            status_line = f"Already {display_name}"
            invalidate(); return
        new_is_done = _is_done_name(display_name)
        original_status = row.status or ""
        original_option = row.status_option_id or ""
        original_is_done = row.is_done
        try:
            db.mark_status_pending(row.url, display_name, option_id, new_is_done)
        except Exception as exc:
            status_line = f"Failed to mark pending: {exc}"
            invalidate(); return
        pending_status_urls.add(row.url)
        all_rows = load_all()
        status_line = f"Updating status to {display_name}…"
        invalidate()

        loop = asyncio.get_running_loop()

        def _do_update():
            set_project_status(token, row.project_id, row.item_id, row.status_field_id, option_id)

        try:
            await loop.run_in_executor(None, _do_update)
        except Exception as exc:
            db.reset_status(row.url, original_status, original_option, original_is_done)
            status_line = f"Status update failed: {exc}"
        else:
            db.mark_status_synced(row.url)
            status_line = f"Status set to {display_name}"
        finally:
            pending_status_urls.discard(row.url)
            all_rows = load_all()
            invalidate()

    async def _update_task_date(field_type: str, new_value: str):
        nonlocal all_rows, status_line, current_index
        label = 'Start Date' if field_type == 'start' else 'Focus Day'
        rows = filtered_rows()
        if not rows:
            status_line = "No task selected"
            if edit_task_mode:
                task_edit_state['message'] = status_line
            invalidate(); return
        row = rows[current_index]
        if not token:
            msg = "GITHUB_TOKEN required for date updates"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        project_id = row.project_id or ''
        item_id = row.item_id or ''
        if not (project_id and item_id):
            msg = "Task missing project metadata"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        try:
            if new_value:
                dt.date.fromisoformat(new_value)
        except Exception:
            msg = f"Bad date '{new_value}' (use YYYY-MM-DD)"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        field_id = row.start_field_id if field_type == 'start' else getattr(row, 'focus_field_id', '')
        field_name = row.start_field if field_type == 'start' else (row.focus_field or 'Focus Day')
        loop = asyncio.get_running_loop()
        if not field_id and token:
            try:
                lookup_name = (field_name or '').strip() or ('Focus Day' if field_type == 'focus' else 'Start date')
                field_id = await loop.run_in_executor(None, lambda: get_project_field_id_by_name(token, project_id, lookup_name)) or ''
            except Exception as exc:
                field_id = ''
                try:
                    logger.warning("Failed to resolve %s field id for %s: %s", field_type, row.project_title, exc)
                except Exception:
                    pass
        if not field_id:
            msg = f"No {field_name or label} field id"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return

        def _do_update():
            set_project_date(token, project_id, item_id, field_id, new_value)

        try:
            await loop.run_in_executor(None, _do_update)
        except Exception as exc:
            msg = f"{label} update failed: {exc}"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return

        if field_type == 'start':
            db.update_start_date(row.url, new_value, field_id)
        else:
            db.update_focus_date(row.url, new_value, field_id)
        all_rows = load_all()
        status_line = f"{label} updated"
        if edit_task_mode:
            task_edit_state['message'] = status_line
            _refresh_task_editor_state()
        invalidate()

    def _calendar_adjust(days: int = 0, months: int = 0) -> None:
        if not edit_task_mode or task_edit_state.get('mode') != 'edit-date-calendar':
            return
        fields = task_edit_state.get('fields') or []
        editing = task_edit_state.get('editing') or {}
        idx = editing.get('field_idx')
        if idx is None or idx >= len(fields):
            return
        iso = editing.get('calendar_date') or fields[idx].get('value') or dt.date.today().isoformat()
        try:
            current = dt.date.fromisoformat(iso)
        except Exception:
            current = dt.date.today()
        if months:
            month = current.month - 1 + months
            year = current.year + month // 12
            month = month % 12 + 1
            day = min(current.day, calendar.monthrange(year, month)[1])
            current = dt.date(year, month, day)
        if days:
            current += dt.timedelta(days=days)
        editing['calendar_date'] = current.isoformat()
        fields[idx]['value'] = current.isoformat()
        task_edit_state['editing'] = editing
        task_edit_state['message'] = current.isoformat()
        invalidate()

    async def _apply_assignees(logins: List[str]) -> None:
        nonlocal all_rows, status_line
        rows = filtered_rows()
        if not rows:
            msg = "No task selected"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        row = rows[current_index]
        if not token:
            msg = "GITHUB_TOKEN required for assignee updates"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        if not row.assignee_field_id:
            msg = "Task missing People field"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        loop = asyncio.get_running_loop()
        clean_logins = []
        seen = set()
        for login in logins:
            norm = login.strip().lstrip('@')
            if not norm:
                continue
            if norm.lower() in seen:
                continue
            seen.add(norm.lower())
            clean_logins.append(norm)
        user_ids: List[str] = []
        errors: List[str] = []
        for login in clean_logins:
            try:
                node_id = await loop.run_in_executor(None, lambda l=login: get_user_node_id(token, l))
            except Exception as exc:
                errors.append(f"{login} ({exc})")
                continue
            if not node_id:
                errors.append(login)
            else:
                user_ids.append(node_id)
        if errors:
            msg = f"Unknown user(s): {', '.join(errors)}"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        try:
            await loop.run_in_executor(None, lambda: set_project_users(token, row.project_id, row.item_id, row.assignee_field_id, user_ids))
        except Exception as exc:
            msg = f"People field update failed: {exc}"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        try:
            await loop.run_in_executor(None, lambda: set_issue_assignees(token, row.url, clean_logins))
        except Exception as exc:
            try:
                logger.warning("Issue assignee update failed for %s: %s", row.url, exc)
            except Exception:
                pass
        db.update_assignees(row.url, user_ids, clean_logins)
        all_rows = load_all()
        status_line = 'Assignees updated'
        if edit_task_mode:
            task_edit_state['message'] = status_line
            _refresh_task_editor_state()
        invalidate()

    async def _apply_labels(labels_new: List[str]) -> None:
        nonlocal all_rows, status_line
        rows = filtered_rows()
        if not rows:
            msg = "No task selected"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        row = rows[current_index]
        if not token:
            msg = "GITHUB_TOKEN required for label updates"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        parts = _parse_issue_url(row.url)
        if not parts:
            msg = "Labels only supported for issues/PRs"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        loop = asyncio.get_running_loop()
        clean_labels = []
        seen = set()
        for lab in labels_new:
            nm = lab.strip()
            if not nm:
                continue
            if nm.lower() in seen:
                continue
            seen.add(nm.lower())
            clean_labels.append(nm)
        try:
            await loop.run_in_executor(None, lambda: set_issue_labels(token, row.url, clean_labels))
        except Exception as exc:
            msg = f"Label update failed: {exc}"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        db.update_labels(row.url, clean_labels)
        all_rows = load_all()
        status_line = 'Labels updated'
        if edit_task_mode:
            task_edit_state['message'] = status_line
            _refresh_task_editor_state()
        invalidate()

    async def _load_label_choices_for_editor(repo_full: str, initial_selection: Set[str]) -> None:
        if not token:
            if edit_task_mode and task_edit_state.get('labels_repo') == repo_full:
                task_edit_state['labels_error'] = 'GITHUB_TOKEN required for labels'
                task_edit_state['labels_loading'] = False
                task_edit_state['labels_task'] = None
                invalidate()
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        try:
            labels_raw = await loop.run_in_executor(None, lambda: list_repo_labels(token, repo_full))
        except asyncio.CancelledError:
            return
        except Exception as exc:
            if edit_task_mode and task_edit_state.get('labels_repo') == repo_full:
                task_edit_state['labels_error'] = f'Label fetch failed: {exc}'
                task_edit_state['labels_loading'] = False
                task_edit_state['labels_task'] = None
                invalidate()
            return
        names: List[str] = []
        seen: Set[str] = set()
        for item in labels_raw:
            if not isinstance(item, dict):
                continue
            name_val = (item.get('name') or '').strip()
            if not name_val:
                continue
            key = name_val.lower()
            if key in seen:
                continue
            seen.add(key)
            names.append(name_val)
        # Preserve any existing labels that might not be in the repo list
        extras = [lab for lab in initial_selection if lab and lab.lower() not in seen]
        names.extend(extras)
        selected = {lab for lab in initial_selection if lab}
        if edit_task_mode and task_edit_state.get('labels_repo') == repo_full and task_edit_state.get('mode') == 'edit-labels':
            task_edit_state['label_choices'] = names
            task_edit_state['labels_selected'] = set(selected)
            if names:
                task_edit_state['label_index'] = max(0, min(task_edit_state.get('label_index', 0), len(names)-1))
                task_edit_state['labels_error'] = ''
            else:
                task_edit_state['label_index'] = 0
                task_edit_state['labels_error'] = 'No labels available'
            task_edit_state['labels_loading'] = False
            task_edit_state['labels_task'] = None
            if names:
                task_edit_state['message'] = 'Labels loaded'
            else:
                task_edit_state['message'] = task_edit_state['labels_error']
            invalidate()

    async def _add_comment(comment: str) -> None:
        nonlocal status_line
        rows = filtered_rows()
        if not rows:
            msg = "No task selected"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        row = rows[current_index]
        if not token:
            msg = "GITHUB_TOKEN required for comments"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        if not _parse_issue_url(row.url):
            msg = "Comments supported only for issues/PRs"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        body = (comment or '').strip()
        if not body:
            msg = "Comment cannot be empty"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, lambda: add_issue_comment(token, row.url, body))
        except Exception as exc:
            msg = f"Comment failed: {exc}"
            status_line = msg
            if edit_task_mode:
                task_edit_state['message'] = msg
            invalidate(); return
        status_line = 'Comment posted'
        if edit_task_mode:
            task_edit_state['message'] = status_line
        invalidate()

    async def _change_priority(delta: Optional[int] = None, option_id: Optional[str] = None):
        nonlocal all_rows, status_line, current_index
        if not token:
            status_line = "GITHUB_TOKEN required for priority updates"
            invalidate(); return
        rows = filtered_rows()
        if not rows:
            status_line = "No task selected"
            invalidate(); return
        row = rows[current_index]
        selected_url = row.url
        if not selected_url:
            status_line = "Selected task missing URL"
            invalidate(); return
        if selected_url in pending_priority_urls:
            status_line = "Priority update already in progress"
            invalidate(); return
        if not (row.project_id and row.item_id and row.priority_field_id):
            status_line = "Task missing priority metadata"
            invalidate(); return
        options = _priority_options(row)
        if (not options) and token and row.priority_field_id:
            try:
                fetched_opts = get_project_field_options(token, row.priority_field_id)
            except Exception as exc:
                fetched_opts = []
                try:
                    logger.warning("Unable to fetch priority options for %s: %s", row.priority_field_id, exc)
                except Exception:
                    pass
            if fetched_opts:
                try:
                    db.update_priority_options_by_field(row.priority_field_id, fetched_opts)
                except Exception:
                    try:
                        db.update_priority_options(selected_url, fetched_opts)
                    except Exception:
                        pass
                all_rows = load_all()
                rows = filtered_rows()
                if rows:
                    for idx, candidate in enumerate(rows):
                        if candidate.url == selected_url:
                            current_index = idx
                            row = candidate
                            break
                    else:
                        current_index = max(0, min(len(rows)-1, current_index))
                        row = rows[current_index]
                options = _priority_options(row)
        if not options:
            status_line = "No priority options available"
            invalidate(); return
        option_map = [opt for opt in options if isinstance(opt, dict) and opt.get('id')]
        if not option_map:
            status_line = "Priority options missing ids"
            invalidate(); return
        try:
            current_idx = next((idx for idx, opt in enumerate(option_map) if (opt.get('id') or '') == (row.priority_option_id or '')), 0)
        except Exception:
            current_idx = 0
        if option_id is not None:
            new_opt = next((opt for opt in option_map if (opt.get('id') or '').strip() == option_id.strip()), None)
            if not new_opt:
                status_line = "Priority option not found"
                invalidate(); return
            new_idx = option_map.index(new_opt)
        else:
            if delta is None:
                status_line = "No priority change provided"
                invalidate(); return
            new_idx = (current_idx + delta) % len(option_map)
            new_opt = option_map[new_idx]
        new_option_id = (new_opt.get('id') or '').strip()
        display_name = (new_opt.get('name') or '').strip() or '(unset)'
        if not new_option_id:
            status_line = "Selected priority option missing id"
            invalidate(); return
        if (row.priority_option_id == new_option_id) and not getattr(row, 'priority_dirty', 0):
            status_line = f"Priority already {display_name}"
            invalidate(); return
        original_priority = row.priority or ""
        original_option = row.priority_option_id or ""
        try:
            db.mark_priority_pending(selected_url, display_name, new_option_id)
        except Exception as exc:
            status_line = f"Failed to mark priority pending: {exc}"
            invalidate(); return
        pending_priority_urls.add(selected_url)
        all_rows = load_all()
        status_line = f"Updating priority to {display_name}…"
        invalidate()

        loop = asyncio.get_running_loop()

        def _do_update():
            set_project_priority(token, row.project_id, row.item_id, row.priority_field_id, new_option_id)

        try:
            await loop.run_in_executor(None, _do_update)
        except Exception as exc:
            db.reset_priority(selected_url, original_priority, original_option)
            status_line = f"Priority update failed: {exc}"
        else:
            db.mark_priority_synced(selected_url)
            status_line = f"Priority set to {display_name}"
        finally:
            pending_priority_urls.discard(selected_url)
            all_rows = load_all()
            if edit_task_mode:
                task_edit_state['message'] = status_line
                _refresh_task_editor_state()
            invalidate()

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
                                   needle in (r.priority or '').lower() or
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
        preset = sort_presets[max(0, min(sort_index, len(sort_presets)-1))]
        key_func = preset.get('key', lambda r: (r.project_title or '', _safe_date(r.focus_date) or dt.date.max, r.title or ''))
        reverse = bool(preset.get('reverse'))
        out = sorted(out, key=key_func, reverse=reverse)
        return out

    def projects_list(rows: Iterable[TaskRow]) -> List[str]:
        seen = []
        for r in rows:
            if r.project_title not in seen:
                seen.append(r.project_title)
        return seen

    sort_presets: List[Dict[str, object]] = [
        {
            'name': 'Project → Focus → Priority',
            'key': lambda r: (
                r.project_title or '',
                _safe_date(r.focus_date) or dt.date.max,
                _priority_rank(r),
                r.title or ''
            ),
        },
        {
            'name': 'Focus Day → Priority → Project',
            'key': lambda r: (
                _safe_date(r.focus_date) or dt.date.max,
                _priority_rank(r),
                r.project_title or '',
                r.title or ''
            ),
        },
        {
            'name': 'Priority → Focus → Project',
            'key': lambda r: (
                _priority_rank(r),
                _safe_date(r.focus_date) or dt.date.max,
                r.project_title or '',
                r.title or ''
            ),
        },
        {
            'name': 'Updated ↓ → Priority',
            'key': lambda r: (
                r.updated_at or '',
                _priority_rank(r)
            ),
            'reverse': True,
        },
    ]

    if not (0 <= sort_index < len(sort_presets)):
        sort_index = 0

    def filtered_rows() -> List[TaskRow]:
        return apply_filters(all_rows)

    def _task_labels(row: TaskRow) -> List[str]:
        try:
            data = json.loads(row.labels or "[]")
            return [str(x) for x in data if isinstance(x, str) and x]
        except Exception:
            return []

    def _priority_options(row: TaskRow) -> List[Dict[str, object]]:
        try:
            data = json.loads(row.priority_options or "[]")
            if isinstance(data, list):
                return [opt for opt in data if isinstance(opt, dict)]
        except Exception:
            pass
        return []

    def _priority_rank(row: TaskRow) -> int:
        opts = _priority_options(row)
        if opts:
            id_lookup = {str(opt.get('id') or ''): idx for idx, opt in enumerate(opts)}
            opt_id = (row.priority_option_id or '').strip()
            if opt_id in id_lookup:
                return id_lookup[opt_id]
            name_lookup = {(opt.get('name') or '').strip().lower(): idx for idx, opt in enumerate(opts)}
            pname = (row.priority or '').strip().lower()
            if pname in name_lookup:
                return name_lookup[pname]
            return len(opts)
        pname = (row.priority or '').strip().lower()
        if pname in ('urgent', 'highest', 'high'):  # conventional mapping
            return 0
        if pname in ('medium', 'normal'):  # mid tier
            return 1
        if pname in ('low', 'lowest', 'minor'):
            return 2
        return 99

    def _cycle_sort(delta: int) -> None:
        nonlocal sort_index, status_line
        count = len(sort_presets)
        if not count:
            return
        sort_index = (sort_index + delta) % count
        status_line = f"Sort: {sort_presets[sort_index]['name']}"
        if edit_task_mode:
            _refresh_task_editor_state()
        invalidate()

    def _build_task_edit_fields_from_row(row: TaskRow) -> List[Dict[str, object]]:
        fields: List[Dict[str, object]] = []
        fields.append({
            'name': row.start_field or 'Start Date',
            'type': 'date',
            'field_key': 'start',
            'value': row.start_date or '',
        })
        focus_label = row.focus_field or 'Focus Day'
        fields.append({
            'name': focus_label,
            'type': 'date',
            'field_key': 'focus',
            'value': row.focus_date or '',
        })
        priority_opts = _priority_options(row)
        if priority_opts:
            try:
                current_idx = next((idx for idx, opt in enumerate(priority_opts) if (opt.get('id') or '').strip() == (row.priority_option_id or '').strip()), 0)
            except Exception:
                current_idx = 0
            fields.append({
                'name': 'Priority',
                'type': 'priority',
                'field_key': 'priority',
                'options': priority_opts,
                'index': current_idx,
            })
        elif (row.priority or '').strip():
            fields.append({
                'name': 'Priority',
                'type': 'priority-text',
                'field_key': 'priority',
                'value': row.priority.strip(),
            })
        try:
            assignees = json.loads(row.assignee_logins or "[]")
            if not isinstance(assignees, list):
                assignees = []
        except Exception:
            assignees = []
        if assignees or (row.assignee_field_id and row.assignee_field_id.strip()):
            fields.append({
                'name': 'Assignees',
                'type': 'assignees',
                'field_key': 'assignees',
                'value': assignees,
            })
        try:
            label_values = json.loads(row.labels or "[]")
            if not isinstance(label_values, list):
                label_values = []
        except Exception:
            label_values = []
        if label_values or _parse_issue_url(row.url):
            fields.append({
                'name': 'Labels',
                'type': 'labels',
                'field_key': 'labels',
                'value': label_values,
            })
        if _parse_issue_url(row.url):
            fields.append({
                'name': 'Comment',
                'type': 'comment',
                'field_key': 'comment',
                'value': '',
            })
        return fields

    def _refresh_task_editor_state(preserve_cursor: bool = True, do_invalidate: bool = True) -> None:
        if not edit_task_mode:
            return
        rows = filtered_rows()
        if not rows:
            close_task_editor('No tasks available')
            return
        row = rows[current_index]
        fields = _build_task_edit_fields_from_row(row)
        if not fields:
            close_task_editor('No editable fields')
            return
        cursor = int(task_edit_state.get('cursor', 0) or 0) if preserve_cursor else 0
        cursor = max(0, min(cursor, len(fields)-1))
        task_edit_state['fields'] = fields
        task_edit_state['cursor'] = cursor
        task_edit_state['task_url'] = row.url
        if task_edit_state.get('mode') != 'list':
            task_edit_state['mode'] = 'list'
            task_edit_state['input'] = ''
            task_edit_state['editing'] = None
        task_edit_state.setdefault('message', 'Use j/k to select, Enter to edit, Esc to close')
        if do_invalidate:
            invalidate()

    def build_table_fragments() -> List[Tuple[str,str]]:
        nonlocal task_duration_cache
        nonlocal current_index
        nonlocal v_offset
        rows = filtered_rows()
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
        priority_w = 10
        if use_iteration:
            iter_min = 15
            title_min = 20
            label_min = 16
            proj_min = 12
            spaces_width = 2 * 6  # gaps between columns
            fixed_base = 2 + 10 + priority_w + time_w + spaces_width  # marker + status + priority + time + spaces
            dyn_total = max(iter_min + title_min + label_min + proj_min, avail_cols - fixed_base)
            extra = dyn_total - (iter_min + title_min + label_min + proj_min)
            weights = [1, 2, 1, 1]
            weight_sum = sum(weights)
            iter_w = iter_min + (extra * weights[0]) // weight_sum
            title_w = title_min + (extra * weights[1]) // weight_sum
            label_w = label_min + (extra * weights[2]) // weight_sum
            proj_w = dyn_total - iter_w - title_w - label_w
            if proj_w < proj_min:
                deficit = proj_min - proj_w
                take = min(deficit, max(0, label_w - label_min))
                label_w -= take
                deficit -= take
                take = min(deficit, max(0, title_w - title_min))
                title_w -= take
                deficit -= take
                take = min(deficit, max(0, iter_w - iter_min))
                iter_w -= take
                deficit -= take
                proj_w = proj_min
                if deficit > 0:
                    title_w += deficit  # best-effort rebalance
            header = (
                "  " + _pad_display("Iteration", iter_w) +
                "  " + _pad_display("Status", 10) +
                "  " + _pad_display("Priority", priority_w) +
                "  " + _pad_display("Time", time_w, align='right') +
                "  " + _pad_display("Title", title_w) +
                "  " + _pad_display("Labels", label_w) +
                "  " + _pad_display("Project", proj_w)
            )
        else:
            proj_min = 12
            title_min = 20
            label_min = 16
            spaces_width = 2 * 7  # gaps between columns
            fixed = 2 + 11 + 12 + 10 + priority_w + time_w + spaces_width  # marker + focus + start + status + priority + time + spaces
            dyn = max(title_min + label_min + proj_min, avail_cols - fixed)
            extra = dyn - (title_min + label_min + proj_min)
            weights = [2, 1, 1]
            weight_sum = sum(weights)
            title_w = title_min + (extra * weights[0]) // weight_sum
            label_w = label_min + (extra * weights[1]) // weight_sum
            proj_w = dyn - title_w - label_w
            if proj_w < proj_min:
                deficit = proj_min - proj_w
                take = min(deficit, max(0, label_w - label_min))
                label_w -= take
                deficit -= take
                take = min(deficit, max(0, title_w - title_min))
                title_w -= take
                deficit -= take
                proj_w = proj_min
                if deficit > 0:
                    title_w += deficit
            header = (
                "  " + _pad_display("Focus Day", 11) +
                "  " + _pad_display("Start Date", 12) +
                "  " + _pad_display("Status", 10) +
                "  " + _pad_display("Priority", priority_w) +
                "  " + _pad_display("Time", time_w, align='right') +
                "  " + _pad_display("Title", title_w) +
                "  " + _pad_display("Labels", label_w) +
                "  " + _pad_display("Project", proj_w)
            )
        frags.append((_style_class('table.header'), header[h_offset:]))
        frags.append(("", "\n"))
        if not rows:
            frags.append(("italic", "(no tasks match filters)"))
            return frags
        today = today_date
        active_urls = db.active_task_urls()
        display_slice = rows[v_offset:v_offset+visible_rows]
        duration_urls = [t.url for t in display_slice if t.url]
        task_duration_cache = db.task_duration_snapshot(duration_urls)

        date_palette = {
            'today': _style_class('table.date.today'),
            'past': _style_class('table.date.past'),
            'future': _style_class('table.date.future'),
            'unknown': _style_class('table.date.unknown'),
        }

        def status_style_for(name: Optional[str]) -> str:
            if not name:
                return _style_class('table.status.other')
            norm = name.strip().lower()
            if 'block' in norm or 'hold' in norm or 'stuck' in norm:
                return _style_class('table.status.blocked')
            if 'progress' in norm or 'doing' in norm or 'active' in norm or 'working' in norm:
                return _style_class('table.status.in_progress')
            if 'done' in norm or 'complete' in norm or 'closed' in norm or 'shipped' in norm:
                return _style_class('table.status.done')
            if 'todo' in norm or 'backlog' in norm or 'ready' in norm or 'plan' in norm:
                return _style_class('table.status.todo')
            return _style_class('table.status.other')

        def trim_segments(segments: List[Tuple[str, str]], offset: int) -> List[Tuple[str, str]]:
            if offset <= 0:
                return segments
            remaining = offset
            trimmed: List[Tuple[str, str]] = []
            for style_txt, text in segments:
                if not text:
                    continue
                if remaining >= len(text):
                    remaining -= len(text)
                    continue
                if remaining > 0:
                    trimmed.append((style_txt, text[remaining:]))
                    remaining = 0
                else:
                    trimmed.append((style_txt, text))
            return trimmed

        def highlight_segments(segments: List[Tuple[str, str]], needle: str) -> List[Tuple[str, str]]:
            if not needle:
                return segments
            result: List[Tuple[str, str]] = []
            needle_lower = needle.lower()
            for style_txt, text in segments:
                if not text:
                    continue
                lower_text = text.lower()
                if needle_lower not in lower_text:
                    result.append((style_txt, text))
                    continue
                start = 0
                n_len = len(needle)
                while True:
                    idx = lower_text.find(needle_lower, start)
                    if idx == -1:
                        if start < len(text):
                            result.append((style_txt, text[start:]))
                        break
                    if idx > start:
                        result.append((style_txt, text[start:idx]))
                    highlight_style = (style_txt + ' underline').strip() if style_txt else 'underline'
                    result.append((highlight_style, text[idx:idx+n_len]))
                    start = idx + n_len
            return result

        for rel_idx, t in enumerate(display_slice):
            idx = v_offset + rel_idx
            is_sel = (idx == current_index)
            style_row = "reverse" if is_sel else ""
            col = color_for_date(t.focus_date, today, date_palette)
            running = bool(t.url and (t.url in active_urls))
            # Base style follows date palette; running tasks accent key columns.
            base_style = col
            marker = '⏱ ' if running else '  '
            # Time column: current run (mm:ss) and total (H:MM)
            snapshot = task_duration_cache.get(t.url) if t.url else None
            tot_s = snapshot.get('total', 0) if snapshot else 0
            cur_s = snapshot.get('current', 0) if snapshot else 0
            if not running:
                cur_s = 0
            mm, ss = divmod(int(max(0, cur_s)), 60)
            # total in H:MM (no leading zeros on hours)
            th, rem = divmod(int(max(0, tot_s)), 3600)
            tm, _ = divmod(rem, 60)
            time_text = f"{mm:02d}:{ss:02d}|{th:d}:{tm:02d}" if tot_s else f"{mm:02d}:{ss:02d}|0:00"
            status_cell = _pad_display(t.status or '-', 10)
            if t.status_dirty:
                status_cell = _pad_display((t.status or '-') + '*', 10)
            time_cell = _pad_display(time_text, time_w, align='right')
            title_cell = _pad_display(t.title, title_w)
            project_cell = _pad_display(t.project_title, proj_w)
            labels_list = _task_labels(t)
            labels_text = ", ".join(labels_list)
            priority_display = (t.priority or '-') + ('*' if getattr(t, 'priority_dirty', 0) else '')
            priority_cell = _pad_display(priority_display, priority_w)
            status_style = status_style_for(t.status)
            running_style = 'ansicyan bold' if running else None

            segments: List[Tuple[str, str]] = []

            def add_segment(text: str, seg_style: Optional[str] = None) -> None:
                if not text:
                    return
                if is_sel:
                    segments.append((style_row, text))
                    return
                style_token = seg_style if seg_style is not None else base_style
                segments.append((style_token.strip(), text))

            def add_column(text: str, seg_style: Optional[str] = None) -> None:
                add_segment('  ', running_style if running_style and seg_style is None and not is_sel else (base_style if not is_sel else style_row))
                add_segment(text, seg_style)

            marker_style = running_style if running_style else base_style

            add_segment(marker, marker_style)
            if use_iteration:
                iter_label = t.iteration_title or t.iteration_start or '-'
                if t.iteration_title and t.iteration_start:
                    iter_label = f"{t.iteration_title} ({t.iteration_start})"
                iteration_cell = _pad_display(iter_label or '-', iter_w)
                labels_cell = _pad_display(labels_text or '-', label_w)
                add_segment(iteration_cell, base_style)
                add_column(status_cell, status_style)
                add_column(priority_cell)
                add_column(time_cell, running_style if running_style else base_style)
                add_column(title_cell)
                add_column(labels_cell)
                add_column(project_cell)
            else:
                focus_cell = _pad_display(t.focus_date or '-', 11)
                start_cell = _pad_display(t.start_date, 12)
                labels_cell = _pad_display(labels_text or '-', label_w)
                add_segment(focus_cell, base_style)
                add_column(start_cell)
                add_column(status_cell, status_style)
                add_column(priority_cell)
                add_column(time_cell, running_style if running_style else base_style)
                add_column(title_cell)
                add_column(labels_cell)
                add_column(project_cell)

            segments = trim_segments(segments, h_offset)
            active_search = search_buffer if in_search else search_term
            if active_search and not is_sel:
                segments = highlight_segments(segments, active_search)

            for seg_style, seg_text in segments:
                frags.append((seg_style, seg_text))
            frags.append(("", "\n"))
        if frags and frags[-1][1] == "\n":
            frags.pop()
        return frags

    def summarize() -> List[Tuple[str,str]]:
        nonlocal task_duration_cache
        rows = filtered_rows()
        total = len(rows)
        done_ct = sum(1 for r in rows if r.is_done)
        now_mon = time.monotonic()

        def _fmt_hm(ts: int) -> str:
            s = int(max(0, ts))
            h, r = divmod(s, 3600)
            m, _ = divmod(r, 60)
            return f"{h:d}:{m:02d}"

        def _fmt_mmss(ts: int) -> str:
            s = int(max(0, ts))
            m, s = divmod(s, 60)
            return f"{m:02d}:{s:02d}"

        # Timer snapshot
        now_s = task_s = proj_s = 0
        active_count = 0
        try:
            active_count = len(db.active_task_urls())
        except Exception:
            active_count = 0
        if rows:
            cur = rows[current_index]
            if cur.url:
                snapshot = task_duration_cache.get(cur.url)
                if snapshot is None:
                    extra = db.task_duration_snapshot([cur.url])
                    snapshot = extra.get(cur.url)
                    if snapshot is not None:
                        task_duration_cache[cur.url] = snapshot
                if snapshot:
                    now_s = snapshot.get('current', 0)
                    task_s = snapshot.get('total', 0)
            if cur.project_title:
                proj_s = db.project_total_seconds(cur.project_title)

        active_search_val = (search_buffer if in_search else search_term) or '-'

        heading_style = _style_class('summary.title')
        label_style = _style_class('summary.label')
        value_style = _style_class('summary.value')
        accent_style = _style_class('summary.accent')

        fr: List[Tuple[str, str]] = []

        is_horizontal_layout = current_layout_name == 'horizontal'

        def add_heading(text: str, *, leading_blank: bool = False) -> None:
            if leading_blank and fr and not fr[-1][1].endswith("\n\n"):
                fr.append(("", "\n"))
            fr.append((heading_style, f"{text}\n"))

        def value_limit(columns: int, requested: Optional[int] = None) -> int:
            if requested is not None:
                return requested
            return 30 if columns > 1 else 24

        def render_rows(entries: List[Tuple[str, str, str]], *, label_width: int, value_cap: Optional[int] = None, label_style_override: Optional[str] = None, value_style_override: Optional[str] = None) -> None:
            if not entries:
                return
            columns = 2 if is_horizontal_layout else 1
            cap = value_limit(columns, value_cap)
            col_sep = '   '
            bucket: List[Tuple[str, str, str]] = []

            for idx, entry in enumerate(entries):
                bucket.append(entry)
                if len(bucket) == columns or idx == len(entries) - 1:
                    for col_idx, (icon, label, value) in enumerate(bucket):
                        label_text = f"{icon} {label}" if icon else label
                        label_block = _pad_display(label_text, label_width)
                        value_block = _truncate(value, cap)
                        fr.append((label_style_override or label_style, label_block))
                        fr.append((value_style_override or value_style, f" {value_block}"))
                        if col_idx < len(bucket) - 1:
                            fr.append(("", col_sep))
                    fr.append(("", "\n"))
                    bucket = []

        add_heading('Overview')
        overview_rows = [
            ('👤', 'User', cfg.user),
            ('📝', 'Tasks', f"{total} • Done {done_ct}"),
            ('⏱', 'Now', _fmt_mmss(now_s)),
            ('🧩', 'Task', _fmt_hm(task_s)),
            ('📦', 'Project', _fmt_hm(proj_s)),
            ('⚡', 'Active', str(active_count)),
        ]
        render_rows(overview_rows, label_width=14 if is_horizontal_layout else 12)

        add_heading('Filters', leading_blank=True)
        filter_rows = [
            ('🔎', 'Search', active_search_val),
            ('📁', 'Project', project_cycle or 'All'),
            ('☑️', 'Done', 'Hide' if hide_done else 'Show'),
            ('🚫', 'No-Date', 'Hide' if hide_no_date else 'Show'),
            ('↕', 'Sort', sort_presets[sort_index]['name']),
        ]
        if date_max:
            filter_rows.insert(2, ('📅', 'Date Max', date_max))
        render_rows(filter_rows, label_width=14 if is_horizontal_layout else 12, value_cap=36 if is_horizontal_layout else 24)

        # Top 5 projects by time (30d)
        proj_cache_entry = summary_cache['project']
        if (now_mon - float(proj_cache_entry.get('ts', 0.0))) >= SUMMARY_CACHE_TTL or not proj_cache_entry.get('tops'):
            try:
                proj_data = db.aggregate_project_totals(since_days=30)
            except Exception:
                proj_data = proj_cache_entry.get('data', {}) or {}
            else:
                proj_cache_entry['data'] = proj_data
                proj_cache_entry['tops'] = sorted(proj_data.items(), key=lambda kv: kv[1], reverse=True)[:5]
            proj_cache_entry['ts'] = now_mon
        project_tops = proj_cache_entry.get('tops', []) or []
        if project_tops:
            add_heading('Top Projects (30d)', leading_blank=True)
            project_rows = [('•', name or '-', _fmt_hm(secs)) for name, secs in project_tops]
            render_rows(project_rows, label_width=22 if is_horizontal_layout else 18, value_cap=12, label_style_override=accent_style)

        label_cache_entry = summary_cache['label']
        if (now_mon - float(label_cache_entry.get('ts', 0.0))) >= SUMMARY_CACHE_TTL or not label_cache_entry.get('tops'):
            try:
                label_data = db.aggregate_label_totals(since_days=30)
            except Exception:
                label_data = label_cache_entry.get('data', {}) or {}
            else:
                label_cache_entry['data'] = label_data
                label_cache_entry['tops'] = sorted(label_data.items(), key=lambda kv: kv[1], reverse=True)[:5]
            label_cache_entry['ts'] = now_mon
        label_tops = label_cache_entry.get('tops', []) or []
        if label_tops:
            add_heading('Top Labels (30d)', leading_blank=True)
            label_rows = [('•', name or '-', _fmt_hm(secs)) for name, secs in label_tops]
            render_rows(label_rows, label_width=22 if is_horizontal_layout else 18, value_cap=12, label_style_override=accent_style)

        if fr and fr[-1][1].endswith("\n"):
            fr[-1] = (fr[-1][0], fr[-1][1][:-1])
        return fr

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
                snapshot = task_duration_cache.get(t.url)
                if snapshot is None:
                    extra = db.task_duration_snapshot([t.url])
                    snapshot = extra.get(t.url)
                    if snapshot is not None:
                        task_duration_cache[t.url] = snapshot
                if snapshot:
                    now_s = snapshot.get('current', 0)
                    task_s = snapshot.get('total', 0)
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
        sort_label = _truncate(sort_presets[sort_index]['name'], 20)
        txt = f"{timers}| Date: {today_date.isoformat()}  | Project: {_truncate(active_proj,30)}  | View: {view_label}  | Sort: {sort_label}  | Shown: {total}  | Search: {_truncate(active_search,30)} "
        return [("reverse", txt)]
    top_status_control = FormattedTextControl(text=lambda: build_top_status())
    top_status_window = Window(height=1, content=top_status_control)
    stats_control = FormattedTextControl(text=lambda: summarize())

    def _build_stats_window(layout_name: str) -> Window:
        panel_style = _style_class('summary.panel') or ''
        if layout_name == 'horizontal':
            return Window(content=stats_control, wrap_lines=False, always_hide_cursor=True, style=panel_style)
        return Window(width=32, content=stats_control, wrap_lines=False, always_hide_cursor=True, style=panel_style)

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

    def _parse_session_dt(raw: Optional[str]) -> Optional[dt.datetime]:
        if not raw:
            return None
        try:
            dt_val = dt.datetime.fromisoformat(raw)
        except Exception:
            try:
                dt_val = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except Exception:
                return None
        if dt_val.tzinfo is None:
            local_tz = dt.datetime.now(dt.timezone.utc).astimezone().tzinfo
            if local_tz is not None:
                dt_val = dt_val.replace(tzinfo=local_tz)
        return dt_val.astimezone()

    def _fmt_session_dt(value: Optional[dt.datetime]) -> str:
        if not value:
            return '-'
        return value.astimezone().strftime('%Y-%m-%d %H:%M')

    def _refresh_session_editor(preserve_id: Optional[int] = None) -> None:
        task_url = session_state.get('task_url') if session_state else None
        entries: List[Dict[str, object]] = []
        selected_id = preserve_id or session_state.get('selected_id') if session_state else None
        if task_url:
            try:
                raw_sessions = db.get_sessions_for_task(task_url)
            except Exception as exc:
                session_state['message'] = f"Load failed: {exc}"  # type: ignore[index]
                raw_sessions = []
            now = dt.datetime.now(dt.timezone.utc).astimezone()
            for row in raw_sessions:
                sid = row.get('id')
                try:
                    sid_int = int(sid) if sid is not None else None
                except Exception:
                    sid_int = None
                st_raw = row.get('started_at')
                en_raw = row.get('ended_at')
                st_dt = _parse_session_dt(st_raw)
                en_dt = _parse_session_dt(en_raw)
                duration = 0
                if st_dt:
                    start_ref = st_dt.astimezone(dt.timezone.utc)
                    end_for_calc = (en_dt or now).astimezone(dt.timezone.utc)
                    duration = max(0, int((end_for_calc - start_ref).total_seconds()))
                entries.append({
                    'id': sid_int,
                    'start_raw': st_raw,
                    'end_raw': en_raw,
                    'start_dt': st_dt,
                    'end_dt': en_dt,
                    'start_display': _fmt_session_dt(st_dt),
                    'end_display': _fmt_session_dt(en_dt) if en_dt else ('running...' if en_raw is None else '-'),
                    'duration': duration,
                    'open': en_raw is None,
                })
        session_state['sessions'] = entries  # type: ignore[index]
        if not entries:
            session_state['cursor'] = 0  # type: ignore[index]
            session_state['selected_id'] = None  # type: ignore[index]
            session_state['total_duration'] = 0  # type: ignore[index]
            return
        target_id = selected_id
        if target_id is None or not any(e.get('id') == target_id for e in entries):
            target_id = entries[0].get('id')
        idx = 0
        for i, entry in enumerate(entries):
            if entry.get('id') == target_id:
                idx = i
                break
        session_state['cursor'] = idx  # type: ignore[index]
        session_state['selected_id'] = entries[idx].get('id')  # type: ignore[index]
        session_state['total_duration'] = sum(int(e.get('duration') or 0) for e in entries)  # type: ignore[index]

    def _current_session() -> Optional[Dict[str, object]]:
        sessions = session_state.get('sessions') if session_state else []
        if not sessions:
            return None
        idx = session_state.get('cursor', 0)
        idx = max(0, min(int(idx), len(sessions)-1))
        return sessions[idx]

    def _set_session_message(msg: str) -> None:
        session_state['message'] = msg  # type: ignore[index]

    def _format_input_value(dt_val: Optional[dt.datetime]) -> str:
        if not dt_val:
            return ''
        return dt_val.astimezone().strftime('%Y-%m-%d %H:%M')

    def _parse_user_datetime(value: str, fallback: Optional[dt.datetime]) -> Optional[dt.datetime]:
        raw = (value or '').strip()
        if not raw:
            return None
        if raw.lower() == 'now':
            return dt.datetime.now(dt.timezone.utc).astimezone()
        candidates = [raw, raw.replace(' ', 'T')]
        tz_hint = (fallback.tzinfo if fallback and fallback.tzinfo else dt.datetime.now(dt.timezone.utc).astimezone().tzinfo)
        for cand in candidates:
            try:
                dt_val = dt.datetime.fromisoformat(cand)
            except Exception:
                continue
            if dt_val.tzinfo is None and tz_hint is not None:
                dt_val = dt_val.replace(tzinfo=tz_hint)
            return dt_val.astimezone()
        return None

    def _move_session_cursor(delta: int) -> None:
        sessions = session_state.get('sessions') if session_state else []
        if not sessions:
            session_state['cursor'] = 0  # type: ignore[index]
            session_state['selected_id'] = None  # type: ignore[index]
            return
        idx = int(session_state.get('cursor', 0) or 0)
        idx = (idx + delta) % len(sessions)
        session_state['cursor'] = idx  # type: ignore[index]
        session_state['selected_id'] = sessions[idx].get('id')  # type: ignore[index]
        invalidate()

    def open_session_editor() -> None:
        nonlocal edit_sessions_mode, session_float, session_state, status_line, detail_mode, show_report, in_search, in_date_filter
        rows = filtered_rows()
        if not rows:
            status_line = "No task selected"
            invalidate()
            return
        task = rows[current_index]
        if not task.url:
            status_line = "Selected task missing URL"
            invalidate()
            return
        if edit_task_mode:
            close_task_editor(None)
        edit_sessions_mode = True
        detail_mode = False
        show_report = False
        in_search = False
        in_date_filter = False
        session_state = {
            'task_url': task.url,
            'task_title': task.title or task.url,
            'project_title': task.project_title or '',
            'cursor': 0,
            'sessions': [],
            'edit_field': None,
            'input': '',
            'message': '',
            'selected_id': None,
            'total_duration': 0,
        }
        _refresh_session_editor()
        if session_float and session_float in floats:
            floats.remove(session_float)
        session_float = Float(content=session_window, top=2, left=4)
        floats.append(session_float)
        status_line = 'Timer sessions editor open'
        invalidate()

    def close_session_editor(message: Optional[str] = None) -> None:
        nonlocal edit_sessions_mode, session_float, session_state, status_line
        if session_float and session_float in floats:
            floats.remove(session_float)
        session_float = None
        edit_sessions_mode = False
        session_state = {}
        if message is not None:
            status_line = message
        invalidate()

    def _begin_session_edit(field: str) -> None:
        if session_state.get('edit_field') is not None:
            return
        current = _current_session()
        if not current:
            _set_session_message('No session selected')
            invalidate()
            return
        if field == 'start':
            default_dt = current.get('start_dt')
        else:
            default_dt = current.get('end_dt')
        session_state['edit_field'] = field  # type: ignore[index]
        session_state['input'] = _format_input_value(default_dt)  # type: ignore[index]
        _set_session_message('')
        invalidate()

    def _cancel_session_edit(message: Optional[str] = None) -> None:
        session_state['edit_field'] = None  # type: ignore[index]
        session_state['input'] = ''  # type: ignore[index]
        if message is not None:
            _set_session_message(message)
        invalidate()

    def _commit_session_edit() -> None:
        nonlocal status_line
        field = session_state.get('edit_field')
        if not field:
            return
        current = _current_session()
        if not current:
            _cancel_session_edit('No session selected')
            return
        raw_input = session_state.get('input', '')
        start_dt = current.get('start_dt')
        end_dt = current.get('end_dt')
        session_id = current.get('id')
        try:
            session_id_int = int(session_id)
        except Exception:
            _cancel_session_edit('Session metadata missing id')
            return
        try:
            if field == 'start':
                new_start = _parse_user_datetime(raw_input, start_dt or end_dt)
                if not new_start:
                    _set_session_message('Invalid start timestamp')
                    return
                if end_dt and new_start > end_dt:
                    _set_session_message('Start must be before end')
                    return
                iso_val = new_start.astimezone(dt.timezone.utc).isoformat(timespec='seconds')
                db.update_session_times(session_id_int, started_at=iso_val)
                _reset_timer_caches()
                msg = 'Start updated'
            else:
                if not raw_input.strip():
                    new_end = None
                else:
                    new_end = _parse_user_datetime(raw_input, end_dt or start_dt or dt.datetime.now(dt.timezone.utc).astimezone())
                if new_end and start_dt and new_end < start_dt:
                    _set_session_message('End must be after start')
                    return
                iso_val = new_end.astimezone(dt.timezone.utc).isoformat(timespec='seconds') if new_end else None
                db.update_session_times(session_id_int, ended_at=iso_val)
                _reset_timer_caches()
                msg = 'End updated' if new_end else 'End cleared'
            _cancel_session_edit()
            _refresh_session_editor(preserve_id=session_id_int)
            _set_session_message(msg)
            status_line = msg
            invalidate()
        except Exception as exc:
            _set_session_message(f'Update failed: {exc}')
            invalidate()

    def _adjust_session_end(minutes: int) -> None:
        nonlocal status_line
        current = _current_session()
        if not current:
            _set_session_message('No session selected')
            return
        start_dt = current.get('start_dt')
        end_dt = current.get('end_dt') or dt.datetime.now(dt.timezone.utc).astimezone()
        session_id = current.get('id')
        try:
            session_id_int = int(session_id)
        except Exception:
            _set_session_message('Session metadata missing id')
            invalidate()
            return
        new_end = end_dt + dt.timedelta(minutes=minutes)
        if start_dt and new_end < start_dt:
            new_end = start_dt
        iso_val = new_end.astimezone(dt.timezone.utc).isoformat(timespec='seconds')
        try:
            db.update_session_times(session_id_int, ended_at=iso_val)
        except Exception as exc:
            _set_session_message(f'Adjust failed: {exc}')
            return
        _reset_timer_caches()
        _refresh_session_editor(preserve_id=session_id_int)
        msg = f'End adjusted by {minutes:+d} min'
        _set_session_message(msg)
        status_line = msg
        invalidate()

    def _delete_current_session() -> None:
        nonlocal status_line
        current = _current_session()
        if not current:
            _set_session_message('No session selected')
            invalidate()
            return
        session_id = current.get('id')
        try:
            session_id_int = int(session_id)
        except Exception:
            _set_session_message('Session metadata missing id')
            invalidate()
            return
        try:
            db.delete_session(session_id_int)
        except Exception as exc:
            _set_session_message(f'Delete failed: {exc}')
            return
        _reset_timer_caches()
        _refresh_session_editor()
        _set_session_message('Session deleted')
        status_line = 'Session deleted'
        invalidate()


    def build_session_editor_text() -> List[Tuple[str, str]]:
        if not edit_sessions_mode:
            return []
        title = session_state.get('task_title') or session_state.get('task_url') or 'Timer Sessions'
        project_title = session_state.get('project_title') or ''
        sessions = session_state.get('sessions') or []
        cursor = int(session_state.get('cursor', 0) or 0)
        total_secs = int(session_state.get('total_duration', 0) or 0)
        edit_field = session_state.get('edit_field')
        buffer_val = session_state.get('input', '')
        message = session_state.get('message', '')
        lines: List[str] = []
        lines.append(f"Timer Sessions - {title}")
        if project_title:
            lines.append(f"Project: {project_title}")
        lines.append("")
        if not sessions:
            lines.append("  No recorded sessions for this task.")
        else:
            lines.append(f"  Total logged: {_fmt_hms_full(total_secs)}")
            lines.append("")
            for idx, sess in enumerate(sessions):
                marker = '>' if idx == cursor else ' '
                start_disp = sess.get('start_display') or '-'
                end_disp = sess.get('end_display') or '-'
                dur_disp = _fmt_hms_full(int(sess.get('duration') or 0))
                running_flag = ' ⏱' if sess.get('open') else ''
                lines.append(f"{marker} {idx+1:02d}  {start_disp}  ->  {end_disp:<19}  {dur_disp:>9}{running_flag}")
        lines.append("")
        if edit_field:
            lines.append(f"Editing {edit_field} (Enter=save | Esc=cancel)")
            lines.append(f"Value: {buffer_val}")
        else:
            lines.append("Commands: Enter=end  S=start  +/- adjust 5m  </> adjust 1m  x/Del delete  R=refresh  Esc/q close")
            lines.append("Tip: Provide 'YYYY-MM-DD HH:MM' or ISO timestamps when editing.")
        if message:
            lines.append("")
            lines.append(message)
        block = "\n".join(lines)
        if '\n' in block:
            head, rest = block.split('\n', 1)
            return [("bold", head), ("", "\n" + rest)]
        return [("bold", block)]

    session_control = FormattedTextControl(text=lambda: build_session_editor_text())
    session_window = Window(width=96, height=24, content=session_control, wrap_lines=False, always_hide_cursor=True, style="bg:#202020 #ffffff")

    def build_task_edit_text() -> List[Tuple[str, str]]:
        if not edit_task_mode:
            return []
        rows = filtered_rows()
        row = rows[current_index] if rows else None
        if row and task_edit_state.get('task_url') != row.url:
            _refresh_task_editor_state(preserve_cursor=False, do_invalidate=False)
        fields = task_edit_state.get('fields') or []
        cursor = int(task_edit_state.get('cursor', 0) or 0)
        cursor = max(0, min(cursor, len(fields)-1)) if fields else 0
        mode = task_edit_state.get('mode', 'list')

        segments: List[Tuple[str, str]] = []

        def add_line(text: str, style: str = 'class:editor.text') -> None:
            segments.append((style, text + '\n'))

        def add_blank() -> None:
            segments.append(('', '\n'))

        if row:
            header = f"📋 {row.title or row.url}"
        else:
            header = "📋 Task Field Editor"
        add_line(header, 'class:editor.header')
        if row:
            add_line(f"🏷️ Project: {row.project_title or '-'}", 'class:editor.meta')
            add_line(f"🔗 URL: {row.url}", 'class:editor.meta')

        if not fields:
            add_blank()
            add_line("No editable fields", 'class:editor.warning')
        else:
            add_blank()
            for idx, field in enumerate(fields):
                marker = '➤' if idx == cursor else ' '
                ftype = field.get('type')
                if ftype == 'priority':
                    opts = field.get('options') or []
                    index = max(0, min(field.get('index', 0), len(opts)-1)) if opts else 0
                    value = opts[index].get('name') if opts else '(no options)'
                    if opts and row and getattr(row, 'priority_dirty', 0):
                        value = (value or '-') + '*'
                elif ftype == 'assignees':
                    vals = field.get('value') or []
                    if isinstance(vals, list):
                        value = ', '.join(vals) or '-'
                    else:
                        value = str(vals) or '-'
                elif ftype == 'labels':
                    vals = field.get('value') or []
                    if isinstance(vals, list):
                        value = ', '.join(vals) or '-'
                    else:
                        value = str(vals) or '-'
                elif ftype == 'priority-text':
                    value = field.get('value') or '-'
                elif ftype == 'comment':
                    value = '(add comment)'
                else:
                    value = field.get('value', '')
                value = value or '-'
                style = 'class:editor.field.cursor' if idx == cursor else 'class:editor.field'
                add_line(f" {marker} {field.get('name')} : {value}", style)

        if mode == 'edit-date-calendar' and fields:
            editing = task_edit_state.get('editing') or {}
            iso = editing.get('calendar_date') or fields[cursor].get('value') or dt.date.today().isoformat()
            try:
                cursor_date = dt.date.fromisoformat(iso)
            except Exception:
                cursor_date = dt.date.today()
            cal = calendar.Calendar(firstweekday=0)
            add_blank()
            add_line(f"🗓️ Select {fields[cursor].get('name')} (h/l day, j/k week, </> month, t today)", 'class:editor.instructions')
            add_line(cursor_date.strftime("%B %Y"), 'class:editor.calendar')
            add_line(" Mo Tu We Th Fr Sa Su", 'class:editor.calendar')
            for week in cal.monthdatescalendar(cursor_date.year, cursor_date.month):
                row_cells: List[str] = []
                for day in week:
                    label = f"{day.day:2d}"
                    if day == cursor_date:
                        cell = f"[{label}]"
                    elif day.month != cursor_date.month:
                        cell = f"({label})"
                    else:
                        cell = f" {label} "
                    row_cells.append(cell)
                add_line("".join(row_cells), 'class:editor.calendar')
            add_blank()
            add_line("Enter=save · Esc=cancel", 'class:editor.instructions')
        elif mode == 'priority-select':
            field = fields[cursor] if cursor < len(fields) else None
            opts = (field or {}).get('options') or []
            idx = (field or {}).get('index', 0)
            add_blank()
            add_line("⚡ Select priority (j/k move, Enter=save, Esc=cancel)", 'class:editor.instructions')
            for i, opt in enumerate(opts):
                marker = '➤' if i == idx else ' '
                name = (opt.get('name') if isinstance(opt, dict) else None) or '(option)'
                style = 'class:editor.priority.cursor' if i == idx else 'class:editor.priority'
                add_line(f"   {marker} {name}", style)
        elif mode == 'edit-assignees':
            add_blank()
            add_line("👥 Assignees (comma-separated GitHub logins)", 'class:editor.instructions')
            add_line(f"✏️  {task_edit_state.get('input', '')}", 'class:editor.entry')
            add_line("Enter=save · Esc=cancel", 'class:editor.instructions')
        elif mode == 'edit-labels':
            repo_label = task_edit_state.get('labels_repo') or '(unknown repo)'
            add_blank()
            if task_edit_state.get('labels_loading'):
                add_line(f"⏳ Loading labels for {repo_label}…", 'class:editor.instructions')
            else:
                labels_error = task_edit_state.get('labels_error') or ''
                choices = task_edit_state.get('label_choices') or []
                selected = task_edit_state.get('labels_selected') or set()
                if not isinstance(selected, set):
                    selected = set(selected)
                idx = max(0, min(len(choices)-1, task_edit_state.get('label_index', 0))) if choices else 0
                if labels_error:
                    add_line(f"⚠️ {labels_error}", 'class:editor.warning')
                if choices:
                    add_line("🏷️ Use j/k to move, Space to toggle, Enter=save, Esc=cancel", 'class:editor.instructions')
                    for i, name in enumerate(choices):
                        pointer = '➤' if i == idx else ' '
                        checkbox = '☑' if name in selected else '☐'
                        if i == idx and name in selected:
                            style = 'class:editor.label.cursor.selected'
                        elif i == idx:
                            style = 'class:editor.label.cursor'
                        elif name in selected:
                            style = 'class:editor.label.selected'
                        else:
                            style = 'class:editor.label'
                        add_line(f" {pointer} {checkbox} {name}", style)
                else:
                    add_line(f"No labels available for {repo_label}", 'class:editor.warning')
                    add_line("Enter=save (keep current) · Esc=cancel", 'class:editor.instructions')
                if selected:
                    add_blank()
                    add_line('Selection: ' + ', '.join(sorted(selected)), 'class:editor.meta')
        elif mode == 'edit-comment':
            add_blank()
            add_line("💬 New comment", 'class:editor.instructions')
            add_line(f"✏️  {task_edit_state.get('input', '')}", 'class:editor.entry')
            add_line("Enter=post · Esc=cancel", 'class:editor.instructions')
        else:
            add_blank()
            add_line("🧭 Use j/k to select a field. Enter=edit · Esc/Q=close", 'class:editor.instructions')
            add_line("Space toggles options when available.", 'class:editor.instructions')

        message = task_edit_state.get('message') or ''
        if message:
            add_blank()
            add_line(f"💡 {message}", 'class:editor.message')

        if segments:
            style_last, text_last = segments[-1]
            if text_last.endswith('\n'):
                segments[-1] = (style_last, text_last[:-1])
                if not segments[-1][1]:
                    segments.pop()
        return segments

    task_edit_control = FormattedTextControl(text=lambda: build_task_edit_text())
    task_edit_body = Window(
        width=Dimension(preferred=100, max=120),
        height=Dimension(preferred=32, max=50),
        content=task_edit_control,
        wrap_lines=True,
        always_hide_cursor=True,
        style="class:editor.body",
    )
    task_edit_window = Frame(body=task_edit_body, title="🛠 Task Field Editor", style="class:editor.frame")

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
        lines.append("")
        lines.append("Top labels (window):")
        label_totals = db.aggregate_label_totals(since_days=since_days)
        label_tops = sorted(label_totals.items(), key=lambda x: x[1], reverse=True)[:10]
        if not label_tops:
            lines.append("  (no data)")
        else:
            maxv_lab = max(v for _, v in label_tops) or 1
            for name, secs in label_tops:
                nm = (name or '-')
                bar = '█' * max(1, int(30 * secs / maxv_lab))
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
        labels_list = _task_labels(t)
        labels_display = ", ".join(labels_list) if labels_list else "-"
        priority_display = (t.priority or '-') + ('*' if getattr(t, 'priority_dirty', 0) else '')
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
            f"Labels:  {labels_display}",
            f"Priority:{priority_display}",
            f"URL:     {t.url}",
            f"Start:   {t.start_date} ({t.start_field})",
            f"Focus:   {t.focus_date or '-'} ({t.focus_field or '-'})",
            f"Iter:    {iter_display}{iter_meta}",
            f"Status:  {t.status}",
            f"Done:    {'Yes' if t.is_done else 'No'}",
            f"Pending: {'Yes' if t.status_dirty else 'No'}",
            f"Assigned:{'Yes' if t.assigned_to_me else 'No'}",
            f"Created: {'Yes' if t.created_by_me else 'No'}",
            f"FieldID: {t.status_field_id or '-'}",
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
        if in_date_filter:
            mode = "🗓️ DATE"
        elif in_search:
            mode = "🔎 SEARCH"
        elif detail_mode:
            mode = "📄 DETAIL"
        elif show_report:
            mode = "📊 REPORT"
        elif edit_sessions_mode:
            mode = "TIMER EDIT"
        elif show_help:
            mode = "❓ HELP"
        else:
            mode = "🧭 BROWSE"
        # Minimal, elegant bottom bar with live timers only
        rows = filtered_rows()
        now_s = task_s = proj_s = 0
        active_count = 0
        try:
            active_count = len(db.active_task_urls())
        except Exception:
            active_count = 0
        if rows:
            t = rows[current_index]
            if t.url:
                now_s = db.task_current_elapsed_seconds(t.url)
                task_s = db.task_total_seconds(t.url)
            if t.project_title:
                proj_s = db.project_total_seconds(t.project_title)
        def _mmss(s:int)->str:
            s = int(max(0, s)); m, s = divmod(s, 60); return f"{m:02d}:{s:02d}"
        def _hm(s:int)->str:
            s = int(max(0, s)); h, r = divmod(s, 3600); m, _ = divmod(r, 60); return f"{h:d}:{m:02d}"
        theme_label = theme_presets[current_theme_index].name if theme_presets else 'Default'
        base = f" {mode}  ⏱ {_mmss(now_s)}  🧩 {_hm(task_s)}  📦 {_hm(proj_s)}  🟢 {active_count}  🎨 {theme_label}"
        if status_line:
            base += "  " + status_line
        return base

    from prompt_toolkit.layout.containers import Float, FloatContainer
    add_control = FormattedTextControl(text=lambda: build_add_overlay())
    add_window = Window(width=92, height=Dimension(preferred=26, max=44), content=add_control, wrap_lines=False, always_hide_cursor=True, style="bg:#202020 #ffffff")
    floats = []

    def close_add_mode(message: Optional[str] = None):
        nonlocal add_mode, add_state, add_float, status_line
        if add_float and add_float in floats:
            floats.remove(add_float)
        add_float = None
        task = add_state.get('repo_metadata_task') if isinstance(add_state, dict) else None
        if isinstance(task, asyncio.Task):
            task.cancel()
        add_mode = False
        add_state = {}
        if message is not None:
            status_line = message
        invalidate()

    def close_task_editor(message: Optional[str] = None) -> None:
        nonlocal edit_task_mode, task_edit_state, task_edit_float, status_line
        if task_edit_float and task_edit_float in floats:
            floats.remove(task_edit_float)
        task_edit_float = None
        task = task_edit_state.get('labels_task') if isinstance(task_edit_state, dict) else None
        if isinstance(task, asyncio.Task):
            task.cancel()
        edit_task_mode = False
        task_edit_state = {}
        if message is not None:
            status_line = message
        invalidate()

    def open_task_editor() -> None:
        nonlocal edit_task_mode, task_edit_state, task_edit_float, status_line, detail_mode, show_report, in_search, in_date_filter
        rows = filtered_rows()
        if not rows:
            status_line = "No task selected"
            invalidate(); return
        row = rows[current_index]
        if not (row.project_id and row.item_id):
            status_line = "Task missing project metadata"
            invalidate(); return
        if edit_sessions_mode:
            close_session_editor(None)
        fields = _build_task_edit_fields_from_row(row)
        if not fields:
            status_line = "No editable fields"
            invalidate(); return
        edit_task_mode = True
        detail_mode = False
        show_report = False
        in_search = False
        in_date_filter = False
        task_edit_state = {
            'fields': fields,
            'cursor': 0,
            'mode': 'list',
            'input': '',
            'message': 'Use j/k to select, Enter to edit, Esc to close',
            'task_url': row.url,
            'editing': None,
        }
        if task_edit_float and task_edit_float in floats:
            floats.remove(task_edit_float)
        task_edit_float = Float(content=task_edit_window, top=3, left=4)
        floats.append(task_edit_float)
        status_line = 'Field editor open'
        invalidate()

    def _cancel_task_edit(message: Optional[str] = None) -> None:
        if not edit_task_mode:
            return
        mode = task_edit_state.get('mode')
        editing = task_edit_state.get('editing') or {}
        fields = task_edit_state.get('fields') or []
        idx = editing.get('field_idx')
        if mode == 'edit-date-calendar' and idx is not None and idx < len(fields):
            prev_val = editing.get('prev_value', fields[idx].get('value', ''))
            fields[idx]['value'] = prev_val
        if mode == 'priority-select' and idx is not None and idx < len(fields):
            prev_idx = editing.get('prev_index', fields[idx].get('index', 0))
            fields[idx]['index'] = prev_idx
        if mode == 'edit-assignees' and idx is not None and idx < len(fields):
            prev_list = editing.get('prev_value', fields[idx].get('value', []))
            fields[idx]['value'] = list(prev_list) if isinstance(prev_list, list) else []
        if mode == 'edit-labels' and idx is not None and idx < len(fields):
            prev_list = editing.get('prev_value', fields[idx].get('value', []))
            fields[idx]['value'] = list(prev_list) if isinstance(prev_list, list) else []
            task = task_edit_state.get('labels_task')
            if isinstance(task, asyncio.Task):
                task.cancel()
            task_edit_state['labels_task'] = None
            task_edit_state['labels_loading'] = False
            task_edit_state['labels_error'] = ''
        task_edit_state['mode'] = 'list'
        task_edit_state['input'] = ''
        task_edit_state['editing'] = None
        task_edit_state['message'] = message or 'Edit cancelled'
        invalidate()

    def _start_task_field_edit() -> None:
        if not edit_task_mode:
            return
        fields = task_edit_state.get('fields') or []
        if not fields:
            task_edit_state['message'] = 'No editable fields'
            invalidate(); return
        cursor = int(task_edit_state.get('cursor', 0) or 0)
        cursor = max(0, min(cursor, len(fields)-1))
        field = fields[cursor]
        ftype = field.get('type')
        if ftype == 'date':
            raw = field.get('value') or ''
            try:
                base_date = dt.date.fromisoformat(raw)
            except Exception:
                base_date = dt.date.today()
            editing_state = {
                'field_idx': cursor,
                'prev_value': field.get('value', ''),
                'calendar_date': base_date.isoformat(),
            }
            field['value'] = base_date.isoformat()
            task_edit_state['mode'] = 'edit-date-calendar'
            task_edit_state['editing'] = editing_state
            task_edit_state['message'] = 'Use h/j/k/l to move, </> month, t=Today, Enter=save, Esc=cancel'
        elif ftype == 'priority':
            options = field.get('options') or []
            if not options:
                task_edit_state['message'] = 'Priority options unavailable'
                return
            task_edit_state['mode'] = 'priority-select'
            task_edit_state['editing'] = {'field_idx': cursor, 'prev_index': field.get('index', 0)}
            task_edit_state['message'] = 'Use j/k to choose priority (Enter=save, Esc=cancel)'
        elif ftype == 'assignees':
            current = field.get('value') or []
            if not isinstance(current, list):
                current = []
            task_edit_state['mode'] = 'edit-assignees'
            task_edit_state['input'] = ', '.join(current)
            task_edit_state['editing'] = {'field_idx': cursor, 'prev_value': list(current)}
            task_edit_state['message'] = 'Comma-separated GitHub logins (Enter=save, Esc=cancel)'
        elif ftype == 'labels':
            rows = filtered_rows()
            if not rows:
                task_edit_state['message'] = 'No task selected'
                invalidate(); return
            row = rows[current_index]
            repo_full = (row.repo or '').strip()
            parts = _parse_issue_url(row.url)
            if (not repo_full) and parts:
                repo_full = f"{parts[0]}/{parts[1]}"
            if not repo_full:
                task_edit_state['message'] = 'Repository unknown; cannot edit labels'
                return
            if not token:
                task_edit_state['message'] = 'GITHUB_TOKEN required for labels'
                return
            current = field.get('value') or []
            if not isinstance(current, list):
                current = []
            existing_task = task_edit_state.get('labels_task')
            if isinstance(existing_task, asyncio.Task):
                existing_task.cancel()
            selected = {str(lbl).strip() for lbl in current if str(lbl).strip()}
            task_edit_state['mode'] = 'edit-labels'
            task_edit_state['input'] = ''
            task_edit_state['editing'] = {'field_idx': cursor, 'prev_value': list(current)}
            task_edit_state['label_choices'] = []
            task_edit_state['label_index'] = 0
            task_edit_state['labels_selected'] = set(selected)
            task_edit_state['labels_loading'] = True
            task_edit_state['labels_error'] = ''
            task_edit_state['labels_repo'] = repo_full
            task_edit_state['labels_task'] = asyncio.create_task(_load_label_choices_for_editor(repo_full, set(selected)))
            task_edit_state['message'] = f'Loading labels for {repo_full}…'
        elif ftype == 'comment':
            task_edit_state['mode'] = 'edit-comment'
            task_edit_state['input'] = ''
            task_edit_state['editing'] = {'field_idx': cursor}
            task_edit_state['message'] = 'Type comment text (Enter=post, Esc=cancel)'
        else:
            task_edit_state['message'] = 'Field not editable'
        invalidate()
    def _build_root_body() -> object:
        if current_layout_name == 'horizontal':
            return HSplit([
                table_window,
                Window(height=1, char='─'),
                _build_stats_window('horizontal'),
            ])
        return VSplit([
            table_window,
            Window(width=1, char='│'),
            _build_stats_window('vertical'),
        ])

    root_body = _build_root_body()
    root_content = HSplit([top_status_window, root_body, status_window])
    container = FloatContainer(content=root_content, floats=floats)

    def _refresh_root_body() -> None:
        nonlocal root_body, root_content
        new_body = _build_root_body()
        root_body = new_body
        updated = False
        try:
            if hasattr(root_content, 'children') and len(getattr(root_content, 'children')) >= 3:
                root_content.children[1] = new_body
                updated = True
        except Exception:
            updated = False
        if not updated:
            try:
                root_content = HSplit([top_status_window, new_body, status_window])
                container.content = root_content
            except Exception:
                pass

    app: Optional[Application] = None

    kb = KeyBindings()
    # Mode filters to enable/disable keybindings contextually
    is_search = Condition(lambda: in_search)
    is_date = Condition(lambda: in_date_filter)
    is_detail = Condition(lambda: detail_mode)
    is_add_mode = Condition(lambda: add_mode)
    is_session_input = Condition(lambda: edit_sessions_mode and session_state.get('edit_field') is not None)
    is_session_idle = Condition(lambda: edit_sessions_mode and session_state.get('edit_field') is None)
    is_task_edit_mode = Condition(lambda: edit_task_mode)
    is_task_edit_text = Condition(lambda: edit_task_mode and task_edit_state.get('mode') in ('edit-assignees', 'edit-comment'))
    is_task_edit_nav = Condition(lambda: edit_task_mode and task_edit_state.get('mode') in ('edit-date-calendar', 'priority-select', 'edit-labels'))
    is_task_edit_calendar = Condition(lambda: edit_task_mode and task_edit_state.get('mode') == 'edit-date-calendar')
    is_task_edit_priority = Condition(lambda: edit_task_mode and task_edit_state.get('mode') == 'priority-select')
    is_task_edit_labels = Condition(lambda: edit_task_mode and task_edit_state.get('mode') == 'edit-labels')
    is_task_edit_idle = Condition(lambda: edit_task_mode and task_edit_state.get('mode') == 'list')
    is_input_mode = Condition(lambda: in_search or in_date_filter or detail_mode or show_report or add_mode or edit_sessions_mode or edit_task_mode)
    is_normal = Condition(lambda: not (in_search or in_date_filter or detail_mode or show_report or add_mode or edit_sessions_mode or edit_task_mode))

    def invalidate():
        table_control.text = lambda: build_table_fragments()  # ensure recalculated
        stats_control.text = lambda: summarize()
        if app is not None:
            app.invalidate()

    def apply_theme(index: int, announce: bool = True) -> None:
        nonlocal current_theme_index, current_layout_name, style, status_line
        if not (0 <= index < len(theme_presets)):
            return
        if index == current_theme_index and not announce:
            return
        current_theme_index = index
        preset = theme_presets[index]
        current_layout_name = preset.layout or DEFAULT_THEME_LAYOUT
        _refresh_root_body()
        style = Style.from_dict(preset.style)
        if app is not None:
            app.style = style
        if announce:
            status_line = f"Theme: {preset.name}"
            _save_state()
        invalidate()

    for idx, key_name in enumerate(SHIFTED_DIGIT_KEYS):
        if idx >= len(theme_presets):
            break

        @kb.add(key_name, filter=is_normal)
        def _(event, index=idx):
            apply_theme(index)

    def _invalidate_summary_cache() -> None:
        summary_cache['project'].update({'ts': 0.0, 'data': {}, 'tops': []})
        summary_cache['label'].update({'ts': 0.0, 'data': {}, 'tops': []})

    def _reset_timer_caches() -> None:
        nonlocal task_duration_cache
        task_duration_cache = {}
        _invalidate_summary_cache()

    async def update_worker(status_msg: Optional[str] = None):
        nonlocal status_line, all_rows, current_index, today_date, update_in_progress, task_duration_cache
        if update_in_progress:
            return
        update_in_progress = True
        if status_msg:
            status_line = status_msg
        else:
            status_line = "Updating..."
        invalidate()

        def progress(done_val: int, total_val: int, line: str):
            nonlocal status_line
            status_line = line
            invalidate()

        try:
            loop = asyncio.get_running_loop()

            def do_fetch():
                if os.environ.get('MOCK_FETCH') == '1':
                    try:
                        logger.info("MOCK_FETCH enabled; generating mock tasks")
                    except Exception:
                        pass
                    rows = generate_mock_tasks(cfg)
                    progress(1, 1, '[########################################] 100% Done')
                    return FetchTasksResult(rows=rows, partial=False, message='Mock data loaded')
                if not token:
                    raise RuntimeError('TOKEN not set')
                try:
                    logger.info("Fetching tasks from GitHub… (cutoff=%s, include_unassigned=%s)", today_date, show_unassigned)
                except Exception:
                    pass
                return fetch_tasks_github(token, cfg, date_cutoff=today_date, progress=progress, include_unassigned=show_unassigned)

            fetch_result = await loop.run_in_executor(None, do_fetch)
            replaced_cache = False
            if fetch_result.partial:
                msg = fetch_result.message or 'Fetch returned partial results; cache kept'
                try:
                    logger.warning("Fetch returned partial results (%d rows); cache unchanged", len(fetch_result.rows))
                except Exception:
                    pass
                progress(len(fetch_result.rows), max(1, len(fetch_result.rows)), msg)
                status_line = msg
            else:
                rows = fetch_result.rows
                try:
                    logger.info("Fetched %d tasks; replacing DB rows", len(rows))
                except Exception:
                    pass
                db.replace_all(rows)
                replaced_cache = True
            try:
                today_date = dt.date.today()
                logger.debug("today_date refreshed after update: %s", today_date)
            except Exception:
                pass
            all_rows = load_all()
            _reset_timer_caches()
            current_index = 0 if all_rows else 0
            if replaced_cache:
                progress(len(rows), len(rows), 'Updated')
                try:
                    logger.info("Update finished successfully. Cached rows: %d", len(all_rows))
                except Exception:
                    pass
        except Exception as e:
            status_line = f"Error: {e}"
            try:
                logger.exception("Update failed")
            except Exception:
                pass
        finally:
            update_in_progress = False
            if edit_task_mode:
                _refresh_task_editor_state()
            invalidate()

    async def create_task_async(
        project_choice: Dict[str, object],
        title: str,
        start_val: str,
        end_val: str,
        focus_val: str,
        iteration_id: str,
        mode: str,
        repo_choice: Optional[Dict[str, str]],
        repo_manual: Optional[str],
        repo_full: str,
        labels: List[str],
        priority_label: str,
        priority_options: List[Dict[str, object]],
        assignees: List[str],
        comment: str,
    ):
        nonlocal status_line
        if not token:
            status_line = "GITHUB_TOKEN required to add tasks"
            invalidate()
            return
        loop = asyncio.get_running_loop()
        issue_url = ''
        try:
            project_id = project_choice.get('project_id') or ''
            if not project_id:
                raise RuntimeError('Project metadata missing ID')
            repo_source = repo_full or (repo_choice or {}).get('repo') or repo_manual or ''
            repo_id = ''
            assignee_node_ids: List[str] = []
            if mode == 'issue':
                repo_id = (repo_choice or {}).get('repo_id') or ''
                repo_label = (repo_choice or {}).get('repo') or repo_source
                if not repo_id:
                    lookup_source = repo_manual or repo_source
                    if not lookup_source:
                        raise RuntimeError('Repository metadata unavailable')
                    repo_lookup = await loop.run_in_executor(None, lambda: get_repo_id(token, lookup_source))
                    repo_id = repo_lookup.get('repo_id') or ''
                    repo_label = repo_lookup.get('repo') or lookup_source
                if not repo_id:
                    raise RuntimeError('Repository metadata unavailable')
                for login in assignees:
                    try:
                        user_id = get_user_node_id(token, login)
                        if user_id and user_id not in assignee_node_ids:
                            assignee_node_ids.append(user_id)
                    except Exception as exc:
                        logger.warning("Could not resolve user id for %s: %s", login, exc)
                issue_result = await loop.run_in_executor(None, lambda: create_issue(token, repo_id, title, '', assignee_node_ids))
                issue_id = issue_result.get('issue_id')
                issue_url = issue_result.get('url') or ''
                if not issue_id:
                    raise RuntimeError('Issue creation did not return id')
                item_id = await loop.run_in_executor(None, lambda: add_project_item(token, project_id, issue_id))
            else:
                item_id = await loop.run_in_executor(None, lambda: create_project_draft(token, project_id, title))
                if not item_id:
                    raise RuntimeError('GitHub did not return item id')
            # Dates
            start_field_id = project_choice.get('start_field_id') or ''
            if start_field_id:
                value = start_val.strip() or dt.date.today().isoformat()
                await loop.run_in_executor(None, lambda: set_project_date(token, project_id, item_id, start_field_id, value))
            if end_val.strip():
                end_field_id = project_choice.get('end_field_id') or ''
                if not end_field_id:
                    for candidate in ('end date', 'due date', 'target date', 'finish date'):
                        fid = await loop.run_in_executor(None, lambda name=candidate: get_project_field_id_by_name(token, project_id, name))
                        if fid:
                            project_choice['end_field_id'] = fid
                            end_field_id = fid
                            break
                if end_field_id:
                    await loop.run_in_executor(None, lambda: set_project_date(token, project_id, item_id, end_field_id, end_val.strip()))
            if focus_val.strip():
                focus_field_id = project_choice.get('focus_field_id') or ''
                if not focus_field_id:
                    focus_field_id = await loop.run_in_executor(None, lambda: get_project_field_id_by_name(token, project_id, 'focus day'))
                    if focus_field_id:
                        project_choice['focus_field_id'] = focus_field_id
                if focus_field_id:
                    await loop.run_in_executor(None, lambda: set_project_date(token, project_id, item_id, focus_field_id, focus_val.strip()))
            iteration_field_id = project_choice.get('iteration_field_id') or ''
            if iteration_id and iteration_field_id:
                await loop.run_in_executor(None, lambda: set_project_iteration(token, project_id, item_id, iteration_field_id, iteration_id))

            priority_field_id = project_choice.get('priority_field_id') or ''
            def _match_priority_option(label: str, options: List[Dict[str, object]]) -> str:
                if not label:
                    return ''
                norm = label.strip().lower()
                core = norm.split(':', 1)[-1].strip() if ':' in norm else norm
                for opt in options:
                    name = (opt.get('name') or '').strip()
                    if not name:
                        continue
                    low = name.lower()
                    if low == norm or low == core:
                        return opt.get('id') or ''
                for opt in options:
                    name_low = (opt.get('name') or '').strip().lower()
                    if core and core in name_low:
                        return opt.get('id') or ''
                return ''

            if priority_field_id and priority_label.strip():
                options = priority_options or project_choice.get('priority_options') or []
                if not options:
                    options = await loop.run_in_executor(None, lambda: get_project_field_options(token, priority_field_id))
                    if options:
                        project_choice['priority_options'] = options
                option_id = _match_priority_option(priority_label, options or [])
                if option_id:
                    await loop.run_in_executor(None, lambda: set_project_priority(token, project_id, item_id, priority_field_id, option_id))

            assignee_field_id = project_choice.get('assignee_field_id') or ''
            if assignee_field_id and assignees:
                project_user_ids: List[str] = []
                for login in assignees:
                    try:
                        uid = get_user_node_id(token, login)
                        if uid and uid not in project_user_ids:
                            project_user_ids.append(uid)
                    except Exception as exc:
                        logger.warning("Could not resolve user id for %s: %s", login, exc)
                if project_user_ids:
                    try:
                        await loop.run_in_executor(None, lambda: set_project_users(token, project_id, item_id, assignee_field_id, project_user_ids))
                    except Exception as exc:
                        logger.warning("Unable to set project users for %s: %s", assignee_field_id, exc)

            if mode == 'issue':
                if labels:
                    try:
                        await loop.run_in_executor(None, lambda: set_issue_labels(token, issue_url, labels))
                    except Exception as exc:
                        logger.warning("Unable to set labels for %s: %s", issue_url, exc)
                if assignees:
                    try:
                        await loop.run_in_executor(None, lambda: set_issue_assignees(token, issue_url, assignees))
                    except Exception as exc:
                        logger.warning("Unable to set assignees for %s: %s", issue_url, exc)
                if comment.strip():
                    try:
                        await loop.run_in_executor(None, lambda: add_issue_comment(token, issue_url, comment.strip()))
                    except Exception as exc:
                        logger.warning("Unable to add comment for %s: %s", issue_url, exc)
        except Exception as exc:
            status_line = f"Create failed: {exc}"
            try:
                logger.exception("Create task failed: %s", exc)
            except Exception:
                pass
        else:
            status_line = "Issue created; refreshing…" if mode == 'issue' else "Task created; refreshing…"
            asyncio.create_task(update_worker())
        finally:
            invalidate()

    @kb.add('q')
    def _(event):
        nonlocal detail_mode, in_search, search_buffer, show_report
        if edit_task_mode:
            if task_edit_state.get('mode') in ('edit-date-calendar', 'priority-select'):
                _cancel_task_edit('Edit cancelled')
            else:
                close_task_editor('Field editor closed')
            return
        if edit_sessions_mode:
            if session_state.get('edit_field') is not None:
                _cancel_session_edit('Edit cancelled')
            else:
                close_session_editor('Timer editor closed')
            return
        if add_mode:
            close_add_mode('Add cancelled')
            return
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
        if edit_task_mode:
            mode = task_edit_state.get('mode')
            fields = task_edit_state.get('fields') or []
            cursor = int(task_edit_state.get('cursor', 0) or 0)
            cursor = max(0, min(cursor, len(fields)-1)) if fields else 0
            if mode == 'edit-date-calendar':
                field = fields[cursor] if cursor < len(fields) else None
                editing = task_edit_state.get('editing') or {}
                if not field or editing.get('calendar_date') is None:
                    _cancel_task_edit('Field unavailable')
                    return
                value = editing.get('calendar_date') or field.get('value', '')
                if not value:
                    task_edit_state['message'] = 'Select a date'
                    invalidate(); return
                field['value'] = value
                task_edit_state['mode'] = 'list'
                task_edit_state['editing'] = None
                task_edit_state['message'] = f"Updating {field.get('name')}…"
                asyncio.create_task(_update_task_date(field.get('field_key', 'start'), value))
            elif mode == 'priority-select':
                field = fields[cursor] if cursor < len(fields) else None
                options = (field or {}).get('options') or []
                if not field or not options:
                    _cancel_task_edit('Priority options unavailable')
                    return
                idx = max(0, min(field.get('index', 0), len(options)-1))
                option_id = (options[idx].get('id') or '').strip()
                task_edit_state['mode'] = 'list'
                task_edit_state['editing'] = None
                task_edit_state['message'] = f"Updating {field.get('name')}…"
                asyncio.create_task(_change_priority(option_id=option_id))
            elif mode == 'edit-assignees':
                field = fields[cursor] if cursor < len(fields) else None
                value = task_edit_state.get('input', '')
                logins = [part.strip().lstrip('@') for part in value.replace(';', ',').split(',')]
                logins = [login for login in logins if login]
                if field is not None:
                    field['value'] = logins
                task_edit_state['mode'] = 'list'
                task_edit_state['input'] = ''
                task_edit_state['editing'] = None
                task_edit_state['message'] = 'Updating assignees…'
                asyncio.create_task(_apply_assignees(logins))
            elif mode == 'edit-labels':
                if task_edit_state.get('labels_loading'):
                    task_edit_state['message'] = 'Labels still loading…'
                    invalidate(); return
                field = fields[cursor] if cursor < len(fields) else None
                selected = task_edit_state.get('labels_selected') or set()
                if not isinstance(selected, set):
                    selected = set(selected)
                labels_list = sorted(selected)
                labels_copy = list(labels_list)
                editing_meta = task_edit_state.get('editing') or {}
                target_idx = editing_meta.get('field_idx', cursor)
                if fields:
                    target_idx = max(0, min(target_idx, len(fields)-1))
                stored_fields = task_edit_state.get('fields')
                field = fields[target_idx] if target_idx < len(fields) else None
                if field is not None:
                    field['value'] = labels_copy
                if target_idx < len(fields):
                    fields[target_idx]['value'] = labels_copy
                if isinstance(stored_fields, list) and target_idx < len(stored_fields):
                    stored_fields[target_idx]['value'] = labels_copy
                task_edit_state['cursor'] = target_idx
                task_edit_state['mode'] = 'list'
                task_edit_state['input'] = ''
                task_edit_state['editing'] = None
                task_edit_state['labels_task'] = None
                task_edit_state['labels_loading'] = False
                task_edit_state['labels_error'] = ''
                task_edit_state['message'] = 'Updating labels…'
                asyncio.create_task(_apply_labels(labels_list))
            elif mode == 'edit-comment':
                comment = (task_edit_state.get('input', '') or '').strip()
                if not comment:
                    task_edit_state['message'] = 'Comment cannot be empty'
                    invalidate(); return
                task_edit_state['mode'] = 'list'
                task_edit_state['input'] = ''
                task_edit_state['editing'] = None
                task_edit_state['message'] = 'Posting comment…'
                asyncio.create_task(_add_comment(comment))
            else:
                _start_task_field_edit()
            return
        if edit_sessions_mode:
            if session_state.get('edit_field') is not None:
                _commit_session_edit()
            else:
                _begin_session_edit('end')
            return
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
        if edit_task_mode:
            _refresh_task_editor_state()
        invalidate()

    @kb.add('j', filter=is_normal)
    @kb.add('down', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        move(1)

    @kb.add('k', filter=is_normal)
    @kb.add('up', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        move(-1)

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

    @kb.add('V', filter=is_normal)
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

    @kb.add('E', filter=is_normal)
    def _(event):
        open_session_editor()

    @kb.add('O', filter=is_normal)
    def _(event):
        open_task_editor()

    @kb.add('o', filter=is_normal)
    def _(event):
        open_task_editor()

    @kb.add(']', filter=is_normal)
    def _(event):
        asyncio.create_task(_change_priority(1))

    @kb.add('[', filter=is_normal)
    def _(event):
        asyncio.create_task(_change_priority(-1))

    @kb.add('j', filter=is_task_edit_idle)
    @kb.add('down', filter=is_task_edit_idle)
    def _(event):
        fields = task_edit_state.get('fields') or []
        if not fields:
            return
        cursor = (int(task_edit_state.get('cursor', 0) or 0) + 1) % len(fields)
        task_edit_state['cursor'] = cursor
        task_edit_state['message'] = 'Enter to edit, Esc/Q to close'
        invalidate()

    @kb.add('k', filter=is_task_edit_idle)
    @kb.add('up', filter=is_task_edit_idle)
    def _(event):
        fields = task_edit_state.get('fields') or []
        if not fields:
            return
        cursor = (int(task_edit_state.get('cursor', 0) or 0) - 1) % len(fields)
        task_edit_state['cursor'] = cursor
        task_edit_state['message'] = 'Enter to edit, Esc/Q to close'
        invalidate()

    @kb.add('j', filter=is_task_edit_nav)
    @kb.add('down', filter=is_task_edit_nav)
    def _(event):
        mode = task_edit_state.get('mode')
        if mode == 'priority-select':
            fields = task_edit_state.get('fields') or []
            editing = task_edit_state.get('editing') or {}
            idx = editing.get('field_idx')
            if idx is None or idx >= len(fields):
                return
            options = fields[idx].get('options') or []
            if not options:
                return
            fields[idx]['index'] = (fields[idx].get('index', 0) + 1) % len(options)
            task_edit_state['message'] = options[fields[idx]['index']].get('name') or '-'
            invalidate()
        elif mode == 'edit-date-calendar':
            _calendar_adjust(days=7)
        elif mode == 'edit-labels':
            if task_edit_state.get('labels_loading'):
                return
            choices = task_edit_state.get('label_choices') or []
            if not choices:
                return
            idx = (task_edit_state.get('label_index', 0) + 1) % len(choices)
            task_edit_state['label_index'] = idx
            task_edit_state['message'] = choices[idx]
            invalidate()

    @kb.add('k', filter=is_task_edit_nav)
    @kb.add('up', filter=is_task_edit_nav)
    def _(event):
        mode = task_edit_state.get('mode')
        if mode == 'priority-select':
            fields = task_edit_state.get('fields') or []
            editing = task_edit_state.get('editing') or {}
            idx = editing.get('field_idx')
            if idx is None or idx >= len(fields):
                return
            options = fields[idx].get('options') or []
            if not options:
                return
            fields[idx]['index'] = (fields[idx].get('index', 0) - 1) % len(options)
            task_edit_state['message'] = options[fields[idx]['index']].get('name') or '-'
            invalidate()
        elif mode == 'edit-date-calendar':
            _calendar_adjust(days=-7)
        elif mode == 'edit-labels':
            if task_edit_state.get('labels_loading'):
                return
            choices = task_edit_state.get('label_choices') or []
            if not choices:
                return
            idx = (task_edit_state.get('label_index', 0) - 1) % len(choices)
            task_edit_state['label_index'] = idx
            task_edit_state['message'] = choices[idx]
            invalidate()

    @kb.add('h', filter=is_task_edit_calendar)
    @kb.add('left', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        _calendar_adjust(days=-1)

    @kb.add('l', filter=is_task_edit_calendar)
    @kb.add('right', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        _calendar_adjust(days=1)

    @kb.add('t', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        today = dt.date.today()
        editing = task_edit_state.get('editing') or {}
        editing['calendar_date'] = today.isoformat()
        task_edit_state['editing'] = editing
        fields = task_edit_state.get('fields') or []
        idx = editing.get('field_idx')
        if idx is not None and idx < len(fields):
            fields[idx]['value'] = today.isoformat()
        task_edit_state['message'] = 'Today'
        invalidate()

    @kb.add('<', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        _calendar_adjust(months=-1)

    @kb.add('>', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        _calendar_adjust(months=1)

    @kb.add('pageup', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        _calendar_adjust(months=-1)

    @kb.add('pagedown', filter=is_task_edit_calendar)
    def _(event):
        if task_edit_state.get('mode') != 'edit-date-calendar':
            return
        _calendar_adjust(months=1)

    @kb.add('j', filter=is_session_idle)
    @kb.add('down', filter=is_session_idle)
    def _(event):
        _move_session_cursor(1)

    @kb.add('k', filter=is_session_idle)
    @kb.add('up', filter=is_session_idle)
    def _(event):
        _move_session_cursor(-1)

    @kb.add('s', filter=is_session_idle)
    def _(event):
        _begin_session_edit('start')

    @kb.add('r', filter=is_session_idle)
    def _(event):
        _refresh_session_editor(session_state.get('selected_id'))
        invalidate()

    @kb.add('+', filter=is_session_idle)
    @kb.add('=', filter=is_session_idle)
    def _(event):
        _adjust_session_end(5)

    @kb.add('-', filter=is_session_idle)
    @kb.add('_', filter=is_session_idle)
    def _(event):
        _adjust_session_end(-5)

    @kb.add('>', filter=is_session_idle)
    def _(event):
        _adjust_session_end(1)

    @kb.add('<', filter=is_session_idle)
    def _(event):
        _adjust_session_end(-1)

    @kb.add('x', filter=is_session_idle)
    @kb.add('delete', filter=is_session_idle)
    def _(event):
        _delete_current_session()

    @kb.add('backspace', filter=is_session_input)
    def _(event):
        buf = session_state.get('input', '')
        if buf:
            session_state['input'] = buf[:-1]  # type: ignore[index]
        invalidate()

    @kb.add(Keys.Any, filter=is_session_input)
    def _(event):
        ch = event.data or ''
        if not ch or ch in ('\n', '\r'):
            return
        buf = session_state.get('input', '')
        if len(buf) >= 64:
            return
        session_state['input'] = buf + ch  # type: ignore[index]
        invalidate()

    @kb.add('A', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        choices = build_project_choices()
        if not choices:
            status_line = "No projects available to add task"
            invalidate()
            return
        nonlocal add_mode, add_state, add_float
        add_mode = True
        mode_choices = ['Create Issue', 'Add Project Task']
        today_iso = dt.date.today().isoformat()
        add_state = {
            'step': 'mode',
            'mode_choices': mode_choices,
            'mode_index': 0,
            'mode': 'issue',
            'project_choices_all': choices,
            'project_choices': [],
            'project_index': 0,
            'project_meta_cache': {},
            'repo_choices': [],
            'repo_index': 0,
            'repo_manual': '',
            'repo_cursor': 0,
            'repo_full_name': '',
            'title': '',
            'title_cursor': 0,
            'start_date': today_iso,
            'start_cursor': len(today_iso),
            'end_date': '',
            'end_cursor': 0,
            'focus_date': today_iso,
            'focus_cursor': len(today_iso),
            'iteration_choices': [],
            'iteration_index': 0,
            'label_choices': [],
            'label_index': 0,
            'labels_selected': set(),
            'priority_choices': [],
            'priority_index': 0,
            'priority_label': '',
            'priority_options': [],
            'assignee_choices': [],
            'assignee_index': 0,
            'assignees_selected': set(),
            'comment': '',
            'comment_cursor': 0,
            'loading_repo_metadata': False,
            'metadata_error': '',
            'repo_metadata_source': '',
            'repo_metadata_task': None,
        }
        _set_project_choices_for_mode('issue')
        if add_float and add_float in floats:
            floats.remove(add_float)
        add_float = Float(content=add_window, top=3, left=4)
        floats.append(add_float)
        status_line = 'Select item type'
        invalidate()

    @kb.add('D', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        asyncio.create_task(_apply_status_change('done'))

    @kb.add('I', filter=is_normal)
    def _(event):
        if detail_mode or in_search:
            return
        asyncio.create_task(_apply_status_change('in_progress'))

    @kb.add('escape', filter=is_add_mode)
    def _(event):
        close_add_mode('Add cancelled')

    def _move_cursor(field_key: str, delta: int, cursor_key: Optional[str] = None):
        key = cursor_key or f"{field_key}_cursor"
        s = add_state.get(field_key, '')
        cur = max(0, min(len(s), add_state.get(key, len(s))))
        cur = max(0, min(len(s), cur + delta))
        add_state[key] = cur

    def _add_delete_one():
        step = add_state.get('step')
        field_map = {
            'title': ('title', 'title_cursor'),
            'start': ('start_date', 'start_cursor'),
            'end': ('end_date', 'end_cursor'),
            'focus': ('focus_date', 'focus_cursor'),
            'comment': ('comment', 'comment_cursor'),
        }
        if step in field_map:
            field_key, cursor_key = field_map[step]
            val = add_state.get(field_key, '')
            cur = max(0, min(len(val), add_state.get(cursor_key, len(val))))
            if cur > 0:
                add_state[field_key] = val[:cur-1] + val[cur:]
                add_state[cursor_key] = cur-1
        elif step == 'repo' and not add_state.get('repo_choices'):
            r = add_state.get('repo_manual', '')
            cur = max(0, min(len(r), add_state.get('repo_cursor', len(r))))
            if cur > 0:
                add_state['repo_manual'] = r[:cur-1] + r[cur:]
                add_state['repo_cursor'] = cur-1
        invalidate()

    @kb.add('backspace', filter=is_add_mode)
    def _(event):
        _add_delete_one()

    @kb.add('delete', filter=is_add_mode)
    def _(event):
        # Forward delete at cursor
        step = add_state.get('step')
        changed = False
        field_map = {
            'title': ('title', 'title_cursor'),
            'start': ('start_date', 'start_cursor'),
            'end': ('end_date', 'end_cursor'),
            'focus': ('focus_date', 'focus_cursor'),
            'comment': ('comment', 'comment_cursor'),
        }
        if step in field_map:
            field_key, cursor_key = field_map[step]
            val = add_state.get(field_key, '')
            cur = max(0, min(len(val), add_state.get(cursor_key, len(val))))
            if cur < len(val):
                add_state[field_key] = val[:cur] + val[cur+1:]
                changed = True
        elif step == 'repo' and not add_state.get('repo_choices'):
            r = add_state.get('repo_manual','')
            cur = max(0, min(len(r), add_state.get('repo_cursor', len(r))))
            if cur < len(r):
                add_state['repo_manual'] = r[:cur] + r[cur+1:]
                changed = True
        if changed:
            invalidate()

    @kb.add('left', filter=is_add_mode)
    def _(event):
        step = add_state.get('step')
        if step == 'title':
            _move_cursor('title', -1)
        elif step == 'start':
            _move_cursor('start_date', -1)
        elif step == 'end':
            _move_cursor('end_date', -1)
        elif step == 'focus':
            _move_cursor('focus_date', -1)
        elif step == 'comment':
            _move_cursor('comment', -1)
        elif step == 'repo' and not add_state.get('repo_choices'):
            _move_cursor('repo_manual', -1, 'repo_cursor')
        invalidate()

    @kb.add('right', filter=is_add_mode)
    def _(event):
        step = add_state.get('step')
        if step == 'title':
            _move_cursor('title', 1)
        elif step == 'start':
            _move_cursor('start_date', 1)
        elif step == 'end':
            _move_cursor('end_date', 1)
        elif step == 'focus':
            _move_cursor('focus_date', 1)
        elif step == 'comment':
            _move_cursor('comment', 1)
        elif step == 'repo' and not add_state.get('repo_choices'):
            _move_cursor('repo_manual', 1, 'repo_cursor')
        invalidate()

    def _cycle_add(delta: int):
        step = add_state.get('step')
        if step == 'mode':
            choices = add_state.get('mode_choices') or []
            if not choices:
                return
            idx = (add_state.get('mode_index', 0) + delta) % len(choices)
            add_state['mode_index'] = idx
            add_state['mode'] = 'issue' if idx == 0 else 'task'
            _set_project_choices_for_mode(add_state['mode'])
        elif step == 'project':
            choices = add_state.get('project_choices') or []
            if not choices:
                return
            idx = (add_state.get('project_index', 0) + delta) % len(choices)
            add_state['project_index'] = idx
            selected_project = choices[idx]
            add_state['iteration_choices'] = _build_iteration_choices(selected_project)
            add_state['iteration_index'] = 0
            add_state['repo_choices'] = _build_repo_choices(selected_project)
            add_state['repo_index'] = 0
            add_state['repo_manual'] = ''
            add_state['repo_full_name'] = ''
            add_state['priority_options'] = (selected_project.get('priority_options') if selected_project else []) or []
            _reset_repo_metadata_state()
        elif step == 'repo':
            choices = add_state.get('repo_choices') or []
            if not choices:
                return
            idx = (add_state.get('repo_index', 0) + delta) % len(choices)
            add_state['repo_index'] = idx
        elif step == 'iteration':
            choices = add_state.get('iteration_choices') or []
            if not choices:
                return
            idx = (add_state.get('iteration_index', 0) + delta) % len(choices)
            add_state['iteration_index'] = idx
        elif step == 'labels':
            choices = add_state.get('label_choices') or []
            if not choices:
                return
            idx = (add_state.get('label_index', 0) + delta) % len(choices)
            add_state['label_index'] = idx
        elif step == 'priority':
            choices = add_state.get('priority_choices') or []
            if not choices:
                return
            idx = (add_state.get('priority_index', 0) + delta) % len(choices)
            add_state['priority_index'] = idx
        elif step == 'assignee':
            choices = add_state.get('assignee_choices') or []
            if not choices:
                return
            idx = (add_state.get('assignee_index', 0) + delta) % len(choices)
            add_state['assignee_index'] = idx

    @kb.add('j', filter=Condition(lambda: add_mode and add_state.get('step') in ('mode','project','repo','iteration','labels','priority','assignee')))
    @kb.add('down', filter=Condition(lambda: add_mode and add_state.get('step') in ('mode','project','repo','iteration','labels','priority','assignee')))
    def _(event):
        _cycle_add(1)
        invalidate()

    @kb.add('k', filter=Condition(lambda: add_mode and add_state.get('step') in ('mode','project','repo','iteration','labels','priority','assignee')))
    @kb.add('up', filter=Condition(lambda: add_mode and add_state.get('step') in ('mode','project','repo','iteration','labels','priority','assignee')))
    def _(event):
        _cycle_add(-1)
        invalidate()

    @kb.add(Keys.Any, filter=Condition(lambda: add_mode and add_state.get('step') in ('title','comment')))
    def _(event):
        ch = event.data or ""
        if not ch or ch in ('\n', '\r'):
            return
        step = add_state.get('step')
        field_map = {
            'title': ('title', 'title_cursor'),
            'comment': ('comment', 'comment_cursor'),
        }
        field_key, cursor_key = field_map.get(step, ('title', 'title_cursor'))
        text = add_state.get(field_key, '')
        cur = max(0, min(len(text), add_state.get(cursor_key, len(text))))
        add_state[field_key] = text[:cur] + ch + text[cur:]
        add_state[cursor_key] = cur + len(ch)
        invalidate()

    @kb.add(Keys.Any, filter=Condition(lambda: add_mode and add_state.get('step') in ('start','end','focus')))
    def _(event):
        ch = event.data or ""
        if not ch or (ch not in '0123456789-' and ch.lower() != 't'):
            return
        step = add_state.get('step')
        field_map = {
            'start': ('start_date', 'start_cursor'),
            'end': ('end_date', 'end_cursor'),
            'focus': ('focus_date', 'focus_cursor'),
        }
        field_key, cursor_key = field_map.get(step, ('start_date', 'start_cursor'))
        date_val = add_state.get(field_key, '')
        cur = max(0, min(len(date_val), add_state.get(cursor_key, len(date_val))))
        if ch.lower() == 't':
            today = dt.date.today().isoformat()
            add_state[field_key] = today
            add_state[cursor_key] = len(today)
        else:
            if len(date_val) >= 10:
                return
            add_state[field_key] = date_val[:cur] + ch + date_val[cur:]
            add_state[cursor_key] = cur + 1
        invalidate()

    @kb.add(Keys.Any, filter=Condition(lambda: add_mode and add_state.get('step') == 'repo' and not add_state.get('repo_choices')))
    def _(event):
        ch = event.data or ""
        if not ch or ch in ('\n', '\r'):
            return
        r = add_state.get('repo_manual','')
        cur = max(0, min(len(r), add_state.get('repo_cursor', len(r))))
        add_state['repo_manual'] = r[:cur] + ch + r[cur:]
        add_state['repo_cursor'] = cur + len(ch)
        invalidate()

    @kb.add(' ', filter=Condition(lambda: add_mode and add_state.get('step') in ('labels','priority','assignee')))
    def _(event):
        step = add_state.get('step')
        if step == 'labels':
            choices = add_state.get('label_choices') or []
            if not choices:
                return
            idx = max(0, min(len(choices)-1, add_state.get('label_index', 0)))
            label = choices[idx]
            selected = add_state.get('labels_selected') or set()
            if not isinstance(selected, set):
                selected = set(selected)
            if label in selected:
                selected.remove(label)
            else:
                selected.add(label)
            add_state['labels_selected'] = selected
        elif step == 'priority':
            choices = add_state.get('priority_choices') or []
            if not choices:
                return
            idx = max(0, min(len(choices)-1, add_state.get('priority_index', 0)))
            name = choices[idx]
            current = (add_state.get('priority_label') or '').strip()
            add_state['priority_label'] = '' if current == name else name
        elif step == 'assignee':
            choices = add_state.get('assignee_choices') or []
            if not choices:
                return
            idx = max(0, min(len(choices)-1, add_state.get('assignee_index', 0)))
            entry = choices[idx]
            login = (entry.get('login') or '').strip()
            if not login:
                return
            selected = add_state.get('assignees_selected') or set()
            if not isinstance(selected, set):
                selected = set(selected)
            if login in selected:
                selected.remove(login)
            else:
                selected.add(login)
            add_state['assignees_selected'] = selected
        invalidate()

    @kb.add('enter', filter=is_add_mode)
    def _(event):
        step = add_state.get('step')
        if step == 'mode':
            idx = add_state.get('mode_index', 0)
            add_state['mode'] = 'issue' if idx == 0 else 'task'
            _set_project_choices_for_mode(add_state['mode'])
            add_state['step'] = 'project'
            if not add_state.get('project_choices'):
                status_line = 'No projects available for selected type'
            else:
                status_line = 'Select project'
        elif step == 'project':
            project = _current_add_project()
            if not project:
                status_line = 'No project available; press Esc'
                invalidate(); return
            add_state['repo_choices'] = _build_repo_choices(project) if project else []
            add_state['repo_index'] = 0
            add_state['iteration_choices'] = _build_iteration_choices(project) if project else []
            add_state['iteration_index'] = 0
            add_state['priority_options'] = (project.get('priority_options') if project else []) or []
            add_state['priority_label'] = ''
            add_state['labels_selected'] = set()
            add_state['assignees_selected'] = set()
            add_state['comment'] = add_state.get('comment', '')
            if add_state.get('mode', 'issue') == 'issue':
                add_state['step'] = 'repo'
                if add_state.get('repo_choices'):
                    status_line = 'Select repository'
                else:
                    status_line = 'Enter repository owner/name'
                    add_state['repo_cursor'] = len(add_state.get('repo_manual', ''))
            else:
                add_state['step'] = 'title'
                add_state['title_cursor'] = len(add_state.get('title', ''))
                status_line = 'Enter title'
        elif step == 'title':
            if not add_state.get('title', '').strip():
                status_line = 'Title is required'
            else:
                add_state['step'] = 'start'
                add_state['start_cursor'] = len(add_state.get('start_date', ''))
                status_line = 'Set start date'
        elif step == 'start':
            start_val = (add_state.get('start_date') or '').strip()
            if not start_val:
                start_val = dt.date.today().isoformat()
                add_state['start_date'] = start_val
                add_state['start_cursor'] = len(start_val)
            try:
                dt.date.fromisoformat(start_val)
            except Exception:
                status_line = 'Invalid start date (YYYY-MM-DD)'
                invalidate(); return
            add_state['step'] = 'end'
            add_state['end_cursor'] = len(add_state.get('end_date', ''))
            status_line = 'Set end date (optional)'
        elif step == 'end':
            end_val = (add_state.get('end_date') or '').strip()
            if end_val:
                try:
                    dt.date.fromisoformat(end_val)
                except Exception:
                    status_line = 'Invalid end date (YYYY-MM-DD)'
                    invalidate(); return
            add_state['step'] = 'focus'
            add_state['focus_cursor'] = len(add_state.get('focus_date', ''))
            status_line = 'Set focus day'
        elif step == 'focus':
            focus_val = (add_state.get('focus_date') or '').strip()
            if focus_val:
                try:
                    dt.date.fromisoformat(focus_val)
                except Exception:
                    status_line = 'Invalid focus day (YYYY-MM-DD)'
                    invalidate(); return
            project = _current_add_project()
            iter_choices = _build_iteration_choices(project) if project else []
            add_state['iteration_choices'] = iter_choices
            add_state['iteration_index'] = 0
            if iter_choices:
                add_state['step'] = 'iteration'
                status_line = 'Select iteration (Enter to confirm)'
            else:
                if add_state.get('mode', 'issue') == 'issue':
                    add_state['step'] = 'labels'
                    status_line = 'Select labels (space to toggle)'
                else:
                    add_state['step'] = 'confirm'
                    status_line = 'Review and confirm'
        elif step == 'repo':
            repo_choices = add_state.get('repo_choices') or []
            if repo_choices:
                idx = max(0, min(len(repo_choices)-1, add_state.get('repo_index', 0)))
                repo_choice = repo_choices[idx]
                repo_full = (repo_choice.get('repo') or '').strip()
                if not repo_full or '/' not in repo_full:
                    status_line = 'Repository metadata missing owner/name'
                    invalidate(); return
                add_state['repo_full_name'] = repo_full
                _start_repo_metadata_fetch(repo_full)
            else:
                manual = (add_state.get('repo_manual') or '').strip()
                if '/' not in manual:
                    status_line = 'Repository must be owner/name'
                    invalidate(); return
                add_state['repo_full_name'] = manual
                _start_repo_metadata_fetch(manual)
            add_state['step'] = 'title'
            add_state['title_cursor'] = len(add_state.get('title', ''))
            status_line = 'Enter title'
        elif step == 'iteration':
            if add_state.get('mode', 'issue') == 'issue':
                add_state['step'] = 'labels'
                status_line = 'Select labels (space to toggle)'
            else:
                add_state['step'] = 'confirm'
                status_line = 'Review and confirm'
        elif step == 'labels':
            if add_state.get('loading_repo_metadata'):
                status_line = 'Labels still loading…'
                invalidate(); return
            labels = add_state.get('label_choices') or []
            if not labels:
                status_line = 'No labels available yet'
                invalidate(); return
            selected = add_state.get('labels_selected') or set()
            if not isinstance(selected, set):
                selected = set(selected)
            if not selected:
                status_line = 'Select at least one label (space to toggle)'
                invalidate(); return
            add_state['labels_selected'] = selected
            add_state['step'] = 'priority'
            status_line = 'Select priority (space to choose)'
        elif step == 'priority':
            if add_state.get('loading_repo_metadata'):
                status_line = 'Priority options still loading…'
                invalidate(); return
            priorities = add_state.get('priority_choices') or []
            if not priorities:
                add_state['step'] = 'assignee'
                status_line = 'No priority options available; continuing'
                invalidate(); return
            if not (add_state.get('priority_label') or '').strip():
                status_line = 'Pick a priority (space to choose)'
                invalidate(); return
            add_state['step'] = 'assignee'
            status_line = 'Choose assignees (space to toggle)'
        elif step == 'assignee':
            if add_state.get('loading_repo_metadata'):
                status_line = 'Assignees still loading…'
                invalidate(); return
            choices = add_state.get('assignee_choices') or []
            if not choices:
                status_line = 'No assignable users found'
                invalidate(); return
            selected = add_state.get('assignees_selected') or set()
            if not isinstance(selected, set):
                selected = set(selected)
            if not selected:
                status_line = 'Select at least one assignee'
                invalidate(); return
            add_state['assignees_selected'] = selected
            add_state['step'] = 'comment'
            add_state['comment_cursor'] = len(add_state.get('comment', ''))
            status_line = 'Add optional comment'
        elif step == 'comment':
            add_state['step'] = 'confirm'
            status_line = 'Review and confirm'
        elif step == 'iteration':
            add_state['step'] = 'confirm'
        elif step == 'confirm':
            project = _current_add_project()
            if not project:
                close_add_mode('Project metadata unavailable')
                return
            title_val = add_state.get('title', '').strip()
            if not title_val:
                add_state['step'] = 'title'
                status_line = 'Title is required'
                invalidate(); return
            start_val = (add_state.get('start_date') or '').strip()
            end_val = (add_state.get('end_date') or '').strip()
            focus_val = (add_state.get('focus_date') or '').strip()
            iteration_choices = add_state.get('iteration_choices') or []
            iteration_id = ''
            if iteration_choices:
                idx = add_state.get('iteration_index', 0)
                opt = iteration_choices[max(0, min(idx, len(iteration_choices)-1))]
                iteration_id = opt.get('id') or ''
            repo_choice = None
            repo_manual = None
            repo_full = (add_state.get('repo_full_name') or '').strip()
            if add_state.get('mode', 'issue') == 'issue':
                repo_choices = add_state.get('repo_choices') or []
                if repo_choices:
                    repo_idx = add_state.get('repo_index', 0)
                    repo_choice = repo_choices[max(0, min(repo_idx, len(repo_choices)-1))]
                else:
                    repo_manual = repo_full or add_state.get('repo_manual', '').strip()
                    if not repo_manual:
                        status_line = 'Repository is required'
                        invalidate(); return
            labels_selected = add_state.get('labels_selected') or set()
            if not isinstance(labels_selected, set):
                labels_selected = set(labels_selected)
            label_choices = add_state.get('label_choices') or []
            ordered_labels = [name for name in label_choices if name in labels_selected]
            priority_label = (add_state.get('priority_label') or '').strip()
            assignees_selected = add_state.get('assignees_selected') or set()
            if not isinstance(assignees_selected, set):
                assignees_selected = set(assignees_selected)
            assignees_ordered: List[str] = []
            for entry in add_state.get('assignee_choices') or []:
                login = (entry.get('login') or '').strip()
                if login and login in assignees_selected:
                    assignees_ordered.append(login)
            for login in sorted(assignees_selected):
                if login not in assignees_ordered:
                    assignees_ordered.append(login)
            comment_val = add_state.get('comment', '').strip()
            mode_val = add_state.get('mode', 'issue')
            priority_options = (add_state.get('priority_options') or [])
            close_add_mode('Creating item…')
            asyncio.create_task(create_task_async(
                project,
                title_val,
                start_val,
                end_val,
                focus_val,
                iteration_id,
                mode_val,
                repo_choice,
                repo_manual,
                repo_full,
                ordered_labels,
                priority_label,
                priority_options,
                assignees_ordered,
                comment_val,
            ))
            return
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
        if edit_task_mode:
            if task_edit_state.get('mode') in ('edit-date', 'priority-select'):
                _cancel_task_edit('Edit cancelled')
            else:
                close_task_editor('Field editor closed')
            return
        if edit_sessions_mode:
            if session_state.get('edit_field') is not None:
                _cancel_session_edit('Edit cancelled')
            else:
                close_session_editor('Timer editor closed')
            return
        if add_mode:
            close_add_mode('Add cancelled')
            return
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

    @kb.add('backspace', filter=Condition(lambda: (not add_mode) and (in_search or in_date_filter)))
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

    @kb.add('backspace', filter=is_task_edit_text)
    def _(event):
        mode = task_edit_state.get('mode')
        if mode in ('edit-assignees', 'edit-comment'):
            buf = task_edit_state.get('input', '')
            if buf:
                task_edit_state['input'] = buf[:-1]
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

    @kb.add(Keys.Any, filter=is_task_edit_text)
    def _(event):
        mode = task_edit_state.get('mode')
        if mode not in ('edit-assignees', 'edit-comment'):
            return
        ch = event.data or ''
        if not ch or ch in ('\r', '\n'):
            return
        buf = task_edit_state.get('input', '')
        task_edit_state['input'] = buf + ch
        invalidate()

    @kb.add(' ', filter=is_task_edit_labels)
    def _(event):
        if task_edit_state.get('labels_loading'):
            return
        choices = task_edit_state.get('label_choices') or []
        if not choices:
            return
        idx = max(0, min(len(choices)-1, task_edit_state.get('label_index', 0)))
        label_name = choices[idx]
        selected = task_edit_state.get('labels_selected') or set()
        if not isinstance(selected, set):
            selected = set(selected)
        if label_name in selected:
            selected.remove(label_name)
        else:
            selected.add(label_name)
        task_edit_state['labels_selected'] = selected
        task_edit_state['message'] = ', '.join(sorted(selected)) or '(none)'
        invalidate()

    @kb.add('u', filter=is_normal)
    def _(event):
        if detail_mode or in_search or in_date_filter:
            return
        asyncio.create_task(update_worker())

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
            labels = fetch_labels_for_url(token, t.url) if token else []
            db.stop_session(t.url, t.project_title, t.repo, json.dumps(labels))
            try:
                logger.info("Stopped timer for %s", t.url)
            except Exception:
                pass
        else:
            labels = fetch_labels_for_url(token, t.url) if token else []
            db.start_session(t.url, t.project_title, t.repo, json.dumps(labels))
            try:
                logger.info("Started timer for %s labels=%s", t.url, labels)
            except Exception:
                pass
        _reset_timer_caches()
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
        _cycle_sort(1)

    @kb.add('S', filter=is_normal)
    def _(event):
        _cycle_sort(-1)
    # Date <= filter input
    @kb.add('F', filter=is_normal)
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
        if edit_task_mode:
            close_task_editor(None)
        detail_mode = False
        show_report = False
        in_search = False
        show_help = not show_help
        floats.clear()
        if show_help:
            help_lines = [
                "🧭 Navigation",
                "  j/k • arrows        Move selection",
                "  gg / G              Top / Bottom",
                "  h/l • arrows        Horizontal scroll",
                "  Enter               Toggle detail",
                "",
                "🔎 Search & Sort",
                "  /                   Start search (Enter apply, Esc cancel)",
                "  s / S               Cycle sort forward / backward",
                "",
                "🎛️ Filters",
                "  p / P               Cycle / Clear project",
                "  d                   Hide done",
                "  N                   Hide no-date",
                "  F                   Date ≤ YYYY-MM-DD",
                "  t / a               Today / All",
                "  C                   Show created (no assignee)",
                "  V                   Toggle iteration/date view",
                "",
                "🛠 Task Actions",
                "  A                   Add issue / project task",
                "  D / I               Set status Done / In Progress",
                "  ] / [               Priority next / previous",
                "  O                   Edit task fields",
                "  E                   Edit work sessions",
                "",
                "⏱ Timers & Reports",
                "  W                   Toggle work timer",
                "  R                   Open timer report",
                "  d / w / m (report)  Day / Week / Month view",
                "  X                   Export JSON report",
                "  Z                   Export PDF report",
                "",
                "🌐 Fetch",
                "  u                   Update (fetch GitHub)",
                "",
                "🎨 Themes",
                "  Shift+1..0         Switch theme preset",
                "  Add YAML under themes/ to create presets",
                f"  Current: {theme_presets[current_theme_index].name}",
                "",
                "❓ General",
                "  ?                   Toggle help",
                "  q / Esc             Quit / Close",
                "",
                f"Current tasks shown: {len(filtered_rows())}",
                "Visual: ⏱ + cyan row = task timer running",
                "Press ? to close help."
            ]
            txt = "\n".join(help_lines)
            hl_control = FormattedTextControl(text=txt)
            # Compute size based on terminal
            try:
                from prompt_toolkit.application.current import get_app
                size = get_app().output.get_size()
                cols = size.columns
                rows = size.rows
            except Exception:
                cols, rows = 120, 40
            w = max(60, min(100, cols - 6))
            h = max(12, min(rows - 4, 32))
            body = Window(width=Dimension.exact(w-2), height=Dimension.exact(h-2), content=hl_control, wrap_lines=False, always_hide_cursor=True)
            frame = Frame(body=body, title="Help")
            floats.append(Float(content=frame, top=1, left=2))
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
    app = Application(layout=Layout(container), key_bindings=kb, full_screen=True, mouse_support=True, style=style, editing_mode=EditingMode.VI)
    apply_theme(current_theme_index, announce=False)

    # Background ticker to refresh timers & status once per second
    async def _ticker():
        while True:
            try:
                await asyncio.sleep(1)
                update_search_status()
                invalidate()
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
    status_options = [
        {"id": "opt-todo", "name": "Todo"},
        {"id": "opt-in-progress", "name": "In Progress"},
        {"id": "opt-done", "name": "Done"},
        {"id": "opt-blocked", "name": "Blocked"},
    ]
    priority_field_id = "priority-field"
    priority_options = [
        {"id": "prio-high", "name": "High"},
        {"id": "prio-medium", "name": "Medium"},
        {"id": "prio-low", "name": "Low"},
    ]
    iteration_options = [
        {"id": "iter-1", "title": "Sprint 1", "startDate": today.isoformat(), "duration": 14},
        {"id": "iter-2", "title": "Sprint 2", "startDate": (today + dt.timedelta(days=14)).isoformat(), "duration": 14},
    ]
    assignee_field_id = "assignee-field"
    assignee_user_ids = json.dumps(["MDQ6VXNlcjEyMzQ1"], ensure_ascii=False)
    for i, proj in enumerate(projects, start=1):
        for d_off in range(-2, 5):
            date_str = (today + dt.timedelta(days=d_off)).isoformat()
            status = statuses[(i + d_off) % len(statuses)]
            option_id = next((opt["id"] for opt in status_options if opt["name"] == status), "opt-todo")
            pr_idx = (i + d_off) % len(priority_options)
            pr_opt = priority_options[pr_idx]
            rows.append(TaskRow(
                owner_type="org", owner="example", project_number=i, project_title=proj,
                start_field="Start date", start_date=date_str,
                focus_field="Focus Day", focus_date=date_str,
                focus_field_id="focus-field",
                title=f"Task {i}-{d_off}",
                repo_id=f"repo-{i}",
                repo="demo/repo",
                labels=json.dumps(["Label", f"L{i}"], ensure_ascii=False),
                priority=pr_opt.get("name"),
                priority_field_id=priority_field_id,
                priority_option_id=pr_opt.get("id"),
                priority_options=json.dumps(priority_options, ensure_ascii=False),
                url=f"https://example.com/{i}-{d_off}", updated_at=iso_now, status=status,
                is_done=1 if status.lower()=="done" else 0,
                assigned_to_me=1 if (i + d_off) % 2 == 0 else 0,
                created_by_me=1 if (i + d_off) % 3 == 0 else 0,
                item_id=f"item-{i}-{d_off}",
                project_id=f"proj-{i}",
                status_field_id="status-field",
                status_option_id=option_id,
                status_options=json.dumps(status_options, ensure_ascii=False),
                priority_dirty=0,
                priority_pending_option_id="",
                start_field_id="start-field",
                iteration_field_id="iteration-field",
                iteration_options=json.dumps(iteration_options, ensure_ascii=False),
                assignee_field_id=assignee_field_id,
                assignee_user_ids=assignee_user_ids,
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
