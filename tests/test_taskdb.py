import json
import sqlite3

import pytest

import gh_task_viewer as ght


def make_task_row(**overrides) -> ght.TaskRow:
    base = {
        'owner_type': 'org',
        'owner': 'acme',
        'project_number': 1,
        'project_title': 'Project X',
        'start_field': 'Start date',
        'start_date': '2024-01-01',
        'end_field': 'End date',
        'end_date': '2024-01-05',
        'focus_field': 'Focus Day',
        'focus_date': '2024-01-02',
        'title': 'Task Alpha',
        'url': 'https://example.com/tasks/1',
        'updated_at': '2024-01-02T00:00:00',
    }
    base.update(overrides)
    return ght.TaskRow(**base)


def _table_names(conn) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }


def _index_names(conn) -> set[str]:
    return {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        if not row[0].startswith('sqlite_autoindex')
    }


@pytest.mark.parametrize('db_path', [':memory:'])
def test_taskdb_fresh_setup_creates_schema(db_path):
    db = ght.TaskDB(db_path)
    try:
        tables = _table_names(db.conn)
        assert {'tasks', 'work_sessions', 'timer_events'} <= tables

        indexes = _index_names(db.conn)
        assert {'idx_tasks_date', 'idx_tasks_end_date', 'idx_tasks_focus_date'} <= indexes
        assert {'idx_ws_task', 'idx_ws_open', 'idx_te_task_at'} <= indexes

        mode = db.conn.execute("PRAGMA journal_mode").fetchone()[0]
        # In-memory DBs report 'memory'; file-backed DBs honour WAL.
        assert str(mode).lower() in {'wal', 'memory'}
    finally:
        db.conn.close()


def test_taskdb_migrates_legacy_schema(temp_db_path):
    legacy_sql = """
    CREATE TABLE tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        owner_type TEXT NOT NULL,
        owner TEXT NOT NULL,
        project_number INTEGER NOT NULL,
        project_title TEXT NOT NULL,
        start_field TEXT NOT NULL,
        start_date TEXT NOT NULL,
        focus_field TEXT NOT NULL,
        focus_date TEXT NOT NULL
    );
    """
    with sqlite3.connect(temp_db_path) as conn:
        conn.execute(legacy_sql)
        conn.execute(
            "INSERT INTO tasks(owner_type, owner, project_number, project_title, start_field, start_date, focus_field, focus_date) VALUES (?,?,?,?,?,?,?,?)",
            ('org', 'acme', 1, 'Project X', 'Start date', '2024-01-01', 'Focus Day', '2024-01-02'),
        )
        conn.commit()

    db = ght.TaskDB(str(temp_db_path))
    try:
        columns = {row[1] for row in db.conn.execute("PRAGMA table_info(tasks)")}
        assert 'content_node_id' in columns
        assert 'assignee_logins' in columns
        assert 'end_field' in columns
        assert 'end_date' in columns

        tables = _table_names(db.conn)
        assert {'tasks', 'work_sessions', 'timer_events'} <= tables

        indexes = _index_names(db.conn)
        assert {'idx_tasks_date', 'idx_tasks_end_date', 'idx_tasks_focus_date'} <= indexes

        rows = db.load()
        assert len(rows) == 1
        assert rows[0].owner == 'acme'
        assert rows[0].start_field == 'Start date'
        assert rows[0].end_field == ''
        assert rows[0].end_date == ''
    finally:
        db.conn.close()


def test_taskdb_upsert_conflict_and_serialization():
    db = ght.TaskDB(':memory:')
    try:
        base_labels = json.dumps(['initial'])
        row = make_task_row(
            labels=base_labels,
            priority='High',
            priority_field_id='prio-field',
            priority_option_id='prio-high',
            priority_options=json.dumps([{'id': 'prio-high', 'name': 'High'}]),
            status='Todo',
            status_field_id='status-field',
            status_option_id='status-todo',
            status_options=json.dumps([{'id': 'status-todo', 'name': 'Todo'}]),
            priority_dirty=0,
            status_dirty=0,
        )

        db.upsert_many([row])

        updated_row = make_task_row(
            labels=json.dumps(['updated']),
            end_field='Due date',
            end_date='2024-01-10',
            priority='Medium',
            priority_field_id='prio-field',
            priority_option_id='prio-med',
            priority_options=json.dumps([{'id': 'prio-med', 'name': 'Medium'}]),
            status='In Progress',
            status_field_id='status-field',
            status_option_id='status-progress',
            status_options=json.dumps([{'id': 'status-progress', 'name': 'In Progress'}]),
            priority_dirty=1,
            priority_pending_option_id='pending-prio',
            status_dirty=1,
            status_pending_option_id='pending-status',
        )

        db.upsert_many([updated_row])
        db.upsert_many([updated_row])  # idempotency check

        stored = db.load()[0]
        assert stored.end_field == 'Due date'
        assert stored.end_date == '2024-01-10'
        assert stored.priority == 'Medium'
        assert stored.priority_dirty == 1
        assert stored.priority_pending_option_id == 'pending-prio'
        assert stored.status == 'In Progress'
        assert stored.status_dirty == 1
        assert stored.status_pending_option_id == 'pending-status'
        assert json.loads(stored.labels) == ['updated']

        # serialization fallback: labels -> [] when json.dumps fails
        db.update_labels(stored.url, [object()])
        value = db.conn.execute(
            "SELECT labels FROM tasks WHERE url=?", (stored.url,)
        ).fetchone()[0]
        assert value == '[]'

        # ensure priority options fallback works as well
        db.update_priority_options(stored.url, [object()])
        pr_value = db.conn.execute(
            "SELECT priority_options FROM tasks WHERE url=?", (stored.url,)
        ).fetchone()[0]
        assert pr_value == '[]'
    finally:
        db.conn.close()


def test_taskdb_update_helpers_handle_invalid_payloads():
    db = ght.TaskDB(':memory:')
    try:
        shared_options_first = json.dumps([{'id': 'x', 'name': 'Initial'}])
        shared_options_second = json.dumps([{'id': 'y', 'name': 'Initial'}])
        other_options = json.dumps([{'id': 'z', 'name': 'Initial'}])

        first = make_task_row(
            priority_field_id='prio-shared',
            priority_options=shared_options_first,
        )
        second = make_task_row(
            title='Task Beta',
            url='https://example.com/tasks/2',
            priority_field_id='prio-shared',
            priority_options=shared_options_second,
        )
        third = make_task_row(
            title='Task Gamma',
            url='https://example.com/tasks/3',
            priority_field_id='prio-other',
            priority_options=other_options,
        )
        db.upsert_many([first, second, third])

        db.update_assignees(first.url, [object()], [object()])
        assignees = db.conn.execute(
            "SELECT assignee_user_ids, assignee_logins FROM tasks WHERE url=?",
            (first.url,),
        ).fetchone()
        assert assignees == ('[]', '[]')

        db.update_priority_options_by_field('prio-shared', [object()])
        shared_values = db.conn.execute(
            "SELECT url, priority_options FROM tasks WHERE priority_field_id=?",
            ('prio-shared',),
        ).fetchall()
        assert all(value == '[]' for _, value in shared_values)

        other_value = db.conn.execute(
            "SELECT priority_options FROM tasks WHERE priority_field_id=?",
            ('prio-other',),
        ).fetchone()[0]
        assert other_value == other_options
    finally:
        db.conn.close()
