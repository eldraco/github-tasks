import datetime as _dt
import json

import pytest

import gh_task_viewer as ght


def _iso(year, month, day, hour, minute=0, second=0):
    return _dt.datetime(year, month, day, hour, minute, second, tzinfo=_dt.timezone.utc).isoformat()


@pytest.fixture
def fixed_now(monkeypatch):
    base = _dt.datetime(2024, 1, 10, 12, 0, 0, tzinfo=_dt.timezone.utc)
    real_datetime = ght.dt.datetime

    class FixedDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return base.astimezone(tz)
            return base

    monkeypatch.setattr(ght.dt, 'datetime', FixedDateTime)
    try:
        yield base
    finally:
        monkeypatch.setattr(ght.dt, 'datetime', real_datetime)


@pytest.fixture
def analytics_db(fixed_now):
    db = ght.TaskDB(':memory:')
    cur = db.conn.cursor()

    sessions = [
        ('task1', 'Project Alpha', _iso(2024, 1, 9, 10), _iso(2024, 1, 9, 12), json.dumps(['bug', 'frontend'])),
        ('task1', 'Project Alpha', _iso(2024, 1, 9, 22), _iso(2024, 1, 10, 2), json.dumps(['bug', 'ops'])),
        ('task2', 'Project Beta', _iso(2024, 1, 7, 12), _iso(2024, 1, 7, 13), json.dumps(['legacy'])),
        ('task1', 'Project Alpha', _iso(2024, 1, 10, 9), None, json.dumps(['bug'])),
    ]

    cur.executemany(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        sessions,
    )
    db.conn.commit()
    try:
        yield db
    finally:
        db.conn.close()


def test_time_helpers_use_timezone(fixed_now):
    db = ght.TaskDB(':memory:')
    try:
        rows = [
            (_iso(2024, 1, 9, 10), _iso(2024, 1, 9, 12, 30)),
            (_iso(2024, 1, 9, 14), None),
        ]
        total = db._sum_rows_seconds(rows)
        # 2.5h + (from Jan 9 14:00 to Jan 10 12:00) = 9000 + 79200 = 88200
        assert total == 88200

        start, end, keep = db._clip_range(
            _dt.datetime(2024, 1, 8, 10, tzinfo=_dt.timezone.utc),
            _dt.datetime(2024, 1, 9, 4, tzinfo=_dt.timezone.utc),
            _dt.datetime(2024, 1, 9, 0, tzinfo=_dt.timezone.utc),
        )
        assert keep is True
        assert start == _dt.datetime(2024, 1, 9, 0, tzinfo=_dt.timezone.utc)
        assert end == _dt.datetime(2024, 1, 9, 4, tzinfo=_dt.timezone.utc)

        _, _, keep2 = db._clip_range(
            _dt.datetime(2024, 1, 8, 10, tzinfo=_dt.timezone.utc),
            _dt.datetime(2024, 1, 8, 11, tzinfo=_dt.timezone.utc),
            _dt.datetime(2024, 1, 9, 0, tzinfo=_dt.timezone.utc),
        )
        assert keep2 is False

        point = _dt.datetime(2024, 1, 10, 15, tzinfo=_dt.timezone.utc)
        assert db._period_key(point, 'day') == '2024-01-10'
        assert db._period_key(point, 'week') == '2024-W02'
        assert db._period_key(point, 'month') == '2024-01'

        assert db._next_boundary(point, 'day') == _dt.datetime(2024, 1, 11, 0, tzinfo=_dt.timezone.utc)
        assert db._next_boundary(point, 'week') == _dt.datetime(2024, 1, 15, 0, tzinfo=_dt.timezone.utc)
        assert db._next_boundary(point, 'month') == _dt.datetime(2024, 2, 1, 0, tzinfo=_dt.timezone.utc)
    finally:
        db.conn.close()


def test_aggregate_functions_with_running_sessions(analytics_db):
    totals_day = analytics_db.aggregate_period_totals('day', since_days=2)
    assert totals_day['2024-01-09'] == 14400  # 4 hours on Jan 9
    assert totals_day['2024-01-10'] == 18000  # 5 hours on Jan 10
    assert '2024-01-07' not in totals_day

    totals_week = analytics_db.aggregate_period_totals('week', since_days=7)
    assert totals_week['2024-W02'] == 32400
    assert totals_week['2024-W01'] == 3600

    totals_month = analytics_db.aggregate_period_totals('month', since_days=30)
    assert totals_month['2024-01'] == 36000

    proj_totals = analytics_db.aggregate_project_totals(since_days=2)
    assert proj_totals['Project Alpha'] == 32400
    assert 'Project Beta' not in proj_totals

    task_totals = analytics_db.aggregate_task_totals(since_days=2)
    assert task_totals['task1'] == 32400
    assert 'task2' not in task_totals

    snapshot = analytics_db.task_duration_snapshot(['task1', 'task1', 'task2'])
    assert set(snapshot.keys()) == {'task1', 'task2'}
    assert snapshot['task1']['current'] == 10800
    assert snapshot['task1']['total'] == 32400
    assert snapshot['task2']['current'] == 0
    assert snapshot['task2']['total'] == 3600

    label_totals = analytics_db.aggregate_label_totals(since_days=2)
    assert label_totals['bug'] == 32400
    assert label_totals['frontend'] == 7200
    assert label_totals['ops'] == 14400
    assert 'legacy' not in label_totals

    # Verify project and task filters reuse the same math
    proj_filtered = analytics_db.aggregate_period_totals('day', since_days=2, project_title='Project Alpha')
    assert proj_filtered == totals_day

    task_filtered = analytics_db.aggregate_period_totals('day', since_days=2, task_url='task1')
    assert task_filtered == totals_day

    all_day = analytics_db.aggregate_period_totals('day', since_days=7)
    assert all_day['2024-01-07'] == 3600
