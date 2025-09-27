import datetime as dt

import pytest

import gh_task_viewer as ght


def test_fetch_tasks_github_rate_limit_partial(monkeypatch):
    cfg = ght.Config(
        user='tester',
        date_field_regex='.',
        projects=[ght.ProjectSpec(owner_type='org', owner='octo', numbers=[1])],
    )

    monkeypatch.setattr(ght, '_load_target_cache', lambda: {})
    monkeypatch.setattr(ght, '_save_target_cache', lambda data: None)

    class DummySession:
        pass

    monkeypatch.setattr(ght, '_session', lambda token: DummySession())

    def fake_graphql(_session, _query, _variables, on_wait=None):
        return {'errors': [{'type': 'RATE_LIMITED'}]}

    monkeypatch.setattr(ght, '_graphql_with_backoff', fake_graphql)

    result = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=False,
    )

    assert isinstance(result, ght.FetchTasksResult)
    assert result.partial is True
    assert result.rows == []
    assert 'Rate limited' in (result.message or '')


def test_task_duration_snapshot_running_and_completed(monkeypatch):
    db = ght.TaskDB(':memory:')
    cur = db.conn.cursor()

    cur.execute(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        ('task1', 'Project', '2024-01-01T00:00:00+00:00', '2024-01-01T01:00:00+00:00', '[]'),
    )
    cur.execute(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        ('task1', 'Project', '2024-01-01T02:00:00+00:00', None, '[]'),
    )
    cur.execute(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        ('task2', 'Project', '2024-01-01T00:00:00+00:00', '2024-01-01T01:00:00+00:00', '[]'),
    )
    db.conn.commit()

    real_datetime = ght.dt.datetime

    class FixedDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            base = real_datetime(2024, 1, 1, 3, 0, 0)
            if tz is not None:
                return base.replace(tzinfo=tz)
            return base

    monkeypatch.setattr(ght.dt, 'datetime', FixedDateTime)

    snapshot = db.task_duration_snapshot(['task1', 'task2'])

    assert snapshot['task1']['total'] == 7200  # 1h completed + 1h running
    assert snapshot['task1']['current'] == 3600  # running session duration
    assert snapshot['task2']['total'] == 3600
    assert snapshot['task2']['current'] == 0

    db.conn.close()
