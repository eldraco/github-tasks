import asyncio
from types import SimpleNamespace

import pytest

import gh_task_viewer as ght
import prompt_toolkit

from .helpers import make_task


@pytest.fixture
def ui_context(monkeypatch, tmp_path):
    prompt_toolkit.Application.instances.clear()
    prompt_toolkit.key_binding.KeyBindings.instances.clear()

    db = ght.TaskDB(':memory:')
    row = make_task()
    db.upsert_many([row])

    cfg = ght.Config(user='tester', date_field_regex='Start', projects=[])

    pending_tasks = []

    class DummyTask:
        def __init__(self, coro=None):
            self._coro = coro
            self.cancelled = False

        def cancel(self):
            self.cancelled = True
            if self._coro in pending_tasks:
                pending_tasks.remove(self._coro)

    def _run_coro(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    def fake_create_task(coro):
        pending_tasks.append(coro)
        return DummyTask(coro)

    def run_pending(name=None):
        to_run = []
        for coro in list(pending_tasks):
            code = getattr(coro, 'cr_code', None)
            func_name = code.co_name if code else ''
            match = False
            if name is None:
                match = True
            elif isinstance(name, str):
                match = func_name == name
            elif callable(name):
                match = bool(name(func_name))
            if match:
                to_run.append(coro)
        for coro in to_run:
            pending_tasks.remove(coro)
            _run_coro(coro)

    async def fake_sleep(_seconds):
        return None

    monkeypatch.setattr(ght.asyncio, 'create_task', fake_create_task)
    monkeypatch.setattr(ght.asyncio, 'sleep', fake_sleep)
    monkeypatch.setattr(ght, 'list_repo_labels', lambda token, repo, max_pages=5: [{'name': 'bug'}, {'name': 'feature'}])
    monkeypatch.setattr(ght, 'list_repo_assignees', lambda token, repo, max_pages=5: [{'login': 'octocat', 'name': 'Octocat'}])
    monkeypatch.setattr(ght, 'set_issue_labels', lambda *args, **kwargs: None)
    monkeypatch.setattr(ght, 'set_issue_assignees', lambda *args, **kwargs: None)
    monkeypatch.setattr(ght, 'add_issue_comment', lambda *args, **kwargs: None)
    monkeypatch.setattr(ght, 'fetch_tasks_github', lambda *args, **kwargs: ght.FetchTasksResult(rows=[], partial=False, message=''))

    state_path = tmp_path / 'ui_state.json'
    ght.run_ui(db, cfg, token='token', state_path=str(state_path), log_level='ERROR')

    app_cls = prompt_toolkit.Application
    kb_cls = prompt_toolkit.key_binding.KeyBindings
    app = app_cls.instances[-1]
    kb = kb_cls.instances[-1]

    def find_binding(key, predicate=None):
        for keys, kwargs, func in kb.bindings:
            if key in keys and (predicate is None or predicate(kwargs)):
                return func
        raise AssertionError(f'Binding for {key!r} not found')

    ctx = SimpleNamespace(
        db=db,
        cfg=cfg,
        app=app,
        kb=kb,
        find_binding=find_binding,
        pending_tasks=pending_tasks,
        run_pending=run_pending,
    )
    try:
        yield ctx
    finally:
        for coro in list(pending_tasks):
            try:
                coro.close()
            except RuntimeError:
                pass
            if coro in pending_tasks:
                pending_tasks.remove(coro)
        background = getattr(app, 'background_tasks', None) or []
        for coro in list(background):
            try:
                coro.close()
            except RuntimeError:
                pass
            if coro in background:
                background.remove(coro)
        db.conn.close()
