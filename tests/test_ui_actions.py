import asyncio
import datetime as dt
import json
import os
from types import SimpleNamespace

import pytest

import gh_task_viewer as ght


def _reset_prompt_toolkit_stubs():
    from prompt_toolkit import Application
    from prompt_toolkit.key_binding import KeyBindings

    if hasattr(Application, "instances"):
        Application.instances.clear()
    if hasattr(KeyBindings, "instances"):
        KeyBindings.instances.clear()


def _build_ui(db, cfg, token, state_path, log_level="ERROR"):
    _reset_prompt_toolkit_stubs()
    ght.run_ui(db, cfg, token=token, state_path=state_path, log_level=log_level)
    from prompt_toolkit import Application
    from prompt_toolkit.key_binding import KeyBindings

    app = Application.instances[-1]
    kb = KeyBindings.instances[-1]
    return SimpleNamespace(app=app, kb=kb)


def _closure_cells(func):
    cells = func.__closure__ or ()
    names = func.__code__.co_freevars
    return {name: cell for name, cell in zip(names, cells)}


def _find_binding(kb, key, predicate=None):
    for keys, _kwargs, func in kb.bindings:
        if keys == (key,) and (predicate is None or predicate(func)):
            return func
    raise AssertionError(f"binding for {key!r} not found")


@pytest.fixture
def ui_config():
    return ght.Config(
        user="tester",
        date_field_regex=".",
        projects=[ght.ProjectSpec(owner_type="org", owner="octo", numbers=[1])],
        iteration_field_regex=None,
    )


@pytest.fixture
def scheduled_tasks(monkeypatch):
    captured = []

    class DummyTask:
        def __init__(self, coro):
            self._coro = coro

        def cancel(self):
            pass

    def fake_create_task(coro):
        captured.append(coro)
        return DummyTask(coro)

    monkeypatch.setattr(ght.asyncio, "create_task", fake_create_task)
    return captured


def _make_task_row(url="https://github.com/octo/repo/issues/1", **overrides):
    base = dict(
        owner_type="org",
        owner="octo",
        project_number=1,
        project_title="Project",
        start_field="Start",
        start_date="2024-01-01",
        end_field="End",
        end_date="2024-01-02",
        focus_field="Focus",
        focus_date="2024-01-03",
        title="Sample task",
        repo_id="r1",
        repo="octo/repo",
        labels="[]",
        priority="Medium",
        priority_field_id="priority-field",
        priority_option_id="priority-medium",
        priority_options=json.dumps([
            {"id": "priority-low", "name": "Low"},
            {"id": "priority-medium", "name": "Medium"},
            {"id": "priority-high", "name": "High"},
        ]),
        url=url,
        updated_at="2024-01-01T00:00:00Z",
        status="Todo",
        is_done=0,
        assigned_to_me=1,
        created_by_me=0,
        item_id="item-123",
        project_id="proj-123",
        status_field_id="status-field",
        status_option_id="status-todo",
        status_options=json.dumps([
            {"id": "status-todo", "name": "Todo"},
            {"id": "status-in-progress", "name": "In Progress"},
            {"id": "status-done", "name": "Done"},
        ]),
        status_dirty=0,
        status_pending_option_id="",
        start_field_id="start-field",
        focus_field_id="focus-field",
        iteration_field="Iteration",
        iteration_title="Sprint",
        iteration_start="",
        iteration_duration=0,
        iteration_options="[]",
        assignee_field_id="people-field",
        assignee_user_ids="[]",
        assignee_logins="[]",
        content_node_id="node-123",
    )
    base.update(overrides)
    return ght.TaskRow(**base)


def test_save_state_handles_unwritable_directory(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    db.upsert_many([_make_task_row()])

    state_path = tmp_path / "missing" / "state.json"
    harness = _build_ui(db, ui_config, token="token", state_path=str(state_path))

    called = {}

    def fake_makedirs(path, exist_ok):
        called["path"] = path
        called["exist_ok"] = exist_ok
        raise PermissionError("permission denied for test")

    monkeypatch.setattr(ght.os, "makedirs", fake_makedirs)

    handler = _find_binding(harness.kb, "!")
    handler(SimpleNamespace())

    assert called, "expected makedirs to be invoked when saving state"
    assert called["path"] == os.path.dirname(str(state_path))
    assert called["exist_ok"] is True
    assert not state_path.exists()

    db.conn.close()


def test_save_state_handles_json_dump_error(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    db.upsert_many([_make_task_row()])

    state_path = tmp_path / "state.json"
    harness = _build_ui(db, ui_config, token="token", state_path=str(state_path))

    captured = {}

    def fake_dump(data, fh, indent=2):
        captured["data"] = data
        raise TypeError("non-serializable data")

    monkeypatch.setattr(ght.json, "dump", fake_dump)

    handler = _find_binding(harness.kb, "!")
    handler(SimpleNamespace())

    assert captured, "expected json.dump to be invoked when saving state"
    assert captured["data"]["theme_index"] == 0
    assert state_path.exists()
    assert state_path.read_text() == ""

    db.conn.close()


def test_apply_status_change_sets_done_and_stops_timer(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(status="In Progress", status_option_id="status-in-progress")
    db.upsert_many([row])

    cur = db.conn.cursor()
    cur.execute(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        (row.url, row.project_title, "2024-01-01T09:00:00+00:00", None, "[]"),
    )
    db.conn.commit()

    status_calls = []

    def fake_set_project_status(token, project_id, item_id, field_id, option_id):
        status_calls.append({
            "token": token,
            "project_id": project_id,
            "item_id": item_id,
            "field_id": field_id,
            "option_id": option_id,
        })

    monkeypatch.setattr(ght, "set_project_status", fake_set_project_status)

    label_calls = []
    monkeypatch.setattr(ght, "fetch_labels_for_url", lambda token, url: label_calls.append((token, url)) or ["bug"])

    original_stop = db.stop_session
    stop_calls = []

    def tracking_stop(task_url, project_title, repo, labels_json):
        stop_calls.append({
            "task_url": task_url,
            "project_title": project_title,
            "repo": repo,
            "labels": labels_json,
        })
        return original_stop(task_url, project_title, repo, labels_json)

    monkeypatch.setattr(db, "stop_session", tracking_stop, raising=False)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    handler = _find_binding(harness.kb, "D")
    handler_cells = _closure_cells(handler)
    apply_status = handler_cells["_apply_status_change"].cell_contents
    apply_cells = _closure_cells(apply_status)
    pending_urls = apply_cells["pending_status_urls"].cell_contents
    status_line_cell = apply_cells["status_line"]

    assert row.url in db.active_task_urls(), "precondition: task should have active timer"

    handler(SimpleNamespace())
    assert scheduled_tasks, "status handler should schedule coroutine"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert status_calls and status_calls[0]["option_id"] == "status-done"
    assert label_calls == [("token", row.url)]
    assert stop_calls and stop_calls[0]["task_url"] == row.url
    assert row.url not in pending_urls
    assert "Status set to" in status_line_cell.cell_contents

    stored = db.load()[0]
    assert stored.status == "Done"
    assert stored.status_option_id == "status-done"
    assert stored.status_dirty == 0
    assert stored.status_pending_option_id == ""
    assert stored.is_done == 1
    assert row.url not in db.active_task_urls()

    from prompt_toolkit import Application

    assert Application.instances[-1].invalidate_calls > 0

    db.conn.close()


def test_apply_status_change_queues_and_handles_error(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    pending_ref = {"set": None, "calls": 0}

    def fake_set_project_status(token, project_id, item_id, field_id, option_id):
        pending_ref["calls"] += 1
        assert pending_ref["set"] is not None
        assert row.url in pending_ref["set"], "status should be marked pending before API call"
        raise RuntimeError("API boom")

    monkeypatch.setattr(ght, "set_project_status", fake_set_project_status)
    monkeypatch.setattr(ght, "fetch_labels_for_url", lambda *args, **kwargs: [])

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))
    handler = _find_binding(harness.kb, "D")
    handler_cells = _closure_cells(handler)
    apply_status = handler_cells["_apply_status_change"].cell_contents
    apply_cells = _closure_cells(apply_status)
    pending_ref["set"] = apply_cells["pending_status_urls"].cell_contents
    status_line_cell = apply_cells["status_line"]

    handler(SimpleNamespace())
    assert scheduled_tasks, "status hotkey should queue coroutine"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert pending_ref["calls"] == 1
    assert row.url not in pending_ref["set"], "pending flag should be cleared after failure"
    status_text = status_line_cell.cell_contents
    assert "Status update failed" in status_text
    assert "API boom" in status_text

    stored = db.load()[0]
    assert stored.status_option_id == "status-todo"
    assert stored.status == "Todo"
    assert stored.status_dirty == 0
    assert stored.status_pending_option_id == ""

    from prompt_toolkit import Application

    assert Application.instances[-1].invalidate_calls > 0

    db.conn.close()

def test_update_task_date_with_validation_and_success(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    date_calls = []

    def fake_set_project_date(token, project_id, item_id, field_id, value, field_name=None):
        date_calls.append((token, project_id, item_id, field_id, value))

    monkeypatch.setattr(ght, "set_project_date", fake_set_project_date)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    open_handler = _find_binding(harness.kb, "O")
    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "task_edit_state" in func.__code__.co_freevars,
    )

    enter_cells = _closure_cells(enter_handler)
    task_edit_state_cell = enter_cells["task_edit_state"]
    update_task = enter_cells["_update_task_date"].cell_contents
    update_cells = _closure_cells(update_task)
    status_line_cell = update_cells["status_line"]

    open_handler(SimpleNamespace())
    task_state = task_edit_state_cell.cell_contents
    assert task_state["mode"] == "list"
    assert task_state["fields"], "task editor should populate fields"

    enter_handler(SimpleNamespace())  # begin editing first field (Start date)
    task_state = task_edit_state_cell.cell_contents
    assert task_state["mode"] == "edit-date-calendar"
    editing = task_state.get("editing") or {}
    editing["calendar_date"] = "2024-13-40"
    task_state["editing"] = editing

    enter_handler(SimpleNamespace())  # attempt to commit invalid date
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    task_state = task_edit_state_cell.cell_contents
    assert not date_calls
    assert "Bad date" in task_state["message"]
    assert "Bad date" in status_line_cell.cell_contents

    enter_handler(SimpleNamespace())  # reopen editor for same field
    task_state = task_edit_state_cell.cell_contents
    editing = task_state.get("editing") or {}
    editing["calendar_date"] = "2024-01-15"
    task_state["editing"] = editing

    enter_handler(SimpleNamespace())  # commit valid date
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    task_state = task_edit_state_cell.cell_contents
    assert date_calls == [("token", "proj-123", "item-123", "start-field", "2024-01-15")]
    assert task_state["message"] == "Start Date updated"
    assert status_line_cell.cell_contents == "Start Date updated"

    stored = db.load()[0]
    assert stored.start_date == "2024-01-15"

    db.conn.close()


def test_update_task_date_missing_metadata_triggers_lookup(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(start_field_id="", focus_field_id="")
    db.upsert_many([row])

    lookup_calls = []

    def fake_get_project_field_id_by_name(token, project_id, name):
        lookup_calls.append((token, project_id, name))
        if len(lookup_calls) == 1:
            raise RuntimeError("not found")
        return "focus-field-resolved"

    monkeypatch.setattr(ght, "get_project_field_id_by_name", fake_get_project_field_id_by_name)

    date_calls = []

    def fake_set_project_date(token, project_id, item_id, field_id, value, field_name=None):
        date_calls.append((token, project_id, item_id, field_id, value))

    monkeypatch.setattr(ght, "set_project_date", fake_set_project_date)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    open_handler = _find_binding(harness.kb, "O")
    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "task_edit_state" in func.__code__.co_freevars,
    )

    enter_cells = _closure_cells(enter_handler)
    task_edit_state_cell = enter_cells["task_edit_state"]
    update_task = enter_cells["_update_task_date"].cell_contents
    update_cells = _closure_cells(update_task)
    status_line_cell = update_cells["status_line"]
    logger_cell = update_cells["logger"]

    warnings = []

    def fake_warning(msg, *args, **kwargs):
        warnings.append((msg, args, kwargs))

    monkeypatch.setattr(logger_cell.cell_contents, "warning", fake_warning, raising=False)

    open_handler(SimpleNamespace())
    task_state = task_edit_state_cell.cell_contents
    assert task_state["mode"] == "list"

    # Attempt to update Start date with missing field metadata (lookup fails)
    task_state["cursor"] = 0
    enter_handler(SimpleNamespace())
    task_state = task_edit_state_cell.cell_contents
    editing = task_state.get("editing") or {}
    editing["calendar_date"] = "2024-01-20"
    task_state["editing"] = editing

    enter_handler(SimpleNamespace())
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert lookup_calls[0] == ("token", "proj-123", "Start")
    assert warnings, "expected logger.warning when lookup fails"
    assert status_line_cell.cell_contents == "No Start field id"
    task_state = task_edit_state_cell.cell_contents
    assert task_state["message"] == "No Start field id"
    assert not date_calls, "start update should not proceed without field id"

    # Update Focus date where lookup succeeds and new field id is used
    task_state["cursor"] = 1
    task_state["mode"] = "list"
    enter_handler(SimpleNamespace())
    task_state = task_edit_state_cell.cell_contents
    editing = task_state.get("editing") or {}
    editing["calendar_date"] = "2024-02-02"
    task_state["editing"] = editing

    enter_handler(SimpleNamespace())
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert lookup_calls[1] == ("token", "proj-123", "Focus")
    assert date_calls == [("token", "proj-123", "item-123", "focus-field-resolved", "2024-02-02")]
    task_state = task_edit_state_cell.cell_contents
    assert task_state["message"] == "Focus Day updated"
    assert status_line_cell.cell_contents == "Focus Day updated"

    stored = db.load()[0]
    assert stored.focus_date == "2024-02-02"
    assert stored.focus_field_id == "focus-field-resolved"
    assert stored.start_field_id == ""  # unchanged after failed lookup

    db.conn.close()

def test_change_priority_handles_fetch_and_editor(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(priority_options="[]")
    db.upsert_many([row])

    fetch_calls = []

    priority_options = [
        {"id": "priority-low", "name": "Low"},
        {"id": "priority-medium", "name": "Medium"},
        {"id": "priority-high", "name": "High"},
    ]

    def fake_get_project_field_options(token, field_id):
        fetch_calls.append((token, field_id))
        return priority_options

    priority_calls = []

    def fake_set_project_priority(token, project_id, item_id, field_id, option_id):
        priority_calls.append((token, project_id, item_id, field_id, option_id))

    monkeypatch.setattr(ght, "get_project_field_options", fake_get_project_field_options)
    monkeypatch.setattr(ght, "set_project_priority", fake_set_project_priority)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    advance_handler = _find_binding(harness.kb, "]")
    advance_cells = _closure_cells(advance_handler)
    change_priority = advance_cells["_change_priority"].cell_contents
    change_cells = _closure_cells(change_priority)
    status_line_cell = change_cells["status_line"]

    advance_handler(SimpleNamespace())
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert fetch_calls == [("token", "priority-field")]
    assert priority_calls[0] == ("token", "proj-123", "item-123", "priority-field", "priority-high")
    assert status_line_cell.cell_contents == "Priority set to High"
    stored = db.load()[0]
    assert stored.priority_option_id == "priority-high"
    assert stored.priority == "High"

    open_handler = _find_binding(harness.kb, "O")
    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "task_edit_state" in func.__code__.co_freevars,
    )

    task_edit_state_cell = _closure_cells(enter_handler)["task_edit_state"]

    open_handler(SimpleNamespace())
    task_state = task_edit_state_cell.cell_contents
    fields = task_state["fields"]
    priority_idx = next(i for i, field in enumerate(fields) if field.get("field_key") == "priority")
    task_state["cursor"] = priority_idx

    enter_handler(SimpleNamespace())  # start priority select mode
    task_state = task_edit_state_cell.cell_contents
    assert task_state["mode"] == "priority-select"
    field = (task_state.get("fields") or [])[priority_idx]
    field["index"] = 0  # choose Low

    enter_handler(SimpleNamespace())  # commit explicit option id
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert priority_calls[1] == ("token", "proj-123", "item-123", "priority-field", "priority-low")
    assert status_line_cell.cell_contents == "Priority set to Low"
    stored = db.load()[0]
    assert stored.priority_option_id == "priority-low"
    assert stored.priority == "Low"

    db.conn.close()

def test_change_priority_no_options_and_failure_rolls_back(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(priority_options="[]")
    db.upsert_many([row])

    fetch_calls = []

    def fake_get_project_field_options(token, field_id):
        fetch_calls.append((token, field_id))
        return []

    monkeypatch.setattr(ght, "get_project_field_options", fake_get_project_field_options)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    advance_handler = _find_binding(harness.kb, "]")
    advance_cells = _closure_cells(advance_handler)
    change_priority = advance_cells["_change_priority"].cell_contents
    change_cells = _closure_cells(change_priority)
    status_line_cell = change_cells["status_line"]
    pending_urls = change_cells["pending_priority_urls"].cell_contents

    advance_handler(SimpleNamespace())
    assert scheduled_tasks, "expected coroutine for priority change"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert fetch_calls == [("token", "priority-field")]
    assert status_line_cell.cell_contents == "No priority options available"
    assert row.url not in pending_urls

    stored = db.load()[0]
    assert stored.priority_option_id == "priority-medium"
    assert stored.priority == "Medium"

    priority_options = [
        {"id": "priority-low", "name": "Low"},
        {"id": "priority-medium", "name": "Medium"},
        {"id": "priority-high", "name": "High"},
    ]
    db.update_priority_options(row.url, priority_options)
    change_cells["all_rows"].cell_contents = db.load()

    failure_calls = []

    def failing_set_project_priority(token, project_id, item_id, field_id, option_id):
        failure_calls.append((token, project_id, item_id, field_id, option_id))
        raise RuntimeError("API down")

    monkeypatch.setattr(ght, "set_project_priority", failing_set_project_priority)

    original_reset = db.reset_priority
    reset_calls = []

    def tracking_reset(url, priority_text, option_id):
        reset_calls.append((url, priority_text, option_id))
        return original_reset(url, priority_text, option_id)

    monkeypatch.setattr(db, "reset_priority", tracking_reset, raising=False)

    advance_handler(SimpleNamespace())
    assert scheduled_tasks, "expected coroutine for priority change failure"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert failure_calls == [("token", "proj-123", "item-123", "priority-field", "priority-high")]
    assert reset_calls == [(row.url, "Medium", "priority-medium")]
    assert status_line_cell.cell_contents == "Priority update failed: API down"
    assert row.url not in pending_urls

    stored = db.load()[0]
    assert stored.priority_option_id == "priority-medium"
    assert stored.priority == "Medium"

    db.conn.close()

def test_label_editor_fetch_cancel_and_error(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(labels=json.dumps(["Existing"]))
    db.upsert_many([row])

    behaviors = [
        [{"name": "Bug"}, {"name": "Chore"}],
        RuntimeError("API down"),
    ]
    fetch_calls = []

    def fake_list_repo_labels(token, repo):
        idx = len(fetch_calls)
        fetch_calls.append((token, repo))
        result = behaviors[idx]
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(ght, "list_repo_labels", fake_list_repo_labels)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    open_handler = _find_binding(harness.kb, "O")
    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "task_edit_state" in func.__code__.co_freevars,
    )

    enter_cells = _closure_cells(enter_handler)
    task_edit_state_cell = enter_cells["task_edit_state"]
    cancel_edit = enter_cells["_cancel_task_edit"].cell_contents

    open_handler(SimpleNamespace())
    task_state = task_edit_state_cell.cell_contents
    fields = task_state["fields"]
    label_idx = next(i for i, field in enumerate(fields) if field.get("field_key") == "labels")

    # Success path
    task_state["cursor"] = label_idx
    enter_handler(SimpleNamespace())
    assert task_state["mode"] == "edit-labels"
    assert task_state["labels_loading"] is True
    assert scheduled_tasks, "label load should schedule background task"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert fetch_calls == [("token", "octo/repo")]
    assert task_state["message"] == "Labels loaded"
    assert task_state["labels_loading"] is False
    assert task_state.get("labels_task") is None
    assert task_state["label_choices"] == ["Bug", "Chore", "Existing"]
    assert task_state["labels_selected"] == {"Existing"}
    assert task_state.get("labels_error") == ""

    cancel_edit("Back to list")
    assert task_state["mode"] == "list"
    assert task_state["message"] == "Back to list"

    # Cancellation while loading
    task_state["cursor"] = label_idx
    enter_handler(SimpleNamespace())
    assert task_state["mode"] == "edit-labels"
    assert task_state["labels_loading"] is True
    assert scheduled_tasks, "second label load should schedule background task"

    pending_coro = scheduled_tasks.pop()
    pending_coro.close()
    cancel_edit("Edit cancelled")
    assert task_state["mode"] == "list"
    assert task_state["message"] == "Edit cancelled"
    assert task_state["labels_loading"] is False
    assert task_state.get("labels_task") is None
    assert task_state.get("labels_error") == ""
    assert fields[label_idx]["value"] == ["Existing"]
    assert len(fetch_calls) == 1

    # Error path
    task_state["cursor"] = label_idx
    enter_handler(SimpleNamespace())
    assert scheduled_tasks, "failing label load should schedule background task"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert fetch_calls == [("token", "octo/repo"), ("token", "octo/repo")]
    assert task_state["labels_loading"] is False
    assert task_state.get("labels_task") is None
    assert task_state.get("labels_error") == "Label fetch failed: API down"
    assert task_state["labels_selected"] == {"Existing"}

    cancel_edit("Edit cancelled")
    db.conn.close()

def test_apply_labels_deduplicates_and_reports_errors(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(labels=json.dumps(["old"]))
    db.upsert_many([row])

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "_apply_labels" in func.__code__.co_freevars,
    )
    enter_cells = _closure_cells(enter_handler)
    task_edit_state_cell = enter_cells["task_edit_state"]
    apply_labels = enter_cells["_apply_labels"].cell_contents
    apply_cells = _closure_cells(apply_labels)
    status_line_cell = apply_cells["status_line"]

    open_handler = _find_binding(harness.kb, "O")
    open_handler(SimpleNamespace())

    label_calls = []

    def fake_set_issue_labels(token, url, labels):
        label_calls.append((token, url, labels))

    monkeypatch.setattr(ght, "set_issue_labels", fake_set_issue_labels)

    asyncio.run(apply_labels([" review ", "Bug", "bug", ""]))

    assert label_calls == [("token", row.url, ["review", "Bug"])]
    assert status_line_cell.cell_contents == "Labels updated"
    task_state = task_edit_state_cell.cell_contents
    assert task_state["message"] == "Labels updated"

    stored = db.load()[0]
    assert json.loads(stored.labels) == ["review", "Bug"]

    def failing_set_issue_labels(*_args, **_kwargs):
        raise RuntimeError("API down")

    monkeypatch.setattr(ght, "set_issue_labels", failing_set_issue_labels)

    asyncio.run(apply_labels(["Bug"]))

    task_state = task_edit_state_cell.cell_contents
    fail_message = task_state["message"]
    assert fail_message.startswith("Label update failed")
    assert "API down" in fail_message
    assert status_line_cell.cell_contents == fail_message
    stored = db.load()[0]
    assert json.loads(stored.labels) == ["review", "Bug"]

    db.conn.close()

def test_session_editor_edits_adjusts_and_deletes(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    cur = db.conn.cursor()
    cur.execute(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        (row.url, row.project_title, "2024-01-01T09:00:00+00:00", "2024-01-01T10:00:00+00:00", "[]"),
    )
    session_id = cur.lastrowid
    db.conn.commit()

    update_calls = []
    original_update = ght.TaskDB.update_session_times

    def tracking_update(self, session_id_int, *, started_at=ght._UNSET, ended_at=ght._UNSET):
        update_calls.append({
            "session_id": session_id_int,
            "started_at": started_at,
            "ended_at": ended_at,
        })
        return original_update(self, session_id_int, started_at=started_at, ended_at=ended_at)

    original_delete = ght.TaskDB.delete_session
    delete_calls = []

    def tracking_delete(self, session_id_int):
        delete_calls.append(session_id_int)
        return original_delete(self, session_id_int)

    monkeypatch.setattr(ght.TaskDB, "update_session_times", tracking_update)
    monkeypatch.setattr(ght.TaskDB, "delete_session", tracking_delete)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "session_state" in func.__code__.co_freevars,
    )
    start_edit_handler = _find_binding(
        harness.kb,
        "s",
        predicate=lambda func: "_begin_session_edit" in func.__code__.co_freevars,
    )
    adjust_plus = _find_binding(
        harness.kb,
        "+",
        predicate=lambda func: "_adjust_session_end" in func.__code__.co_freevars,
    )
    adjust_minus = _find_binding(
        harness.kb,
        "-",
        predicate=lambda func: "_adjust_session_end" in func.__code__.co_freevars,
    )
    delete_handler = _find_binding(
        harness.kb,
        "x",
        predicate=lambda func: "_delete_current_session" in func.__code__.co_freevars,
    )

    enter_cells = _closure_cells(enter_handler)
    session_state_cell = enter_cells["session_state"]
    edit_sessions_mode_cell = enter_cells["edit_sessions_mode"]
    status_line_cell = _closure_cells(enter_cells["_commit_session_edit"].cell_contents)["status_line"]
    session_entry = {
        "id": session_id,
        "start_dt": dt.datetime(2024, 1, 1, 9, 0, tzinfo=dt.timezone.utc),
        "end_dt": dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
        "start_display": "2024-01-01 09:00",
        "end_display": "2024-01-01 10:00",
        "duration": 3600,
        "open": False,
        "start_raw": "2024-01-01T09:00:00+00:00",
        "end_raw": "2024-01-01T10:00:00+00:00",
    }
    session_state_cell.cell_contents = {
        "task_url": row.url,
        "task_title": row.title,
        "project_title": row.project_title,
        "cursor": 0,
        "sessions": [session_entry],
        "edit_field": None,
        "input": "",
        "message": "",
        "selected_id": session_id,
        "total_duration": session_entry["duration"],
    }
    edit_sessions_mode_cell.cell_contents = True
    session_state = session_state_cell.cell_contents
    assert session_state.get("sessions"), "session editor should load sessions"

    start_edit_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["edit_field"] == "start"
    session_state["input"] = "2024-01-01 08:45"

    expected_start = dt.datetime(2024, 1, 1, 8, 45, tzinfo=session_entry["start_dt"].tzinfo).astimezone(dt.timezone.utc)

    enter_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert update_calls, "start edit should update session"
    assert session_state["message"] == "Start updated"
    assert status_line_cell.cell_contents == "Start updated"
    assert update_calls[-1]["session_id"] == session_id
    assert dt.datetime.fromisoformat(update_calls[-1]["started_at"]) == expected_start

    enter_handler(SimpleNamespace())  # begin end edit
    session_state = session_state_cell.cell_contents
    assert session_state["edit_field"] == "end"
    session_state["input"] = "2024-01-01 11:15"
    fallback_tz = session_state["sessions"][session_state.get("cursor", 0)]["end_dt"].tzinfo or dt.timezone.utc
    expected_end = dt.datetime(2024, 1, 1, 11, 15, tzinfo=fallback_tz).astimezone(dt.timezone.utc)

    enter_handler(SimpleNamespace())  # commit end edit
    session_state = session_state_cell.cell_contents
    assert session_state["message"] == "End updated"
    assert status_line_cell.cell_contents == "End updated"
    assert dt.datetime.fromisoformat(update_calls[-1]["ended_at"]) == expected_end

    adjust_plus(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["message"] == "End adjusted by +5 min"
    assert status_line_cell.cell_contents == "End adjusted by +5 min"
    expected_end = expected_end + dt.timedelta(minutes=5)
    assert dt.datetime.fromisoformat(update_calls[-1]["ended_at"]) == expected_end

    adjust_minus(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["message"].startswith("End adjusted by -5 min")
    assert status_line_cell.cell_contents.startswith("End adjusted by -5 min")
    expected_end = expected_end - dt.timedelta(minutes=5)
    assert dt.datetime.fromisoformat(update_calls[-1]["ended_at"]) == expected_end

    delete_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["message"] == "Session deleted"
    assert status_line_cell.cell_contents == "Session deleted"
    assert delete_calls == [session_id]
    assert not session_state["sessions"], "sessions should be empty after delete"

    db.conn.close()


def test_session_editor_validation_blocks_invalid_updates(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    cur = db.conn.cursor()
    cur.execute(
        "INSERT INTO work_sessions(task_url, project_title, started_at, ended_at, labels) VALUES (?,?,?,?,?)",
        (row.url, row.project_title, "2024-01-01T09:00:00+00:00", "2024-01-01T10:00:00+00:00", "[]"),
    )
    session_id = cur.lastrowid
    db.conn.commit()

    update_calls = []

    def tracking_update(self, session_id_int, *, started_at=ght._UNSET, ended_at=ght._UNSET):
        update_calls.append({
            "session_id": session_id_int,
            "started_at": started_at,
            "ended_at": ended_at,
        })
        return None

    monkeypatch.setattr(ght.TaskDB, "update_session_times", tracking_update)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "session_state" in func.__code__.co_freevars,
    )
    start_edit_handler = _find_binding(
        harness.kb,
        "s",
        predicate=lambda func: "_begin_session_edit" in func.__code__.co_freevars,
    )

    enter_cells = _closure_cells(enter_handler)
    session_state_cell = enter_cells["session_state"]
    edit_sessions_mode_cell = enter_cells["edit_sessions_mode"]
    commit_cells = _closure_cells(enter_cells["_commit_session_edit"].cell_contents)
    status_line_cell = commit_cells["status_line"]

    session_entry = {
        "id": session_id,
        "start_dt": dt.datetime(2024, 1, 1, 9, 0, tzinfo=dt.timezone.utc),
        "end_dt": dt.datetime(2024, 1, 1, 10, 0, tzinfo=dt.timezone.utc),
        "start_display": "2024-01-01 09:00",
        "end_display": "2024-01-01 10:00",
        "duration": 3600,
        "open": False,
        "start_raw": "2024-01-01T09:00:00+00:00",
        "end_raw": "2024-01-01T10:00:00+00:00",
    }
    session_state_cell.cell_contents = {
        "task_url": row.url,
        "task_title": row.title,
        "project_title": row.project_title,
        "cursor": 0,
        "sessions": [session_entry],
        "edit_field": None,
        "input": "",
        "message": "",
        "selected_id": session_id,
        "total_duration": session_entry["duration"],
    }
    edit_sessions_mode_cell.cell_contents = True

    start_edit_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["edit_field"] == "start"
    session_state["input"] = "not-a-date"

    enter_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["message"] == "Invalid start timestamp"
    assert session_state["edit_field"] == "start"
    assert update_calls == []
    assert status_line_cell.cell_contents == ""

    session_state["edit_field"] = None
    session_state["input"] = ""
    session_state["message"] = ""
    session_state = session_state_cell.cell_contents
    assert session_state["edit_field"] is None

    enter_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["edit_field"] == "end"
    session_state["input"] = "2024-01-01 08:30"

    enter_handler(SimpleNamespace())
    session_state = session_state_cell.cell_contents
    assert session_state["message"] == "End must be after start"
    assert session_state["edit_field"] == "end"
    assert update_calls == []
    assert status_line_cell.cell_contents == ""

    session_state["edit_field"] = None
    session_state["input"] = ""
    session_state["message"] = ""

    cur.execute(
        "SELECT started_at, ended_at FROM work_sessions WHERE id=?",
        (session_id,),
    )
    stored_start, stored_end = cur.fetchone()
    assert stored_start == "2024-01-01T09:00:00+00:00"
    assert stored_end == "2024-01-01T10:00:00+00:00"

    db.conn.close()


def test_start_repo_metadata_fetch_handles_failure(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    def failing_list_repo_labels(token, repo):
        raise RuntimeError("label boom")

    monkeypatch.setattr(ght, "list_repo_labels", failing_list_repo_labels)
    monkeypatch.setattr(ght, "list_repo_assignees", lambda *args, **kwargs: [])

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    add_handler = _find_binding(harness.kb, "A")
    add_handler(SimpleNamespace())

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "add_state" in func.__code__.co_freevars,
    )
    enter_cells = _closure_cells(enter_handler)
    add_state_cell = enter_cells["add_state"]
    start_repo_fetch = enter_cells["_start_repo_metadata_fetch"].cell_contents

    start_repo_fetch('octo/repo')

    state = add_state_cell.cell_contents
    assert state['loading_repo_metadata'] is True
    assert state['metadata_error'] == ''
    assert state['label_choices'] == []
    assert scheduled_tasks, "expected metadata fetch task to be scheduled"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    state = add_state_cell.cell_contents
    assert state['repo_metadata_source'] == 'octo/repo'
    assert state['loading_repo_metadata'] is False
    assert state['repo_metadata_task'] is None
    assert state['metadata_error'] == 'Metadata error: label boom'
    assert state['label_choices'] == []
    assert state['priority_choices'] == []
    assert state['assignee_choices'] == []

    start_repo_fetch('octo/repo')
    assert scheduled_tasks, "expected metadata fetch to reschedule after failure"
    coro = scheduled_tasks.pop()
    asyncio.run(coro)
    state = add_state_cell.cell_contents
    assert state['metadata_error'] == 'Metadata error: label boom'
    assert state['loading_repo_metadata'] is False

    db.conn.close()


def test_update_worker_handles_fetch_error(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    def failing_fetch(token, cfg, date_cutoff, progress, include_unassigned):
        raise RuntimeError("boom fetch")

    monkeypatch.setattr(ght, "fetch_tasks_github", failing_fetch)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    update_handler = _find_binding(harness.kb, "u")
    update_cells = _closure_cells(update_handler)
    update_worker_fn = update_cells["update_worker"].cell_contents
    worker_cells = _closure_cells(update_worker_fn)
    status_line_cell = worker_cells["status_line"]
    update_flag_cell = worker_cells["update_in_progress"]

    assert update_flag_cell.cell_contents is False

    update_handler(SimpleNamespace())
    assert scheduled_tasks, "expected update_worker to be scheduled"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert status_line_cell.cell_contents == "Error: boom fetch"
    assert update_flag_cell.cell_contents is False

    update_handler(SimpleNamespace())
    assert scheduled_tasks, "expected update to reschedule after failure"
    coro = scheduled_tasks.pop()
    asyncio.run(coro)
    assert update_flag_cell.cell_contents is False

    db.conn.close()


def test_add_comment_validation_and_error(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    calls = []

    def fake_add_issue_comment(token, url, body):
        calls.append((token, url, body))

    monkeypatch.setattr(ght, "add_issue_comment", fake_add_issue_comment)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    open_handler = _find_binding(harness.kb, "O")
    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "task_edit_state" in func.__code__.co_freevars,
    )

    enter_cells = _closure_cells(enter_handler)
    task_edit_state_cell = enter_cells["task_edit_state"]
    add_comment_fn = enter_cells["_add_comment"].cell_contents
    comment_cells = _closure_cells(add_comment_fn)
    status_line_cell = comment_cells["status_line"]

    open_handler(SimpleNamespace())

    task_state = task_edit_state_cell.cell_contents
    fields = task_state["fields"]
    comment_idx = next(i for i, field in enumerate(fields) if field.get("field_key") == "comment")
    task_state["cursor"] = comment_idx

    enter_handler(SimpleNamespace())  # begin comment edit
    task_state = task_edit_state_cell.cell_contents
    assert task_state["mode"] == "edit-comment"

    initial_status = status_line_cell.cell_contents

    enter_handler(SimpleNamespace())  # attempt empty comment
    task_state = task_edit_state_cell.cell_contents
    assert task_state["message"] == "Comment cannot be empty"
    assert status_line_cell.cell_contents == initial_status
    assert calls == []
    assert not scheduled_tasks

    task_state["input"] = "First comment"
    enter_handler(SimpleNamespace())  # commit valid comment

    assert scheduled_tasks, "posting comment should schedule coroutine"
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    task_state = task_edit_state_cell.cell_contents
    assert task_state["message"] == "Comment posted"
    assert status_line_cell.cell_contents == "Comment posted"
    assert calls == [("token", row.url, "First comment")]

    def failing_add_issue_comment(token, url, body):
        calls.append((token, url, body))
        raise RuntimeError("comment 500")

    monkeypatch.setattr(ght, "add_issue_comment", failing_add_issue_comment)

    task_state["cursor"] = comment_idx
    enter_handler(SimpleNamespace())  # reopen editor
    task_state = task_edit_state_cell.cell_contents
    assert task_state["mode"] == "edit-comment"
    task_state["input"] = "Boom"

    enter_handler(SimpleNamespace())
    assert scheduled_tasks, "failing comment should still schedule coroutine"
    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    task_state = task_edit_state_cell.cell_contents
    assert task_state["message"] == "Comment failed: comment 500"
    assert status_line_cell.cell_contents == "Comment failed: comment 500"
    assert calls[-1] == ("token", row.url, "Boom")

    db.conn.close()


def test_focus_shift_hotkeys_adjust_dates_and_status(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(focus_date="2024-01-01")
    db.upsert_many([row])

    date_calls = []

    def fake_set_project_date(token, project_id, item_id, field_id, value, field_name=None):
        date_calls.append((token, project_id, item_id, field_id, value))

    monkeypatch.setattr(ght, "set_project_date", fake_set_project_date)

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "_update_task_date" in func.__code__.co_freevars,
    )
    enter_cells = _closure_cells(enter_handler)
    status_line_cell = _closure_cells(enter_cells["_update_task_date"].cell_contents)["status_line"]

    shift_down_handler = _find_binding(
        harness.kb,
        "y",
        predicate=lambda func: "_focus_shift" in func.__code__.co_freevars,
    )

    shift_down_handler(SimpleNamespace())
    assert scheduled_tasks, "focus shift should schedule update"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert date_calls == [("token", "proj-123", "item-123", "focus-field", "2023-12-31")]
    assert status_line_cell.cell_contents == "Focus Day updated"
    stored = db.load()[0]
    assert stored.focus_date == "2023-12-31"

    shift_up_handler = _find_binding(
        harness.kb,
        "Y",
        predicate=lambda func: "_focus_shift" in func.__code__.co_freevars,
    )
    shift_up_handler(SimpleNamespace())
    assert scheduled_tasks, "focus shift up should schedule update"

    coro = scheduled_tasks.pop()
    asyncio.run(coro)

    assert date_calls[-1] == ("token", "proj-123", "item-123", "focus-field", "2024-01-01")
    assert status_line_cell.cell_contents == "Focus Day updated"
    stored = db.load()[0]
    assert stored.focus_date == "2024-01-01"

    db.conn.close()


def test_add_mode_iteration_comment_and_confirm(monkeypatch, temp_db_path, tmp_path, ui_config, scheduled_tasks):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row(
        iteration_options=json.dumps([
            {"id": "iter-1", "title": "Sprint 1"},
            {"id": "iter-2", "title": "Sprint 2"},
        ]),
        priority_field_id="prio-field",
        assignee_field_id="assignee-field",
    )
    db.upsert_many([row])

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    add_handler = _find_binding(harness.kb, "A")
    add_cells = _closure_cells(add_handler)
    add_handler(SimpleNamespace())

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "add_state" in func.__code__.co_freevars,
    )
    enter_cells = _closure_cells(enter_handler)
    add_state_cell = enter_cells["add_state"]
    status_line_cell = enter_cells["status_line"]
    start_repo_fetch_cell = enter_cells["_start_repo_metadata_fetch"]
    start_repo_fetch_cell.cell_contents = lambda _name: None
    create_task_cell = enter_cells["create_task_async"]

    repo_metadata_calls = []

    def fake_start_repo_metadata_fetch(repo_full):
        repo_metadata_calls.append(repo_full)
        add_state = add_state_cell.cell_contents
        add_state['loading_repo_metadata'] = False
        add_state['label_choices'] = ['bug', 'feature']
        add_state['labels_selected'] = {'bug'}
        add_state['priority_choices'] = ['High', 'Low']
        add_state['priority_label'] = 'High'
        add_state['priority_index'] = 0
        add_state['assignee_choices'] = [{'login': 'alice'}, {'login': 'bob'}]
        add_state['assignee_index'] = 0
        add_state['assignees_selected'] = {'alice'}

    start_repo_fetch_cell.cell_contents = fake_start_repo_metadata_fetch

    # Step through mode -> project -> repo
    enter_handler(SimpleNamespace())  # mode -> project
    enter_handler(SimpleNamespace())  # project -> repo
    state = add_state_cell.cell_contents
    assert state['step'] == 'repo'
    assert state['repo_choices'], "expected repo choices"

    enter_handler(SimpleNamespace())  # repo -> title (triggers metadata stub)
    state = add_state_cell.cell_contents
    assert repo_metadata_calls == [state['repo_full_name']]
    assert state['step'] == 'title'

    # Title validation
    enter_handler(SimpleNamespace())  # attempt without title
    assert status_line_cell.cell_contents == 'Title is required'
    state['title'] = 'New Issue'

    enter_handler(SimpleNamespace())  # title -> start
    enter_handler(SimpleNamespace())  # start -> end
    enter_handler(SimpleNamespace())  # end -> focus
    assert add_state_cell.cell_contents['step'] == 'focus'

    enter_handler(SimpleNamespace())  # focus -> iteration
    assert add_state_cell.cell_contents['step'] == 'iteration'

    enter_handler(SimpleNamespace())  # iteration -> labels
    assert add_state_cell.cell_contents['step'] == 'labels'

    # Labels already selected via stub
    enter_handler(SimpleNamespace())  # labels -> priority
    assert add_state_cell.cell_contents['step'] == 'priority'

    enter_handler(SimpleNamespace())  # priority -> assignee
    assert add_state_cell.cell_contents['step'] == 'assignee'

    enter_handler(SimpleNamespace())  # assignee -> comment
    assert add_state_cell.cell_contents['step'] == 'comment'

    state = add_state_cell.cell_contents
    state['comment'] = 'Hi'
    assert state['iteration_choices']
    assert isinstance(state['iteration_choices'][0], dict)
    state['iteration_index'] = 1

    enter_handler(SimpleNamespace())  # comment -> confirm
    assert add_state_cell.cell_contents['step'] == 'confirm'

    # Repository required error
    state = add_state_cell.cell_contents
    state['repo_choices'] = []
    state['repo_manual'] = ''
    state['repo_full_name'] = ''
    enter_handler(SimpleNamespace())
    assert status_line_cell.cell_contents == 'Repository is required'
    assert add_state_cell.cell_contents['step'] == 'confirm'

    # Title required error
    state['repo_manual'] = 'octo/repo'
    state['repo_full_name'] = 'octo/repo'
    state['title'] = ''
    enter_handler(SimpleNamespace())
    assert status_line_cell.cell_contents == 'Title is required'
    assert add_state_cell.cell_contents['step'] == 'title'

    # Prepare for successful confirm
    state['title'] = 'New Issue'
    state['step'] = 'confirm'

    recorded_create = {}

    async def fake_create_task_async(project_choice, title, start_val, end_val, focus_val, iteration_id, mode, repo_choice, repo_manual, repo_full, labels, priority_label, priority_options, assignees, comment):
        recorded_create.update(
            project_choice=project_choice,
            title=title,
            start=start_val,
            end=end_val,
            focus=focus_val,
            iteration_id=iteration_id,
            mode=mode,
            repo_choice=repo_choice,
            repo_manual=repo_manual,
            repo_full=repo_full,
            labels=labels,
            priority_label=priority_label,
            priority_options=priority_options,
            assignees=assignees,
            comment=comment,
        )

    create_task_cell.cell_contents = fake_create_task_async

    enter_handler(SimpleNamespace())
    assert status_line_cell.cell_contents == 'Creating item…'
    assert scheduled_tasks, 'expected create_task_async to be scheduled'
    asyncio.run(scheduled_tasks.pop())

    assert recorded_create['title'] == 'New Issue'
    assert recorded_create['mode'] == 'issue'
    assert recorded_create['repo_full'] == 'octo/repo'
    assert recorded_create['labels'] == ['bug']
    assert recorded_create['priority_label'] == 'High'
    assert recorded_create['assignees'] == ['alice']
    assert recorded_create['comment'] == 'Hi'
    assert recorded_create['iteration_id'] == 'iter-1'

    add_mode_cell = add_cells['add_mode']
    assert add_mode_cell.cell_contents is False

    db.conn.close()

def test_add_mode_calendar_navigation_and_cancel(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    add_handler = _find_binding(harness.kb, "A")
    add_handler(SimpleNamespace())

    enter_handler = _find_binding(
        harness.kb,
        "enter",
        predicate=lambda func: "add_state" in func.__code__.co_freevars,
    )
    enter_cells = _closure_cells(enter_handler)
    add_state_cell = enter_cells["add_state"]
    status_line_cell = enter_cells["status_line"]
    start_repo_fetch_cell = enter_cells["_start_repo_metadata_fetch"]
    start_repo_fetch_cell.cell_contents = lambda _name: None

    # Progress to start date step
    enter_handler(SimpleNamespace())  # mode -> project
    enter_handler(SimpleNamespace())  # project -> repo
    state = add_state_cell.cell_contents
    state['title'] = 'Calendar Test'
    enter_handler(SimpleNamespace())  # repo -> title
    enter_handler(SimpleNamespace())  # title -> start

    state = add_state_cell.cell_contents
    state['start_date'] = '2024-01-31'
    state['start_cursor'] = len(state['start_date'])

    calendar_handler = _find_binding(
        harness.kb,
        'c',
        predicate=lambda func: "_open_add_calendar" in func.__code__.co_freevars,
    )
    calendar_handler(SimpleNamespace())
    state = add_state_cell.cell_contents
    assert state['calendar_active'] is True
    assert state['calendar_date'] == '2024-01-31'

    right_handler = _find_binding(
        harness.kb,
        'right',
        predicate=lambda func: "_add_calendar_adjust" in func.__code__.co_freevars,
    )
    left_handler = _find_binding(
        harness.kb,
        'left',
        predicate=lambda func: "_add_calendar_adjust" in func.__code__.co_freevars,
    )
    month_forward_handler = _find_binding(
        harness.kb,
        '>',
        predicate=lambda func: "_add_calendar_adjust" in func.__code__.co_freevars,
    )
    month_back_handler = _find_binding(
        harness.kb,
        '<',
        predicate=lambda func: "_add_calendar_adjust" in func.__code__.co_freevars,
    )

    right_handler(SimpleNamespace())
    assert add_state_cell.cell_contents['calendar_date'] == '2024-02-01'
    left_handler(SimpleNamespace())
    assert add_state_cell.cell_contents['calendar_date'] == '2024-01-31'
    month_forward_handler(SimpleNamespace())
    assert add_state_cell.cell_contents['calendar_date'] == '2024-02-29'
    month_back_handler(SimpleNamespace())
    assert add_state_cell.cell_contents['calendar_date'] == '2024-01-29'

    escape_handler = _find_binding(
        harness.kb,
        'escape',
        predicate=lambda func: 'calendar_active' in func.__code__.co_consts,
    )
    escape_handler(SimpleNamespace())
    assert add_state_cell.cell_contents.get('calendar_active') is False
    assert status_line_cell.cell_contents == 'Calendar cancelled'

    db.conn.close()

def test_escape_handler_clears_modes(monkeypatch, temp_db_path, tmp_path, ui_config):
    db = ght.TaskDB(str(temp_db_path))
    row = _make_task_row()
    db.upsert_many([row])

    harness = _build_ui(db, ui_config, token="token", state_path=str(tmp_path / "state.json"))

    escape_handler = _find_binding(
        harness.kb,
        'escape',
        predicate=lambda func: 'calendar_active' in func.__code__.co_consts,
    )
    escape_cells = _closure_cells(escape_handler)
    status_line_cell = escape_cells['status_line']

    # Search mode
    escape_cells['in_search'].cell_contents = True
    escape_cells['search_buffer'].cell_contents = 'abc'
    escape_handler(SimpleNamespace())
    assert escape_cells['in_search'].cell_contents is False
    assert escape_cells['search_buffer'].cell_contents == ''
    assert status_line_cell.cell_contents == ''

    # Date filter
    escape_cells['in_date_filter'].cell_contents = True
    escape_cells['date_buffer'].cell_contents = '2024-01-01'
    escape_handler(SimpleNamespace())
    assert escape_cells['in_date_filter'].cell_contents is False
    assert escape_cells['date_buffer'].cell_contents == ''

    # Detail mode
    escape_cells['detail_mode'].cell_contents = True
    floats_cell = escape_cells['floats']
    floats_cell.cell_contents = [object()]
    escape_handler(SimpleNamespace())
    assert escape_cells['detail_mode'].cell_contents is False
    assert floats_cell.cell_contents == []

    # Report mode
    escape_cells['show_report'].cell_contents = True
    floats_cell.cell_contents = [object()]
    escape_handler(SimpleNamespace())
    assert escape_cells['show_report'].cell_contents is False
    assert floats_cell.cell_contents == []

    # Add mode
    escape_cells['add_mode'].cell_contents = True
    add_state_cell = escape_cells['add_state']
    add_state_cell.cell_contents = {'calendar_active': False}
    escape_handler(SimpleNamespace())
    assert escape_cells['add_mode'].cell_contents is False
    assert add_state_cell.cell_contents == {}
    assert status_line_cell.cell_contents == 'Add cancelled'

    # Session edit mode
    escape_cells['edit_sessions_mode'].cell_contents = True
    session_state_cell = escape_cells['session_state']
    session_state_cell.cell_contents = {'edit_field': None}
    escape_handler(SimpleNamespace())
    assert escape_cells['edit_sessions_mode'].cell_contents is False
    assert session_state_cell.cell_contents == {}
    assert status_line_cell.cell_contents == 'Timer editor closed'

    db.conn.close()
