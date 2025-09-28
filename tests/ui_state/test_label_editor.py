import asyncio

import gh_task_viewer as ght
import prompt_toolkit

from .helpers import (
    dummy_event,
    editor_state_from,
    find_binding_for_state,
    find_state_change_binding,
    make_task,
    move_to_field,
)


def test_label_editor_metadata_and_toggle(ui_context):
    open_editor = ui_context.find_binding('O')
    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    move_idle = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'cursor', 'idle move binding')
    enter = find_binding_for_state(ui_context.kb.bindings, 'enter', state, 'enter binding for label editor')
    space_toggle = find_binding_for_state(ui_context.kb.bindings, ' ', state, 'label toggle binding')
    assert state['mode'] == 'list'

    move_to_field(enter, move_idle, state, 'labels')
    enter(dummy_event())
    assert state['mode'] == 'edit-labels'
    assert state['labels_loading'] is True
    assert state['label_choices'] == []
    labels_task = state['labels_task']
    assert labels_task is not None and hasattr(labels_task, 'cancel')

    ui_context.run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['label_choices'] == ['bug', 'feature']
    assert state['labels_selected'] == {'bug'}
    assert state['labels_task'] is None

    nav_down = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'label_index', 'label navigation binding')
    nav_down(dummy_event())  # move to 'feature'
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug', 'feature'}
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug'}


def test_label_editor_commit_triggers_apply(ui_context):
    open_editor = ui_context.find_binding('O')
    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    move_idle = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'cursor', 'idle move binding')
    enter = find_binding_for_state(ui_context.kb.bindings, 'enter', state, 'enter binding for label editor')
    space_toggle = find_binding_for_state(ui_context.kb.bindings, ' ', state, 'label toggle binding')

    move_to_field(enter, move_idle, state, 'labels')
    enter(dummy_event())
    assert state['mode'] == 'edit-labels'
    assert state['labels_loading'] is True
    label_cursor = state.get('cursor', 0)

    enter(dummy_event())  # attempt to save while still loading
    assert state['mode'] == 'edit-labels'
    assert 'Labels still loading' in (state.get('message') or '')

    ui_context.run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['labels_selected'] == {'bug'}

    nav_down = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'label_index', 'label navigation binding')
    nav_down(dummy_event())
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug', 'feature'}

    enter(dummy_event())  # commit label edits
    assert state['mode'] == 'list'
    assert state['labels_task'] is None
    assert state['labels_loading'] is False
    assert state['labels_error'] == ''
    assert 'Updating labels' in (state.get('message') or '')

    label_field = state['fields'][label_cursor]
    assert label_field['value'] == ['bug', 'feature']

    apply_tasks = [coro for coro in ui_context.pending_tasks if getattr(getattr(coro, 'cr_code', None), 'co_name', '') == '_apply_labels']
    assert apply_tasks, 'apply labels task not scheduled'
    for coro in apply_tasks:
        coro.close()
        ui_context.pending_tasks.remove(coro)


def test_label_editor_retains_unknown_labels(ui_context):
    open_editor = ui_context.find_binding('O')
    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    move_idle = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'cursor', 'idle move binding')
    enter = ui_context.find_binding('enter')

    label_field = next(f for f in state['fields'] if f.get('type') == 'labels')
    label_field['value'] = ['bug', 'urgent']

    move_to_field(enter, move_idle, state, 'labels')
    enter(dummy_event())
    assert state['labels_loading'] is True

    ui_context.run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['labels_error'] == ''
    assert 'Labels loaded' in (state.get('message') or '')
    assert 'urgent' in state.get('label_choices', [])
    assert state['labels_selected'] == {'bug', 'urgent'}


def test_label_editor_metadata_error(monkeypatch, tmp_path):
    prompt_toolkit.Application.instances.clear()
    prompt_toolkit.key_binding.KeyBindings.instances.clear()

    db = ght.TaskDB(':memory:')
    db.upsert_many([make_task()])
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

    def run_pending(name=None):
        to_run = []
        for coro in list(pending_tasks):
            code = getattr(coro, 'cr_code', None)
            func_name = code.co_name if code else ''
            if name is None or func_name == name:
                to_run.append(coro)
        for coro in to_run:
            pending_tasks.remove(coro)
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(coro)
            finally:
                loop.close()

    def fake_create_task(coro):
        pending_tasks.append(coro)
        return DummyTask(coro)

    async def fake_sleep(_seconds):
        return None

    def raise_labels(*args, **kwargs):
        raise RuntimeError('boom')

    monkeypatch.setattr(ght.asyncio, 'create_task', fake_create_task)
    monkeypatch.setattr(ght.asyncio, 'sleep', fake_sleep)
    monkeypatch.setattr(ght, 'list_repo_labels', raise_labels)
    monkeypatch.setattr(ght, 'fetch_tasks_github', lambda *a, **k: ght.FetchTasksResult(rows=[], partial=False, message=''))

    state_path = tmp_path / 'ui_state.json'
    ght.run_ui(db, cfg, token='token', state_path=str(state_path), log_level='ERROR')
    kb_cls = prompt_toolkit.key_binding.KeyBindings
    kb = kb_cls.instances[-1]
    open_editor = next(func for keys, _, func in kb.bindings if 'O' in keys)

    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    move_idle = find_state_change_binding(kb.bindings, 'j', state, 'cursor', 'idle move binding')
    enter = find_binding_for_state(kb.bindings, 'enter', state, 'enter binding for label editor')
    move_to_field(enter, move_idle, state, 'labels')
    enter(dummy_event())
    assert state['labels_loading'] is True

    run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['labels_error'].startswith('Label fetch failed')

    for coro in list(pending_tasks):
        coro.close()
        pending_tasks.remove(coro)

    app = prompt_toolkit.Application.instances[-1]
    background = getattr(app, 'background_tasks', None) or []
    for coro in list(background):
        try:
            coro.close()
        except RuntimeError:
            pass
        if coro in background:
            background.remove(coro)

    db.conn.close()
