import json
from types import SimpleNamespace

import gh_task_viewer as ght


def make_task(**overrides) -> ght.TaskRow:
    base = dict(
        owner_type='org',
        owner='acme',
        project_number=1,
        project_title='Project Alpha',
        start_field='Start date',
        start_date='2024-01-09',
        end_field='End date',
        end_date='2024-01-11',
        focus_field='Focus Day',
        focus_date='2024-01-10',
        title='Task One',
        url='https://github.com/acme/repo/issues/1',
        updated_at='2024-01-10T10:00:00',
        repo='acme/repo',
        repo_id='repo-1',
        labels=json.dumps(['bug']),
        priority='High',
        priority_field_id='prio-field',
        priority_option_id='prio-high',
        priority_options=json.dumps([
            {'id': 'prio-high', 'name': 'High'},
            {'id': 'prio-medium', 'name': 'Medium'},
            {'id': 'prio-low', 'name': 'Low'},
        ], ensure_ascii=False),
        status='Todo',
        status_field_id='status-field',
        status_option_id='status-todo',
        status_options=json.dumps([
            {'id': 'status-todo', 'name': 'Todo'},
            {'id': 'status-progress', 'name': 'In Progress'},
            {'id': 'status-done', 'name': 'Done'},
        ], ensure_ascii=False),
        item_id='item-1',
        project_id='proj-1',
        assignee_field_id='assignee-field',
        assignee_user_ids=json.dumps(['user-1']),
        assignee_logins=json.dumps(['octocat']),
    )
    base.update(overrides)
    return ght.TaskRow(**base)


def closure_map(func):
    return {var: cell for var, cell in zip(func.__code__.co_freevars, func.__closure__ or [])}


def closure_value(func, name):
    return closure_map(func)[name].cell_contents


def dummy_event(data='', app=None):
    return SimpleNamespace(data=data, app=app or SimpleNamespace(exit=lambda: None))


def editor_state_from(binding):
    """Return the task editor state dict captured by a keybinding closure."""
    try:
        return closure_value(binding, 'task_edit_state')
    except KeyError:
        opener = closure_value(binding, 'open_task_editor')
        return closure_value(opener, 'task_edit_state')


def find_state_change_binding(bindings, key, state, attr, description):
    """Locate the binding that mutates a particular attribute on the state."""
    had_attr = attr in state
    before = state.get(attr, 0)
    for keys, _, func in bindings:
        if key not in keys:
            continue
        closure_cells = func.__closure__ or ()
        if closure_cells and not any(getattr(cell, 'cell_contents', None) is state for cell in closure_cells):
            continue
        func(dummy_event())
        if state.get(attr, before) != before:
            if had_attr:
                state[attr] = before
            else:
                state.pop(attr, None)
            return func
    raise AssertionError(f'{description} not found')


def find_binding_for_state(bindings, key, state, description):
    """Find the first binding for ``key`` that references ``state`` in its closure."""
    for keys, _, func in bindings:
        if key not in keys:
            continue
        try:
            if editor_state_from(func) is state:
                return func
        except KeyError:
            pass
        closure_cells = func.__closure__ or ()
        if any(getattr(cell, 'cell_contents', None) is state for cell in closure_cells):
            return func
    raise AssertionError(f'{description} not found')


def ticker_update(app):
    if not app.background_tasks:
        raise AssertionError('Ticker coroutine not registered')
    coro = app.background_tasks[-1]
    frame = getattr(coro, 'cr_frame', None)
    if frame is None or 'update_search_status' not in frame.f_locals:
        raise AssertionError('update_search_status unavailable')
    return frame.f_locals['update_search_status']


def cycle_until(condition, func):
    for _ in range(10):
        if condition():
            break
        func(dummy_event())
    assert condition()


def move_to_field(func_enter, func_move, state, field_type):
    for _ in range(10):
        current = state['fields'][state.get('cursor', 0)]
        if current.get('type') == field_type:
            return
        func_move(dummy_event())
    raise AssertionError(f'Field {field_type} not reachable')


__all__ = [
    'make_task',
    'closure_map',
    'closure_value',
    'dummy_event',
    'editor_state_from',
    'find_state_change_binding',
    'find_binding_for_state',
    'ticker_update',
    'cycle_until',
    'move_to_field',
]
