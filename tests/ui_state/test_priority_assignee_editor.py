from .helpers import (
    dummy_event,
    editor_state_from,
    find_binding_for_state,
    find_state_change_binding,
    move_to_field,
)


def test_priority_and_assignee_editor_states(ui_context):
    open_editor = ui_context.find_binding('O')
    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    enter = find_binding_for_state(ui_context.kb.bindings, 'enter', state, 'enter binding for task editor')
    move_idle = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'cursor', 'idle move binding')

    def find_priority_nav():
        for keys, _, func in ui_context.kb.bindings:
            if 'j' not in keys:
                continue
            try:
                if editor_state_from(func) is not state:
                    continue
            except KeyError:
                continue
            fields = state.get('fields') or []
            editing = state.get('editing') or {}
            idx = editing.get('field_idx')
            if idx is None or idx >= len(fields):
                continue
            before_cursor = state.get('cursor', 0)
            before_index = fields[idx].get('index', 0)
            func(dummy_event())
            after_index = fields[idx].get('index', 0)
            after_cursor = state.get('cursor', 0)
            if after_cursor != before_cursor:
                state['cursor'] = before_cursor
            if after_index != before_index:
                fields[idx]['index'] = before_index
                return func
        raise AssertionError('priority navigation binding not found')

    # priority field
    move_to_field(enter, move_idle, state, 'priority')
    enter(dummy_event())
    assert state['mode'] == 'priority-select'
    nav_priority = find_priority_nav()
    nav_priority(dummy_event())
    fields = state['fields']
    idx = fields[state['editing']['field_idx']]['index']
    assert idx == 1  # moved to next option
    assert state['message'] == fields[state['editing']['field_idx']]['options'][idx]['name']

    enter(dummy_event())  # commit priority change (schedules task)
    assert state['mode'] == 'list'
    scheduled_priority = [
        coro for coro in ui_context.pending_tasks
        if getattr(getattr(coro, 'cr_code', None), 'co_name', '') == '_change_priority'
    ]
    assert scheduled_priority, 'priority change coroutine not scheduled'
    for coro in scheduled_priority:
        coro.close()
        ui_context.pending_tasks.remove(coro)

    # assignee field
    move_to_field(enter, move_idle, state, 'assignees')
    enter(dummy_event())
    assert state['mode'] == 'edit-assignees'
    assert state['input'] == 'octocat'
    state['input'] = 'octocat, hubot'
    enter(dummy_event())
    assert state['mode'] == 'list'
    fields = state['fields']
    field = next(f for f in fields if f.get('type') == 'assignees')
    assert field['value'] == ['octocat', 'hubot']
    scheduled_assignees = [
        coro for coro in ui_context.pending_tasks
        if getattr(getattr(coro, 'cr_code', None), 'co_name', '') == '_apply_assignees'
    ]
    assert scheduled_assignees, 'assignee apply coroutine not scheduled'
    for coro in scheduled_assignees:
        coro.close()
        ui_context.pending_tasks.remove(coro)
