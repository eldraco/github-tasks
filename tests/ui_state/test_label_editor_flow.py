from .helpers import (
    closure_value,
    dummy_event,
    editor_state_from,
    find_binding_for_state,
    find_state_change_binding,
    move_to_field,
)


def _open_label_editor(ui_context):
    open_editor = ui_context.find_binding('O')
    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    move_idle = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'cursor', 'idle move binding')
    enter = find_binding_for_state(ui_context.kb.bindings, 'enter', state, 'enter binding for label editor')
    move_to_field(enter, move_idle, state, 'labels')
    return state, enter


def test_label_editor_metadata_and_toggle(ui_context):
    state, enter = _open_label_editor(ui_context)
    space_toggle = find_binding_for_state(ui_context.kb.bindings, ' ', state, 'label toggle binding')

    enter(dummy_event())
    assert state['mode'] == 'edit-labels'
    assert state['labels_loading'] is True
    ui_context.run_pending('_load_label_choices_for_editor')

    assert state['labels_loading'] is False
    assert state['label_choices'] == ['bug', 'feature']
    assert state['labels_selected'] == {'bug'}

    nav_down = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'label_index', 'label navigation binding')
    nav_down(dummy_event())
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug', 'feature'}
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug'}


def test_label_editor_commit_triggers_apply(ui_context):
    state, enter = _open_label_editor(ui_context)
    space_toggle = find_binding_for_state(ui_context.kb.bindings, ' ', state, 'label toggle binding')

    enter(dummy_event())
    assert state['mode'] == 'edit-labels'
    assert state['labels_loading'] is True
    label_cursor = state.get('cursor', 0)

    enter(dummy_event())
    assert state['mode'] == 'edit-labels'
    assert 'Labels still loading' in (state.get('message') or '')

    ui_context.run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['labels_selected'] == {'bug'}

    nav_down = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'label_index', 'label navigation binding')
    nav_down(dummy_event())
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug', 'feature'}

    enter(dummy_event())
    assert state['mode'] == 'list'
    assert state['labels_task'] is None
    assert state['labels_loading'] is False
    assert state['labels_error'] == ''
    assert 'Updating labels' in (state.get('message') or '')

    label_field = state['fields'][label_cursor]
    assert label_field['value'] == ['bug', 'feature']

    apply_tasks = [
        coro
        for coro in ui_context.pending_tasks
        if getattr(getattr(coro, 'cr_code', None), 'co_name', '') == '_apply_labels'
    ]
    assert apply_tasks, 'apply labels task not scheduled'
    for coro in apply_tasks:
        coro.close()
        ui_context.pending_tasks.remove(coro)


def test_label_editor_cancel_reverts_changes(ui_context):
    state, enter = _open_label_editor(ui_context)
    space_toggle = find_binding_for_state(ui_context.kb.bindings, ' ', state, 'label toggle binding')

    enter(dummy_event())
    ui_context.run_pending('_load_label_choices_for_editor')

    nav_down = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'label_index', 'label navigation binding')
    nav_down(dummy_event())
    space_toggle(dummy_event())
    assert state['labels_selected'] == {'bug', 'feature'}

    cancel = None
    for keys, _kwargs, func in ui_context.kb.bindings:
        if 'escape' not in keys:
            continue
        try:
            cancel_func = closure_value(func, '_cancel_task_edit')
            state_ref = closure_value(func, 'task_edit_state')
        except KeyError:
            continue
        if state_ref is state:
            cancel = cancel_func
            break

    assert cancel is not None, 'cancel function for label editor not found'
    cancel('Edit cancelled')

    assert state['mode'] == 'list'
    assert state.get('labels_task') is None
    assert state.get('labels_loading') is False
    assert state.get('labels_error', '') == ''
    assert state.get('message') == 'Edit cancelled'

    label_field = next(f for f in state['fields'] if f.get('type') == 'labels')
    assert label_field['value'] == ['bug']
