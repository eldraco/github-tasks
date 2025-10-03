import gh_task_viewer as ght

from .helpers import (
    dummy_event,
    editor_state_from,
    find_binding_for_state,
    find_state_change_binding,
    move_to_field,
)


def _prepare_label_edit(ui_context):
    open_editor = ui_context.find_binding('O')
    open_editor(dummy_event())
    state = editor_state_from(open_editor)
    move_idle = find_state_change_binding(ui_context.kb.bindings, 'j', state, 'cursor', 'idle move binding')
    enter = find_binding_for_state(ui_context.kb.bindings, 'enter', state, 'enter binding for label editor')
    move_to_field(enter, move_idle, state, 'labels')
    return state, enter


def test_label_editor_retains_unknown_labels(ui_context, monkeypatch):
    state, enter = _prepare_label_edit(ui_context)

    monkeypatch.setattr(
        ght,
        'list_repo_labels',
        lambda *args, **kwargs: [
            {'name': 'Bug'},
            {'name': 'bug'},
            {'name': 'Feature'},
        ],
    )

    label_field = next(f for f in state['fields'] if f.get('type') == 'labels')
    label_field['value'] = ['bug', 'urgent']

    enter(dummy_event())
    assert state['labels_loading'] is True

    ui_context.run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['labels_error'] == ''
    assert 'Labels loaded' in (state.get('message') or '')
    assert state['label_choices'] == ['Bug', 'Feature', 'urgent']
    assert 'bug' not in state['label_choices']
    assert state['labels_selected'] == {'bug', 'urgent'}


def test_label_editor_handles_empty_label_list(ui_context, monkeypatch):
    state, enter = _prepare_label_edit(ui_context)
    monkeypatch.setattr(ght, 'list_repo_labels', lambda *args, **kwargs: [])

    label_field = next(f for f in state['fields'] if f.get('type') == 'labels')
    label_field['value'] = []

    enter(dummy_event())
    assert state['mode'] == 'edit-labels'
    assert state['labels_loading'] is True

    ui_context.run_pending('_load_label_choices_for_editor')
    assert state['labels_loading'] is False
    assert state['label_choices'] == []
    assert state['labels_selected'] == set()
    assert state['labels_error'] == 'No labels available'
    assert state.get('message') == 'No labels available'
    assert state.get('label_index') == 0
    assert state['labels_task'] is None
