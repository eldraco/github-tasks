import gh_task_viewer as ght

from .helpers import closure_value, dummy_event, find_binding_for_state


def _open_add_overlay(ui_context):
    open_add = ui_context.find_binding('A')
    open_add(dummy_event())
    add_state = closure_value(open_add, 'add_state')
    enter = find_binding_for_state(
        ui_context.kb.bindings,
        'enter',
        add_state,
        'enter binding for add overlay',
    )
    return open_add, add_state, enter


def _advance_to_repo_step(enter, add_state):
    # mode -> project
    enter(dummy_event())
    assert add_state['step'] == 'project'
    # project -> repo
    enter(dummy_event())
    assert add_state['step'] == 'repo'


def test_add_metadata_fetch_success(ui_context, monkeypatch):
    open_add, add_state, enter = _open_add_overlay(ui_context)

    monkeypatch.setattr(
        ght,
        'list_repo_labels',
        lambda *args, **kwargs: [{'name': 'Bug'}, {'name': 'Docs'}],
    )
    monkeypatch.setattr(
        ght,
        'list_repo_assignees',
        lambda *args, **kwargs: [
            {'login': 'octocat', 'name': 'Octocat'},
            {'login': 'robot', 'name': ''},
        ],
    )

    _advance_to_repo_step(enter, add_state)

    # Trigger metadata fetch and ensure it starts loading
    enter(dummy_event())
    assert add_state['step'] == 'title'
    assert add_state['loading_repo_metadata'] is True
    assert add_state['repo_metadata_task'] is not None

    ui_context.run_pending('_fetch_repo_metadata')

    assert add_state['loading_repo_metadata'] is False
    assert add_state['repo_metadata_task'] is None
    assert add_state['metadata_error'] == ''
    assert add_state['label_choices'] == ['Bug', 'Docs']
    assert add_state['labels_selected'] == set()
    # Project priority options are preloaded; ensure they are exposed
    assert add_state['priority_choices']
    assignees = add_state.get('assignee_choices') or []
    assert [entry['login'] for entry in assignees] == ['octocat', 'robot']


def test_add_metadata_fetch_failure(ui_context, monkeypatch):
    open_add, add_state, enter = _open_add_overlay(ui_context)

    def boom(*args, **kwargs):
        raise RuntimeError('boom')

    monkeypatch.setattr(ght, 'list_repo_labels', boom)
    monkeypatch.setattr(ght, 'list_repo_assignees', boom)

    _advance_to_repo_step(enter, add_state)

    enter(dummy_event())
    assert add_state['loading_repo_metadata'] is True

    ui_context.run_pending('_fetch_repo_metadata')

    assert add_state['loading_repo_metadata'] is False
    assert add_state['repo_metadata_task'] is None
    assert add_state['label_choices'] == []
    assert add_state['priority_choices'] == []
    assert add_state['assignee_choices'] == []
    assert add_state['metadata_error'] == 'Metadata error: boom'
