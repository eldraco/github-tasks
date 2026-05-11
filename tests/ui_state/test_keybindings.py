import inspect
from types import SimpleNamespace

import prompt_toolkit
from prompt_toolkit.keys import Keys
from prompt_toolkit.layout.dimension import Dimension as _Dimension

from .helpers import closure_map, closure_value, dummy_event, make_task, ticker_update


if not hasattr(_Dimension, 'exact'):
    def _dimension_exact(value):
        """Provide prompt_toolkit Dimension.exact compatibility for older versions."""
        params = inspect.signature(_Dimension).parameters
        kwargs = {}
        for name in ('min', 'preferred', 'max'):
            if name in params:
                kwargs[name] = value
        try:
            return _Dimension(**kwargs) if kwargs else _Dimension(value)
        except TypeError:
            # Fallback: some builds expect positional args only
            return _Dimension(value)

    _Dimension.exact = _dimension_exact  # type: ignore[attr-defined]


def get_binding(ui_context, key, *, requires=None):
    """Find the key binding that captures specific closure variables."""
    required = set(requires or ())
    for keys, _kwargs, func in ui_context.kb.bindings:
        if key not in keys:
            continue
        if not required:
            return func
        captures = set(closure_map(func).keys())
        if required.issubset(captures):
            return func
    raise AssertionError(f"Binding for {key!r} with closures {sorted(required)} not found")


def test_search_and_sort_keybindings(ui_context):
    slash = get_binding(ui_context, '/', requires={'in_search'})
    any_key = get_binding(ui_context, Keys.Any, requires={'search_buffer'})
    enter = get_binding(ui_context, 'enter', requires={'finalize_search'})
    escape = get_binding(ui_context, 'escape', requires={'in_search'})
    sort_forward = get_binding(ui_context, 's', requires={'_cycle_sort'})
    project_cycle = get_binding(ui_context, 'p', requires={'project_cycle'})
    project_clear = get_binding(ui_context, 'P', requires={'project_cycle'})

    slash(dummy_event())
    assert closure_value(slash, 'in_search') is True
    assert closure_value(slash, 'status_line') == 'Search: '

    any_key(dummy_event(data='x'))
    assert closure_value(slash, 'search_buffer') == 'x'
    assert closure_value(slash, 'status_line') == 'Search: x'

    update_fn = ticker_update(ui_context.app)
    update_fn()
    assert closure_value(slash, 'status_line') == 'Search: x'

    enter(dummy_event())
    assert closure_value(slash, 'in_search') is False
    finalize_search = closure_value(enter, 'finalize_search')
    assert closure_value(finalize_search, 'search_term') == 'x'

    update_fn()
    assert closure_value(slash, 'status_line') == ''

    sort_forward(dummy_event())
    assert 'Sort:' in closure_value(slash, 'status_line')

    project_cycle(dummy_event())
    assert closure_value(project_cycle, 'project_cycle') == 'Project Alpha'
    project_clear(dummy_event())
    assert closure_value(project_clear, 'project_cycle') is None

    slash(dummy_event())
    any_key(dummy_event(data='y'))
    assert closure_value(slash, 'search_buffer') == 'y'

    escape(dummy_event())
    assert closure_value(slash, 'in_search') is False
    assert closure_value(slash, 'search_buffer') == ''
    assert closure_value(slash, 'status_line') == ''


def test_search_mode_disables_quick_add_hotkey(ui_context):
    slash = get_binding(ui_context, '/', requires={'in_search'})

    quick_add_filters = []
    for kb in prompt_toolkit.key_binding.KeyBindings.instances:
        for keys, kwargs, _func in kb.bindings:
            if 'n' in keys and 'filter' in kwargs:
                quick_add_filters.append(kwargs['filter'])

    assert quick_add_filters, "Quick add hotkey bindings not found"
    assert all(filter_fn() for filter_fn in quick_add_filters)

    slash(dummy_event())

    assert closure_value(slash, 'in_search') is True
    assert all(not filter_fn() for filter_fn in quick_add_filters)


def test_date_filter_flow(ui_context):
    date_start = get_binding(ui_context, 'F', requires={'in_date_filter'})
    any_key = get_binding(ui_context, Keys.Any, requires={'search_buffer', 'date_buffer'})
    enter = get_binding(ui_context, 'enter', requires={'finalize_date'})

    date_start(dummy_event())
    assert closure_value(date_start, 'in_date_filter') is True
    for ch in '2024-01-10':
        any_key(dummy_event(data=ch))
    assert closure_value(date_start, 'date_buffer') == '2024-01-10'

    enter(dummy_event())
    assert closure_value(date_start, 'in_date_filter') is False
    finalize_date = closure_value(enter, 'finalize_date')
    assert closure_value(finalize_date, 'date_max') == '2024-01-10'
    assert closure_value(date_start, 'status_line') == ''


def test_filter_toggle_keybindings(ui_context):
    toggle_done = get_binding(ui_context, 'd', requires={'hide_done'})
    show_today = get_binding(ui_context, 't', requires={'show_today_only'})
    show_all = get_binding(ui_context, 'a', requires={'show_today_only'})
    toggle_no_date = get_binding(ui_context, 'N', requires={'hide_no_date'})
    toggle_iteration = get_binding(ui_context, 'V', requires={'use_iteration'})
    toggle_created = get_binding(ui_context, 'C', requires={'include_created'})

    toggle_done(dummy_event())
    assert closure_value(toggle_done, 'hide_done') is True
    assert closure_value(toggle_done, 'current_index') == 0
    toggle_done(dummy_event())
    assert closure_value(toggle_done, 'hide_done') is False

    show_today(dummy_event())
    assert closure_value(show_today, 'show_today_only') is True
    assert closure_value(show_today, 'current_index') == 0
    show_all(dummy_event())
    assert closure_value(show_all, 'show_today_only') is False

    toggle_no_date(dummy_event())
    assert closure_value(toggle_no_date, 'hide_no_date') is True
    toggle_no_date(dummy_event())
    assert closure_value(toggle_no_date, 'hide_no_date') is False

    toggle_iteration(dummy_event())
    assert closure_value(toggle_iteration, 'use_iteration') is True
    assert closure_value(toggle_iteration, 'status_line') == 'Iteration view ON'
    toggle_iteration(dummy_event())
    assert closure_value(toggle_iteration, 'use_iteration') is False
    assert closure_value(toggle_iteration, 'status_line') == 'Iteration view OFF'

    toggle_created(dummy_event())
    assert closure_value(toggle_created, 'include_created') is False
    assert closure_value(toggle_created, 'status_line') == 'Hiding created-only tasks'
    toggle_created(dummy_event())
    assert closure_value(toggle_created, 'include_created') is True
    assert closure_value(toggle_created, 'status_line') == 'Including created tasks'


def test_project_filter_excludes_pending_tasks_from_other_projects(ui_context):
    show_all = get_binding(ui_context, 'a', requires={'show_today_only'})
    project_cycle = get_binding(ui_context, 'p', requires={'project_cycle'})
    open_editor = get_binding(ui_context, 'O')

    open_task_editor = closure_value(open_editor, 'open_task_editor')
    filtered_rows = closure_value(open_task_editor, 'filtered_rows')

    ui_context.db.upsert_many([
        make_task(
            project_title='Project Beta',
            title='Pending Beta Task',
            url='pending://project-beta-task',
        )
    ])

    show_all(dummy_event())
    before = filtered_rows()
    assert {row.project_title for row in before} == {'Project Alpha', 'Project Beta'}

    project_cycle(dummy_event())
    assert closure_value(project_cycle, 'project_cycle') == 'Project Alpha'

    after = filtered_rows()
    assert after
    assert {row.project_title for row in after} == {'Project Alpha'}


def test_help_overlay_closes_with_escape_and_q(ui_context):
    toggle_help = get_binding(ui_context, '?', requires={'show_help', 'build_help_text'})
    escape = get_binding(ui_context, 'escape', requires={'show_help'})
    quit_key = get_binding(ui_context, 'q', requires={'show_help'})

    toggle_help(dummy_event())
    assert closure_value(toggle_help, 'show_help') is True
    assert closure_value(toggle_help, 'floats')

    escape(dummy_event())
    assert closure_value(toggle_help, 'show_help') is False
    assert closure_value(toggle_help, 'floats') == []

    toggle_help(dummy_event())
    exit_calls = []
    quit_key(dummy_event(app=SimpleNamespace(exit=lambda: exit_calls.append(True))))
    assert closure_value(toggle_help, 'show_help') is False
    assert closure_value(toggle_help, 'floats') == []
    assert exit_calls == []


def test_delete_hotkey_discards_selected_pending_task(ui_context):
    delete_binding = get_binding(ui_context, 'x', requires={'_delete_selected_task'})
    delete_helper = closure_value(delete_binding, '_delete_selected_task')
    all_rows_cell = closure_map(delete_helper)['all_rows']
    current_index_cell = closure_map(delete_helper)['current_index']

    pending_url = 'pending://task-to-delete'
    ui_context.db.add_pending_action('create_task', {
        'placeholder_url': pending_url,
        'title': 'Draft Task',
    })
    ui_context.db.upsert_many([
        make_task(
            title='Draft Task',
            url=pending_url,
            project_title='A Project',
            focus_date='2024-01-10',
            item_id='',
            project_id='',
        )
    ])

    all_rows_cell.cell_contents = ui_context.db.load(today_only=False)
    filtered_rows = closure_value(delete_helper, 'filtered_rows')
    rows = filtered_rows()
    current_index_cell.cell_contents = next(i for i, row in enumerate(rows) if row.url == pending_url)

    delete_binding(dummy_event())

    assert pending_url not in {row.url for row in ui_context.db.load(today_only=False)}
    assert ui_context.db.list_pending_actions() == []
    assert closure_value(delete_helper, 'status_line') == 'Queued local task deleted'


def test_delete_hotkey_removes_synced_task_from_cache(ui_context):
    delete_binding = get_binding(ui_context, 'x', requires={'_delete_selected_task'})
    delete_helper = closure_value(delete_binding, '_delete_selected_task')
    task_url = make_task().url

    ui_context.db.add_pending_action('set_project_date', {
        'url': task_url,
        'field_name': 'Focus Day',
        'value': '2024-01-11',
    })

    delete_binding(dummy_event())

    assert task_url not in {row.url for row in ui_context.db.load(today_only=False)}
    assert ui_context.db.list_pending_actions() == []
    assert closure_value(delete_helper, 'status_line') == "Task removed from local cache (press 'u' to re-sync)"


def test_delete_hotkey_is_eager(ui_context):
    for keys, kwargs, func in ui_context.kb.bindings:
        if 'x' not in keys:
            continue
        if '_delete_selected_task' not in closure_map(func):
            continue
        assert kwargs.get('eager') is True
        return
    raise AssertionError("Eager binding for 'x' delete hotkey not found")
