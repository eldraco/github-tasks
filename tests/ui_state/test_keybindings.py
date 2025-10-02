import inspect

from prompt_toolkit.keys import Keys
from prompt_toolkit.layout.dimension import Dimension as _Dimension

from .helpers import closure_map, closure_value, dummy_event, ticker_update


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
