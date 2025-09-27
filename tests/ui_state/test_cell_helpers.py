import datetime as dt

import gh_task_viewer as ght

from .helpers import make_task


def test_cell_helpers_handle_unicode():
    assert ght._sanitize_cell_text('line1\nline2') == 'line1 line2'
    assert ght._sanitize_cell_text(None) == ''

    text = '你好世界abc'
    truncated = ght._truncate(text, 6)
    assert truncated.endswith('…')
    assert ght._display_width(truncated) <= 6

    padded_right = ght._pad_display('abc', 6, align='right')
    assert padded_right == '   abc'

    padded_center = ght._pad_display('ab', 6, align='center')
    assert padded_center == '  ab  '

    padded_double = ght._pad_display('你', 4)
    assert padded_double.endswith('你')
    assert ght._display_width(padded_double) == 4

    today = dt.date(2024, 1, 10)
    assert ght.color_for_date('2024-01-10', today) == 'ansired bold'
    assert ght.color_for_date('2024-01-09', today) == 'ansiyellow'
    assert ght.color_for_date('2024-01-12', today) == 'ansigreen'
    assert ght.color_for_date('invalid', today) == 'ansigray'
    assert ght.color_for_date(None, today) == 'ansigray'

    primary = make_task(title='Today Focus', focus_date='2024-01-10')
    double_width = make_task(
        project_title='Project Beta',
        focus_date='',
        start_date='2024-01-08',
        title='你好世界任务',
        priority='Medium',
        priority_dirty=1,
        repo='acme/\nrepo',
    )
    fragments = ght.build_fragments([primary, double_width], today)
    styles = [style for style, _ in fragments]
    assert 'ansired bold' in styles
    assert 'ansigray' in styles  # missing focus date -> fallback colour

    focus_dirty = next(text for style, text in fragments if style == 'ansigray')
    assert ght._display_width(focus_dirty) == 11

    line = next(text for _, text in fragments if 'Medium*' in text)
    assert 'Medium*' in line
    assert '你好世界任务' in line
    assert 'acme/ repo' in line  # newline sanitized to space

    header_text = ''.join(text for style, text in fragments if style == 'bold')
    assert '## Project Alpha' in header_text
    assert '## Project Beta' in header_text
