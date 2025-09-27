import copy
import datetime as dt
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gh_task_viewer as ght


def _issue_node(title: str, start_date: str, focus_date: str, *, assigned: bool = True) -> dict:
    """Build a minimal ProjectV2 node structure for fetch tests."""
    login = 'tester'
    assignees = [{'login': login, 'id': 'user-1'}] if assigned else []
    label_nodes = [{'name': 'bug'}, {'name': title.lower()}]
    iteration_field_name = 'Sprint Field'
    return {
        'id': f'item-{title.replace(" ", "").lower()}',
        'content': {
            '__typename': 'Issue',
            'title': title,
            'url': f'https://example.com/{title.replace(" ", "").lower()}',
            'repository': {'id': 'repo-1', 'nameWithOwner': 'acme/repo'},
            'assignees': {'nodes': assignees},
            'author': {'login': login if assigned else 'someone-else'},
            'labels': {'nodes': label_nodes},
        },
        'fieldValues': {
            'nodes': [
                {
                    '__typename': 'ProjectV2ItemFieldUserValue',
                    'users': {'nodes': assignees},
                    'field': {'id': 'assignee-field', 'name': 'Assignees'},
                },
                {
                    '__typename': 'ProjectV2ItemFieldDateValue',
                    'date': start_date,
                    'field': {'id': 'start-field', 'name': 'Start date'},
                },
                {
                    '__typename': 'ProjectV2ItemFieldDateValue',
                    'date': focus_date,
                    'field': {'id': 'focus-field', 'name': 'Focus Day'},
                },
                {
                    '__typename': 'ProjectV2ItemFieldSingleSelectValue',
                    'name': 'In Progress',
                    'optionId': 'status-in-progress',
                    'field': {
                        'id': 'status-field',
                        'name': 'Status',
                        'options': [
                            {'id': 'status-todo', 'name': 'Todo'},
                            {'id': 'status-in-progress', 'name': 'In Progress'},
                        ],
                    },
                },
                {
                    '__typename': 'ProjectV2ItemFieldSingleSelectValue',
                    'name': 'High',
                    'optionId': 'prio-high',
                    'field': {
                        'id': 'priority-field',
                        'name': 'Priority',
                        'options': [
                            {'id': 'prio-high', 'name': 'High'},
                            {'id': 'prio-low', 'name': 'Low'},
                        ],
                    },
                },
                {
                    '__typename': 'ProjectV2ItemFieldIterationValue',
                    'title': 'Sprint 1',
                    'startDate': '2024-01-08',
                    'duration': 14,
                    'iterationId': 'iter-1',
                    'field': {
                        'id': 'iteration-field',
                        'name': iteration_field_name,
                        'configuration': {
                            'iterations': [
                                {'id': 'iter-1', 'title': 'Sprint 1', 'startDate': '2024-01-08', 'duration': 14},
                                {'id': 'iter-2', 'title': 'Sprint 2', 'startDate': '2024-01-22', 'duration': 14},
                            ]
                        },
                    },
                },
            ]
        },
        'project': {'title': 'Sample Project', 'url': 'https://example.com/project', 'id': 'proj-1'},
    }


def _page(nodes, *, has_next=False, end_cursor=None, owner_type: str = 'org') -> dict:
    container = 'organization' if owner_type == 'org' else 'user'
    return {
        'data': {
            container: {
                'projectV2': {
                    'items': {
                        'nodes': nodes,
                        'pageInfo': {
                            'hasNextPage': has_next,
                            'endCursor': end_cursor,
                        },
                    }
                }
            }
        }
    }


def _paginated_responses():
    first = _issue_node('Task 1', '2024-01-10', '2024-01-11', assigned=True)
    second = _issue_node('Task 2', '2024-01-12', '2024-01-13', assigned=False)
    return {
        None: _page([first], has_next=True, end_cursor='cursor-1'),
        'cursor-1': _page([second], has_next=False, end_cursor=None),
    }


def _patch_common(monkeypatch, graphql_impl):
    monkeypatch.setattr(ght, '_session', lambda token: object())
    monkeypatch.setattr(ght, '_load_target_cache', lambda: {})
    monkeypatch.setattr(ght, '_save_target_cache', lambda data: None)
    monkeypatch.setattr(ght, '_graphql_with_backoff', graphql_impl)


def test_fetch_tasks_github_paginates_and_parses_fields(monkeypatch):
    pages = _paginated_responses()

    def fake_graphql(_session, _query, variables, on_wait=None):
        return pages[variables.get('after')]

    _patch_common(monkeypatch, fake_graphql)

    cfg = ght.Config(
        user='tester',
        date_field_regex='Start',
        projects=[ght.ProjectSpec(owner_type='org', owner='acme', numbers=[1])],
        iteration_field_regex='Sprint',
    )

    result = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=True,
    )

    assert result.partial is False
    assert result.message == ''
    assert len(result.rows) == 2

    rows = {row.title: row for row in result.rows}

    task1 = rows['Task 1']
    assert task1.iteration_field == 'Sprint Field'
    assert task1.focus_field == 'Focus Day'
    assert task1.focus_date == '2024-01-11'
    assert task1.assigned_to_me == 1
    assert json.loads(task1.assignee_user_ids) == ['user-1']
    assert json.loads(task1.priority_options)[0]['name'] == 'High'
    assert json.loads(task1.status_options)[1]['name'] == 'In Progress'

    task2 = rows['Task 2']
    assert task2.assigned_to_me == 0
    assert task2.iteration_title == 'Sprint 1'
    assert json.loads(task2.labels) == ['bug', 'task 2']
    assert task2.focus_date == '2024-01-13'


def test_fetch_tasks_github_excludes_unassigned_when_flag_false(monkeypatch):
    pages = _paginated_responses()

    def fake_graphql(_session, _query, variables, on_wait=None):
        return pages[variables.get('after')]

    _patch_common(monkeypatch, fake_graphql)

    cfg = ght.Config(
        user='tester',
        date_field_regex='Start',
        projects=[ght.ProjectSpec(owner_type='org', owner='acme', numbers=[1])],
        iteration_field_regex='Sprint',
    )

    result = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=False,
    )

    titles = [row.title for row in result.rows]
    assert titles == ['Task 1']


def test_fetch_tasks_github_rate_limit_sets_partial(monkeypatch):
    def fake_graphql(_session, _query, _variables, on_wait=None):
        return {'errors': [{'type': 'RATE_LIMITED'}]}

    _patch_common(monkeypatch, fake_graphql)

    cfg = ght.Config(
        user='tester',
        date_field_regex='Start',
        projects=[ght.ProjectSpec(owner_type='org', owner='acme', numbers=[1])],
    )

    result = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=False,
    )

    assert result.partial is True
    assert 'Rate limited; partial results' in (result.message or '')
    assert result.rows == []


def test_fetch_tasks_github_uses_cache_when_discovery_fails(monkeypatch, temp_target_cache_path):
    node = _issue_node('Cached Task', '2024-01-10', '2024-01-11', assigned=True)
    pages = {None: _page([node], has_next=False, end_cursor=None)}

    def fake_graphql(_session, _query, variables, on_wait=None):
        return pages[variables.get('after')]

    monkeypatch.setattr(ght, '_session', lambda token: object())
    monkeypatch.setattr(ght, '_graphql_with_backoff', fake_graphql)

    original_load = ght._load_target_cache
    original_save = ght._save_target_cache

    save_calls = []

    def spy_save(data):
        save_calls.append(copy.deepcopy(data))
        return original_save(data)

    monkeypatch.setattr(ght, '_load_target_cache', original_load)
    monkeypatch.setattr(ght, '_save_target_cache', spy_save)

    discover_calls = {'count': 0}

    def fake_discover(_session, owner_type, owner):
        discover_calls['count'] += 1
        if discover_calls['count'] == 1:
            return [{'number': 1, 'title': 'Cached Project', 'project_id': 'proj-1'}]
        raise RuntimeError('discovery error')

    monkeypatch.setattr(ght, 'discover_open_projects', fake_discover)

    cfg = ght.Config(
        user='tester',
        date_field_regex='Start',
        projects=[ght.ProjectSpec(owner_type='org', owner='acme', numbers=None)],
    )

    first = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=False,
    )

    assert len(first.rows) == 1
    assert temp_target_cache_path.exists()
    assert len(save_calls) == 1

    with temp_target_cache_path.open('r', encoding='utf-8') as handle:
        cache_payload = json.load(handle)
    assert cache_payload['org:acme'][0]['number'] == 1

    second = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=False,
    )

    assert len(second.rows) == 1
    assert discover_calls['count'] == 2
    assert len(save_calls) == 1  # cache not rewritten on failure


def test_fetch_tasks_github_skips_malformed_nodes(monkeypatch):
    malformed_items = [
        {},
        {
            'id': 'item-broken',
            'content': {'__typename': 'Issue', 'title': 'Broken Item'},
            'fieldValues': {
                'nodes': [
                    {'__typename': 'RandomValue', 'field': {'id': 'random', 'name': 'Random'}},
                    {
                        '__typename': 'ProjectV2ItemFieldDateValue',
                        'date': 'not-a-date',
                        'field': {'id': 'start-field', 'name': 'Start date'},
                    },
                ]
            },
            'project': {'title': 'Broken Project', 'url': 'https://example.com/project', 'id': 'proj-broken'},
        },
    ]
    pages = {None: _page(malformed_items, has_next=False, end_cursor=None)}

    def fake_graphql(_session, _query, variables, on_wait=None):
        return pages[variables.get('after')]

    _patch_common(monkeypatch, fake_graphql)

    cfg = ght.Config(
        user='tester',
        date_field_regex='Start',
        projects=[ght.ProjectSpec(owner_type='org', owner='acme', numbers=[1])],
    )

    result = ght.fetch_tasks_github(
        token='token',
        cfg=cfg,
        date_cutoff=dt.date(2024, 1, 1),
        include_unassigned=True,
    )

    assert len(result.rows) == 2
    assert all(row.start_field in {'(no date)', 'Start date'} for row in result.rows)
