import datetime as dt
import sys

import pytest
import yaml

import gh_task_viewer as ght


def _page(nodes, has_next=False, cursor=None):
    return {"data": {"viewer": {"organizations": {
        "nodes": nodes,
        "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
    }}}}


@pytest.fixture
def config_path(tmp_path, monkeypatch):
    path = tmp_path / "config.yml"
    path.write_text(
        "# Keep the original in a backup\n"
        "user: tester\ndate_field_names: [Start date, Due date]\n"
        "iteration_field_regex: Sprint\ncustom_setting: preserved\n"
        "projects:\n"
        "  - org: Acme\n    numbers: [2, 7]\n"
        "  - org: hidden-org\n    numbers: all\n"
        "  - user: tester\n    numbers: [4]\n",
        encoding="utf-8",
    )
    path.chmod(0o600)
    monkeypatch.setattr(ght, "_session", lambda token: object())
    monkeypatch.setattr(ght, "discover_organizations", lambda session: [
        {"login": "acme", "name": "Acme Team"},
        {"login": "Beta", "name": "Beta Team"},
    ])
    return path


def test_membership_discovery_paginates_and_deduplicates(monkeypatch):
    calls = []

    def graphql(session, query, variables):
        assert query == ght.GQL_LIST_ORGANIZATIONS
        calls.append(variables.copy())
        if variables["after"] is None:
            return _page([None, {}, {"login": "Zebra", "name": None}], True, "next")
        return _page([{"login": "Alpha", "name": "Alpha Team"}, {"login": "zebra"}])

    monkeypatch.setattr(ght, "_graphql_with_backoff", graphql)
    assert ght.discover_organizations(object()) == [
        {"login": "Alpha", "name": "Alpha Team"}, {"login": "zebra", "name": ""},
    ]
    assert calls == [{"after": None}, {"after": "next"}]


@pytest.mark.parametrize("bad_response", [
    {"errors": [{"message": "Missing read:org"}]},
    {"data": {"viewer": None}},
    _page([], True, None),
    _page([], True, "repeat"),
])
def test_discovery_does_not_offer_partial_memberships(monkeypatch, bad_response):
    responses = iter([_page([{"login": "Acme"}], True, "repeat"), bad_response])
    monkeypatch.setattr(ght, "_graphql_with_backoff", lambda *args: next(responses))
    with pytest.raises(RuntimeError, match="[Oo]rganization discovery"):
        ght.discover_organizations(object())


def test_selection_preserves_settings_project_numbers_and_backup(config_path, monkeypatch, capsys):
    original = config_path.read_bytes()
    monkeypatch.setattr("builtins.input", lambda prompt: "1,2,2")
    assert ght.select_organizations(str(config_path), "token")
    saved = yaml.safe_load(config_path.read_bytes())
    assert saved["projects"] == [
        {"org": "Acme", "numbers": [2, 7]},
        {"user": "tester", "numbers": [4]},
        {"org": "Beta", "numbers": "all"},
    ]
    before = yaml.safe_load(original)
    assert {k: v for k, v in saved.items() if k != "projects"} == {
        k: v for k, v in before.items() if k != "projects"
    }
    backups = list(config_path.parent.glob("*.bak"))
    assert len(backups) == 1
    assert backups[0].read_bytes() == original
    assert config_path.stat().st_mode & 0o777 == 0o600
    assert backups[0].stat().st_mode & 0o777 == 0o600
    assert "membership not visible" in capsys.readouterr().out


@pytest.mark.parametrize("answer", ["", "q", "quit", "1,3"])
def test_unchanged_selection_does_not_rewrite_config(config_path, monkeypatch, answer):
    original = config_path.read_bytes()
    monkeypatch.setattr("builtins.input", lambda prompt: answer)
    assert not ght.select_organizations(str(config_path), "token")
    assert config_path.read_bytes() == original
    assert not list(config_path.parent.glob("*.bak"))


@pytest.mark.parametrize("exception", [EOFError, KeyboardInterrupt])
def test_interrupted_selection_keeps_config(config_path, monkeypatch, exception):
    original = config_path.read_bytes()

    def interrupted(prompt):
        raise exception

    monkeypatch.setattr("builtins.input", interrupted)
    assert not ght.select_organizations(str(config_path), "token")
    assert config_path.read_bytes() == original


def test_invalid_selection_reprompts_and_none_preserves_personal_projects(config_path, monkeypatch):
    answers = iter(["0", "4", "1,no", "1,", "none"])
    monkeypatch.setattr("builtins.input", lambda prompt: next(answers))
    assert ght.select_organizations(str(config_path), "token")
    assert yaml.safe_load(config_path.read_bytes())["projects"] == [{"user": "tester", "numbers": [4]}]


def test_all_keeps_hidden_org_and_adds_visible_org(config_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda prompt: "all")
    assert ght.select_organizations(str(config_path), "token")
    cfg = ght.load_config(str(config_path))
    assert [(spec.owner, spec.numbers) for spec in cfg.projects] == [
        ("Acme", [2, 7]), ("hidden-org", None), ("tester", [4]), ("Beta", None),
    ]


def test_no_memberships_does_not_change_empty_config(config_path, monkeypatch):
    config_path.write_text("user: tester\nprojects: []\n", encoding="utf-8")
    original = config_path.read_bytes()
    monkeypatch.setattr(ght, "discover_organizations", lambda session: [])
    assert not ght.select_organizations(str(config_path), "token")
    assert config_path.read_bytes() == original


def test_config_edited_during_selection_is_not_overwritten(config_path, monkeypatch):
    edited = "user: changed\nprojects: []\n"

    def choose(prompt):
        config_path.write_text(edited, encoding="utf-8")
        return "all"

    monkeypatch.setattr("builtins.input", choose)
    with pytest.raises(RuntimeError, match="Config changed"):
        ght.select_organizations(str(config_path), "token")
    assert config_path.read_text() == edited
    assert list(config_path.parent.iterdir()) == [config_path]


def test_failed_replace_keeps_original_and_backup(config_path, monkeypatch):
    original = config_path.read_bytes()
    monkeypatch.setattr("builtins.input", lambda prompt: "all")

    def failed_replace(source, destination):
        raise OSError("disk error")

    monkeypatch.setattr(ght.os, "replace", failed_replace)
    with pytest.raises(OSError, match="disk error"):
        ght.select_organizations(str(config_path), "token")
    assert config_path.read_bytes() == original
    assert len(list(config_path.parent.iterdir())) == 2  # config and original backup


def test_fetch_uses_only_selected_owners(config_path, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda prompt: "1,2")
    ght.select_organizations(str(config_path), "token")
    monkeypatch.setattr(ght, "get_viewer_login", lambda token: "tester")
    # Even a cached project for a deselected org must not be fetched.
    monkeypatch.setattr(ght, "_load_target_cache", lambda: {
        "org:hidden-org": [{"number": 99, "title": "Old project"}],
    })
    monkeypatch.setattr(ght, "_save_target_cache", lambda cache: None)
    monkeypatch.setattr(ght, "discover_open_projects", lambda session, kind, owner: [
        {"number": 1, "title": "Beta project"},
    ])
    calls = []

    def graphql(session, query, variables, **kwargs):
        owner = variables.get("org") or variables.get("login")
        calls.append((owner, variables["number"]))
        key = "organization" if "org" in variables else "user"
        return {"data": {key: {"projectV2": {"items": {"nodes": []}}}}}

    monkeypatch.setattr(ght, "_graphql_with_backoff", graphql)
    ght.fetch_tasks_github("token", ght.load_config(str(config_path)), dt.date.today())
    assert set(calls) == {("Acme", 2), ("Acme", 7), ("tester", 4), ("Beta", 1)}


@pytest.mark.parametrize("flag", ["--select-organizations", "--select-orgs"])
def test_cli_selection_exits_without_opening_db(config_path, monkeypatch, flag):
    monkeypatch.setenv("GITHUB_TOKEN", "token")
    monkeypatch.setattr(sys, "argv", ["gh_task_viewer.py", "--config", str(config_path), flag])
    monkeypatch.setattr("builtins.input", lambda prompt: "2")
    monkeypatch.setattr(ght, "TaskDB", lambda *args: pytest.fail("Chooser must not open the task DB"))
    ght.main()
    assert [spec.owner for spec in ght.load_config(str(config_path)).projects] == ["tester", "Beta"]


def test_cli_missing_token_exits_without_changing_config(config_path, monkeypatch, capsys):
    original = config_path.read_bytes()
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(ght, "load_dotenv_token", lambda: None)
    monkeypatch.setattr(sys, "argv", ["gh_task_viewer.py", "--config", str(config_path), "--select-orgs"])
    with pytest.raises(SystemExit) as exc:
        ght.main()
    assert exc.value.code == 2
    assert "GITHUB_TOKEN is not set" in capsys.readouterr().err
    assert config_path.read_bytes() == original
