import os
import sys
import types

import pytest

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)


def _install_prompt_toolkit_stub() -> None:
    if 'prompt_toolkit' in sys.modules:
        return

    class Dummy:
        def __init__(self, *args, **kwargs):
            pass

    class DummyApplication(Dummy):
        instances = []

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.background_tasks = []
            self.invalidate_calls = 0
            DummyApplication.instances.append(self)

        def invalidate(self):
            self.invalidate_calls += 1

        def run(self):
            return None

        def create_background_task(self, coro):
            self.background_tasks.append(coro)
            return None

    class DummyKeyBindings:
        instances = []

        def __init__(self):
            self.bindings = []
            DummyKeyBindings.instances.append(self)

        def add(self, *keys, **kwargs):
            def decorator(func):
                self.bindings.append((keys, kwargs, func))
                return func
            return decorator

    class DummyCondition:
        def __init__(self, func):
            self.func = func

        def __call__(self):
            return self.func()

    def _callable_stub(*args, **kwargs):
        return object()

    pt = types.ModuleType('prompt_toolkit')
    pt.Application = DummyApplication
    sys.modules['prompt_toolkit'] = pt

    enums = types.ModuleType('prompt_toolkit.enums')

    class DummyEditingMode:
        VI = 'vi'

    enums.EditingMode = DummyEditingMode
    sys.modules['prompt_toolkit.enums'] = enums

    key_binding = types.ModuleType('prompt_toolkit.key_binding')
    key_binding.KeyBindings = DummyKeyBindings
    sys.modules['prompt_toolkit.key_binding'] = key_binding
    pt.key_binding = key_binding

    layout = types.ModuleType('prompt_toolkit.layout')

    class DummyLayout(Dummy):
        pass

    class DummyWindow(Dummy):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.args = args
            self.kwargs = kwargs

    class DummySplit(Dummy):
        def __init__(self, children=None, **kwargs):
            super().__init__(children, **kwargs)
            self.children = list(children or [])
            self.kwargs = kwargs

    layout.HSplit = DummySplit
    layout.VSplit = DummySplit
    layout.Layout = DummyLayout
    layout.Window = DummyWindow
    sys.modules['prompt_toolkit.layout'] = layout

    layout_controls = types.ModuleType('prompt_toolkit.layout.controls')

    class DummyFormattedTextControl:
        def __init__(self, text=None, **kwargs):
            self.text = text

    layout_controls.FormattedTextControl = DummyFormattedTextControl
    sys.modules['prompt_toolkit.layout.controls'] = layout_controls

    layout_containers = types.ModuleType('prompt_toolkit.layout.containers')

    class DummyFloat(Dummy):
        def __init__(self, content=None, **kwargs):
            super().__init__(content, **kwargs)
            self.content = content
            self.kwargs = kwargs

    class DummyFloatContainer(Dummy):
        def __init__(self, content=None, floats=None, **kwargs):
            super().__init__(content, floats, **kwargs)
            self.content = content
            self.floats = list(floats or [])
            self.kwargs = kwargs

    layout_containers.FloatContainer = DummyFloatContainer
    layout_containers.Float = DummyFloat
    sys.modules['prompt_toolkit.layout.containers'] = layout_containers

    layout_dimension = types.ModuleType('prompt_toolkit.layout.dimension')
    layout_dimension.Dimension = _callable_stub
    sys.modules['prompt_toolkit.layout.dimension'] = layout_dimension

    widgets = types.ModuleType('prompt_toolkit.widgets')
    widgets.Frame = _callable_stub
    sys.modules['prompt_toolkit.widgets'] = widgets

    utils = types.ModuleType('prompt_toolkit.utils')
    utils.get_cwidth = lambda text: len(text)
    sys.modules['prompt_toolkit.utils'] = utils

    keys = types.ModuleType('prompt_toolkit.keys')
    keys.Keys = types.SimpleNamespace(Any='__ANY__')
    sys.modules['prompt_toolkit.keys'] = keys

    filters = types.ModuleType('prompt_toolkit.filters')
    filters.Condition = DummyCondition
    sys.modules['prompt_toolkit.filters'] = filters

    styles = types.ModuleType('prompt_toolkit.styles')

    class DummyStyle(Dummy):
        instances = []

        def __init__(self, rules=None, **kwargs):
            super().__init__(rules, **kwargs)
            self.rules = rules or {}
            self.kwargs = kwargs
            DummyStyle.instances.append(self)

        @classmethod
        def from_dict(cls, rules):
            return cls(rules)

    styles.Style = DummyStyle
    sys.modules['prompt_toolkit.styles'] = styles


_install_prompt_toolkit_stub()


@pytest.fixture
def temp_target_cache_path(monkeypatch, tmp_path):
    """Provide an isolated target cache path for gh_task_viewer tests."""
    cache_path = tmp_path / "gh_tasks.targets.json"
    monkeypatch.setattr("gh_task_viewer.TARGET_CACHE_PATH", str(cache_path), raising=False)
    return cache_path


@pytest.fixture
def temp_db_path(tmp_path):
    """Return a unique SQLite path per test to avoid cross-test contamination."""
    return tmp_path / "test_tasks.db"
