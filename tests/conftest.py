import sys
import types


def _install_prompt_toolkit_stub() -> None:
    if 'prompt_toolkit' in sys.modules:
        return

    class Dummy:
        def __init__(self, *args, **kwargs):
            pass

    class DummyApplication(Dummy):
        def invalidate(self):
            pass

        def run(self):
            pass

        def create_background_task(self, _coro):
            return None

    class DummyKeyBindings:
        def __init__(self):
            self.bindings = []

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
    enums.EditingMode = object
    sys.modules['prompt_toolkit.enums'] = enums

    key_binding = types.ModuleType('prompt_toolkit.key_binding')
    key_binding.KeyBindings = DummyKeyBindings
    sys.modules['prompt_toolkit.key_binding'] = key_binding

    layout = types.ModuleType('prompt_toolkit.layout')
    class DummyLayout(Dummy):
        pass
    layout.HSplit = _callable_stub
    layout.VSplit = _callable_stub
    layout.Layout = DummyLayout
    layout.Window = _callable_stub
    sys.modules['prompt_toolkit.layout'] = layout

    layout_controls = types.ModuleType('prompt_toolkit.layout.controls')
    layout_controls.FormattedTextControl = _callable_stub
    sys.modules['prompt_toolkit.layout.controls'] = layout_controls

    layout_containers = types.ModuleType('prompt_toolkit.layout.containers')
    layout_containers.FloatContainer = _callable_stub
    layout_containers.Float = _callable_stub
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
    keys.Keys = types.SimpleNamespace()
    sys.modules['prompt_toolkit.keys'] = keys

    filters = types.ModuleType('prompt_toolkit.filters')
    filters.Condition = DummyCondition
    sys.modules['prompt_toolkit.filters'] = filters

    styles = types.ModuleType('prompt_toolkit.styles')
    styles.Style = Dummy
    sys.modules['prompt_toolkit.styles'] = styles


_install_prompt_toolkit_stub()
