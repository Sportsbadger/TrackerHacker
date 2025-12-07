import sys
import types


def _install_cli_stubs():
    pd_errors = types.SimpleNamespace(
        SettingWithCopyWarning=RuntimeWarning,
        FutureWarning=RuntimeWarning,
    )
    pd_stub = types.SimpleNamespace(errors=pd_errors, isna=lambda value: value is None)
    sys.modules.setdefault("pandas", pd_stub)
    sys.modules.setdefault("pandas.errors", pd_errors)

    questionary_module = types.ModuleType("questionary")

    class _Choice:
        def __init__(self, title: str, value: object | None = None):
            self.title = title
            self.value = value

    questionary_module.Choice = _Choice
    questionary_module.select = lambda *_, **__: None
    questionary_module.confirm = lambda *_, **__: None
    questionary_module.path = lambda *_, **__: None
    questionary_module.autocomplete = lambda *_, **__: None
    sys.modules.setdefault("questionary", questionary_module)

    return _Choice


class _StubPromptFactory:
    def __init__(self, responses: list[object]):
        self._responses = responses
        self.calls = 0

    def __call__(self, *_, **__):
        factory = self

        class _StubPrompt:
            @staticmethod
            def ask():
                factory.calls += 1
                return factory._responses[factory.calls - 1]

        return _StubPrompt()


def test_restore_selection_cancel_exits(monkeypatch):
    choice_cls = _install_cli_stubs()

    from tracker_hacker.cli import _prompt_restore_state_selection

    select_stub = _StubPromptFactory([None])
    confirm_stub = _StubPromptFactory([])
    apply_stub = _StubPromptFactory([])

    operation_flow_control, chosen_state_option = _prompt_restore_state_selection(
        [choice_cls("<Cancel>", None)],
        select_fn=select_stub,
        confirm_fn=confirm_stub,
        apply_select_fn=apply_stub,
    )

    assert operation_flow_control == "return_to_menu"
    assert chosen_state_option is None
    assert select_stub.calls == 1
    assert confirm_stub.calls == 0
    assert apply_stub.calls == 0
