"""volnux.context: the scoped execution context and its carrier."""

import asyncio
import contextvars
import importlib
import sys
import threading

import pytest

import volnux.context as ctx
from volnux.config import VolnuxConfig
from volnux.context import (
    CARRIER_KEY,
    ExecutionContextSnapshot,
    dispatch_worker_task,
    extract_carrier,
    get_current_config,
    get_current_correlation_id,
    get_current_execution_id,
    get_current_node_id,
    get_current_parent_execution_id,
    get_current_project_id,
    get_current_trace_id,
    get_current_workflow_id,
    get_current_workflow_version,
    inject_carrier,
    is_speculative_pass,
    snapshot_context,
    volnux_context,
)

GETTERS = {
    "workflow_id": get_current_workflow_id,
    "workflow_version": get_current_workflow_version,
    "correlation_id": get_current_correlation_id,
    "execution_id": get_current_execution_id,
    "parent_execution_id": get_current_parent_execution_id,
    "trace_id": get_current_trace_id,
    "is_speculative": is_speculative_pass,
    "project_id": get_current_project_id,
    "node_id": get_current_node_id,
}
VALUES = {
    "workflow_id": "wf",
    "workflow_version": "1.2.3",
    "correlation_id": "corr",
    "execution_id": "exec",
    "parent_execution_id": "parent",
    "trace_id": "trace",
    "is_speculative": True,
    "project_id": "proj",
    "node_id": "node",
}


def small_config(node_id="cfg-node"):
    return VolnuxConfig.from_dict({"node_id": node_id})


class TestDefaults:
    def test_nothing_is_set_outside_a_context(self):
        for name, getter in GETTERS.items():
            assert getter() == (False if name == "is_speculative" else None)

    def test_the_snapshot_outside_a_context_is_empty_apart_from_the_config(self):
        snap = snapshot_context()
        for name in GETTERS:
            assert getattr(snap, name) == (False if name == "is_speculative" else None)
        assert isinstance(snap.config, dict)


class TestVolnuxContext:
    def test_every_value_is_active_inside_and_restored_after(self):
        with volnux_context(**VALUES):
            for name, getter in GETTERS.items():
                assert getter() == VALUES[name]
        for name, getter in GETTERS.items():
            assert getter() == (False if name == "is_speculative" else None)

    def test_it_yields_the_snapshot_of_what_is_active(self):
        with volnux_context(**VALUES) as snap:
            assert snap == snapshot_context()
            assert snap.workflow_id == "wf" and snap.is_speculative is True

    def test_a_value_left_out_keeps_the_outer_one(self):
        with volnux_context(workflow_id="outer", trace_id="t"):
            with volnux_context(workflow_id="inner"):
                assert get_current_workflow_id() == "inner"
                assert get_current_trace_id() == "t"
            assert get_current_workflow_id() == "outer"

    def test_false_is_a_value_and_overrides(self):
        with volnux_context(is_speculative=True):
            with volnux_context(is_speculative=False):
                assert is_speculative_pass() is False
            assert is_speculative_pass() is True

    def test_it_is_restored_when_the_body_raises(self):
        with pytest.raises(RuntimeError):
            with volnux_context(workflow_id="wf", config=small_config()):
                raise RuntimeError("boom")
        assert get_current_workflow_id() is None

    def test_nested_scopes_unwind_in_order(self):
        with volnux_context(execution_id="a"):
            with volnux_context(execution_id="b"):
                with volnux_context(execution_id="c"):
                    assert get_current_execution_id() == "c"
                assert get_current_execution_id() == "b"
            assert get_current_execution_id() == "a"
        assert get_current_execution_id() is None

    def test_a_failure_while_activating_does_not_leak_what_was_set(self, monkeypatch):
        class Boom(Exception):
            pass

        real = ctx._NODE_ID

        class FailingVar:
            def set(self, value):
                raise Boom()

        # setting the node id fails after the workflow id was already set
        monkeypatch.setattr(ctx, "_NODE_ID", FailingVar())
        try:
            with pytest.raises(Boom):
                with volnux_context(workflow_id="wf", node_id="n"):
                    pass
            assert get_current_workflow_id() is None
        finally:
            monkeypatch.setattr(ctx, "_NODE_ID", real)
            if get_current_workflow_id() is not None:  # keep other tests clean
                ctx._WORKFLOW_ID.set(None)


class TestSnapshotArgument:
    def make(self, **overrides):
        return ExecutionContextSnapshot(**{**VALUES, **overrides})

    def test_a_snapshot_activates_all_of_its_values(self):
        with volnux_context(snapshot=self.make()):
            for name, getter in GETTERS.items():
                assert getter() == VALUES[name]

    def test_an_explicit_argument_wins_over_the_snapshot(self):
        with volnux_context(snapshot=self.make(), workflow_id="explicit"):
            assert get_current_workflow_id() == "explicit"
            assert get_current_execution_id() == "exec"

    def test_an_explicit_false_wins_over_a_true_snapshot(self):
        with volnux_context(snapshot=self.make(), is_speculative=False):
            assert is_speculative_pass() is False

    def test_the_snapshots_config_is_rebuilt_and_active(self):
        snap = self.make(config=small_config("from-snap").to_dict())
        with volnux_context(snapshot=snap):
            assert get_current_config().get_node_id() == "from-snap"

    def test_an_explicit_config_wins_over_the_snapshots(self):
        snap = self.make(config=small_config("from-snap").to_dict())
        with volnux_context(snapshot=snap, config=small_config("explicit")):
            assert get_current_config().get_node_id() == "explicit"

    def test_an_empty_snapshot_changes_nothing(self):
        with volnux_context(workflow_id="outer"):
            with volnux_context(snapshot=ExecutionContextSnapshot()):
                assert get_current_workflow_id() == "outer"


class TestConfig:
    def test_outside_a_context_it_is_the_process_wide_instance(self):
        assert get_current_config() is VolnuxConfig.get_instance()

    def test_it_follows_the_process_wide_instance_when_that_is_replaced(
        self, monkeypatch
    ):
        replacement = small_config("replacement")
        monkeypatch.setattr(VolnuxConfig, "_instance", replacement)
        assert get_current_config() is replacement
        assert snapshot_context().config["node_id"] == "replacement"

    def test_a_context_config_is_active_then_restored(self):
        config = small_config("scoped")
        with volnux_context(config=config):
            assert get_current_config() is config
        assert get_current_config() is VolnuxConfig.get_instance()

    def test_importing_the_module_does_not_build_the_configuration(self, monkeypatch):
        calls = []
        monkeypatch.setattr(
            VolnuxConfig,
            "get_instance",
            classmethod(lambda cls: calls.append(1) or small_config()),
        )
        import volnux.config as config_module

        saved = sys.modules.pop("volnux.context")
        saved_hook = config_module._active_context_config
        try:
            importlib.import_module("volnux.context")
            assert calls == []
        finally:
            sys.modules["volnux.context"] = saved
            # (importing again made a second module with its own context variables)
            config_module._active_context_config = saved_hook


class TestSnapshotType:
    def test_it_is_immutable(self):
        snap = ExecutionContextSnapshot(workflow_id="x")
        with pytest.raises(Exception):
            snap.workflow_id = "y"

    def test_to_dict_and_from_dict_round_trip(self):
        snap = ExecutionContextSnapshot(**VALUES, config={"node_id": "n"})
        assert ExecutionContextSnapshot.from_dict(snap.to_dict()) == snap

    def test_from_dict_ignores_unknown_keys(self):
        snap = ExecutionContextSnapshot.from_dict({"workflow_id": "w", "other": 1})
        assert snap.workflow_id == "w"

    def test_get_config_instance_without_a_config_is_the_process_wide_one(self):
        assert ExecutionContextSnapshot().get_config_instance() is (
            VolnuxConfig.get_instance()
        )

    def test_get_config_instance_rebuilds_the_carried_config(self):
        snap = ExecutionContextSnapshot(config=small_config("carried").to_dict())
        assert snap.get_config_instance().get_node_id() == "carried"


class TestCarrier:
    def test_inject_adds_the_active_context_under_the_key(self):
        with volnux_context(**VALUES):
            carrier = inject_carrier({"data": 1})
        assert carrier["data"] == 1
        assert carrier[CARRIER_KEY]["workflow_id"] == "wf"
        assert carrier[CARRIER_KEY]["trace_id"] == "trace"

    def test_a_carrier_round_trips_into_a_context(self):
        with volnux_context(**VALUES):
            carrier = inject_carrier({})
        with volnux_context(snapshot=extract_carrier(carrier)):
            for name, getter in GETTERS.items():
                assert getter() == VALUES[name]

    def test_the_node_id_override_applies_to_the_context_and_its_config(self):
        with volnux_context(**VALUES):
            carrier = inject_carrier({}, node_id_override="target")
        body = carrier[CARRIER_KEY]
        assert body["node_id"] == "target" and body["config"]["node_id"] == "target"

    def test_the_override_does_not_change_the_live_config(self):
        config = small_config("live")
        with volnux_context(config=config):
            inject_carrier({}, node_id_override="target")
            assert get_current_config().get_node_id() == "live"
            assert get_current_config().to_dict()["node_id"] == "live"

    def test_the_config_override_replaces_the_carried_config(self):
        carrier = inject_carrier({}, config_override=small_config("other"))
        assert carrier[CARRIER_KEY]["config"]["node_id"] == "other"

    def test_a_falsy_config_override_is_still_used(self, monkeypatch):
        class FalsyConfig:
            def __bool__(self):
                return False

            def to_dict(self):
                return {"node_id": "falsy"}

        carrier = inject_carrier({}, config_override=FalsyConfig())
        assert carrier[CARRIER_KEY]["config"]["node_id"] == "falsy"

    def test_an_empty_node_id_override_is_applied(self):
        carrier = inject_carrier({}, node_id_override="")
        assert carrier[CARRIER_KEY]["node_id"] == ""

    def test_the_carrier_is_json_serialisable(self):
        import json

        with volnux_context(**VALUES):
            json.dumps(inject_carrier({}))

    def test_extract_without_the_key_is_an_empty_snapshot(self):
        assert extract_carrier({}) == ExecutionContextSnapshot()

    @pytest.mark.parametrize("bad", [None, "text", 5, [1], object()])
    def test_extract_from_something_that_is_not_a_dict(self, bad):
        assert extract_carrier(bad) == ExecutionContextSnapshot()

    @pytest.mark.parametrize("bad", [None, "text", 5, ["x"]])
    def test_extract_with_a_malformed_body(self, bad):
        assert extract_carrier({CARRIER_KEY: bad}) == ExecutionContextSnapshot()

    def test_sensitive_settings_are_not_carried(self):
        config = VolnuxConfig.get_instance()
        with volnux_context():
            assert "SECRET_KEY" not in repr(inject_carrier({})[CARRIER_KEY]["config"])
        assert config is VolnuxConfig.get_instance()


class TestDispatchWorkerTask:
    def carrier(self, **values):
        with volnux_context(**(values or VALUES)):
            return inject_carrier({})

    def test_the_context_is_active_inside_a_sync_task(self):
        @dispatch_worker_task
        def task(**kwargs):
            return get_current_workflow_id(), get_current_node_id()

        assert task(**self.carrier()) == ("wf", "node")
        assert get_current_workflow_id() is None

    def test_the_payload_keyword(self):
        @dispatch_worker_task
        def task(payload):
            return get_current_trace_id()

        assert task(payload=self.carrier()) == "trace"

    def test_a_positional_payload(self):
        @dispatch_worker_task
        def task(payload):
            return get_current_trace_id()

        assert task(self.carrier()) == "trace"

    def test_a_payload_that_is_not_a_dict_does_not_fail(self):
        @dispatch_worker_task
        def task(payload):
            return get_current_workflow_id()

        assert task(payload="plain text") is None
        assert task(payload=None) is None

    def test_a_call_without_a_carrier_runs_with_no_context(self):
        @dispatch_worker_task
        def task(a, b=2):
            return a + b, get_current_workflow_id()

        assert task(1, b=3) == (4, None)

    def test_the_arguments_and_the_return_value_pass_through(self):
        @dispatch_worker_task
        def task(a, b, c=0, **rest):
            return (a, b, c, sorted(rest))

        carrier = self.carrier()
        assert task(1, 2, c=3, **carrier) == (1, 2, 3, [CARRIER_KEY])

    def test_the_context_is_restored_when_the_task_raises(self):
        @dispatch_worker_task
        def task(**kwargs):
            raise ValueError("bad")

        with pytest.raises(ValueError):
            task(**self.carrier())
        assert get_current_workflow_id() is None

    def test_the_wrapper_keeps_the_functions_identity(self):
        @dispatch_worker_task
        def my_task(**kwargs):
            """doc"""

        assert my_task.__name__ == "my_task" and my_task.__doc__ == "doc"

    def test_a_sync_function_stays_sync_and_a_coroutine_function_stays_async(self):
        @dispatch_worker_task
        def sync_task(**kwargs):
            return 1

        @dispatch_worker_task
        async def async_task(**kwargs):
            return 1

        assert not asyncio.iscoroutinefunction(sync_task)
        assert asyncio.iscoroutinefunction(async_task)

    @pytest.mark.asyncio
    async def test_the_context_is_active_inside_an_async_task(self):
        @dispatch_worker_task
        async def task(**kwargs):
            await asyncio.sleep(0)
            return get_current_workflow_id(), get_current_execution_id()

        assert await task(**self.carrier()) == ("wf", "exec")
        assert get_current_workflow_id() is None

    @pytest.mark.asyncio
    async def test_an_async_task_restores_the_context_on_error(self):
        @dispatch_worker_task
        async def task(**kwargs):
            raise ValueError("bad")

        with pytest.raises(ValueError):
            await task(**self.carrier())
        assert get_current_workflow_id() is None


class TestIsolation:
    @pytest.mark.asyncio
    async def test_concurrent_tasks_do_not_see_each_others_context(self):
        seen = {}

        async def work(name):
            with volnux_context(workflow_id=name):
                await asyncio.sleep(0.01)
                seen[name] = get_current_workflow_id()

        await asyncio.gather(*(work(f"wf{i}") for i in range(5)))
        assert seen == {f"wf{i}": f"wf{i}" for i in range(5)}
        assert get_current_workflow_id() is None

    def test_a_new_thread_does_not_inherit_the_context(self):
        seen = []
        with volnux_context(workflow_id="main"):
            thread = threading.Thread(
                target=lambda: seen.append(get_current_workflow_id())
            )
            thread.start()
            thread.join()
        assert seen == [None]

    def test_a_copied_context_carries_it_over(self):
        with volnux_context(workflow_id="main"):
            copy = contextvars.copy_context()
        assert copy.run(get_current_workflow_id) == "main"
        assert get_current_workflow_id() is None


class TestCarrierValues:
    def test_a_path_setting_is_carried_as_text(self):
        from pathlib import Path

        from volnux.config import ConfigEntry

        entry = ConfigEntry(name="X", value={"dir": Path("/a/b"), "n": (1, 2)})
        assert entry.to_dict()["value"] == {"dir": "/a/b", "n": [1, 2]}


class TestSettingFollowsTheContext:
    """``setting()`` reads the configuration the active context carries, so a
    workflow on a worker or in a pod sees its dispatcher's settings."""

    def carried(self, **values):
        return VolnuxConfig.from_dict(
            {
                "node_id": "dispatcher",
                "store": [
                    {
                        "name": name,
                        "value": value,
                        "timestamp": 0,
                        "origin_mesh_node": "local",
                        "signature": None,
                    }
                    for name, value in values.items()
                ],
            }
        )

    def test_outside_a_context_it_is_the_process_value(self):
        from volnux.config import setting

        assert setting("K8S_NAMESPACE") == "default"

    def test_inside_a_context_it_is_the_carried_value(self):
        from volnux.config import setting

        with volnux_context(config=self.carried(K8S_NAMESPACE="prod")):
            assert setting("K8S_NAMESPACE") == "prod"
        assert setting("K8S_NAMESPACE") == "default"

    def test_a_setting_the_carrier_lacks_comes_from_the_process(self):
        # (secrets are never carried)
        from volnux.config import setting
        from volnux.conf import override_settings

        with override_settings(SECRET_KEY="process-secret"):
            with volnux_context(config=self.carried(K8S_NAMESPACE="prod")):
                assert setting("SECRET_KEY") == "process-secret"

    def test_a_default_is_used_when_neither_has_it(self):
        from volnux.config import setting

        with volnux_context(config=self.carried()):
            assert setting("NO_SUCH_SETTING_X", "fallback") == "fallback"
            with pytest.raises(AttributeError):
                setting("NO_SUCH_SETTING_X")

    def test_the_workers_environment_wins_over_the_carried_value(self, monkeypatch):
        from volnux.config import setting

        monkeypatch.setenv("VOLNUX_K8S_NAMESPACE", "from-env")
        with volnux_context(config=self.carried(K8S_NAMESPACE="prod")):
            assert setting("K8S_NAMESPACE") == "from-env"

    def test_an_override_of_this_process_wins(self):
        from volnux.config import setting
        from volnux.conf import override_settings

        with override_settings(K8S_NAMESPACE="overridden"):
            with volnux_context(config=self.carried(K8S_NAMESPACE="prod")):
                assert setting("K8S_NAMESPACE") == "overridden"

    @pytest.mark.asyncio
    async def test_a_task_started_in_the_context_sees_it(self):
        from volnux.config import setting

        with volnux_context(config=self.carried(K8S_NAMESPACE="prod")):
            seen = await asyncio.create_task(_read("K8S_NAMESPACE"))
        assert seen == "prod"


async def _read(name):
    from volnux.config import setting

    return setting(name)
