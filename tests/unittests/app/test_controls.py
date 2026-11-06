"""Execution control through the engine's executor, the end-of-run listener, and
triggers registered with the TriggerEngine."""

import hashlib
import hmac
import json
from unittest import mock

import pytest

from tests.unittests.app.conftest import run
from tests.unittests.app.test_governance import (
    FakeExecutor,
    live_engine,  # noqa: F401  (fixture)
    make,
    make_chain,
    new_workflow,
    org,  # noqa: F401  (fixture)
)
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.models import AuditEntry, Execution, TriggerConfig
from volnux.models.enums import ExecutionState, TriggerType


def published(o):
    chain = make_chain(
        o,
        steps=[
            {
                "step_order": 1,
                "approver_type": "role_based",
                "role_id": o.world.roles["workflow-reviewer"].id,
            }
        ],
    ).json()["data"]["id"]
    wid = new_workflow(o, chain)
    o.client.post(f"/api/v1/workflows/{wid}/submit", headers=o.ha)
    o.client.post(f"/api/v1/approvals/{wid}/approve", json={}, headers=o.h1)
    o.client.post(f"/api/v1/workflows/{wid}/publish", headers=o.ha)
    return wid


def started(o):
    wid = published(o)
    r = o.client.post(f"/api/v1/workflows/{wid}/execute", json={}, headers=o.hop)
    assert r.status_code == 202, r.text
    return wid, r.json()["data"]["id"]


def status(o, eid):
    return o.client.get(f"/api/v1/executions/{eid}", headers=o.hop).json()["data"][
        "status"
    ]


class TestControl:
    def test_the_executor_is_asked_first(self, org, live_engine):
        wid, eid = started(org)
        live_engine.executor.live.add(eid)
        c = org.client
        assert (
            c.post(f"/api/v1/executions/{eid}/pause", headers=org.hop).status_code
            == 200
        )
        assert (
            c.post(f"/api/v1/executions/{eid}/resume", headers=org.hop).status_code
            == 200
        )
        assert (
            c.post(f"/api/v1/executions/{eid}/stop", headers=org.hop).status_code == 200
        )
        assert live_engine.executor.calls == [
            ("pause", eid),
            ("resume", eid),
            ("stop", eid),
        ]
        assert status(org, eid) == "stopped"

    def test_a_run_that_is_not_live_here_changes_nothing(self, org, live_engine):
        wid, eid = started(org)  # (never added to executor.live)
        r = org.client.post(f"/api/v1/executions/{eid}/pause", headers=org.hop)
        assert r.status_code == 409 and "not running in this process" in r.text
        assert status(org, eid) == "running"
        kinds = {
            e["event_type"]
            for e in org.client.get(
                "/api/v1/audit/entries?page_size=200", headers=org.haud
            ).json()["data"]
        }
        assert "execution_paused" not in kinds

    def test_executors_that_run_elsewhere_cannot_be_controlled(self, org, live_engine):
        wid, eid = started(org)
        live_engine.executor.supports_control = False
        for action in ("pause", "resume", "stop"):
            r = org.client.post(f"/api/v1/executions/{eid}/{action}", headers=org.hop)
            assert r.status_code in (501, 409)
        r = org.client.post(f"/api/v1/executions/{eid}/pause", headers=org.hop)
        assert r.status_code == 501 and "cannot" in r.text
        assert live_engine.executor.calls == []
        assert status(org, eid) == "running"

    def test_no_executor_at_all(self, org, live_engine):
        wid, eid = started(org)
        live_engine.engine.workflow_executor = None
        r = org.client.post(f"/api/v1/executions/{eid}/pause", headers=org.hop)
        assert r.status_code == 503

    def test_state_rules_are_checked_before_the_executor(self, org, live_engine):
        wid, eid = started(org)
        live_engine.executor.live.add(eid)
        r = org.client.post(f"/api/v1/executions/{eid}/resume", headers=org.hop)
        assert r.status_code == 409 and live_engine.executor.calls == []

    def test_other_organizations_cannot_control_it(self, org, live_engine):
        wid, eid = started(org)
        live_engine.executor.live.add(eid)
        r = org.client.post(f"/api/v1/executions/{eid}/stop", headers=org.hout)
        assert r.status_code == 404 and live_engine.executor.calls == []


class TestEndOfRun:
    def reload(self, eid):
        return run(Execution.get(eid))

    def listener(self):
        from volnux.app.v1.executions import on_execution_finished

        return on_execution_finished

    @pytest.mark.parametrize(
        "state, expected",
        [
            ("completed", ExecutionState.COMPLETED),
            ("failed", ExecutionState.FAILED),
            ("stopped", ExecutionState.STOPPED),
        ],
    )
    def test_the_record_leaves_running(self, org, live_engine, state, expected):
        wid, eid = started(org)
        run(self.listener()(eid, state, "boom" if state == "failed" else None))
        execution = self.reload(eid)
        assert execution.status is expected and execution.ended_at is not None

    def test_completion_and_failure_are_audited(self, org, live_engine):
        wid, eid = started(org)
        run(self.listener()(eid, "failed", "boom"))
        wid2, eid2 = started_again(org, wid)
        run(self.listener()(eid2, "completed", None))
        entries = org.client.get(
            "/api/v1/audit/entries?page_size=200", headers=org.haud
        ).json()["data"]
        by_target = {
            e["target_id"]: e
            for e in entries
            if e["event_type"].startswith("execution_")
            and e["event_type"] != "execution_started"
        }
        assert by_target[eid]["event_type"] == "execution_failed"
        assert "boom" in by_target[eid]["action"]
        assert by_target[eid2]["event_type"] == "execution_completed"
        assert org.client.get("/api/v1/audit/verify", headers=org.haud).json()["data"][
            "intact"
        ]

    def test_unknown_ids_and_states_are_ignored(self, org, live_engine):
        wid, eid = started(org)
        run(self.listener()("not-an-execution", "completed", None))
        run(self.listener()(eid, "weird", None))
        assert self.reload(eid).status is ExecutionState.RUNNING

    def test_a_stop_through_the_api_is_not_audited_twice(self, org, live_engine):
        wid, eid = started(org)
        live_engine.executor.live.add(eid)
        org.client.post(f"/api/v1/executions/{eid}/stop", headers=org.hop)
        before = org.client.get(
            "/api/v1/audit/entries?page_size=200", headers=org.haud
        ).json()["meta"]["total"]
        run(self.listener()(eid, "stopped", None))
        after = org.client.get(
            "/api/v1/audit/entries?page_size=200", headers=org.haud
        ).json()["meta"]["total"]
        assert before == after and self.reload(eid).status is ExecutionState.STOPPED


def started_again(o, wid):
    r = o.client.post(f"/api/v1/workflows/{wid}/execute", json={}, headers=o.hop)
    assert r.status_code == 202, r.text
    return wid, r.json()["data"]["id"]


class TestLifespan:
    def test_the_listener_is_connected_and_removed(self):
        from volnux.app import app as app_module
        from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor

        class Exec(WorkflowExecutor):
            async def execute(self, workflow_name, params, *, execution_id=None):
                return None

        executor = Exec(mock.MagicMock())
        engine = mock.MagicMock()
        engine.has_workflow_executor.return_value = True
        engine.workflow_executor = executor
        with mock.patch(
            "volnux.engine.workflows.trigger.get_trigger_engine", return_value=engine
        ):
            assert app_module._connect_engine() is executor
        assert app_module._execution_listener() in executor._listeners

    def test_a_standalone_api_has_no_engine_to_connect(self):
        from volnux.app import app as app_module

        engine = mock.MagicMock()
        engine.has_workflow_executor.return_value = False
        with mock.patch(
            "volnux.engine.workflows.trigger.get_trigger_engine", return_value=engine
        ):
            assert app_module._connect_engine() is None

    def test_a_broken_engine_does_not_stop_the_api(self):
        from volnux.app import app as app_module

        with mock.patch(
            "volnux.engine.workflows.trigger.get_trigger_engine",
            side_effect=RuntimeError("no engine"),
        ):
            assert app_module._connect_engine() is None


# ------------------------------------------------------------------ triggers
class FakeConfig:
    def __init__(self):
        from volnux.engine.workflows.workflow import TriggerRegistry

        self.triggers = TriggerRegistry()


class FakeRegistry:
    def __init__(self, *names):
        self.configs = {n: FakeConfig() for n in names}

    def get_workflow_config(self, name):
        return self.configs.get(name)


@pytest.fixture
def real_engine(monkeypatch, backend):
    """A real TriggerEngine (the engine's trigger state goes to the same backend)."""
    from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor

    for attr in ("_backend_store", "_backend_initialized"):
        monkeypatch.setattr(
            TriggerStateRecord,
            attr,
            backend if attr == "_backend_store" else True,
            raising=False,
        )

    class Exec(WorkflowExecutor):
        async def execute(self, workflow_name, params, *, execution_id=None):
            return None

    registry = FakeRegistry("wf")
    engine = TriggerEngine(
        Exec(registry), consumer_concurrency=1, state_sync_interval=0.05
    )
    engine._running = (
        True  # (consumers are not needed: only registration and the queue)
    )
    with mock.patch(
        "volnux.engine.workflows.trigger.get_trigger_engine", return_value=engine
    ):
        yield engine
    engine._running = False


def draft(o, name="wf"):
    wid = new_workflow(o, make_chain(o).json()["data"]["id"])
    return wid


class TestTriggers:
    def test_create_registers_and_delete_unregisters(self, org, real_engine):
        c = org.client
        wid = draft(org)  # (named "wf", as the registry knows it)
        r = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        )
        assert r.status_code == 201, r.text
        data = r.json()["data"]
        assert data["engine_trigger_id"] in real_engine.triggers
        live = real_engine.triggers[data["engine_trigger_id"]]
        assert live.workflow_name == "wf" and live.enabled
        assert live.lifecycle.value == "active"  # (started: the engine was running)

        assert (
            c.delete(f"/api/v1/triggers/{data['id']}", headers=org.hadm).status_code
            == 200
        )
        assert data["engine_trigger_id"] not in real_engine.triggers
        assert run(TriggerConfig.filter_async()) == []

    def test_disable_and_enable(self, org, real_engine):
        c = org.client
        wid = draft(org)
        data = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        ).json()["data"]
        live = real_engine.triggers[data["engine_trigger_id"]]
        off = c.put(
            f"/api/v1/triggers/{data['id']}",
            json={"is_active": False},
            headers=org.hadm,
        )
        assert off.status_code == 200 and off.json()["data"]["is_active"] is False
        assert live.enabled is False
        c.put(
            f"/api/v1/triggers/{data['id']}", json={"is_active": True}, headers=org.hadm
        )
        assert live.enabled is True
        assert (
            c.put(
                f"/api/v1/triggers/{data['id']}",
                json={"is_active": False},
                headers=org.hout,
            ).status_code
            == 404
        )
        assert (
            c.put(
                f"/api/v1/triggers/{data['id']}", json={"x": 1}, headers=org.hadm
            ).status_code
            == 422
        )

    def test_a_workflow_the_engine_does_not_know(self, org, real_engine):
        c = org.client
        wid = new_workflow(org, make_chain(org).json()["data"]["id"])
        run(_rename(wid, "not-loaded"))
        r = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        )
        assert r.status_code == 409 and "not loaded" in r.text
        assert run(TriggerConfig.filter_async()) == []

    def test_one_manual_trigger_per_workflow(self, org, real_engine):
        c = org.client
        wid = draft(org)
        body = {"workflow_id": wid, "trigger_type": "manual"}
        assert (
            c.post("/api/v1/triggers", json=body, headers=org.hadm).status_code == 201
        )
        again = c.post("/api/v1/triggers", json=body, headers=org.hadm)
        assert again.status_code == 422
        assert len(run(TriggerConfig.filter_async())) == 1

    def test_unsupported_and_invalid(self, org, real_engine):
        c = org.client
        wid = draft(org)
        for body in (
            {"workflow_id": wid, "trigger_type": "kafka"},
            {"workflow_id": wid, "trigger_type": "webhook", "config": {}},
            {
                "workflow_id": wid,
                "trigger_type": "webhook",
                "config": {"secret_token": "short"},
            },
            {
                "workflow_id": wid,
                "trigger_type": "schedule",
                "config": {"cron": "* * * * *"},
            },
        ):
            r = c.post("/api/v1/triggers", json=body, headers=org.hadm)
            assert r.status_code == 422, (body, r.text)
        assert real_engine.triggers == {}

    def test_schedule_with_a_celery_app(self, org, real_engine):
        from celery import Celery

        c = org.client
        wid = draft(org)
        with mock.patch(
            "volnux.app.trigger_factory._celery_app", return_value=Celery("t")
        ):
            bad = c.post(
                "/api/v1/triggers",
                json={
                    "workflow_id": wid,
                    "trigger_type": "schedule",
                    "config": {"cron": "* *"},
                },
                headers=org.hadm,
            )
            ok = c.post(
                "/api/v1/triggers",
                json={
                    "workflow_id": wid,
                    "trigger_type": "schedule",
                    "config": {"cron": "*/5 * * * *"},
                },
                headers=org.hadm,
            )
        assert bad.status_code == 422 and "five fields" in bad.text
        assert ok.status_code == 201, ok.text

    def test_other_organizations_see_nothing(self, org, real_engine):
        c = org.client
        wid = draft(org)
        data = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        ).json()["data"]
        assert (
            c.post(
                "/api/v1/triggers",
                json={"workflow_id": wid, "trigger_type": "manual"},
                headers=org.hout,
            ).status_code
            == 404
        )
        assert (
            c.delete(f"/api/v1/triggers/{data['id']}", headers=org.hout).status_code
            == 404
        )
        assert data["engine_trigger_id"] in real_engine.triggers

    def test_changes_are_audited(self, org, real_engine):
        c = org.client
        wid = draft(org)
        data = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        ).json()["data"]
        c.put(
            f"/api/v1/triggers/{data['id']}",
            json={"is_active": False},
            headers=org.hadm,
        )
        c.delete(f"/api/v1/triggers/{data['id']}", headers=org.hadm)
        kinds = [
            e["event_type"]
            for e in c.get(
                "/api/v1/audit/entries?page_size=200", headers=org.haud
            ).json()["data"]
        ]
        assert {"trigger_created", "trigger_updated", "trigger_deleted"} <= set(kinds)


def _rename(workflow_id, name):
    from volnux.models import Workflow

    async def go():
        wf = await Workflow.get(workflow_id)
        wf.name = name
        await wf.save()

    return go()


SECRET = "s" * 32


def sign(body):
    return hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()


class TestWebhooks:
    def make(self, org):
        wid = draft(org)
        r = org.client.post(
            "/api/v1/triggers",
            json={
                "workflow_id": wid,
                "trigger_type": "webhook",
                "config": {"secret_token": SECRET},
            },
            headers=org.hadm,
        )
        assert r.status_code == 201, r.text
        return r.json()["data"]

    def test_the_secret_is_neither_returned_nor_stored_in_the_clear(
        self, org, real_engine, backend
    ):
        data = self.make(org)
        assert data["config"]["secret_token"] == "***"
        stored = run(TriggerConfig.get(data["id"]))
        assert stored.config["secret_token"] == SECRET  # (decrypted on load)
        assert SECRET not in json.dumps(backend._serialize_record(stored).decode())

    def test_a_signed_call_fires_the_workflow(self, org, real_engine):
        data = self.make(org)
        body = b'{"x": 1}'
        r = org.client.post(
            f"/api/v1/hooks/{data['engine_trigger_id']}",
            content=body,
            headers={"x-webhook-token": sign(body)},
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "accepted"
        assert real_engine.task_queue.qsize() == 1

    def test_unsigned_or_wrongly_signed_calls_are_refused(self, org, real_engine):
        data = self.make(org)
        url = f"/api/v1/hooks/{data['engine_trigger_id']}"
        assert org.client.post(url, content=b"{}").status_code == 401
        assert (
            org.client.post(
                url, content=b"{}", headers={"x-webhook-token": "0" * 64}
            ).status_code
            == 401
        )
        assert real_engine.task_queue.qsize() == 0

    def test_a_disabled_webhook_ignores_valid_calls(self, org, real_engine):
        data = self.make(org)
        org.client.put(
            f"/api/v1/triggers/{data['id']}",
            json={"is_active": False},
            headers=org.hadm,
        )
        body = b"{}"
        r = org.client.post(
            f"/api/v1/hooks/{data['engine_trigger_id']}",
            content=body,
            headers={"x-webhook-token": sign(body)},
        )
        assert r.json()["status"] == "ignored" and real_engine.task_queue.qsize() == 0

    def test_unknown_hooks_and_big_bodies(self, org, real_engine):
        assert org.client.post("/api/v1/hooks/nope", content=b"{}").status_code == 404
        data = self.make(org)
        big = b"x" * (1024 * 1024 + 1)
        r = org.client.post(
            f"/api/v1/hooks/{data['engine_trigger_id']}",
            content=big,
            headers={"x-webhook-token": sign(big)},
        )
        assert r.status_code == 413

    def test_a_manual_trigger_is_not_a_hook(self, org, real_engine):
        wid = draft(org)
        data = org.client.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        ).json()["data"]
        assert (
            org.client.post(
                f"/api/v1/hooks/{data['engine_trigger_id']}", content=b"{}"
            ).status_code
            == 404
        )


class TestRestore:
    def test_active_triggers_are_registered_again_after_a_restart(
        self, org, real_engine, monkeypatch
    ):
        from volnux.app.trigger_factory import restore_triggers

        c = org.client
        wid = draft(org)
        first = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        ).json()["data"]
        # a restart: a new engine, with the workflow loaded again
        from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor

        class Exec(WorkflowExecutor):
            async def execute(self, workflow_name, params, *, execution_id=None):
                return None

        fresh = TriggerEngine(
            Exec(FakeRegistry("wf")), consumer_concurrency=1, state_sync_interval=0.05
        )
        fresh._running = True
        try:
            with mock.patch(
                "volnux.engine.workflows.trigger.get_trigger_engine", return_value=fresh
            ):
                assert run(restore_triggers()) == 1
                config = run(TriggerConfig.get(first["id"]))
                assert config.engine_trigger_id in fresh.triggers
                assert config.engine_trigger_id != first["engine_trigger_id"]
        finally:
            fresh._running = False

    def test_inactive_and_broken_ones_are_skipped(self, org, real_engine):
        from volnux.app.trigger_factory import restore_triggers

        c = org.client
        wid = draft(org)
        data = c.post(
            "/api/v1/triggers",
            json={"workflow_id": wid, "trigger_type": "manual"},
            headers=org.hadm,
        ).json()["data"]
        c.put(
            f"/api/v1/triggers/{data['id']}",
            json={"is_active": False},
            headers=org.hadm,
        )

        # ...a stored schedule that can no longer be built (no CELERY_APP)
        async def add_bad():
            from volnux.models import TriggerConfig as TC, Workflow

            wf = await Workflow.get(wid)
            await TC(
                workflow=wf,
                organization=wf.organization,
                trigger_type=TriggerType.SCHEDULE,
                created_by=wf.created_by,
                config={"cron": "* * * * *"},
            ).save()

        run(add_bad())
        real_engine.triggers.clear()
        assert run(restore_triggers()) == 0
