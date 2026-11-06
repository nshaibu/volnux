"""CeleryWorkflowExecutor: the volnux context travels with the task."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

celery = pytest.importorskip("celery")

from volnux.conf import override_settings
from volnux.config import VolnuxConfig, setting
from volnux.context import (
    CARRIER_KEY,
    get_current_correlation_id,
    get_current_execution_id,
    get_current_workflow_id,
    volnux_context,
)
from volnux.engine.workflows.trigger.executors import celery as celery_module
from volnux.engine.workflows.trigger.executors.base import WorkflowExecutor
from volnux.engine.workflows.trigger.executors.celery import CeleryWorkflowExecutor
from volnux.engine.workflows.workflow import WorkflowExecutionError


@pytest.fixture
def app():
    app = celery.Celery("t", broker="memory://", backend="cache+memory://")
    app.conf.task_always_eager = True
    app.conf.task_eager_propagates = False
    return app


@pytest.fixture
def worker(monkeypatch):
    """A worker whose engine runs ``run_workflow`` (what is seen is recorded)."""
    seen = {}

    async def run_workflow(params, run_type):
        seen.update(
            params=params,
            run_type=run_type,
            workflow=get_current_workflow_id(),
            execution=get_current_execution_id(),
            correlation=get_current_correlation_id(),
            setting=setting("K8S_NAMESPACE", None),
        )
        return seen.get("result", {"ok": True})

    config = MagicMock()
    config.run_workflow = AsyncMock(side_effect=run_workflow)
    registry = MagicMock()
    registry.get_workflow_config.side_effect = lambda n: config if n == "wf" else None
    engine = MagicMock()
    engine.get_workflow_registry.return_value = registry
    monkeypatch.setattr(celery_module, "_WORKER_ENGINE", engine)
    seen["config"] = config
    return seen


def executor_for(app, **kw):
    return CeleryWorkflowExecutor(MagicMock(), celery_app=app, **kw)


def test_it_is_a_workflow_executor(app):
    assert isinstance(executor_for(app), WorkflowExecutor)


class TestSettings:
    def test_limits_default_from_the_settings(self, app):
        executor = executor_for(app)
        assert executor._task_soft_time_limit is None
        assert executor._task_time_limit is None
        assert executor._result_timeout is None

    def test_limits_come_from_the_settings(self, app):
        with override_settings(
            CELERY_EXECUTOR_SOFT_TIME_LIMIT=30,
            CELERY_EXECUTOR_TIME_LIMIT=60,
            CELERY_EXECUTOR_RESULT_TIMEOUT=90,
        ):
            executor = executor_for(app)
        assert (
            executor._task_soft_time_limit,
            executor._task_time_limit,
            executor._result_timeout,
        ) == (30.0, 60.0, 90.0)

    def test_arguments_win(self, app):
        with override_settings(CELERY_EXECUTOR_TIME_LIMIT=60):
            assert executor_for(app, task_time_limit=5)._task_time_limit == 5.0

    def test_the_task_name_uses_the_queue_in_use(self, app):
        assert executor_for(app)._task_name == "volnux.run_workflow.volnux_workflows"
        assert executor_for(app, queue="q2")._task_name == "volnux.run_workflow.q2"


class TestDispatch:
    @pytest.mark.asyncio
    async def test_the_payload_has_the_params_and_the_carrier(self, app, monkeypatch):
        executor = executor_for(app)
        sent = {}

        class Result:
            state = "SUCCESS"
            result = {"result": 1}

            def ready(self):
                return True

        def apply_async(args, queue, task_id):
            sent.update(args=args, queue=queue, task_id=task_id)
            return Result()

        monkeypatch.setattr(executor._task, "apply_async", apply_async, raising=False)
        params = {"a": [1, 2], "run_type": "batch"}
        with volnux_context(correlation_id="req-9", trace_id="tr"):
            assert await executor.execute("wf", params) == 1

        (payload,) = sent["args"]
        assert payload["workflow_name"] == "wf" and payload["params"] == params
        assert payload["params"] is params  # sent as given...
        assert params == {"a": [1, 2], "run_type": "batch"}  # ...and not changed
        carrier = payload[CARRIER_KEY]
        assert carrier["workflow_id"] == "wf"
        assert carrier["correlation_id"] == "req-9" and carrier["trace_id"] == "tr"
        assert carrier["execution_id"] == sent["task_id"]
        assert sent["queue"] == "volnux_workflows"
        json.dumps(payload)  # it has to go through the broker as JSON

    @pytest.mark.asyncio
    async def test_the_carrier_has_no_secrets(self, app, monkeypatch):
        executor = executor_for(app)
        captured = {}

        class Result:
            state = "SUCCESS"
            result = {}

            def ready(self):
                return True

        monkeypatch.setattr(
            executor._task,
            "apply_async",
            lambda args, queue, task_id: captured.update(p=args[0]) or Result(),
            raising=False,
        )
        with override_settings(SECRET_KEY="s3cret", DEBUG=True):
            await executor.execute("wf", {})
        assert "s3cret" not in json.dumps(captured["p"])


class TestRoundTrip:
    """The Celery app runs tasks eagerly: the executor and the worker entrypoint
    are both exercised."""

    @pytest.mark.asyncio
    async def test_the_workflow_runs_under_the_dispatchers_context(self, app, worker):
        executor = executor_for(app)
        carried = VolnuxConfig.from_dict(
            {
                "node_id": "dispatcher",
                "store": [
                    {
                        "name": "K8S_NAMESPACE",
                        "value": "from-dispatcher",
                        "timestamp": 0,
                        "origin_mesh_node": "local",
                        "signature": None,
                    }
                ],
            }
        )
        with volnux_context(config=carried, correlation_id="req-1"):
            result = await executor.execute("wf", {"x": 1, "run_type": "batch"})

        assert result == {"ok": True}
        assert worker["workflow"] == "wf"
        assert worker["correlation"] == "req-1"
        assert worker["execution"]
        # the worker saw the dispatcher's settings, not its own
        assert worker["setting"] == "from-dispatcher"
        # run_type is taken out of the params
        assert worker["params"] == {"x": 1} and worker["run_type"] == "batch"
        # and the worker is left clean
        assert get_current_workflow_id() is None

    @pytest.mark.asyncio
    async def test_a_result_json_cannot_hold_comes_back_as_text(self, app, worker):
        class Pipeline:
            def __str__(self):
                return "<pipeline>"

        worker["result"] = Pipeline()
        assert await executor_for(app).execute("wf", {}) == "<pipeline>"

    @pytest.mark.asyncio
    async def test_an_unregistered_workflow_fails(self, app, worker):
        with pytest.raises(WorkflowExecutionError, match="not registered"):
            await executor_for(app).execute("missing", {})

    @pytest.mark.asyncio
    async def test_a_failing_workflow_is_a_workflow_execution_error(self, app, worker):
        worker["config"].run_workflow = AsyncMock(side_effect=RuntimeError("boom"))
        with pytest.raises(WorkflowExecutionError, match="boom"):
            await executor_for(app).execute("wf", {})


class TestResultTimeout:
    @pytest.mark.asyncio
    async def test_a_task_that_never_finishes_is_revoked(self, app, monkeypatch):
        executor = executor_for(app, poll_interval=0.01, result_timeout=0.05)
        revoked = []
        monkeypatch.setattr(
            executor, "revoke", lambda task_id, terminate=False: revoked.append(task_id)
        )

        class Pending:
            state = "PENDING"

            def ready(self):
                return False

        monkeypatch.setattr(
            executor._task, "apply_async", lambda **kw: Pending(), raising=False
        )
        with pytest.raises(WorkflowExecutionError, match="no result within"):
            await executor.execute("wf", {})
        assert len(revoked) == 1


class TestWorkerInit:
    def test_the_engine_is_initialised_with_a_running_loop_not_a_coroutine(
        self, monkeypatch
    ):
        calls = []
        engine = object()

        async def initialise_workflows(path):
            calls.append(path)
            return engine

        monkeypatch.setattr("volnux.setup.initialise_workflows", initialise_workflows)
        monkeypatch.setattr(celery_module, "_WORKER_ENGINE", None)
        from celery.signals import worker_process_init

        celery_module.register_worker_init(MagicMock(), "/proj")
        worker_process_init.send(sender=None)
        assert celery_module._WORKER_ENGINE is engine
        assert [str(c) for c in calls] == ["/proj"]
