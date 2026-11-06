"""KubernetesWorkflowExecutor and the in-pod runner: the context goes to the pod."""

import asyncio
import json
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.unittests.support import SerializingMemoryBackend
from volnux.config import VolnuxConfig, setting
from volnux.context import (
    CARRIER_KEY,
    get_current_correlation_id,
    get_current_node_id,
    get_current_workflow_id,
    volnux_context,
)
from volnux.engine.workflows.trigger.executors.k8s_messages import (
    K8sJobError,
    K8sJobPayload,
    K8sJobResult,
    execution_route,
)
from volnux.engine.workflows.workflow import WorkflowExecutionError


class FakeApiException(Exception):
    def __init__(self, status=500):
        super().__init__(status)
        self.status = status


class FakeK8sClient:
    """``client.V1Job(...)`` and the like: the arguments, readable."""

    ApiException = FakeApiException

    def __getattr__(self, name):
        if name == "BatchV1Api" or name == "CoreV1Api":
            return MagicMock()
        return lambda **kw: SimpleNamespace(_type=name, **kw)


@pytest.fixture
def k8s(monkeypatch):
    client = FakeK8sClient()
    module = types.ModuleType("kubernetes")
    module.client = client
    module.config = MagicMock()
    config_module = types.ModuleType("kubernetes.config")
    config_module.ConfigException = type("ConfigException", (Exception,), {})
    config_module.load_incluster_config = MagicMock()
    config_module.load_kube_config = MagicMock()
    module.config = config_module
    client_module = types.ModuleType("kubernetes.client")
    client_module.ApiException = FakeApiException
    client_module.__getattr__ = lambda name: getattr(client, name)
    module.client = client_module
    monkeypatch.setitem(sys.modules, "kubernetes", module)
    monkeypatch.setitem(sys.modules, "kubernetes.client", client_module)
    monkeypatch.setitem(sys.modules, "kubernetes.config", config_module)
    from volnux.engine.workflows.trigger.executors import kubernetes

    return kubernetes


@pytest.fixture
def bus(monkeypatch):
    """The messaging backend the executor and the pod share (in memory here)."""
    backend = SerializingMemoryBackend(namespace_prefix="k8s-test")
    for model in (K8sJobPayload, K8sJobResult, K8sJobError):
        monkeypatch.setattr(model, "_backend_store", backend, raising=False)
        monkeypatch.setattr(model, "_backend_initialized", True, raising=False)
    yield backend
    backend.close()


@pytest.fixture
def executor(k8s, bus):
    return k8s.KubernetesWorkflowExecutor(MagicMock(), poll_interval=0)


def job_env(job):
    container = job.spec.template.spec.containers[0]
    return {e.name: e for e in container.env}


class TestJobSpec:
    def build(self, executor):
        return executor._build_job_spec(
            workflow_name="my_wf",
            job_name="vx-my-wf-1",
            execution_id="123-abc",
        )

    def test_the_pod_runs_the_job_runner(self, executor):
        container = self.build(executor).spec.template.spec.containers[0]
        assert container.command == ["volnux-job-runner"]

    def test_the_node_id_is_the_name_the_config_reads(self, executor):
        env = job_env(self.build(executor))
        assert "VOLNUX_MESH_NODE_ID" in env and "VOLNUX_NODE_ID" not in env
        selector = env["VOLNUX_MESH_NODE_ID"].value_from.field_ref
        assert selector.field_path == "metadata.name"

    def test_the_job_wiring_is_in_the_environment(self, executor):
        env = job_env(self.build(executor))
        assert env["VOLNUX_WORKFLOW_NAME"].value == "my_wf"
        assert env["VOLNUX_JOB_EXECUTION_ID"].value == "123-abc"

    def test_no_redis_wiring_is_left(self, executor):
        names = set(job_env(self.build(executor)))
        assert not {n for n in names if "REDIS" in n or n.endswith("_KEY")}

    def test_the_project_dir_goes_to_the_pod_when_set(self, executor):
        from volnux.conf import override_settings

        assert "VOLNUX_PROJECT_DIR" not in job_env(self.build(executor))
        with override_settings(PROJECT_DIR="/opt/volnux"):
            env = job_env(self.build(executor))
        assert env["VOLNUX_PROJECT_DIR"].value == "/opt/volnux"

    def test_the_labels_are_on_the_job_and_the_pod(self, executor):
        job = self.build(executor)
        assert job.metadata.labels == job.spec.template.metadata.labels
        assert job.metadata.labels["workflow"] == "my_wf"

    def test_only_the_batch_client_is_kept(self, executor):
        assert not hasattr(executor, "_core_client")


class TestPodDatabases:
    """The pod learns the backend from VOLNUX_DATABASES, never with a password."""

    def make(self, k8s, **kw):
        return k8s.KubernetesWorkflowExecutor(MagicMock(), poll_interval=0, **kw)

    def env(self, executor):
        job = executor._build_job_spec(
            workflow_name="wf", job_name="vx-wf-1", execution_id="1-a"
        )
        return job_env(job)

    def test_databases_go_to_the_pod_as_json(self, k8s, bus):
        from volnux.conf import override_settings

        databases = {
            "default": {"ENGINE": "volnux.backends.stores.inmemory.X"},
            "messaging": {
                "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
                "HOST": "redis.internal",
                "PORT": 6379,
            },
        }
        with override_settings(DATABASES=databases):
            env = self.env(self.make(k8s))
        assert json.loads(env["VOLNUX_DATABASES"].value) == databases

    def test_a_password_is_never_put_in_the_job_spec(self, k8s, bus):
        from volnux.conf import override_settings

        databases = {
            "default": {"ENGINE": "volnux.backends.stores.inmemory.X"},
            "messaging": {
                "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
                "HOST": "redis.internal",
                "PASSWORD": "s3cret",
            },
        }
        with override_settings(DATABASES=databases):
            env = self.env(self.make(k8s))
        value = env["VOLNUX_DATABASES"].value
        assert "s3cret" not in value and "PASSWORD" not in value
        assert json.loads(value)["messaging"]["HOST"] == "redis.internal"

    def test_with_a_password_and_a_secret_the_secret_supplies_it(self, k8s, bus):
        from volnux.conf import override_settings

        databases = {
            "default": {
                "ENGINE": "volnux.backends.stores.redis.RedisStoreBackend",
                "PASSWORD": "s3cret",
            }
        }
        with override_settings(DATABASES=databases):
            env = self.env(self.make(k8s, env_secret_name="volnux-secrets"))
        assert "VOLNUX_DATABASES" not in env

    def test_the_in_memory_backend_is_warned_about(self, k8s, bus, caplog):
        self.make(k8s)
        assert "in-memory" in caplog.text and "messaging" in caplog.text

    def test_a_shared_backend_is_not_warned_about(self, k8s, bus, caplog):
        from volnux.conf import override_settings

        databases = {
            "default": {"ENGINE": "volnux.backends.stores.redis.RedisStoreBackend"}
        }
        with override_settings(DATABASES=databases):
            self.make(k8s)
        assert "in-memory" not in caplog.text


class TestDispatch:
    @pytest.fixture
    def run(self, executor, monkeypatch):
        monkeypatch.setattr(executor, "_poll_job_status", AsyncMock())
        monkeypatch.setattr(executor, "_delete_job", AsyncMock())
        sent = {}

        real_create = executor._batch_client.create_namespaced_job

        def create(namespace, body):
            # the payload is already there when the Job is created
            sent["job_env"] = job_env(body)
            return real_create(namespace=namespace, body=body)

        executor._batch_client.create_namespaced_job = create
        return sent

    @pytest.mark.asyncio
    async def test_the_payload_has_the_params_and_the_carrier(
        self, executor, run, bus, monkeypatch
    ):
        seen = {}

        async def poll(job_name, execution_id):
            route = execution_route(execution_id)
            seen["payload"] = await K8sJobPayload.pop(route_override=route)
            await K8sJobResult.push(K8sJobResult(result=7), route_override=route)

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        params = {"a": 1}
        with volnux_context(correlation_id="req-3"):
            assert await executor.execute("wf", params) == 7

        payload = seen["payload"]
        assert payload.params == {"a": 1} and params == {"a": 1}
        assert payload.carrier["workflow_id"] == "wf"
        assert payload.carrier["correlation_id"] == "req-3"
        assert run["job_env"]["VOLNUX_JOB_EXECUTION_ID"].value

    @pytest.mark.asyncio
    async def test_the_payload_queue_has_a_ttl(self, executor, run, bus, monkeypatch):
        seen = {}

        async def poll(job_name, execution_id):
            seen["deadlines"] = dict(getattr(bus, "_queue_deadlines", {}))
            route = execution_route(execution_id)
            await K8sJobResult.push(K8sJobResult(result=1), route_override=route)

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        await executor.execute("wf", {})
        assert len(seen["deadlines"]) == 1
        assert "payload" in next(iter(seen["deadlines"]))

    @pytest.mark.asyncio
    async def test_the_carrier_has_no_secrets(self, executor, run, bus, monkeypatch):
        from volnux.conf import override_settings

        seen = {}

        async def poll(job_name, execution_id):
            route = execution_route(execution_id)
            seen["payload"] = await K8sJobPayload.pop(route_override=route)
            await K8sJobResult.push(K8sJobResult(result=1), route_override=route)

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        with override_settings(SECRET_KEY="s3cret", DEBUG=True):
            await executor.execute("wf", {})
        assert "s3cret" not in json.dumps(seen["payload"].carrier)

    @pytest.mark.asyncio
    async def test_an_error_from_the_pod_is_raised(
        self, executor, run, bus, monkeypatch
    ):
        async def poll(job_name, execution_id):
            await K8sJobError.push(
                K8sJobError(error="boom", traceback="tb"),
                route_override=execution_route(execution_id),
            )

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        with pytest.raises(WorkflowExecutionError, match="boom"):
            await executor.execute("wf", {})

    @pytest.mark.asyncio
    async def test_no_message_from_the_pod_is_an_error(
        self, executor, run, bus, monkeypatch
    ):
        import volnux.engine.workflows.trigger.executors.kubernetes as mod

        monkeypatch.setattr(mod, "_RESULT_GRACE_SECONDS", 0.05)
        with pytest.raises(WorkflowExecutionError, match="sent no result"):
            await executor.execute("wf", {})

    @pytest.mark.asyncio
    async def test_the_queues_are_deleted_afterwards(
        self, executor, run, bus, monkeypatch
    ):
        ids = {}

        async def poll(job_name, execution_id):
            ids["id"] = execution_id
            await K8sJobResult.push(
                K8sJobResult(result=1), route_override=execution_route(execution_id)
            )

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        await executor.execute("wf", {})
        route = execution_route(ids["id"])
        for model in (K8sJobPayload, K8sJobResult, K8sJobError):
            assert await model.queue_length(route_override=route) == 0
        executor._delete_job.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_queues_are_deleted_when_the_job_fails(
        self, executor, run, bus, monkeypatch
    ):
        ids = {}

        async def poll(job_name, execution_id):
            ids["id"] = execution_id
            raise WorkflowExecutionError("job failed")

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        with pytest.raises(WorkflowExecutionError, match="job failed"):
            await executor.execute("wf", {})
        route = execution_route(ids["id"])
        assert await K8sJobPayload.queue_length(route_override=route) == 0

    @pytest.mark.asyncio
    async def test_executions_do_not_see_each_others_messages(
        self, executor, run, bus, monkeypatch
    ):
        async def poll(job_name, execution_id):
            payload = await K8sJobPayload.pop(
                route_override=execution_route(execution_id)
            )
            await K8sJobResult.push(
                K8sJobResult(result=payload.params["n"]),
                route_override=execution_route(execution_id),
            )

        monkeypatch.setattr(executor, "_poll_job_status", poll)
        results = await asyncio.gather(
            *(executor.execute("wf", {"n": n}) for n in range(5))
        )
        assert results == list(range(5))

    @pytest.mark.asyncio
    async def test_a_backend_failure_reading_the_result_is_a_workflow_error(
        self, executor, run, bus, monkeypatch
    ):
        async def down(*a, **k):
            raise ConnectionError("down")

        monkeypatch.setattr(K8sJobError, "pop", classmethod(lambda cls, **k: down()))
        with pytest.raises(WorkflowExecutionError, match="Failed to read"):
            await executor.execute("wf", {})

    @pytest.mark.asyncio
    async def test_a_backend_failure_sending_the_payload_stops_before_the_job(
        self, executor, run, bus, monkeypatch
    ):
        async def down(*a, **k):
            raise ConnectionError("down")

        monkeypatch.setattr(bus, "push", down)
        created = []
        executor._batch_client.create_namespaced_job = lambda **kw: created.append(1)
        with pytest.raises(WorkflowExecutionError, match="Failed to send"):
            await executor.execute("wf", {})
        assert created == []


class Condition(SimpleNamespace):
    pass


def status(succeeded=None, failed=None, active=None, conditions=None):
    return SimpleNamespace(
        succeeded=succeeded, failed=failed, active=active, conditions=conditions
    )


class TestPolling:
    def job(self, **kw):
        return SimpleNamespace(status=status(**kw))

    async def poll(self, executor, *jobs):
        executor._batch_client.read_namespaced_job.side_effect = list(jobs)
        await executor._poll_job_status("job", "exec")

    @pytest.mark.asyncio
    async def test_success(self, executor):
        await self.poll(executor, self.job(active=1), self.job(succeeded=1))

    @pytest.mark.asyncio
    async def test_a_failed_pod_is_not_a_failed_job_while_it_is_retried(self, executor):
        # failed=1 but no "Failed" condition: the backoff limit has not been reached
        await self.poll(
            executor,
            self.job(failed=1, active=1),
            self.job(failed=1, active=1),
            self.job(failed=1, succeeded=1),
        )

    @pytest.mark.asyncio
    async def test_the_failed_condition_fails_the_job(self, executor):
        failed = self.job(
            failed=3,
            conditions=[
                Condition(type="Failed", status="True", message="BackoffLimitExceeded")
            ],
        )
        with pytest.raises(WorkflowExecutionError, match="BackoffLimitExceeded"):
            await self.poll(executor, failed)

    @pytest.mark.asyncio
    async def test_a_failed_condition_that_is_false_does_not(self, executor):
        not_failed = self.job(
            failed=1, conditions=[Condition(type="Failed", status="False", message="x")]
        )
        await self.poll(executor, not_failed, self.job(succeeded=1))

    @pytest.mark.asyncio
    async def test_a_deleted_job(self, executor):
        executor._batch_client.read_namespaced_job.side_effect = FakeApiException(404)
        with pytest.raises(WorkflowExecutionError, match="deleted"):
            await executor._poll_job_status("job", "exec")

    @pytest.mark.asyncio
    async def test_an_api_error_is_retried(self, executor):
        executor._batch_client.read_namespaced_job.side_effect = [
            FakeApiException(503),
            self.job(succeeded=1),
        ]
        await executor._poll_job_status("job", "exec")


@pytest.fixture
def pod_engine(monkeypatch):
    """An engine as the pod initialises it; ``seen`` is what the workflow saw."""
    seen = {}

    async def run_workflow(params, run_type):
        seen.update(
            params=params,
            run_type=run_type,
            workflow=get_current_workflow_id(),
            correlation=get_current_correlation_id(),
            node=get_current_node_id(),
            setting=setting("K8S_NAMESPACE", None),
        )
        return {"value": object()}

    config = MagicMock()
    config.run_workflow = AsyncMock(side_effect=run_workflow)
    registry = MagicMock()
    registry.get_workflow_config.side_effect = lambda n: config if n == "wf" else None
    engine = MagicMock()
    engine.get_workflow_registry.return_value = registry

    async def initialise_workflows(path):
        seen["project"] = str(path)
        return engine

    monkeypatch.setattr("volnux.setup.initialise_workflows", initialise_workflows)
    return seen


def carried_payload():
    config = VolnuxConfig.from_dict(
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
    from volnux.context import inject_carrier, CARRIER_KEY

    with volnux_context(config=config, workflow_id="wf", correlation_id="req-5"):
        carrier = inject_carrier({})[CARRIER_KEY]
    return K8sJobPayload(params={"x": 1, "run_type": "batch"}, carrier=carrier)


class TestRunJob:
    @pytest.mark.asyncio
    async def test_the_workflow_runs_under_the_carried_context(self, k8s, pod_engine):
        outcome = await k8s.run_job("wf", carried_payload(), "/proj")

        assert pod_engine["project"] == "/proj"
        assert pod_engine["workflow"] == "wf" and pod_engine["correlation"] == "req-5"
        assert pod_engine["setting"] == "from-dispatcher"
        assert pod_engine["params"] == {"x": 1} and pod_engine["run_type"] == "batch"
        # the pod is its own node
        assert pod_engine["node"] == VolnuxConfig.get_instance().get_node_id()
        # the result is JSON
        assert set(outcome) == {"result"}
        json.dumps(outcome)
        assert get_current_workflow_id() is None

    @pytest.mark.asyncio
    async def test_an_unregistered_workflow(self, k8s, pod_engine):
        with pytest.raises(WorkflowExecutionError, match="not found"):
            await k8s.run_job("missing", carried_payload(), "/proj")


@pytest.fixture
def pod_env(monkeypatch):
    monkeypatch.setenv("VOLNUX_WORKFLOW_NAME", "wf")
    monkeypatch.setenv("VOLNUX_JOB_EXECUTION_ID", "exec-1")


ROUTE = execution_route("exec-1")


class TestJobRunnerMain:
    def run(self, k8s):
        with pytest.raises(SystemExit) as exit_info:
            k8s.job_runner_main()
        return exit_info.value.code

    @pytest.mark.asyncio
    async def test_success_sends_the_result(self, k8s, pod_engine, bus, pod_env):
        await K8sJobPayload.push(carried_payload(), route_override=ROUTE)
        code = await asyncio.get_running_loop().run_in_executor(
            None, lambda: self.run(k8s)
        )
        assert code == 0
        assert await K8sJobError.pop(route_override=ROUTE) is None
        result = await K8sJobResult.pop(route_override=ROUTE)
        assert result.result is not None and pod_engine["workflow"] == "wf"

    @pytest.mark.asyncio
    async def test_failure_sends_the_error(
        self, k8s, pod_engine, bus, pod_env, monkeypatch
    ):
        await K8sJobPayload.push(carried_payload(), route_override=ROUTE)

        async def broken(path):
            raise RuntimeError("cannot load project")

        monkeypatch.setattr("volnux.setup.initialise_workflows", broken)
        code = await asyncio.get_running_loop().run_in_executor(
            None, lambda: self.run(k8s)
        )
        assert code == 1
        assert await K8sJobResult.pop(route_override=ROUTE) is None
        error = await K8sJobError.pop(route_override=ROUTE)
        assert "cannot load project" in error.error and error.traceback

    @pytest.mark.asyncio
    async def test_a_reply_queue_has_a_ttl(self, k8s, pod_engine, bus, pod_env):
        await K8sJobPayload.push(carried_payload(), route_override=ROUTE)
        await asyncio.get_running_loop().run_in_executor(None, lambda: self.run(k8s))
        assert any("result" in key for key in bus._queue_deadlines)

    @pytest.mark.asyncio
    async def test_missing_payload_exits(
        self, k8s, pod_engine, bus, pod_env, monkeypatch
    ):
        import volnux.engine.workflows.trigger.executors.kubernetes as mod

        monkeypatch.setattr(mod, "_PAYLOAD_WAIT_SECONDS", 0.05)
        code = await asyncio.get_running_loop().run_in_executor(
            None, lambda: self.run(k8s)
        )
        assert code == 1
        assert await K8sJobResult.pop(route_override=ROUTE) is None

    def test_missing_wiring_exits(self, k8s, monkeypatch):
        monkeypatch.delenv("VOLNUX_WORKFLOW_NAME", raising=False)
        assert self.run(k8s) == 1


def test_the_job_runner_is_a_registered_script():
    tomllib = pytest.importorskip("tomllib")
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    scripts = tomllib.loads((root / "pyproject.toml").read_text())["project"]["scripts"]
    target = scripts["volnux-job-runner"]
    module, _, function = target.partition(":")
    import importlib

    assert callable(getattr(importlib.import_module(module), function))
