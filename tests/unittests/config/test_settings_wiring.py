"""Each setting reaches the code that uses it: the default equals what the code did
before it read a setting, and an environment variable changes it."""

import argparse
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

import volnux.conf.global_settings as settings
from volnux.config import VolnuxConfig


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for var in list(os.environ):
        if var.startswith("VOLNUX_") and var != "VOLNUX_MESH_NODE_ID":
            monkeypatch.delenv(var)


class TestTriggerEngine:
    def make(self, **kw):
        from volnux.engine.workflows.trigger.engine import TriggerEngine

        return TriggerEngine(**kw)

    def test_defaults_are_the_previous_hardcoded_values(self):
        engine = self.make()
        assert engine.consumer_concurrency == 5
        assert engine.task_queue.maxsize == 1000
        assert engine.drain_timeout == 30
        assert engine._state_sync_interval == 10

    def test_the_defaults_come_from_settings(self):
        engine = self.make()
        assert (
            engine.consumer_concurrency == settings.TRIGGER_ENGINE_CONSUMER_CONCURRENCY
        )
        assert engine.task_queue.maxsize == settings.TRIGGER_ENGINE_MAX_QUEUE_SIZE
        assert engine.drain_timeout == settings.TRIGGER_ENGINE_DRAIN_TIMEOUT
        assert (
            engine._state_sync_interval == settings.TRIGGER_ENGINE_STATE_SYNC_INTERVAL
        )

    def test_environment_overrides(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_TRIGGER_ENGINE_CONSUMER_CONCURRENCY", "9")
        monkeypatch.setenv("VOLNUX_TRIGGER_ENGINE_MAX_QUEUE_SIZE", "50")
        monkeypatch.setenv("VOLNUX_TRIGGER_ENGINE_DRAIN_TIMEOUT", "7")
        monkeypatch.setenv("VOLNUX_TRIGGER_ENGINE_STATE_SYNC_INTERVAL", "2")
        engine = self.make()
        assert engine.consumer_concurrency == 9
        assert engine.task_queue.maxsize == 50
        assert engine.drain_timeout == 7
        assert engine._state_sync_interval == 2

    def test_an_explicit_argument_wins(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_TRIGGER_ENGINE_CONSUMER_CONCURRENCY", "9")
        assert self.make(consumer_concurrency=2).consumer_concurrency == 2

    def test_invalid_values_are_still_refused(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_TRIGGER_ENGINE_CONSUMER_CONCURRENCY", "0")
        with pytest.raises(ValueError):
            self.make()
        with pytest.raises(ValueError):
            self.make(drain_timeout=0)


class TestCheckpointConfig:
    def test_the_default_setting_equals_the_built_in_default(self):
        from volnux.engine.checkpoint_config import CheckPointConfig

        assert CheckPointConfig.from_settings() == CheckPointConfig()
        assert CheckPointConfig.from_settings().dump(
            _format="dict"
        ) == CheckPointConfig().dump(_format="dict")

    def test_environment_json_overrides(self, monkeypatch):
        from volnux.engine.checkpoint_config import CheckPointConfig

        monkeypatch.setenv(
            "VOLNUX_CHECKPOINT_CONFIG", '{"runtime": {"retry_attempts": 9}}'
        )
        assert CheckPointConfig.from_settings().runtime.retry_attempts == 9

    def test_an_invalid_value_names_the_setting(self, monkeypatch):
        from volnux.engine.checkpoint_config import CheckPointConfig
        from volnux.exceptions import ImproperlyConfigured

        monkeypatch.setenv(
            "VOLNUX_CHECKPOINT_CONFIG", '{"policy": {"frequency": "weird"}}'
        )
        with pytest.raises(ImproperlyConfigured, match="CHECKPOINT_CONFIG"):
            CheckPointConfig.from_settings()

    def test_the_engine_uses_it(self, monkeypatch):
        from volnux.engine.default_engine import DefaultWorkflowEngine

        monkeypatch.setenv(
            "VOLNUX_CHECKPOINT_CONFIG", '{"runtime": {"retry_attempts": 9}}'
        )
        engine = DefaultWorkflowEngine(enable_checkpointing=True)
        assert engine.checkpoint_config.runtime.retry_attempts == 9


class TestPools:
    def test_kv_executor_workers(self, monkeypatch):
        from volnux.concurrency.kv_executor import _configured_workers

        assert _configured_workers() == settings.KV_EXECUTOR_WORKERS == 128
        monkeypatch.setenv("VOLNUX_KV_EXECUTOR_WORKERS", "7")
        assert _configured_workers() == 7
        monkeypatch.setenv("VOLNUX_KV_EXECUTOR_WORKERS", "lots")
        assert _configured_workers() == 128
        monkeypatch.setenv("VOLNUX_KV_EXECUTOR_WORKERS", "0")
        assert _configured_workers() == 1

    def test_shared_pool_sizes_follow_the_environment_live(self, monkeypatch):
        from concurrent.futures import ThreadPoolExecutor

        from volnux.executors.shared import pool_size

        monkeypatch.setenv("VOLNUX_SHARED_THREAD_POOL_SIZE", "3")
        assert pool_size(ThreadPoolExecutor) == 3

    def test_sub_phase_pool_default(self):
        from volnux.mixins.event import checkpointer

        assert checkpointer._SUB_PHASE_EXECUTOR._max_workers == 64


class TestBatching:
    def test_defaults(self):
        import volnux.default_batch_processors as b

        assert (b.DEFAULT_BATCH_SIZE, b.DEFAULT_CHUNK_SIZE) == (100, 10240)
        assert b.DEFAULT_BATCH_SIZE == settings.DEFAULT_BATCH_SIZE

    def test_remote_stream_ttl(self, monkeypatch):
        from volnux.execution.remote.stream import batch_ttl_seconds

        assert batch_ttl_seconds() == 3600 == settings.REMOTE_BATCH_TTL_SECONDS
        monkeypatch.setenv("VOLNUX_REMOTE_BATCH_TTL_SECONDS", "60")
        assert batch_ttl_seconds() == 60


class TestExecutors:
    def test_celery_executor_defaults_and_environment(self, monkeypatch):
        from celery import Celery

        from volnux.engine.workflows.trigger.executors.celery import (
            CeleryWorkflowExecutor,
        )

        executor = CeleryWorkflowExecutor(Mock(), celery_app=Celery("x"))
        assert executor._queue == settings.CELERY_EXECUTOR_QUEUE == "volnux_workflows"
        assert executor._poll_interval == settings.CELERY_EXECUTOR_POLL_INTERVAL == 0.5

        monkeypatch.setenv("VOLNUX_CELERY_EXECUTOR_QUEUE", "prod_q")
        monkeypatch.setenv("VOLNUX_CELERY_EXECUTOR_POLL_INTERVAL", "2")
        executor = CeleryWorkflowExecutor(Mock(), celery_app=Celery("x"))
        assert (executor._queue, executor._poll_interval) == ("prod_q", 2.0)
        assert (
            CeleryWorkflowExecutor(Mock(), celery_app=Celery("x"), queue="mine")._queue
            == "mine"
        )

    @pytest.fixture
    def k8s(self, monkeypatch):
        fake = types.ModuleType("kubernetes")
        fake.client, fake.config = MagicMock(), MagicMock()
        monkeypatch.setitem(sys.modules, "kubernetes", fake)
        monkeypatch.setitem(sys.modules, "kubernetes.client", fake.client)
        monkeypatch.setitem(sys.modules, "kubernetes.config", fake.config)
        from volnux.engine.workflows.trigger.executors.kubernetes import (
            KubernetesWorkflowExecutor,
        )

        return KubernetesWorkflowExecutor

    def test_k8s_defaults_are_the_previous_ones(self, k8s):
        e = k8s(Mock())
        assert (e._namespace, e._image, e._image_pull_policy) == (
            "default",
            "volnux-engine:latest",
            "IfNotPresent",
        )
        assert (e._ttl_seconds_after_finished, e._backoff_limit, e._poll_interval) == (
            3600,
            2,
            2.0,
        )
        assert e._redis_url == "redis://localhost:6379/0"

    def test_k8s_environment_and_arguments(self, k8s, monkeypatch):
        monkeypatch.setenv("VOLNUX_K8S_NAMESPACE", "volnux")
        monkeypatch.setenv("VOLNUX_K8S_IMAGE", "reg/volnux:2.1.0")
        monkeypatch.setenv("VOLNUX_K8S_BACKOFF_LIMIT", "0")
        monkeypatch.setenv("VOLNUX_REDIS_CHECKPOINT_URL", "redis://r:1/2")
        e = k8s(Mock())
        assert (e._namespace, e._image, e._backoff_limit) == (
            "volnux",
            "reg/volnux:2.1.0",
            0,
        )
        assert e._redis_url == "redis://r:1/2"
        assert k8s(Mock(), namespace="mine")._namespace == "mine"

    def test_k8s_key_ttl(self, monkeypatch):
        from volnux.engine.workflows.trigger.executors import kubernetes as mod

        assert mod._key_ttl_seconds() == 3600
        monkeypatch.setenv("VOLNUX_K8S_KEY_TTL_SECONDS", "120")
        assert mod._key_ttl_seconds() == 120


class TestMesh:
    def test_constants_and_defaults(self):
        import volnux.manager.mesh.client as mesh

        assert mesh._GOSSIP_FANOUT == settings.MESH_GOSSIP_FANOUT == 3
        assert mesh._CAPACITY_GATHER_TIMEOUT == settings.MESH_CAPACITY_GATHER_TIMEOUT
        weights = settings.MESH_SCORE_WEIGHTS
        assert (mesh._W_LATENCY, mesh._W_QUEUE, mesh._W_CPU, mesh._W_MEMORY) == (
            weights["latency"],
            weights["queue"],
            weights["cpu"],
            weights["memory"],
        )
        assert abs(sum(weights.values()) - 1.0) < 1e-9

    def test_heartbeat_interval(self, monkeypatch):
        from volnux.manager.mesh.client import MeshClient

        assert MeshClient().heartbeat_interval == 5.0
        monkeypatch.setenv("VOLNUX_MESH_HEARTBEAT_INTERVAL", "11")
        assert MeshClient().heartbeat_interval == 11.0
        assert MeshClient(heartbeat_interval=1.5).heartbeat_interval == 1.5

    @pytest.mark.asyncio
    async def test_listen_address_comes_from_settings(self, monkeypatch):
        from volnux.manager.mesh.client import MeshClient

        client = MeshClient()
        started = []

        async def fake_start_manager(host, port):
            started.append((host, port))

        monkeypatch.setattr(client, "_start_manager", fake_start_manager)
        monkeypatch.setattr(client, "_watcher_loop", Mock(), raising=False)
        monkeypatch.setenv("VOLNUX_MESH_PORT", "6000")
        try:
            await client.start()
        except Exception:
            pass
        assert started and started[0] == ("0.0.0.0", 6000)


class TestAgents:
    def test_class_defaults_are_the_previous_ones(self):
        from volnux.event.agent.base import AgentEventBase as A

        assert (A.temperature, A.max_tokens) == (0.0, 4096)
        assert (A.max_reasoning_steps, A.tool_timeout_seconds) == (20, 30.0)
        assert (A.llm_max_retries, A.llm_retry_delay_s) == (3, 1.0)
        assert (A.max_total_tokens, A.max_context_messages) == (0, 50)
        assert A.max_protocol_violations == 3

    def test_they_equal_the_settings(self):
        from volnux.event.agent.base import AgentEventBase as A
        from volnux.event.agent.providers.http import HTTPProviderAdapter

        assert A.temperature == settings.AGENT_TEMPERATURE
        assert A.max_tokens == settings.AGENT_MAX_TOKENS
        assert A.max_reasoning_steps == settings.AGENT_MAX_REASONING_STEPS
        assert A.tool_timeout_seconds == settings.AGENT_TOOL_TIMEOUT_SECONDS
        assert A.llm_max_retries == settings.AGENT_LLM_MAX_RETRIES
        assert A.llm_retry_delay_s == settings.AGENT_LLM_RETRY_DELAY_SECONDS
        assert A.max_total_tokens == settings.AGENT_MAX_TOTAL_TOKENS
        assert A.max_context_messages == settings.AGENT_MAX_CONTEXT_MESSAGES
        assert A.max_protocol_violations == settings.AGENT_MAX_PROTOCOL_VIOLATIONS
        assert (
            HTTPProviderAdapter.default_timeout == settings.AGENT_HTTP_TIMEOUT_SECONDS
        )


class TestGovernance:
    def test_sampler_capacity(self, monkeypatch):
        from volnux.governance.sampling import EventSampler

        assert EventSampler()._capacity == 32
        monkeypatch.setenv("VOLNUX_GOVERNANCE_SAMPLING_CAPACITY", "8")
        assert EventSampler()._capacity == 8
        assert EventSampler(capacity=3)._capacity == 3
        with pytest.raises(ValueError):
            EventSampler(capacity=0)


class TestObservability:
    def test_sample_rate(self, monkeypatch):
        from volnux.otel.tracer_setup import VolnuxTracerConfig

        assert VolnuxTracerConfig().sample_rate == 1.0
        monkeypatch.setenv("VOLNUX_OTEL_SAMPLE_RATE", "0.25")
        assert VolnuxTracerConfig().sample_rate == 0.25
        assert VolnuxTracerConfig(sample_rate=0.5).sample_rate == 0.5
        monkeypatch.setenv("VOLNUX_OTEL_SAMPLE_RATE", "3")
        with pytest.raises(ValueError):
            VolnuxTracerConfig()

    def test_telemetry_is_off_by_default(self):
        assert settings.OTEL_ENABLED is False


class TestStorage:
    def test_trigger_state_backend_default_is_sqlite(self):
        from volnux.engine.workflows.trigger.state import TriggerStateRecord

        assert TriggerStateRecord.get_backend_config() == {
            "ENGINE": "volnux.backends.stores.sqlite.SqliteStoreBackend",
            "CONNECTOR_CONFIG": {"database": "volnux.db"},
        }

    def test_trigger_state_backend_can_be_replaced(self, monkeypatch):
        from volnux.engine.workflows.trigger.state import TriggerStateRecord

        monkeypatch.setenv(
            "VOLNUX_DATABASES",
            '{"default": {"ENGINE": "a.Default"},'
            ' "triggers": {"ENGINE": "x.Backend", "NAME": "/data/v.db"}}',
        )
        assert TriggerStateRecord.get_backend_config() == {
            "ENGINE": "x.Backend",
            "CONNECTOR_CONFIG": {"database": "/data/v.db"},
        }

    def test_saga_dlq_uses_the_key_value_store_by_default(self):
        # it used to be a Redis at localhost whatever the configuration said
        from volnux.backends.saga._dlq import DeadLetterEntry

        config = DeadLetterEntry.get_backend_config()
        assert config["ENGINE"] == settings.DATABASES["default"]["ENGINE"]

    def test_saga_dlq_can_have_its_own_backend(self, monkeypatch):
        from volnux.backends.saga._dlq import DeadLetterEntry

        monkeypatch.setenv(
            "VOLNUX_DATABASES",
            '{"default": {"ENGINE": "a.Default"},'
            ' "saga": {"ENGINE": "volnux.backends.stores.redis.RedisStoreBackend"}}',
        )
        assert DeadLetterEntry.get_backend_config()["ENGINE"].endswith(
            "RedisStoreBackend"
        )

    def test_workflow_cache_dir(self, monkeypatch, tmp_path):
        from volnux.engine.workflows.registry import _configured_cache_dir

        assert _configured_cache_dir() == settings.WORKFLOWS_CACHE_DIR
        monkeypatch.setenv("VOLNUX_WORKFLOWS_CACHE_DIR", str(tmp_path / "c"))
        assert _configured_cache_dir() == tmp_path / "c"

    def test_migration_dir(self, monkeypatch, tmp_path):
        from volnux.mixins.key_value_store_integration import (
            KeyValueStoreIntegrationMixin,
        )

        class Thing(KeyValueStoreIntegrationMixin):
            pass

        default = Path(Thing.get_migration_dir())
        assert default == settings.MIGRATION_DIR / "Thing"
        monkeypatch.setenv("VOLNUX_MIGRATION_DIR", str(tmp_path))
        assert Path(Thing.get_migration_dir()) == tmp_path / "Thing"


class TestCommandLine:
    def serve_defaults(self):
        from volnux.cli.command.builtins.serve import ServeCommand

        parser = argparse.ArgumentParser()
        ServeCommand().add_arguments(parser)
        return vars(parser.parse_args([]))

    def test_serve_defaults_are_the_previous_ones(self):
        d = self.serve_defaults()
        assert d["host"] == "0.0.0.0" and d["port"] == 8080
        assert d["worker_port"] == 9090 and d["metrics_port"] == 9464
        assert d["log_level"] == "INFO" and d["drain_timeout"] == 120.0

    def test_serve_defaults_equal_the_settings(self):
        d = self.serve_defaults()
        assert d["host"] == settings.API_HOST and d["port"] == settings.API_PORT
        assert d["worker_port"] == settings.WORKER_PORT
        assert d["metrics_port"] == settings.PROMETHEUS_PORT
        assert d["log_level"] == settings.LOG_LEVEL
        assert d["drain_timeout"] == settings.CHECKPOINT_DRAIN_TIMEOUT

    def test_serve_environment_overrides(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_API_PORT", "9000")
        monkeypatch.setenv("VOLNUX_API_HOST", "127.0.0.1")
        monkeypatch.setenv("VOLNUX_LOG_LEVEL", "debug")
        monkeypatch.setenv("VOLNUX_CHECKPOINT_DRAIN_TIMEOUT", "45")
        d = self.serve_defaults()
        assert (d["port"], d["host"], d["log_level"], d["drain_timeout"]) == (
            9000,
            "127.0.0.1",
            "DEBUG",
            45.0,
        )

    def test_client_host_and_credentials(self, monkeypatch, tmp_path):
        from volnux.cli.command.mixins.runtime import APIClientMixin

        class Client(APIClientMixin):
            pass

        parser = argparse.ArgumentParser()
        Client().add_arguments(parser)
        assert parser.parse_args([]).host == "localhost:8080"
        monkeypatch.setenv("VOLNUX_HOST", "volnux.example.com")
        parser = argparse.ArgumentParser()
        Client().add_arguments(parser)
        assert parser.parse_args([]).host == "volnux.example.com"

        assert Client().CREDENTIALS_FILE == settings.CREDENTIALS_FILE
        monkeypatch.setenv("VOLNUX_CREDENTIALS_FILE", str(tmp_path / "cred"))
        assert Client().CREDENTIALS_FILE == tmp_path / "cred"

    def test_pid_file_default_follows_the_setting(self, monkeypatch):
        from volnux.cli.command.builtins.triggers.subcommands.engine.start import (
            StartTriggerEngineSubCommand,
        )
        from volnux.cli.command.builtins.triggers.subcommands.engine.stop import (
            StopTriggerEngineSubCommand,
        )

        for command in (StartTriggerEngineSubCommand, StopTriggerEngineSubCommand):
            parser = argparse.ArgumentParser()
            command.__new__(command).add_arguments(parser)
            assert parser.parse_args([]).pid_file == ".volnux_triggers.pid"

        monkeypatch.setenv("VOLNUX_TRIGGERS_PID_FILE", "/run/volnux.pid")
        parser = argparse.ArgumentParser()
        StartTriggerEngineSubCommand.__new__(
            StartTriggerEngineSubCommand
        ).add_arguments(parser)
        assert parser.parse_args([]).pid_file == "/run/volnux.pid"


class TestApi:
    def test_cors_origins(self, monkeypatch):
        from volnux.app.app import _cors_origins

        assert _cors_origins() == ["*"]
        monkeypatch.setenv("VOLNUX_CORS_ALLOWED_ORIGINS", '["https://a.example"]')
        assert _cors_origins() == ["https://a.example"]
        monkeypatch.setenv(
            "VOLNUX_CORS_ALLOWED_ORIGINS", "https://a.example, https://b.example"
        )
        assert _cors_origins() == ["https://a.example", "https://b.example"]

    def test_the_middleware_uses_the_setting(self):
        from starlette.middleware.cors import CORSMiddleware

        from volnux.app.app import _app, _cors_origins

        (cors,) = [m for m in _app.user_middleware if m.cls is CORSMiddleware]
        options = getattr(cors, "kwargs", None) or getattr(cors, "options")
        assert (
            options["allow_origins"] == _cors_origins() == settings.CORS_ALLOWED_ORIGINS
        )

    def test_the_middleware_follows_the_environment(self):
        # (the app is built when the module is imported, so ask a fresh process)
        import json
        import os
        import subprocess

        code = (
            "import json;"
            "from starlette.middleware.cors import CORSMiddleware;"
            "from volnux.app.app import _app;"
            "m=[m for m in _app.user_middleware if m.cls is CORSMiddleware][0];"
            "o=getattr(m,'kwargs',None) or getattr(m,'options');"
            "print(json.dumps([o['allow_origins'], _app.docs_url]))"
        )
        env = {
            **os.environ,
            "VOLNUX_CORS_ALLOWED_ORIGINS": "https://a.example,https://b.example",
            "VOLNUX_API_DOCS": "true",
        }
        out = subprocess.run(
            [sys.executable, "-c", code], env=env, capture_output=True, text=True
        )
        assert out.returncode == 0, out.stderr
        origins, docs_url = json.loads(out.stdout.strip().splitlines()[-1])
        assert origins == ["https://a.example", "https://b.example"]
        assert docs_url == "/docs"

    def test_the_documentation_pages_are_off_by_default(self):
        from volnux.app.app import _app

        assert _app.docs_url is None and _app.redoc_url is None


class TestVersioning:
    def test_the_namespace_setting(self):
        assert settings.DEFAULT_EVENT_NAMESPACE == "local"

    def test_events_are_semantically_versioned(self):
        from volnux.event.base import EventBase
        from volnux.versioning import SemanticVersioning

        class Plain(EventBase):
            def process(self, *a, **k): ...

        handler = Plain.get_version_handler()
        assert type(handler.scheme) is SemanticVersioning
        assert (handler.namespace, handler.version) == ("local", "1.0.0")

    def test_the_namespace_follows_the_environment(self, monkeypatch):
        from volnux.event.base import EventBase

        monkeypatch.setenv("VOLNUX_DEFAULT_EVENT_NAMESPACE", "trading")

        class Plain2(EventBase):
            def process(self, *a, **k): ...

        assert Plain2.get_version_handler().namespace == "trading"
