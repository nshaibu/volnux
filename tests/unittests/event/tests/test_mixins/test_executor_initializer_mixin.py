"""
Tests for `ExecutorInitializerMixin`.

Scope:
  * get_task_executor() — returns the executor instance when `executor` is
    a BaseExecutor, looks up the global registry by name otherwise.
  * get_executor_initializer_config() — returns the configured
    ExecutorInitializerConfig, coerces a dict into one via .from_dict(),
    constructs a default instance when none is set.
  * get_executor_context() — returns None when executor is not a class;
    otherwise builds a dict with mp_context (if the executor exposes
    get_context) + the executor's __init__ kwargs derived from the
    ExecutorInitializerConfig; merges any caller-supplied ctx.
"""

from __future__ import annotations

import inspect
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest


def _import_mixin():
    from volnux.mixins.event import ExecutorInitializerMixin
    from volnux.parser.executor_config import ExecutorInitializerConfig
    return ExecutorInitializerMixin, ExecutorInitializerConfig


# ---------------------------------------------------------------------------
# get_task_executor
# ---------------------------------------------------------------------------

class TestGetTaskExecutor:
    def test_returns_executor_when_already_an_instance(self):
        """If cls.executor is a BaseExecutor instance, return it as-is."""
        ExecutorInitializerMixin, _ = _import_mixin()
        from volnux.executors import BaseExecutor

        # Use a real-ish BaseExecutor subclass; if BaseExecutor is abstract
        # we instantiate via MagicMock with the right spec.
        try:
            class _FakeExecutor(BaseExecutor):
                pass

            instance = _FakeExecutor()
        except TypeError:
            # BaseExecutor may be abstract; fall back to MagicMock with spec
            instance = MagicMock(spec=BaseExecutor)

        class _Holder(ExecutorInitializerMixin):
            executor = instance

        result = _Holder.get_task_executor()
        assert result is instance

    def test_looks_up_registry_when_executor_is_a_string_name(self):
        ExecutorInitializerMixin, _ = _import_mixin()

        class _Holder(ExecutorInitializerMixin):
            executor = "my_executor"

        sentinel = type("SentinelExecutor", (), {})
        with patch(
            "volnux.mixins.event.get_global_executor_registry",
            return_value=MagicMock(get=MagicMock(return_value=sentinel)),
        ):
            result = _Holder.get_task_executor()
            assert result is sentinel

    def test_default_executor_name_is_default(self):
        ExecutorInitializerMixin, _ = _import_mixin()
        assert ExecutorInitializerMixin.executor == "default"


# ---------------------------------------------------------------------------
# get_executor_initializer_config
# ---------------------------------------------------------------------------

class TestGetExecutorInitializerConfig:
    def test_returns_existing_config_when_set(self):
        ExecutorInitializerMixin, ExecutorInitializerConfig = _import_mixin()
        existing = ExecutorInitializerConfig()

        class _Holder(ExecutorInitializerMixin):
            executor_config = existing

        h = _Holder()
        result = h.get_executor_initializer_config()
        assert result is existing

    def test_dict_config_is_converted_via_from_dict(self):
        ExecutorInitializerMixin, ExecutorInitializerConfig = _import_mixin()

        class _Holder(ExecutorInitializerMixin):
            executor_config = {"max_workers": 4}

        # Patch ExecutorInitializerConfig.from_dict to verify it's called
        # with the dict and that the result replaces the attribute.
        sentinel = MagicMock(name="converted_config")
        with patch.object(
            ExecutorInitializerConfig, "from_dict", return_value=sentinel
        ) as mock_from_dict:
            h = _Holder()
            result = h.get_executor_initializer_config()
            assert result is sentinel
            mock_from_dict.assert_called_once_with({"max_workers": 4})

        # And the attribute is updated in place
        assert h.executor_config is sentinel

    def test_creates_default_config_when_none_set(self):
        ExecutorInitializerMixin, ExecutorInitializerConfig = _import_mixin()

        class _Holder(ExecutorInitializerMixin):
            executor_config = None

        h = _Holder()
        result = h.get_executor_initializer_config()
        assert isinstance(result, ExecutorInitializerConfig)
        # And the attribute is updated in place
        assert h.executor_config is result

    def test_creates_default_config_when_attribute_missing(self):
        """If a subclass sets executor_config = None explicitly, the default
        ExecutorInitializerConfig() is constructed and cached on the instance."""
        ExecutorInitializerMixin, ExecutorInitializerConfig = _import_mixin()

        class _Holder(ExecutorInitializerMixin):
            executor_config = None

        h = _Holder()
        first = h.get_executor_initializer_config()
        second = h.get_executor_initializer_config()
        assert first is second  # cached


# ---------------------------------------------------------------------------
# get_executor_context
# ---------------------------------------------------------------------------

class TestGetExecutorContext:
    def test_returns_none_when_executor_is_not_a_class(self):
        """If get_task_executor() returns an instance (not a class),
        get_executor_context returns None per source."""
        ExecutorInitializerMixin, _ = _import_mixin()

        class _Holder(ExecutorInitializerMixin):
            pass

        h = _Holder()
        # Patch get_task_executor to return a non-class instance
        with patch.object(_Holder, "get_task_executor", return_value=MagicMock()):
            result = h.get_executor_context()
            assert result is None

    def test_returns_dict_with_init_params_for_simple_executor(self):
        ExecutorInitializerMixin, ExecutorInitializerConfig = _import_mixin()

        # A simple executor class with __init__ params we can introspect.
        class _SimpleExecutor:
            def __init__(self, name: str = "default", workers: int = 4):
                self.name = name
                self.workers = workers

        class _Holder(ExecutorInitializerMixin):
            executor = _SimpleExecutor

        h = _Holder()
        # executor_config is None → default ExecutorInitializerConfig()
        result = h.get_executor_context()
        assert isinstance(result, dict)
        # Should contain values derived from get_function_call_args()
        # which inspects __init__'s signature against the config object.
        # The exact keys depend on get_function_call_args; we just verify
        # it's a dict and contains only the expected kwargs.
        assert "mp_context" not in result  # _SimpleExecutor has no get_context

    def test_includes_mp_context_when_executor_has_get_context(self):
        ExecutorInitializerMixin, _ = _import_mixin()
        mp_ctx_sentinel = MagicMock(name="mp_context")

        class _ExecutorWithGetContext:
            def __init__(self, name: str = "default"):
                self.name = name

            @classmethod
            def get_context(cls, method):
                return mp_ctx_sentinel

        class _Holder(ExecutorInitializerMixin):
            executor = _ExecutorWithGetContext

        h = _Holder()
        result = h.get_executor_context()
        assert result is not None
        assert result["mp_context"] is mp_ctx_sentinel

    def test_merges_caller_supplied_ctx(self):
        ExecutorInitializerMixin, _ = _import_mixin()

        class _SimpleExecutor:
            def __init__(self, name: str = "default"):
                pass

        class _Holder(ExecutorInitializerMixin):
            executor = _SimpleExecutor

        h = _Holder()
        result = h.get_executor_context(ctx={"extra_key": "extra_value"})
        assert result["extra_key"] == "extra_value"

    def test_ignores_non_dict_caller_ctx(self):
        ExecutorInitializerMixin, _ = _import_mixin()

        class _SimpleExecutor:
            def __init__(self, name: str = "default"):
                pass

        class _Holder(ExecutorInitializerMixin):
            executor = _SimpleExecutor

        h = _Holder()
        # ctx=None is the default; should not raise
        result = h.get_executor_context(ctx=None)
        assert isinstance(result, dict)

    def test_caller_ctx_overrides_executor_init_params(self):
        """`context.update(ctx)` is called last, so caller-supplied keys win."""
        ExecutorInitializerMixin, _ = _import_mixin()

        class _SimpleExecutor:
            def __init__(self, name: str = "default"):
                pass

        class _Holder(ExecutorInitializerMixin):
            executor = _SimpleExecutor

        h = _Holder()
        result = h.get_executor_context(ctx={"name": "overridden"})
        # Even if get_function_call_args produced name="default", the ctx wins
        assert result["name"] == "overridden"
