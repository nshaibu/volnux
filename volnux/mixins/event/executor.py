import logging
import inspect
import asyncio
from typing import (
    Dict,
    Any,
    Union,
    Type,
    Optional,
)

from volnux.config import VolnuxConfig
from volnux.executors import BaseExecutor
from volnux.parser.executor_config import ExecutorInitializerConfig
from volnux.utils import get_function_call_args
from volnux.executors.utils.registry import get_global_executor_registry


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()

_GLOBAL_EXECUTOR_REGISTRY = get_global_executor_registry()


class ExecutorInitializerMixin:
    """
    Mixin class providing utility methods for executor initialization and configuration.

    This class is designed to manage executor-related configurations, including retrieving
    the appropriate executor instance, handling custom executor configurations, and
    determining the execution context for events. It supports scenarios involving
    multiprocessing and executor-specific initialization parameters.

    :ivar executor: Identifier for the default executor to be used.
    :type executor: str
    :ivar executor_config: Configuration object or dictionary that holds initialization
        parameters for the executor.
    :type executor_config: ExecutorInitializerConfig, optional
    """

    executor: str = "default"

    executor_config: Optional[ExecutorInitializerConfig] = None

    @classmethod
    def get_task_executor(cls) -> Union[Type[BaseExecutor], BaseExecutor]:
        if isinstance(cls.executor, BaseExecutor):
            return cls.executor
        return _GLOBAL_EXECUTOR_REGISTRY.get(cls.executor)

    def get_executor_initializer_config(self) -> ExecutorInitializerConfig:
        if self.executor_config:
            if isinstance(self.executor_config, dict):
                self.executor_config = ExecutorInitializerConfig.from_dict(
                    self.executor_config
                )
        else:
            self.executor_config = ExecutorInitializerConfig()
        return self.executor_config

    def get_executor_context(
        self, ctx: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieves the execution context for the event's executor.

        This method determines the appropriate execution context (e.g., multiprocessing context)
        based on the executor class used for the event. If the executor is configured to use
        multiprocessing, the context is set to "spawn". Additionally, any parameters required
        for the executor's initialization are fetched and added to the context.

        The resulting context dictionary is used to configure the executor for the event execution.

        :param ctx: Optional dictionary of additional context to be merged with the
            computed execution context.
        :type ctx: dict, optional

        :return: A dictionary containing the execution context for the event's executor,
            including any necessary parameters for initialization and multiprocessing context.
        :rtype: dict
        """
        executor = self.get_task_executor()
        if not inspect.isclass(executor):
            return None
        context = dict()

        if hasattr(executor, "get_context"):
            context["mp_context"] = executor.get_context("spawn")
        params = get_function_call_args(
            executor.__init__, self.get_executor_initializer_config()
        )
        context.update(params)
        if ctx and isinstance(ctx, dict):
            context.update(ctx)
        return context
