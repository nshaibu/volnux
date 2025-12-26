import datetime
import inspect
import logging
import os
import sys
import types
import typing
import uuid
import queue
import threading
import queue
import threading
import json
import time
import asyncio
from volnux.concurrency.async_utils import to_thread
from abc import ABC, abstractmethod
try:
    from enum import StrEnum
except ImportError:
    from enum import Enum
    class StrEnum(str, Enum):
        pass

from importlib import import_module
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

from pydantic_mini import BaseModel
from volnux.conf import ConfigLoader
from volnux.parser.options import Options
from volnux import EventBase
from volnux.executors.message import TaskMessage, deserialize_message
from volnux.exceptions import RemoteExecutionError
from volnux.manager.registry import ClientTaskRegistry
from volnux.base import get_event_registry
from volnux.manager.result_store import get_result_store
from volnux.utils import create_error_response
from volnux.constants import ErrorCodes


logger = logging.getLogger(__name__)

CONF = ConfigLoader.get_lazily_loaded_config()

PROJECT_ROOT = CONF.PROJECT_ROOT_DIR

_client_task_registry = ClientTaskRegistry()


def get_client_task_registry():
    """Singleton for client task registry"""
    return _client_task_registry


class Protocol(StrEnum):
    GRPC = "grpc"
    TCP = "tcp"


class TaskNode(BaseModel):
    id: uuid.UUID
    event: typing.Any
    correlation_id: uuid.UUID
    timeout: datetime.timedelta
    created_at: datetime.datetime


class RemoteExecutionContext:
    pass

class RemoteTaskOptions:
    def __init__(self, extras):
        self.extras = extras
        self.retry_attempts = 0
        self.executor = None


def execute_task_wrapper(event_instance: EventBase, correlation_id: str) -> typing.Dict[str, typing.Any]:
    """
    Top-level wrapper for executing tasks in a subprocess.
    """
    # TODO: Sandbox execution in Kubernetes, with predefined events from github.
    try:
        event_result = event_instance()

        return {
            "status": "error" if event_result.error else "success",
            "result": event_result.content,
            "correlation_id": correlation_id,
            "completed_at": str(datetime.datetime.now())
        }
    except Exception as e:
        logger.error(f"Execution failed for task {correlation_id}: {e}")
        return create_error_response(
            code=ErrorCodes.PROCESSING_ERROR,
            message=str(e),
            correlation_id=correlation_id
        )


class BaseManager(ABC):
    """
    Abstract base class for event managers.
    Manages the execution pool and response routing.
    """

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self.executor = ProcessPoolExecutor(max_workers=getattr(CONF, "WORKER_COUNT", 8))
        self.response_queue = queue.Queue()
        self.task_queue = queue.Queue(maxsize=getattr(CONF, "MAX_PENDING_TASKS", 100))
        self._shutdown_event = threading.Event()
        self._router_thread = None
        self._router_thread = None
        self._submission_thread = None
        self._cleanup_thread = None

    @abstractmethod
    async def _route_tcp_response(self, task_info: typing.Dict, result_data: typing.Dict):
        """Route response via TCP"""
        pass

    @abstractmethod
    async def _route_grpc_response(self, task_info: typing.Dict, result_data: typing.Dict):
        """Route response via gRPC"""
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self._host}, port={self._port})"

    def __str__(self):
        return self.__repr__()

    def start(self, *args, **kwargs):
        """Start the task manager services"""
        self._manager_loop_thread = threading.Thread(target=self._run_async_loop, daemon=True)
        self._manager_loop_thread.start()

        logger.info("BaseManager started: Async Event Loop running.")

    def _run_async_loop(self):
        """Entry point for the manager's async loop"""
        asyncio.run(self._main_async_loop())

    async def _main_async_loop(self):
        """Main async orchestrator"""
        # Create tasks
        submission_task = asyncio.create_task(self._task_submission_loop_async())
        router_task = asyncio.create_task(self._response_router_loop_async())
        cleanup_task = asyncio.create_task(self._registry_cleanup_loop_async())

        # Allow subclasses to register their own tasks (e.g. servers)
        extra_tasks = self._get_extra_async_tasks()
        active_tasks = [submission_task, router_task, cleanup_task] + extra_tasks

        # Wait until shutdown
        while not self._shutdown_event.is_set():
            await asyncio.sleep(0.5)

        # Cancel tasks
        for t in active_tasks:
            t.cancel()

        try:
             await asyncio.gather(*active_tasks, return_exceptions=True)
        except Exception:
             pass

    def _get_extra_async_tasks(self) -> typing.List[asyncio.Task]:
        """Hook for subclasses to add async tasks to the main loop"""
        return []

    def shutdown(self):
        """Shutdown the task manager server"""
        logger.info("Shutting down BaseManager...")
        self._shutdown_event.set()

        # Wait for async loop to finish first (cancelling its tasks)
        if hasattr(self, '_manager_loop_thread'):
            self._manager_loop_thread.join(timeout=5)

        # Then shutdown executor
        if self.executor:
            self.executor.shutdown(wait=True)

        logger.info("BaseManager shutdown complete.")

    def handle_task(self, task_message: TaskMessage, protocol: str, client_context: typing.Any = None):
        """
        Handle an incoming task message.
        1. Validate
        2. resolve Event
        3. Register
        4. Submit to Pool

        Args:
            task_message (TaskMessage): The task message to handle.
            protocol (str): The protocol used to send the task message.
            client_context (typing.Any, optional): Protocol-specific client context (socket, context, etc).
        """

        allowed = getattr(CONF, "ALLOWED_EVENTS", [])
        if allowed and task_message.event not in allowed:
            raise RemoteExecutionError("EVENT_NOT_WHITELISTED")

        event_registry = get_event_registry()
        event_class = event_registry.get_by_name(task_message.event)

        if not event_class:
            raise RemoteExecutionError("EVENT_NOT_REGISTERED")

        if task_message.correlation_id:
            correlation_id = str(task_message.correlation_id)
        else:
            correlation_id = str(uuid.uuid4())

        try:
            opts = RemoteTaskOptions(task_message.args)

            event_instance = event_class(
                execution_context=RemoteExecutionContext(),
                task_id=correlation_id,
                options=opts
            )
        except Exception as e:
            raise RemoteExecutionError(f"INVALID_ARGS: {e}")

        client_task_registry = get_client_task_registry()
        client_task_object = {
            "correlation_id": correlation_id,
            "status": "pending",
            "created_at": str(datetime.datetime.now()),
            "start_time": time.perf_counter(),
            "client_id": task_message.args.get("client_id", "unknown"),
            "event_name": task_message.event,
            "protocol": protocol,
            "client_context": client_context
        }
        client_task_registry.register(client_task_object)

        task_node = TaskNode(
            id=uuid.uuid4(),
            event=event_instance,
            correlation_id=uuid.UUID(correlation_id),
            timeout=datetime.timedelta(seconds=getattr(CONF, "TASK_TIMEOUT", 300)),
            created_at=datetime.datetime.now()
        )

        try:
            self.task_queue.put_nowait(task_node)
            logger.info(f"Task enqueued: {correlation_id} [{task_message.event}]")
        except queue.Full:
            raise RemoteExecutionError("QUEUE_FULL")
        except Exception as e:
            raise RemoteExecutionError(f"QUEUE_ERROR: {e}")

    async def _task_submission_loop_async(self):
        """
        Polls task_queue and submits tasks to the executor (Async).
        """
        while not self._shutdown_event.is_set():
            try:
                def get_item():
                    return self.task_queue.get(timeout=0.5)

                try:
                    task_node = await to_thread(get_item)
                except queue.Empty:
                    continue

                # Execute
                # executor.submit is non-blocking (returns future immediately)
                future = self.executor.submit(execute_task_wrapper, task_node.event, str(task_node.correlation_id))
                future.add_done_callback(self._on_task_complete)

                self.task_queue.task_done()
                logger.debug(f"Task submitted to executor: {task_node.correlation_id}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in task submission loop: {e}")
                await asyncio.sleep(1) # Backoff

    def _on_task_complete(self, future):
        try:
            result = future.result()
            self.response_queue.put(result)
        except Exception as e:
            logger.error(f"Task future failed: {e}")

    async def _response_router_loop_async(self):
        """
        Polls response_queue and routes results to clients (Async).
        """
        while not self._shutdown_event.is_set():
            try:
                def get_result():
                    return self.response_queue.get(timeout=0.5)

                try:
                    result = await to_thread(get_result)
                except queue.Empty:
                    continue

                # Now processing async routing directly in the loop
                await self._route_response(result)

                self.response_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in response router: {e}")
                await asyncio.sleep(1)

    async def _registry_cleanup_loop_async(self):
        """
        Periodically cleans up expired tasks from the registry (Async).
        """
        registry = get_client_task_registry()
        cleanup_interval = 60  # Run every minute
        ttl = getattr(CONF, "TASK_REGISTRY_TTL", 600) # 10 minutes

        while not self._shutdown_event.is_set():
            try:
                # Use asyncio sleep instead of Event.wait
                try:
                    await asyncio.wait_for(to_thread(self._shutdown_event.wait, timeout=0.1), timeout=cleanup_interval)
                    if self._shutdown_event.is_set():
                        break
                except asyncio.TimeoutError:
                    pass

                await to_thread(registry.cleanup_expired, ttl_seconds=ttl)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in registry cleanup loop: {e}")
                await asyncio.sleep(5)

    async def _route_response(self, result_data: typing.Dict[str, typing.Any]):
        correlation_id = result_data.get("correlation_id")
        if not correlation_id:
            logger.error("Result missing correlation_id")
            return

        registry = get_client_task_registry()
        task_info = registry.get_task(correlation_id)

        if not task_info:
            logger.debug(f"Task info not found for {correlation_id}. Storing in ResultStore.")
            get_result_store().store(correlation_id, result_data)
            return

        try:
            start_time = task_info.get("start_time")
            duration_ms = (time.perf_counter() - start_time) * 1000 if start_time else 0

            log_payload = {
                "level": "info",
                "event": "task_completed",
                "task_id": str(correlation_id),
                "correlation_id": str(correlation_id),
                "event_name": task_info.get("event_name"),
                "duration_ms": round(duration_ms, 2),
                "status": result_data.get("status"),
                "protocol": task_info.get("protocol"),
                "client_id": task_info.get("client_id")
            }
            logger.info(json.dumps(log_payload))
        except Exception as e:
            logger.warning(f"Failed to log structured event: {e}")

        protocol = task_info.get("protocol")

        try:
            if protocol == Protocol.TCP:
                await self._route_tcp_response(task_info, result_data)
            elif protocol == Protocol.GRPC:
                await self._route_grpc_response(task_info, result_data)
            else:
                logger.warning(f"Unknown protocol {protocol} for task {correlation_id}")
        except Exception as e:
            logger.error(f"Routing failed for {correlation_id}: {e}. Storing in ResultStore.")
            get_result_store().store(correlation_id, result_data)
        finally:
            registry.remove(correlation_id)

    @classmethod
    def auto_load_all_task_modules(cls):
        """
        Auto-discover and load all modules containing event classes that inherit from EventBase.
        Searches through the project directory for Python modules and registers event classes.
        """
        project_root = PROJECT_ROOT
        logger.info(f"Searching for event modules in: {project_root}")

        discovered_events: typing.Set[typing.Type] = set()
        discovered_modules: typing.Set[types.ModuleType] = set()

        def is_event_class(obj: typing.Type) -> bool:
            """Check if an object is a class inheriting from EventBase"""
            return (
                inspect.isclass(obj)
                and hasattr(obj, "__module__")
                and EventBase in [base for base in inspect.getmro(obj)[1:]]
            )

        def explore_module(module_name: str) -> None:
            """Explore a module for event classes"""
            try:
                module = import_module(module_name)
                for name, obj in inspect.getmembers(module):
                    if is_event_class(obj):
                        discovered_events.add(obj)
                        discovered_modules.add(module)
                        logger.debug(
                            f"Discovered event class: {obj.__module__}.{obj.__name__}"
                        )
            except Exception as e:
                logger.warning(f"Failed to load module {module_name}: {e}")

        # Walk through all Python packages in the project
        for root, dirs, files in os.walk(str(project_root)):
            # Skip __pycache__ and virtual environment directories
            dirs[:] = [d for d in dirs if not d.startswith(("__", "."))]

            if "__init__.py" in files:
                # Calculate the module path relative to project root
                rel_path = Path(root).relative_to(project_root)
                module_path = str(rel_path).replace(os.sep, ".")
                if module_path:
                    explore_module(module_path)

        # Register discovered event modules
        for event_class in discovered_events:
            module = sys.modules.get(event_class.__module__)
            if module:
                cls.register_task_module(event_class.__module__, module)
                logger.info(f"Registered event module: {event_class.__module__}")

        # Register discovered modules
        for module in discovered_modules:
            module_name = module.__name__
            if module_name not in discovered_events:
                cls.register_task_module(module_name, module)
            logger.info(f"Discovered module: {module_name}")

        logger.info(
            f"Discovered {len(discovered_events)} event classes and {len(discovered_modules)} modules"
        )

    @staticmethod
    def register_task_module(module_name: str, module: types.ModuleType) -> None:
        """
        Register a module containing event classes for task execution.

        Args:
            module_name: The fully qualified module name
            module: The module object to register
        """
        if module_name not in sys.modules:
            sys.modules[module_name] = module
            logger.debug(f"Registered module in sys.modules: {module_name}")

    def __del__(self):
        self.shutdown()

    def __enter__(self) -> "BaseManager":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.shutdown()
