import copy
import inspect
import logging
import multiprocessing as mp
import os
import re
import threading
import time
import typing
from collections import ChainMap, OrderedDict
from concurrent.futures import CancelledError, Future, ProcessPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from functools import lru_cache, partial
from inspect import Parameter, Signature

from treelib.tree import Tree

try:
    import graphviz
except ImportError:
    graphviz = None

from .conf import ConfigLoader
from .constants import EMPTY, PIPELINE_FIELDS, PIPELINE_STATE, UNKNOWN
from .exceptions import (
    BadPipelineError,
    EventDoesNotExist,
    EventDone,
    ImproperlyConfigured,
    PipelineConfigurationError,
    PointyNotExecutable,
)
from .execution.state_manager import ExecutionStatus
from .fields import InputDataField
from .import_utils import import_string
from .mixins import ObjectIdentityMixin, ScheduleMixin
from .parser.operator import PipeType
from .parser.protocols import TaskType
from .pipeline_wrapper import PipelineWrapper
from .signal.signals import (
    SoftSignal,
    batch_pipeline_finished,
    batch_pipeline_started,
    pipeline_execution_end,
    pipeline_execution_start,
    pipeline_metrics_updated,
    pipeline_post_init,
    pipeline_pre_init,
    pipeline_shutdown,
    pipeline_stop,
)
from .task import build_pipeline_flow_from_pointy_code
from .typing import BatchProcessType
from .utils import validate_batch_processor

if typing.TYPE_CHECKING:
    from .execution.context import ExecutionContext

logger = logging.getLogger(__name__)

conf = ConfigLoader.get_lazily_loaded_config()


class TreeExtraData:
    def __init__(self, pipe_type: typing.Optional[PipeType]):
        self.pipe_type = pipe_type


class CacheFieldDescriptor(object):
    """
    TODO: Add backend for persisting cache in a redis/memcache store
    """

    def __set_name__(self, owner, name):
        self.name = name

    def __set__(self, instance, value):
        if instance is None:
            return self
        if instance.__dict__.get(self.name) is None:
            instance.__dict__[self.name] = OrderedDict()

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        dt = instance.__dict__.get(self.name)
        if dt is None:
            dt = OrderedDict()
            setattr(instance, self.name, dt)
        return instance.__dict__[self.name]


class PipelineState(object):
    pipeline_cache = CacheFieldDescriptor()

    def __init__(self, pipeline: TaskType):
        self.start = pipeline

    def clear(self, instance: typing.Union["Pipeline", str]):
        """
        Clear cache for specific pipeline instance
        Args:
            instance (typing.Union["Pipeline", str]): Pipeline instance or its cache key
        """
        instance_key = self.get_cache_key(instance)
        cache_fields = self.check_cache_exists(instance)
        if cache_fields:
            for field in cache_fields:
                try:
                    self.__dict__[field].pop(instance_key)
                except (AttributeError, KeyError):
                    pass

    @staticmethod
    def get_cache_key(instance: typing.Union["Pipeline", str]) -> str:
        """
        Get cache key for pipeline instance
        Args:
            instance (typing.Union["Pipeline", str]): Pipeline instance or its cache key
        Returns:
            str: Cache key for the pipeline instance
        """
        return instance.get_cache_key() if not isinstance(instance, str) else instance

    def check_cache_exists(
        self, instance: typing.Union["Pipeline", str]
    ) -> typing.Set[str]:
        """
        Check which cache fields have cache for specific pipeline instance
        Args:
            instance (typing.Union["Pipeline", str]): Pipeline instance or its cache key
        Returns:
            typing.Set[str]: Set of cache field names that have cache for the instance
        """
        keys = set()
        instance_key = self.get_cache_key(instance)
        for field, value in self.__dict__.items():
            if isinstance(value, (dict, OrderedDict)) and instance_key in value:
                keys.add(field)
        return keys

    def cache(self, instance: typing.Union["Pipeline", str]) -> ChainMap:
        """
        Get cache for specific pipeline instance
        Args:
            instance (typing.Union["Pipeline", str]): Pipeline instance or its cache key
        Returns:
            ChainMap: Cache for the pipeline instance
        """
        cache_fields = self.check_cache_exists(instance)
        instance_key = self.get_cache_key(instance)
        return ChainMap(*(self.__dict__[field][instance_key] for field in cache_fields))

    def set_cache(
        self,
        instance: "Pipeline",
        instance_cache_field: str,
        *,
        field_name: typing.Optional[str] = None,
        value: typing.Any = None,
    ) -> None:
        """
        Set cache for specific pipeline instance
        Args:
            instance (typing.Union["Pipeline", str]): Pipeline instance or its cache key
            instance_cache_field (str): Cache field name
            field_name (str, optional): Field name to set in cache
            value (any, optional): Value to set in cache
        """
        if value is None:
            return
        instance_key = self.get_cache_key(instance)
        cache = self.__dict__.get(instance_cache_field)
        if cache and instance_key in cache:
            collection = cache[instance_key]
            if instance_key not in collection:
                self.__dict__[instance_cache_field][instance_key] = OrderedDict()
        else:
            self.__dict__[instance_cache_field] = OrderedDict(
                [(instance_key, OrderedDict())]
            )

        self.__dict__[instance_cache_field][instance_key][field_name] = value

    def set_cache_for_pipeline_field(
        self, instance: "Pipeline", field_name: str, value: typing.Any
    ) -> None:
        """
        Set cache for specific pipeline field
        Args:
            instance (typing.Union["Pipeline", str]): Pipeline instance or its cache key
            field_name (str): Field name to set in cache
            value (any): Value to set in cache
        """
        self.set_cache(
            instance=instance,
            instance_cache_field="pipeline_cache",
            field_name=field_name,
            value=value,
        )


class PipelineMeta(type):
    def __new__(cls, name, bases, namespace, **kwargs):
        parents = [b for b in bases if isinstance(b, PipelineMeta)]
        if not parents:
            return super().__new__(cls, name, bases, namespace)

        from .fields import InputDataField

        pointy_path = pointy_str = None

        input_data_fields = OrderedDict()

        for f_name, field in namespace.items():
            if isinstance(field, InputDataField):
                input_data_fields[f_name] = field

        new_class = super().__new__(cls, name, bases, namespace, **kwargs)
        meta_class = getattr(new_class, "Meta", getattr(new_class, "meta", None))
        if meta_class:
            if inspect.isclass(new_class):
                pointy_path = getattr(meta_class, "file", None)
                pointy_str = getattr(meta_class, "pointy", None)
            elif isinstance(meta_class, dict):
                pointy_path = meta_class.pop("file", None)
                pointy_str = meta_class.pop("pointy", None)
        else:
            pointy_path = "."

        if not pointy_str:
            pointy_file = cls.find_pointy_file(
                pointy_path, class_name=f"{new_class.__name__}"
            )
            if pointy_file is None:
                raise ImproperlyConfigured(
                    f"Meta not configured for Pipeline '{new_class.__name__}'"
                )
            with open(pointy_file, "r") as f:
                pointy_str = f.read()

        try:
            workflow = build_pipeline_flow_from_pointy_code(pointy_str)
        except PointyNotExecutable:
            raise
        except Exception as e:
            if isinstance(e, SyntaxError):
                raise
            raise BadPipelineError(
                f"Pipeline is improperly written. Kindly check and fix it: Reason: {str(e)}",
                exception=e,
            )

        if workflow is None:
            raise BadPipelineError("Failed to build pipeline workflow.")

        setattr(new_class, PIPELINE_FIELDS, input_data_fields)
        setattr(new_class, PIPELINE_STATE, PipelineState(workflow))

        return new_class

    @classmethod
    @lru_cache
    def find_pointy_file(
        cls, pipeline_path: str, class_name: str
    ) -> typing.Union[str, None]:
        """
        Find pointy file in given path or directory walk
        Args:
            pipeline_path (str): Path to pointy file or directory
            class_name (str): Class name to look for
        Returns:
            typing.Union[str, None]: Path to pointy file if found, else None
        """
        if os.path.isfile(pipeline_path):
            return pipeline_path
        elif os.path.isdir(pipeline_path):
            return cls.directory_walk(pipeline_path, f"{class_name}.pty")
        return None

    @classmethod
    def directory_walk(cls, dir_path: str, file_name: str):
        for root, dirs, files in os.walk(dir_path):
            for name in files:
                if re.match(file_name, name, flags=re.IGNORECASE):
                    return os.path.join(root, name)
            for vdir in dirs:
                cls.directory_walk(os.path.join(root, vdir), file_name)
        return None


class Pipeline(ObjectIdentityMixin, ScheduleMixin, metaclass=PipelineMeta):
    """
    Represents a pipeline that defines a sequence of tasks or processes
    to be executed. The class is designed to manage the execution flow
    and state of the pipeline, including initializing components and
    handling arguments passed during instantiation.

    The Pipeline class uses a metaclass (`PipelineMeta`) to provide
    additional functionality or customization at the class level.
    """

    __signature__ = None

    def __init__(self, *args, **kwargs):
        pipeline_pre_init.emit(sender=self.__class__, args=args, kwargs=kwargs)

        self.construct_call_signature()

        if self.__signature__:
            bounded_args = self.__signature__.bind(*args, **kwargs)
            for name, value in bounded_args.arguments.items():
                setattr(self, name, value)

        self.execution_context: typing.Optional[ExecutionContext] = None

        super().__init__()

        pipeline_post_init.emit(sender=self.__class__, pipeline=self)

    @classmethod
    def construct_call_signature(cls):
        if cls.__signature__:
            return cls

        positional_args = []
        keyword_args = []
        for name, instance in cls.get_fields():
            if name:
                param_args = {
                    "name": name,
                    "annotation": (
                        instance.data_type
                        if instance.data_type is not UNKNOWN
                        else typing.Any
                    ),
                    "kind": Parameter.POSITIONAL_OR_KEYWORD,
                }
                if instance.default is not EMPTY:
                    param_args["default"] = instance.default
                elif instance.required is False:
                    param_args["default"] = None

                param = Parameter(**param_args)

                if param.default is Parameter.empty:
                    positional_args.append(param)
                else:
                    keyword_args.append(param)

        cls.__signature__ = Signature((*positional_args, *keyword_args))
        return cls

    def __eq__(self, other):
        if not isinstance(other, Pipeline):
            return False
        return self.id == other.id

    def __setstate__(self, state):
        self.__dict__.update(state)

    def __getstate__(self):
        instance_key = self.get_cache_key()
        state = self.__dict__.copy()
        klass_state = self.__class__.__dict__.copy()
        state["_state"] = copy.copy(klass_state["_state"])
        state["_state"].pipeline_cache = {
            instance_key: state["_state"].pipeline_cache.get(instance_key, {}).copy()
        }
        return state

    def __hash__(self):
        return hash(self.id)

    @classmethod
    def get_pipeline_state(cls) -> PipelineState:
        """
        Get the pipeline state instance
        Returns:
            PipelineState: The pipeline state instance
        """
        return getattr(cls, PIPELINE_STATE)

    async def start(
        self, force_rerun: bool = False
    ) -> typing.Optional["ExecutionContext"]:
        """
        Initiates the execution of the pipeline.

        Args:
            force_rerun (bool): If True, allows the pipeline to be executed
                again even if it has already completed. Defaults to False.

        Return:
            ExecutionContext: The execution context of the pipeline. It is a linked list with the ability to
            traverse the various execution contexts for each of the events within the pipeline.
            The context can be filtered based on the event name using the method `filter_by_event`.
            It is also iterable, and thus you can loop over it
            `for context in pipeline.start(): pass`

        Raises:
            EventDone: If the pipeline has already been executed and
                force_rerun is not set to True, this exception is raised
                to indicate that the execution is complete.
        """
        from .engine import run_workflow

        pipeline_execution_start.emit(sender=self.__class__, pipeline=self)

        if self.execution_context and not force_rerun:
            raise EventDone("Done executing pipeline")

        self.execution_context: typing.Optional["ExecutionContext"] = None

        await run_workflow(
            self.get_pipeline_state().start,
            pipeline=self,
        )

        if self.execution_context:
            latest_context = self.execution_context.get_latest_context()
            execution_state = latest_context.state

            if execution_state.status == ExecutionStatus.CANCELLED:
                await pipeline_stop.emit_async(
                    sender=self.__class__,
                    pipeline=self,
                    execution_context=latest_context,
                )
                return self.execution_context
            elif execution_state.status == ExecutionStatus.ABORTED:
                await pipeline_shutdown.emit_async(
                    sender=self.__class__,
                    pipeline=self,
                    execution_context=latest_context,
                )
                return self.execution_context

        pipeline_execution_end.emit(
            sender=self.__class__, execution_context=self.execution_context
        )
        return self.execution_context

    def shutdown(self):
        if self.execution_context:
            latest_context = self.execution_context.get_latest_context()
            latest_context.abort()
            pipeline_shutdown.emit(
                sender=self.__class__,
                pipeline=self,
                execution_context=self.execution_context,
            )

    def stop(self):
        if self.execution_context:
            latest_context = self.execution_context.get_latest_context()
            latest_context.cancel()
            pipeline_stop.emit(
                sender=self.__class__,
                pipeline=self,
                execution_context=self.execution_context,
            )

    def get_cache_key(self) -> str:
        return f"pipeline_{self.__class__.__name__}_{self.id}"

    @classmethod
    def get_fields(cls):
        """
        Yields the fields of the class as key-value pairs.

        This method retrieves the fields defined in the class (stored under
        the `PIPELINE_FIELDS` attribute) and yields each field's name along
        with its associated class type. It is useful for inspecting or
        iterating over the fields of a class dynamically.

        Yields:
            tuple: A tuple containing the field's name and its associated
            class type.

        Notes:
            The method assumes that the class has an attribute `PIPELINE_FIELDS`
            that contains a dictionary mapping field names to class types.
        """
        for name, klass in getattr(cls, PIPELINE_FIELDS, {}).items():
            yield name, klass

    @classmethod
    def get_non_batch_fields(cls):
        for name, field in cls.get_fields():
            if not field.has_batch_operation:
                yield name, field

    def get_pipeline_tree(self) -> typing.Optional[Tree]:
        """
        Constructs and returns the pipeline's execution tree.

        This method retrieves the current state of the pipeline and
        builds a tree representation of its structure using breadth-first
        traversal.

        Returns:
            GraphTree: A tree structure representing the nodes and
            connections in the pipeline.

        Notes:
            The method assumes that the pipeline is in a valid state
            and will perform a breadth-first traversal starting from
            the initial task state.
        """
        state = self.get_pipeline_state().start
        if state:
            tree = Tree()
            for node in state.bf_traversal(state):
                tag = ""
                if node.is_conditional:
                    tag = " (?)"

                if node.is_descriptor_task:
                    tag = " (No)" if node.descriptor == 0 else " (Yes)"

                if node.is_sink:
                    tag = " (Sink)"

                tree.create_node(
                    tag=f"{node.get_event_name()}{tag}",
                    identifier=node.get_id(),
                    parent=node.parent_node.get_id() if node.parent_node else None,
                    data=TreeExtraData(pipe_type=node.get_pointer_to_task()),
                )
            return tree

    def draw_ascii_graph(self):
        """
        Generates and displays an ASCII representation of the pipeline's
        execution graph.

        This method retrieves the current pipeline tree and converts it
        into an ASCII format for visualization. It provides a simple
        way to inspect the structure and flow of tasks within the pipeline.

        Notes:
            This method relies on the `get_pipeline_tree` method to
            obtain the graph data.
        """
        tree = self.get_pipeline_tree()
        if tree:
            print(tree.show(line_type="ascii-emv", stdout=False))

    def draw_graphviz_image(self, directory="pipeline-graphs"):
        """
        Generates a visual representation of the pipeline's execution
        graph using Graphviz and saves it as an image.

        This method constructs the pipeline tree and then uses Graphviz
        to render it as a graphical image. The resulting image is saved
        to the specified directory (default: "pipeline-graphs").

        Args:
            directory (str): The directory where the generated image will
            be saved. Defaults to "pipeline-graphs".

        Notes:
            If the Graphviz library is not available, the method will
            return without performing any operations.
        """
        from volnux.translator.dot import generate_dot_from_task_state

        if graphviz is None:
            logger.warning("Graphviz library is not available")
            return

        data = generate_dot_from_task_state(self.get_pipeline_state().start)
        if data:
            src = graphviz.Source(data, directory=directory)
            src.render(format="png", outfile=f"{self.__class__.__name__}.png")

    @classmethod
    def load_class_by_id(cls, pk: str):
        """
        Loads a class instance based on its unique identifier (ID).

        This method checks if the requested instance exists in the cache.
        If found, it retrieves the object from the cache.

        Args:
            pk (str): The unique identifier of the class
            instance to be loaded.

        Returns:
            The class instance loaded by the provided ID, or None if
            the instance cannot be found or loaded.

        Notes:
            The method uses the class's internal state to check for
            existing cache entries before attempting to load the instance
            from other sources.
        """
        cache_keys = cls.get_pipeline_state().check_cache_exists(pk)
        if not cache_keys:
            return cls()
        cache = cls.get_pipeline_state().cache(pk)

        # restore fields
        kwargs = {}
        for key, value in cache.get("pipeline_cache", {}).items():
            kwargs[key] = value

        instance = cls(**kwargs)
        setattr(instance, "_id", pk)

        # then restore states as well
        # i.e. current and next from pipeline_cache

        return instance

    def get_task_by_id(self, pk: str):
        """
        Retrieves a task from the pipeline by its unique identifier.

        This method searches for a task in the pipeline using the provided unique identifier.
        If the task is not found, it raises an `EventDoesNotExist` exception to signal that the requested
        task does not exist in the queue.

        Args:
            pk (str): The unique identifier (primary key) of the task to retrieve.

        Returns:
            Task: The task object associated with the given primary key if found.

        Raises:
            EventDoesNotExist: If no task with the given primary key exists in the pipeline.
        """
        state: TaskType = self.get_pipeline_state().start
        if state:
            for task in state.bf_traversal(state):
                if task.get_id() == pk:
                    return task
        raise EventDoesNotExist(f"Task '{pk}' does not exists", code=pk)

    def get_first_error_execution_node(self):
        current = self.execution_context
        while current:
            if current.execution_failed():
                break
            current = current.next_context
        return current


class _BatchResult(typing.NamedTuple):
    pipeline: Pipeline
    exception: Exception


class BatchPipelineStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    FINISHED = "finished"


@dataclass
class PipelineExecutionMetrics:
    """Tracks execution metrics for batch pipeline monitoring"""

    total_pipelines: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0
    active: int = 0
    start_time: typing.Optional[float] = None
    end_time: typing.Optional[float] = None
    execution_durations: typing.List[float] = field(default_factory=list)
    errors: typing.List[typing.Dict[str, typing.Any]] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return (
            (self.completed / (self.completed + self.failed) * 100)
            if (self.completed + self.failed) > 0
            else 0.0
        )

    @property
    def average_duration(self) -> float:
        return (
            sum(self.execution_durations) / len(self.execution_durations)
            if self.execution_durations
            else 0.0
        )

    @property
    def total_duration(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return time.time() - self.start_time
        return 0.0

    @property
    def completion_rate(self) -> float:
        return (
            ((self.completed + self.failed) / self.total_pipelines * 100)
            if self.total_pipelines > 0
            else 0.0
        )


class _BatchProcessingMonitor(threading.Thread):
    """
    A thread that monitors the progress of pipeline batch processing, managing
    execution through a process pool executor and listening for signals.

    This class is responsible for tracking the status of ongoing batch processing tasks
    by managing soft signals, and performing any necessary processing on the results.

    Attributes:
        batch (BatchPipeline): The batch pipeline being processed.
    """

    def __init__(self, batch_pipeline: "BatchPipeline", enable_metrics: bool = True):
        self.batch = batch_pipeline
        self.metrics = PipelineExecutionMetrics() if enable_metrics else None

        # Track active pipelines and their states
        self.active_pipelines: typing.Set[str] = set()
        self.pipeline_start_times: typing.Dict[str, float] = {}

        self._shutdown_flag = threading.Event()
        self._signals_connected = False
        self._batch_started_emitted = False
        self._setup_signal_listeners()
        super().__init__(name=f"BatchMonitor-{batch_pipeline.id}")

    def _setup_signal_listeners(self):
        """Setup listeners for pipeline signals with proper sender handling"""
        try:
            # Connect to pipeline signals - use GenericSender to catch all pipeline signals
            pipeline_execution_start.connect(
                sender=None, listener=self._on_pipeline_started
            )
            pipeline_execution_end.connect(
                sender=None, listener=self._on_pipeline_ended
            )

            # Connect to batch monitoring signals - these we control, so we can be specific
            pipeline_metrics_updated.connect(
                sender=self.batch.__class__, listener=self._on_metrics_updated
            )
            batch_pipeline_started.connect(
                sender=self.batch.__class__, listener=self._on_batch_started
            )
            batch_pipeline_finished.connect(
                sender=self.batch.__class__, listener=self._on_batch_finished
            )

            self._signals_connected = True
            logger.debug("Signal listeners connected for batch monitoring")
        except Exception as e:
            logger.warning(f"Failed to setup signal listeners: {e}")

    def _cleanup_signal_listeners(self):
        """Cleanup signal connections"""
        if not self._signals_connected:
            return

        try:
            pipeline_execution_start.disconnect(
                sender=self.batch.__class__, listener=self._on_pipeline_started
            )
            pipeline_execution_end.disconnect(
                sender=self.batch.__class__, listener=self._on_pipeline_ended
            )
            batch_pipeline_started.disconnect(
                sender=self.batch.__class__, listener=self._on_batch_started
            )
            batch_pipeline_finished.disconnect(
                sender=self.batch.__class__, listener=self._on_batch_finished
            )
            pipeline_metrics_updated.disconnect(
                sender=self.batch.__class__, listener=self._on_metrics_updated
            )
            self._signals_connected = False
            logger.debug("Signal listeners disconnected")
        except Exception as e:
            logger.debug(f"Error cleaning up signal listeners: {e}")

    def _emit_batch_started(self):
        """Emit batch_pipeline_started signal"""
        if not self._batch_started_emitted and self.metrics:
            # Emit the batch started signal
            batch_pipeline_started.emit(
                sender=self.batch.__class__,
                batch=self.batch,
                total_pipelines=self.metrics.total_pipelines,
                timestamp=time.time(),
            )
            self._batch_started_emitted = True

    def _emit_batch_finished(self):
        """Emit batch_pipeline_finished signal"""
        if self.metrics:
            batch_pipeline_finished.emit(
                sender=self.batch.__class__,
                batch=self.batch,
                metrics=self.metrics,
                success_rate=self.metrics.success_rate,
                total_duration=self.metrics.total_duration,
                timestamp=time.time(),
            )

    def _emit_metrics_updated(self):
        """Emit pipeline_metrics_updated"""
        if self.metrics:
            pipeline_metrics_updated.emit(
                sender=self.batch.__class__,
                batch_id=self.batch.id,
                metrics=self.metrics,
                active_count=self.metrics.active,
                completion_rate=self.metrics.completion_rate,
                timestamp=time.time(),
            )

    def _get_sender_name(self, sender) -> str:
        """Safely get sender name with fallbacks"""
        if sender is None:
            return "Unknown"

        if hasattr(sender, "__name__"):
            return sender.__name__
        elif hasattr(sender, "__class__"):
            return sender.__class__.__name__
        else:
            return str(sender)

    def get_current_metrics(self) -> typing.Optional[PipelineExecutionMetrics]:
        """Get a copy of current execution metrics"""
        return self.metrics

    def _on_batch_started(self, sender, **kwargs):
        """Handle batch started signal"""
        if self.metrics and not self.metrics.start_time:
            self.metrics.start_time = time.time()
            self.metrics.total_pipelines = kwargs.get("total_pipelines", 1)

        sender_name = self._get_sender_name(sender)
        logger.info(
            f"❕❕Batch pipeline started: {sender_name} (Expected: {self.metrics.total_pipelines if self.metrics else 'N/A'} pipelines)",
            extra={
                "batch_id": self.batch.id,
                "sender": sender_name,
                "total_pipelines": self.metrics.total_pipelines if self.metrics else 0,
            },
        )

    def _on_batch_finished(self, sender, **kwargs):
        """Handle batch finished signal"""
        # TODO update the failed count of metrics if pipeline is not executed.
        if self.metrics and not self.metrics.end_time:
            self.metrics.end_time = time.time()

        sender_name = self._get_sender_name(sender)
        if self.metrics:
            logger.info(
                f"❕❕Batch pipeline finished: {sender_name} "
                f"(Success: {self.metrics.completed}/{self.metrics.total_pipelines}, "
                f"Failed: {self.metrics.failed}, Duration: {self.metrics.total_duration:.2f}s)",
                extra={
                    "batch_id": self.batch.id,
                    "sender": sender_name,
                    "success_rate": self.metrics.success_rate,
                    "total_duration": self.metrics.total_duration,
                },
            )

    def _on_pipeline_started(self, sender, **kwargs):
        """Handle pipeline started signal from subprocess"""
        pipeline = kwargs.get("pipeline")
        if pipeline:
            self.active_pipelines.add(pipeline)
            self.pipeline_start_times[pipeline] = kwargs.get("timestamp", time.time())

            if self.metrics:
                self.metrics.started += 1
                self.metrics.active = len(self.active_pipelines)

                # Emit metrics updatep
                self._emit_metrics_updated()

            sender_name = self._get_sender_name(sender)
            logger.info(
                f"❕❕Pipeline started: {pipeline} from {sender_name} (Active: {len(self.active_pipelines)})",
                extra={
                    "pipeline_id": pipeline.id,
                    "sender": sender_name,
                    "active_count": len(self.active_pipelines),
                },
            )

    def _on_metrics_updated(self, sender, **kwargs):
        metrics = kwargs.get("metrics")
        active_count = kwargs.get("active_count")
        completion_rate = kwargs.get("completion_rate")
        timestamp = kwargs.get("timestamp")
        sender = self._get_sender_name(sender)
        logger.info(
            f"❕❕Pipeline Execution Metrics -- \n {metrics}, active count -- {active_count}, "
            f"completion rate -- {completion_rate}, timestamp -- {timestamp}"
        )

    def _on_pipeline_ended(self, sender, **kwargs):
        """Handle pipeline ended signal from subprocess"""
        execution_context = kwargs.get("execution_context")
        if not execution_context:
            logger.warning("❗️Execution context not found after pipeline ended.")

        if pipeline := execution_context.pipeline:
            self.active_pipelines.discard(pipeline)

            # Calculate Duration
            start_time = self.pipeline_start_times.pop(pipeline, None)
            duration = None
            if start_time:
                duration = kwargs.get("timestamp", time.time()) - start_time
                if self.metrics:
                    self.metrics.execution_durations.append(duration)

            if self.metrics:
                success = True

                if execution_context and hasattr(
                    execution_context, "get_latest_execution_context"
                ):
                    try:
                        latest_context = (
                            execution_context.get_latest_execution_context()
                        )
                        success = not latest_context.execution_failed()
                    except Exception as e:
                        success = not kwargs.get("exception")
                        logger.debug(f"exception was raised: {e}")
                else:
                    success = not kwargs.get("exception")

                if success:
                    self.metrics.completed += 1
                else:
                    self.metrics.failed += 1

                self.metrics.active = len(self.active_pipelines)

                # emit metrics update
                self._emit_metrics_updated()

            sender_name = self._get_sender_name(sender)
            status = "completed" if success else "failed"
            duration_str = f"(Duration: {duration:.2f}s)" if duration else ""

            logger.info(
                f"❕❕Pipeline {status}: {pipeline} from {sender_name}{duration_str}",
                extra={
                    "pipeline_id": pipeline.id,
                    "sender": sender_name,
                    "success": success,
                    "duration": duration,
                },
            )

    def _handle_wrapper_lifecycle_event(self, message_data: typing.Dict) -> None:
        """Handle wrapper-specific lifecycle events"""
        event_type = message_data.get("event_type")
        wrapper_id = message_data.get("wrapper_id")
        pipeline_id = message_data.get("pipeline_id")
        execution_state = message_data.get("execution_state")

        logger.info(
            f"❕❕Wrapper lifecycle event: {event_type} - {execution_state}",
            extra={
                "wrapper_id": wrapper_id,
                "pipeline_id": pipeline_id,
                "event_type": event_type,
            },
        )

        if event_type == "pipeline_execution_start":
            pipeline_execution_start.emit(
                sender=self.batch.__class__, pipeline=message_data.get("pipeline")
            )
        elif event_type == "pipeline_execution_end":
            pipeline_execution_end.emit(
                sender=self.batch.__class__,
                execution_context=message_data.get("execution_context"),
            )
        # Handle specific wrapper events if needed
        elif event_type == "wrapper_started":
            logger.info(
                "❕❕Wrapper started event received",
                extra={
                    "wrapper_id": wrapper_id,
                    "pipeline_id": pipeline_id,
                    "event_type": event_type,
                },
            )
        elif event_type == "wrapper_finished":
            execution_duration = message_data.get("execution_duration")
            if execution_duration:
                logger.info(
                    f"❕❕Wrapper {wrapper_id} finished in {execution_duration:.2f}s",
                    extra={"wrapper_id": wrapper_id, "duration": execution_duration},
                )
        elif event_type == "wrapper_failed":
            error_message = message_data.get("error_message", "Unknown wrapper error")
            logger.error(
                f"❕❕Wrapper failed: {error_message}",
                extra={"wrapper_id": wrapper_id, "pipeline_id": pipeline_id},
            )

    @staticmethod
    def construct_signal(signal_data: typing.Dict):
        try:
            data = signal_data.get("kwargs")
            sender = data.pop("sender", None)
            signal: SoftSignal = data.pop("signal", None)

            if sender and signal:
                parent_process_signal = import_string(signal.__instance_import_str__)
                parent_process_signal.emit(sender=sender, **data)
        except Exception:
            logger.exception("❗️Exception raised while processing signal %s", signal)

    def shutdown(self, timeout: float = 5.0) -> None:
        """Gracefully shutdown the monitoring thread"""
        # Emit batch finished signal before shutdown
        self._emit_batch_finished()

        self._cleanup_signal_listeners()
        self._shutdown_flag.set()

    def run(self) -> None:
        try:
            if self.metrics:
                self.metrics.start_time = time.time()
                # Estimate total pipeline
                self.metrics.total_pipelines = (
                    len(self.batch._field_batch_op_map)
                    if self.batch._field_batch_op_map
                    else 1
                )

                self._emit_batch_started()

            while True:
                self.batch.check_memory_usage()
                try:
                    signal_data = self.batch.signals_queue.get(timeout=1.0)
                    if signal_data is None:
                        break
                    # Process different message types properly
                    message_type = signal_data.get("message_type")

                    if message_type == "pipeline_signal":
                        # Handle forwarded pipeline signals from subprocess
                        self.construct_signal(signal_data)
                    elif message_type == "wrapper_lifecycle":
                        # Handle wrapper-specific lifecycle events
                        self._handle_wrapper_lifecycle_event(signal_data)
                    else:
                        # Handle legacy message formats (backward compatibility)
                        # This covers any older message formats that might still be in use
                        self.construct_signal(signal_data)
                except Exception as e:
                    logger.warning(f"❗️Error processing message in monitor: {e}")
        finally:
            # Emit batch finished signal
            # self._emit_batch_finished()
            self.shutdown()
            try:
                self.batch.signals_queue.task_done()
            except Exception as e:
                logger.warning(f"❗️error updating signal queue: {e}")


class BatchPipeline(ObjectIdentityMixin, ScheduleMixin):
    """
    A class representing a batch pipeline, responsible for managing and processing
    data through a series of pipeline stages. The pipeline is constructed from a
    user-defined template that must be a subclass of `Pipeline`.

    Attributes:
        pipeline_template (typing.Type[Pipeline]): The class used to generate the pipeline.
        listen_to_signals (typing.List[str]): A list of signal import strings to be used within the pipeline.
    """

    pipeline_template: typing.Type[Pipeline] = None

    listen_to_signals: typing.List[str] = SoftSignal.registered_signals()

    __signature__ = None

    max_workers: int = None

    memory_limit: int = None

    max_memory_percent: float = 90.00

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.lock = mp.Lock()
        self.results: typing.List[_BatchResult] = []

        self.status: BatchPipelineStatus = BatchPipelineStatus.PENDING

        pipeline_template = self.get_pipeline_template()

        if pipeline_template is None:
            raise ImproperlyConfigured(
                "No pipeline template provided. Please provide a pipeline template"
            )
        elif not issubclass(pipeline_template, Pipeline):
            raise ImproperlyConfigured(
                "Pipeline template must be a subclass of Pipeline"
            )

        template_cls = self.pipeline_template.construct_call_signature()
        self.__signature__ = self.__signature__ or template_cls.__signature__

        bounded_args = self.__signature__.bind(*args, **kwargs)

        for field, value in bounded_args.arguments.items():
            setattr(self, field, value)

        self._field_batch_op_map: typing.Dict[InputDataField, typing.Iterator] = {}
        self._configured_pipelines: typing.Set[Pipeline] = set()
        self._configured_pipelines_count: int = 0

        self._signals_queue = None

        self._monitor_thread: typing.Optional[_BatchProcessingMonitor] = None

    def get_pipeline_template(self):
        return self.pipeline_template

    def get_fields(self):
        yield from self.get_pipeline_template().get_fields()

    @property
    def signals_queue(self):
        return self._signals_queue

    @staticmethod
    def _validate_batch_processor(batch_processor: BatchProcessType):
        is_iterable = validate_batch_processor(batch_processor)
        if not is_iterable:
            raise ImproperlyConfigured(
                "Batch processor error. Batch processor must be iterable and generators"
            )

    @property
    def get_executor_config(self):
        return {
            "max_workers": self.max_workers or conf.MAX_BATCH_PROCESSING_WORKERS,
            "mp_context": mp.get_context("spawn"),
            "memory_limit": self.memory_limit,
            "max_memory_percent": self.max_memory_percent,
        }

    def check_memory_usage(self):
        """Monitor memory usage and adjust batch size if needed"""
        import psutil

        memory_percent = psutil.Process().memory_percent()
        if memory_percent > self.max_memory_percent:
            self._adjust_batch_size()

    def _adjust_batch_size(self):
        """Dynamically adjust batch size based on memory usage"""
        for _field in self._field_batch_op_map:
            if hasattr(_field, "batch_size"):
                _field.batch_size = max(
                    1, int(_field.batch_size * 0.8)
                )  # 20% reduction in batch size
        logger.info("❕batch size has been adjusted")

    def _gather_field_batch_methods(
        self, field: InputDataField, batch_processor: BatchProcessType
    ):
        """
        Gathers and applies all batch processing operations for the specified field.

        This method validates the provided batch processor, then checks if the specified field
        already has a batch operation associated with it. If not, it creates a new batch operation
        using the provided batch processor and the field's value. If a batch operation already exists
        for the field, it applies the batch processor to the existing operation.

        Args:
            field (InputDataField): The input data field for which the batch processing is being applied.
            batch_processor (BATCH_PROCESSOR_TYPE): A callable function or class used to process the field in batches.

        Raises:
            ImproperlyConfigured: If the provided batch processor is invalid or improperly configured.
        """
        self._validate_batch_processor(batch_processor)

        if field not in self._field_batch_op_map:
            self._field_batch_op_map[field] = batch_processor(
                getattr(self, field.name), field.batch_size
            )
        else:
            self._field_batch_op_map[field] = batch_processor(
                self._field_batch_op_map[field], field.batch_size
            )

    def _gather_and_init_field_batch_iterators(self):
        for field_name, field in self.get_fields():
            if field.has_batch_operation:
                self._validate_batch_processor(field.batch_processor)

                self._field_batch_op_map[field] = field.batch_processor(
                    getattr(self, field_name, []), field.batch_size
                )

            method_name = f"{field_name}_batch"
            if hasattr(self, method_name):
                batch_operation = getattr(self, method_name)
                self._gather_field_batch_methods(field, batch_operation)

    @staticmethod
    def _convert_value_to_field_data_type(field: InputDataField, value: typing.Any):
        if value is None:
            # TODO: return type's empty version
            return
        value_dtype = type(value)
        if value_dtype in field.data_type:
            return value
        else:
            return field.data_type[0](value)

    def _execute_field_batch_processors(
        self, batch_processor_map: typing.Dict[InputDataField, typing.Iterator]
    ):
        # Initialize the new batch maps to keep track of unprocessed iterators
        new_batch_maps = {}
        kwargs = {}

        while batch_processor_map:
            all_iterators_consumed = True
            # Iterate over the current batch processor map
            for field, batch_operation in batch_processor_map.items():
                try:
                    value = next(batch_operation)
                    all_iterators_consumed &= False
                except StopIteration:
                    all_iterators_consumed &= True
                    value = None

                # Convert the value to the appropriate field data type
                value = self._convert_value_to_field_data_type(field, value)
                kwargs[field.name] = value
                new_batch_maps[field] = batch_operation

            yield kwargs

            # If not all iterators are consumed, continue processing the remaining iterators
            if not all_iterators_consumed:
                # Update the batch_processor_map with only the remaining iterators
                batch_processor_map = new_batch_maps
            else:
                # If all iterators are consumed, exit the loop
                break

    def _prepare_args_for_non_batch_fields(self):
        args = {}
        template = self.get_pipeline_template()
        for field_name, _ in template.get_non_batch_fields():
            args[field_name] = getattr(self, field_name, None)
        return args

    def execute(self):
        """
        Initializes, configures processing pipelines and executes them.
        """
        template = self.get_pipeline_template()

        if not self._field_batch_op_map:
            self._gather_and_init_field_batch_iterators()

        non_batch_kwargs = self._prepare_args_for_non_batch_fields()

        self.status = BatchPipelineStatus.RUNNING

        if not self._field_batch_op_map:
            self._configured_pipelines_count += 1
            # execute pipeline here
            pipeline = template(**non_batch_kwargs)
            pipeline.start(force_rerun=True)
        else:

            def _process_futures(fut: Future, batch: "BatchPipeline" = self):
                event = exception = None
                try:
                    data = fut.result()
                    event = _BatchResult(*data)
                except TimeoutError:
                    return
                except CancelledError as exc:
                    exception = exc
                except Exception as exc:
                    logger.error(str(exc), exc_info=exc)
                    exception = exc
                    print(f"Error in processing future: {exc}")

                if event is None:
                    event = _BatchResult(None, exception=exception)

                with batch.lock:
                    batch.results.append(event)

            manager = mp.Manager()
            mp_context = mp.get_context("spawn")

            self._signals_queue = manager.Queue()

            self._monitor_thread = _BatchProcessingMonitor(self)

            try:
                # let's start monitoring the execution
                self._monitor_thread.start()

                with ProcessPoolExecutor(
                    max_workers=self.max_workers or conf.MAX_BATCH_PROCESSING_WORKERS,
                    mp_context=mp_context,
                ) as executor:
                    # call memory check method to do resizing when memory percent limit is been reached
                    for kwargs in self._execute_field_batch_processors(
                        self._field_batch_op_map
                    ):
                        if any([value for value in kwargs.values()]):
                            kwargs.update(non_batch_kwargs)
                            pipeline = template(**kwargs)

                            future = executor.submit(
                                self._pipeline_executor,
                                pipeline=pipeline,
                                focus_on_signals=self.listen_to_signals,
                                signals_queue=self._signals_queue,
                            )
                            self._configured_pipelines_count += 1
                            future.add_done_callback(
                                partial(_process_futures, batch=self)
                            )
            except Exception as e:
                logger.error(
                    f"❗️error occurred while initialising and executing pipelines {e}"
                )
            finally:
                self._signals_queue.put(None)
                self._monitor_thread.join()

        if self._configured_pipelines_count == 0:
            raise PipelineConfigurationError(
                message=f"Starting batch execution of pipeline '{self.pipeline_template}' failed. "
                f"No pipeline were configured.",
                code="not_configured_pipeline",
            )

    @staticmethod
    def _pipeline_executor(
        pipeline: Pipeline,
        focus_on_signals: typing.List[str],
        signals_queue: mp.Queue,
    ):
        wrapper = PipelineWrapper(
            pipeline,
            focus_on_signals=focus_on_signals,
            signals_queue=signals_queue,
            import_string_fn=import_string,
            logger=logger,
        )
        return wrapper.run()

    def close(self, timeout=2):
        """Clean up resources"""
        try:
            if hasattr(self, "_monitor_thread") and self._monitor_thread:
                self._monitor_thread.join(timeout=timeout)
        except Exception as e:
            logger.warning(f"Exception during BatchPipeline cleanup: {e}")

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
