import copy
import functools
import inspect
import logging
import os
import re
import typing
from collections import ChainMap, OrderedDict
from inspect import Parameter, Signature

try:
    import graphviz
except ImportError:
    graphviz = None

from treelib.tree import Tree

from volnux.config import VolnuxConfig
from volnux.constants import (
    EMPTY,
    PIPELINE_FIELDS,
    PIPELINE_STATE,
    PIPELINE_AST,
    UNKNOWN,
)
from volnux.exceptions import (
    BadPipelineError,
    EventDoesNotExist,
    EventDone,
    ImproperlyConfigured,
    PointyNotExecutable,
)
from volnux.execution.status import ExecutionStatus
from volnux.fields import InputDataField
from volnux.import_utils import import_string
from volnux.mixins import ObjectIdentityMixin
from volnux.parser.operator import PipeType
from volnux.parser.protocols import TaskType
from volnux.execution.pipeline.wrapper import PipelineWrapper
from volnux.signal.signals import (
    pipeline_execution_end,
    pipeline_execution_start,
    pipeline_metrics_updated,
    pipeline_post_init,
    pipeline_pre_init,
    pipeline_shutdown,
    pipeline_stop,
)
from volnux.task import build_pipeline_flow_from_pointy_code
from volnux.mixins.internal_metadata import InternalMetadataMixin

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)
conf = VolnuxConfig.get_instance()


@functools.lru_cache(maxsize=256)
def _find_pointy_file_cached(
    pipeline_path: typing.Optional[str],
    class_name: str,
) -> typing.Optional[str]:
    """
    Locate a .pty file for a pipeline class.

    Args:
        pipeline_path: A path to a specific file, a directory to search, or
                       None (falls back to current working directory).
        class_name:    The Pipeline class name used to find
                       ``{ClassName}.pty`` in directory searches.

    Returns:
        Absolute path to the .pty file, or None if not found.
    """
    if not pipeline_path:
        pipeline_path = "."

    if os.path.isfile(pipeline_path):
        return pipeline_path

    if os.path.isdir(pipeline_path):
        return _directory_walk(pipeline_path, f"{class_name}.pty")

    return None


def _directory_walk(
    dir_path: str,
    file_name: str,
) -> typing.Optional[str]:
    """
    Recursively search a directory for a file matching ``file_name``.

    Uses ``os.walk`` which handles recursive descent natively. The manual
    recursive call to subdirectories that existed in the original code was
    dead code — os.walk already visits all subdirectories — and was removed.

    Args:
        dir_path:  Root directory to search.
        file_name: Filename pattern (matched case-insensitively via regex).

    Returns:
        Absolute path to the first matching file, or None.
    """
    for root, _dirs, files in os.walk(dir_path):
        for name in files:
            if re.match(file_name, name, flags=re.IGNORECASE):
                return os.path.join(root, name)
    return None


class TreeExtraData:
    def __init__(self, pipe_type: typing.Optional[PipeType]):
        self.pipe_type = pipe_type


class CacheFieldDescriptor(object):
    """
    Descriptor that lazily initialises an OrderedDict on the instance.

    TODO: Add backend for persisting cache in a Redis/Memcache store.
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
    """
    Holds the compiled task graph and a per-instance field value cache.

    The task graph (``start``) is shared across all instances of the same
    Pipeline class — it is immutable after metaclass construction.

    The cache maps instance cache keys to their field values, allowing
    ``load_class_by_id()`` to restore a Pipeline instance from stored state
    without re-running the pipeline.
    """

    pipeline_cache = CacheFieldDescriptor()

    def __init__(self, pipeline: TaskType):
        self.start = pipeline

    def clear(self, instance: typing.Union["Pipeline", str]) -> None:
        """Clear all cached fields for a specific pipeline instance."""
        instance_key = self.get_cache_key(instance)
        for field_name in self.check_cache_exists(instance):
            self.__dict__.get(field_name, {}).pop(instance_key, None)

    @staticmethod
    def get_cache_key(instance: typing.Union["Pipeline", str]) -> str:
        """Return the cache key for a pipeline instance or raw key string."""
        return instance.get_cache_key() if not isinstance(instance, str) else instance

    def check_cache_exists(
        self,
        instance: typing.Union["Pipeline", str],
    ) -> typing.Set[str]:
        """
        Return the set of cache field names that have an entry for ``instance``.
        """
        instance_key = self.get_cache_key(instance)
        return {
            attr
            for attr, value in self.__dict__.items()
            if isinstance(value, (dict, OrderedDict)) and instance_key in value
        }

    def cache(self, instance: typing.Union["Pipeline", str]) -> ChainMap:
        """
        Return the field-value cache for a pipeline instance as a ChainMap.

        Each map in the chain corresponds to one cache field (e.g.
        ``pipeline_cache``), keyed by field name → cached value.
        """
        instance_key = self.get_cache_key(instance)
        cache_fields = self.check_cache_exists(instance)
        if not cache_fields:
            return ChainMap()
        return ChainMap(*(self.__dict__[f][instance_key] for f in cache_fields))

    def set_cache(
        self,
        instance: "Pipeline",
        instance_cache_field: str,
        *,
        field_name: typing.Optional[str] = None,
        value: typing.Any = EMPTY,
    ) -> None:
        """
        Store a field value in the cache for a pipeline instance.

        Unlike the original implementation, this correctly handles multiple
        fields per instance without destroying previously cached entries.

        Args:
            instance: The Pipeline instance being cached.
            instance_cache_field: Name of the top-level cache attribute
                                  (e.g. ``"pipeline_cache"``).
            field_name:           The field being cached (key in inner dict).
            value:                The value to store. Pass ``EMPTY`` to skip.
        """
        if value is EMPTY:
            return

        instance_key = self.get_cache_key(instance)

        # Ensure the outer cache dict exists for this field
        if instance_cache_field not in self.__dict__:
            self.__dict__[instance_cache_field] = OrderedDict()

        # Ensure the inner dict exists for this instance key
        outer = self.__dict__[instance_cache_field]
        if instance_key not in outer:
            outer[instance_key] = OrderedDict()

        # Write the field value — never wipes existing entries
        outer[instance_key][field_name] = value

    def set_cache_for_pipeline_field(
        self,
        instance: "Pipeline",
        field_name: str,
        value: typing.Any,
    ) -> None:
        """Convenience wrapper: cache a field value under ``pipeline_cache``."""
        self.set_cache(
            instance=instance,
            instance_cache_field="pipeline_cache",
            field_name=field_name,
            value=value,
        )


class PipelineMeta(type):
    """
    Metaclass for Pipeline.

    At class definition time:
        1. Collects InputDataField declarations from the namespace.
        2. Locates the Pointy-Lang source (Meta.file, Meta.pointy, or auto-discovery).
        3. Compiles the Pointy-Lang source into an executable task graph.
        4. Stores the field map and compiled graph on the class.

    Pointy-Lang source resolution order:
        1. ``Meta.pointy``  — inline string
        2. ``Meta.file``    — explicit path to a .pty file
        3. Auto-discovery  — searches ``"."`` or the configured path for
                             ``{ClassName}.pty``
    """

    def __new__(cls, name, bases, namespace, **kwargs):
        parents = [b for b in bases if isinstance(b, PipelineMeta)]
        if not parents:
            return super().__new__(cls, name, bases, namespace)

        input_data_fields = OrderedDict(
            (f_name, f)
            for f_name, f in namespace.items()
            if isinstance(f, InputDataField)
        )

        new_class = super().__new__(cls, name, bases, namespace, **kwargs)
        meta_class = getattr(new_class, "Meta", getattr(new_class, "meta", None))

        pointy_path = None
        pointy_str = None

        if meta_class:
            if inspect.isclass(meta_class):
                pointy_path = getattr(meta_class, "file", None)
                pointy_str = getattr(meta_class, "pointy", None)
            elif isinstance(meta_class, dict):
                pointy_path = meta_class.pop("file", None)
                pointy_str = meta_class.pop("pointy", None)
        # No Meta → fall through; pointy_path stays None → auto-discover from "."

        if not pointy_str:
            # Resolve the .pty file path.
            # pointy_path is the user-declared path (or None for auto-discovery).
            # _find_pointy_file_cached handles None by defaulting to ".".
            pointy_file = _find_pointy_file_cached(
                pointy_path, class_name=new_class.__name__
            )
            if pointy_file is None:
                raise ImproperlyConfigured(
                    f"No Pointy-Lang source found for Pipeline "
                    f"{new_class.__name__!r}. Either set Meta.pointy to an "
                    f"inline workflow string, set Meta.file to the path of a "
                    f".pty file, or place a file named "
                    f"'{new_class.__name__}.pty' in the search path."
                )
            with open(pointy_file, "r", encoding="utf-8") as fh:
                pointy_str = fh.read()

        try:
            workflow, ast = build_pipeline_flow_from_pointy_code(pointy_str)
        except (PointyNotExecutable, SyntaxError):
            raise
        except Exception as exc:
            raise BadPipelineError(
                f"Pipeline {new_class.__name__!r} has an improperly written "
                f"workflow. Reason: {exc}",
                exception=exc,
            ) from exc

        if workflow is None:
            raise BadPipelineError(
                f"build_pipeline_flow_from_pointy_code returned None for "
                f"Pipeline {new_class.__name__!r}. Check the .pty source."
            )

        setattr(new_class, PIPELINE_FIELDS, input_data_fields)
        setattr(new_class, PIPELINE_STATE, PipelineState(workflow))
        setattr(new_class, PIPELINE_AST, ast)
        return new_class


class Pipeline(ObjectIdentityMixin, InternalMetadataMixin, metaclass=PipelineMeta):
    """
    Represents a Volnux workflow pipeline.

    A Pipeline subclass declares its input fields as class-level
    InputDataField instances and points to a Pointy-Lang workflow via a
    Meta inner class. The metaclass compiles the workflow graph at class
    definition time.

    Usage
    -----
    class TradePipeline(Pipeline):
        class Meta:
            file = "workflows/trade.pty"

        trades:    InputDataField[list]  = InputDataField(required=True)
        threshold: InputDataField[float] = InputDataField(default=0.05)

    pipeline = TradePipeline(trades=records, threshold=0.03)
    context = await pipeline.start()
    """

    # Class-level signature. Set by construct_call_signature() on first call.
    # Stored in the class's own __dict__ so subclasses do not inherit it.
    __signature__ = None

    def __init__(self, *args, **kwargs):
        # Build the call signature before binding args so that
        # pipeline_pre_init only fires when construction is likely to succeed.
        self.__class__.construct_call_signature()

        if self.__class__.__dict__.get("__signature__"):
            bounded_args = self.__signature__.bind(*args, **kwargs)
            for attr_name, value in bounded_args.arguments.items():
                setattr(self, attr_name, value)

        # Emit pre_init AFTER signature binding — avoids orphaned signals
        # when construct_call_signature() or bind() raises.
        pipeline_pre_init.emit(sender=self.__class__, args=args, kwargs=kwargs)

        self.execution_context: typing.Optional[ExecutionContext] = None

        super().__init__()

        pipeline_post_init.emit(sender=self.__class__, pipeline=self)

    @classmethod
    def construct_call_signature(cls) -> "type[Pipeline]":
        """
        Build and cache the call signature from the class's InputDataField
        declarations.

        Checks ``cls.__dict__`` (not the MRO) to ensure that a parent class's
        signature is not mistaken for the subclass's own signature. This
        prevents subclasses from silently inheriting the wrong signature.

        Returns:
            The class (for chaining).
        """
        # Check the class's OWN dict — not inherited via MRO.
        # cls.__signature__ would return a parent's cached value, incorrectly
        # short-circuiting signature construction for subclasses.
        if cls.__dict__.get("__signature__") is not None:
            return cls

        positional_args = []
        keyword_args = []

        for field_name, field_instance in cls.get_fields():
            if not field_name:
                continue

            param_kwargs: dict[str, typing.Any] = {
                "name": field_name,
                "annotation": (
                    field_instance.data_type
                    if field_instance.data_type is not UNKNOWN
                    else typing.Any
                ),
                "kind": Parameter.POSITIONAL_OR_KEYWORD,
            }

            if field_instance.default is not EMPTY:
                param_kwargs["default"] = field_instance.default
            elif field_instance.required is False:
                param_kwargs["default"] = None

            param = Parameter(**param_kwargs)
            if param.default is Parameter.empty:
                positional_args.append(param)
            else:
                keyword_args.append(param)

        # Set directly on this class's __dict__ to avoid polluting subclasses
        cls.__signature__ = Signature((*positional_args, *keyword_args))
        return cls

    async def start(
        self,
        force_rerun: bool = False,
    ) -> typing.Optional["ExecutionContext"]:
        """
        Execute the pipeline.

        Args:
            force_rerun: Allow re-execution even if the pipeline has already
                         completed. Defaults to False.

        Returns:
            The ExecutionContext linked list. Iterable; filterable by event name
            via ``context.filter_by_event(name)``.

        Raises:
            EventDone: If the pipeline has already executed and
                       ``force_rerun`` is False.
        """
        from volnux.engine import run_workflow

        await pipeline_execution_start.emit_async(sender=self.__class__, pipeline=self)

        if self.execution_context and not force_rerun:
            raise EventDone(
                "Pipeline has already executed. Pass force_rerun=True to re-run."
            )

        self.execution_context = None

        pipeline_state = self.get_pipeline_state()
        await run_workflow(pipeline_state.start, pipeline=self)

        if self.execution_context:
            latest_context = self.execution_context.get_latest_context()

            if latest_context.status == ExecutionStatus.CANCELLED:
                await pipeline_stop.emit_async(
                    sender=self.__class__,
                    pipeline=self,
                    execution_context=latest_context,
                )
                return self.execution_context

            if latest_context.status == ExecutionStatus.ABORTED:
                await pipeline_shutdown.emit_async(
                    sender=self.__class__,
                    pipeline=self,
                    execution_context=latest_context,
                )
                return self.execution_context

        await pipeline_execution_end.emit_async(
            sender=self.__class__,
            execution_context=self.execution_context,
        )
        return self.execution_context

    async def shutdown(self) -> None:
        """
        Abort the pipeline's active execution asynchronously.

        Marks the latest execution context as aborted and emits
        ``pipeline_shutdown``. Async because the execution context's
        ``abort()`` method may be a coroutine.
        """
        if not self.execution_context:
            return
        latest_context = self.execution_context.get_latest_context()
        if hasattr(latest_context, "abort"):
            result = latest_context.abort()
            if inspect.isawaitable(result):
                await result
        await pipeline_shutdown.emit_async(
            sender=self.__class__,
            pipeline=self,
            execution_context=self.execution_context,
        )

    async def stop(self) -> None:
        """
        Cancel the pipeline's active execution asynchronously.

        Marks the latest execution context as cancelled and emits
        ``pipeline_stop``. Async because ``cancel()`` may be a coroutine.
        """
        if not self.execution_context:
            return
        latest_context = self.execution_context.get_latest_context()
        if hasattr(latest_context, "cancel"):
            result = latest_context.cancel()
            if inspect.isawaitable(result):
                await result
        await pipeline_stop.emit_async(
            sender=self.__class__,
            pipeline=self,
            execution_context=self.execution_context,
        )

    def get_cache_key(self) -> str:
        return f"pipeline_{self.__class__.__name__}_{self.id}"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Pipeline):
            return False
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)

    def __getstate__(self) -> dict:
        """
        Prepare the pipeline for pickling (used by ProcessPoolExecutor).

        Copies instance state and the per-instance slice of the class-level
        pipeline cache. Uses getattr() rather than direct __dict__ access so
        that inherited PIPELINE_STATE is found correctly.
        """
        instance_key = self.get_cache_key()
        state = self.__dict__.copy()

        pipeline_state = getattr(self.__class__, PIPELINE_STATE, None)
        if pipeline_state is not None:
            state["_state"] = copy.copy(pipeline_state)
            per_instance_cache = pipeline_state.__dict__.get("pipeline_cache", {})
            state["_state"].pipeline_cache = {
                instance_key: per_instance_cache.get(instance_key, {}).copy()
            }
        return state

    @classmethod
    def get_pipeline_state(cls) -> PipelineState:
        return getattr(cls, PIPELINE_STATE)

    @classmethod
    def get_pointy_ast(cls):
        return getattr(cls, PIPELINE_AST)

    @classmethod
    def get_fields(
        cls,
    ) -> typing.Iterator[typing.Tuple[str, InputDataField]]:
        """Yield (field_name, field_instance) for all declared InputDataFields."""
        for name, klass in getattr(cls, PIPELINE_FIELDS, {}).items():
            yield name, klass

    @classmethod
    def get_non_batch_fields(
        cls,
    ) -> typing.Iterator[typing.Tuple[str, InputDataField]]:
        """Yield (field_name, field) for fields with no batch operation."""
        for name, f in cls.get_fields():
            if not f.has_batch_operation:
                yield name, f

    @classmethod
    def load_class_by_id(cls, pk: str) -> "Pipeline":
        """
        Restore a Pipeline instance from its cached field values.

        The cache stores field-name → value mappings under the instance's
        cache key. ``PipelineState.cache()`` returns a ChainMap over these
        field dicts — iterating it directly yields the cached field values.

        Args:
            pk: The pipeline instance cache key (from ``get_cache_key()``).

        Returns:
            A new Pipeline instance with field values restored from cache,
            or a default instance if no cache entry exists.
        """
        pipeline_state = cls.get_pipeline_state()
        cache_fields = pipeline_state.check_cache_exists(pk)

        if not cache_fields:
            return cls()

        # cache() returns a ChainMap of {field_name: value} dicts.
        # Iterating the ChainMap directly gives field names.
        combined_cache = pipeline_state.cache(pk)
        kwargs = dict(combined_cache)

        instance = cls(**kwargs)
        setattr(instance, "_id", pk)
        return instance

    def get_pipeline_tree(self) -> typing.Optional[Tree]:
        """
        Build a treelib Tree representing the pipeline execution graph.

        Returns:
            A Tree of task nodes, or None if the pipeline state has no
            starting node.
        """
        state = self.get_pipeline_state().start
        if not state:
            return None

        tree = Tree()
        for node in state.bf_traversal(state):
            tag = ""
            if node.is_conditional:
                tag = " (?)"
            elif node.is_descriptor_task:
                tag = " (No)" if node.descriptor == 0 else " (Yes)"
            elif node.is_sink:
                tag = " (Sink)"

            tree.create_node(
                tag=f"{node.get_event_name()}{tag}",
                identifier=node.get_id(),
                parent=node.parent_node.get_id() if node.parent_node else None,
                data=TreeExtraData(pipe_type=node.get_pointer_to_task()),
            )
        return tree

    def draw_ascii_graph(self, stream: typing.Optional[typing.IO] = None) -> str:
        """
        Render the pipeline graph as an ASCII string.

        Args:
            stream: Optional writable stream. If provided, the ASCII graph
                    is written to it in addition to being returned.

        Returns:
            The ASCII graph string, or an empty string if the tree is empty.
        """
        tree = self.get_pipeline_tree()
        if not tree:
            return ""
        output = tree.show(line_type="ascii-emv", stdout=False) or ""
        if stream is not None:
            stream.write(output)
        return output

    def draw_graphviz_image(self, directory: str = "pipeline-graphs") -> None:
        """
        Render the pipeline graph as a PNG image using Graphviz.

        Args:
            directory: Output directory. Defaults to ``"pipeline-graphs"``.
        """
        from volnux.translator.dot import generate_dot_from_task_state

        if graphviz is None:
            logger.warning(
                "graphviz is not installed — cannot render pipeline graph. "
                "Install with: pip install graphviz"
            )
            return

        data = generate_dot_from_task_state(self.get_pipeline_state().start)
        if data:
            src = graphviz.Source(data, directory=directory)
            src.render(format="png", outfile=f"{self.__class__.__name__}.png")

    def get_task_by_id(self, pk: str) -> TaskType:
        """
        Retrieve a task from the pipeline graph by its unique ID.

        Args:
            pk: Task identifier.

        Returns:
            The matching task node.

        Raises:
            EventDoesNotExist: If no task with ``pk`` exists in the graph.
        """
        state: TaskType = self.get_pipeline_state().start
        if state:
            for task in state.bf_traversal(state):
                if task.get_id() == pk:
                    return task
        raise EventDoesNotExist(
            f"Task {pk!r} does not exist in pipeline {self.__class__.__name__!r}.",
            code=pk,
        )

    def get_first_error_execution_node(
        self,
    ) -> typing.Optional["ExecutionContext"]:
        """
        Walk the execution context chain and return the first failed node.

        Returns:
            The first ExecutionContext where ``execution_failed()`` is True,
            or None if no context has failed.
        """
        current = self.execution_context
        while current:
            if current.execution_failed():
                return current
            current = current.next_context
        return None

    def _persist_internal_metadata(self) -> None:
        # Write to the PipelineState cache for checkpoint inclusion.
        # Non-fatal — cache may not yet be initialized during early __init__.
        try:
            pipeline_state = self.get_pipeline_state()
            pipeline_state.set_cache_for_pipeline_field(
                self,
                self._INTERNAL_KEY,
                self.__dict__[self._INTERNAL_KEY],
            )
        except Exception:
            pass
