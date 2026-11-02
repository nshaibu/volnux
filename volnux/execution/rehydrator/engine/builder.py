import typing

from .snapshot import EngineStateSnapshot, TaskSnapshot
from .serializer import StateSerializer
from volnux.utils import get_obj_klass_import_str

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType
    from volnux.execution.context import ExecutionContext


class SnapshotBuilder:
    def __init__(self, serializer=StateSerializer):
        self.serializer = serializer

    def build_engine_state(self, context: "ExecutionContext") -> EngineStateSnapshot:
        """
        Snapshot the engine working on ``context``.

        The context's own state is persisted by the context itself
        (``await context.save()``); this captures only what the engine adds.
        """
        engine = context.get_engine()
        if engine is None:
            raise ValueError(
                f"Context {context.id} has no engine attached; nothing to snapshot"
            )

        instance = EngineStateSnapshot(
            context_id=context.id,
            engine_class_path=get_obj_klass_import_str(engine),
            current_task=(
                self.serializer.serialise_queue_task(0, engine.current_task_node)
                if engine.current_task_node
                else None
            ),
            task_queue_snapshot=[
                self.serializer.serialise_queue_task(i, task)
                for i, task in enumerate(engine.task_queue)
            ],
            queue_index=0,
            current_task_queue_size=len(engine.task_queue),
            sink_queue_snapshot=[
                self.serializer.serialise_queue_task(i, task)
                for i, task in enumerate(engine.sink_queue)
            ],
            current_sink_queue_size=len(engine.sink_queue),
            tasks_processed=engine.tasks_processed,
        )
        instance.change_object_id(context.id)
        return instance

    def build_pipeline_task(
        self,
        task_profile: "TaskType",
        context_id: typing.Optional[str] = None,
        task_checkpoint: typing.Optional[dict] = None,
    ) -> TaskSnapshot:
        """
        Snapshot a task (or task grouping).

        Args:
            task_profile: The task to snapshot.
            context_id: Id of the execution context the task was created in.
            task_checkpoint: The task's internal state, for idempotent restarts.
        """
        snapshot = TaskSnapshot(
            context_id=context_id,
            task_checkpoint=task_checkpoint,
            **self.serializer.serialize_task(task_profile),
        )
        snapshot.change_object_id(task_profile.get_id())
        return snapshot
