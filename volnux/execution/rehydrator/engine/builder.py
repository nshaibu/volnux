import typing

from .snapshot import ContextSnapshot, TraversalSnapshot, TaskSnapshot
from .serializer import StateSerializer
from volnux.utils import get_obj_klass_import_str

if typing.TYPE_CHECKING:
    from volnux.parser.protocols import TaskType
    from volnux.engine.base import WorkflowEngine
    from volnux.execution.context import ExecutionContext


class SnapshotBuilder:
    def __init__(self, serializer=StateSerializer):
        self.serializer = serializer

    async def build(self, context: "ExecutionContext") -> ContextSnapshot:
        engine = context.get_engine()

        traversal = self._build_traversal(engine)

        pipeline_id, pipeline_class_path = self.serializer.serialize_pipeline_ref(
            context.pipeline
        )

        parent_id = context.parent_context.state_id if context.parent_context else None
        child_ids = [child.state_id for child in context.child_contexts]
        previous_context_id = (
            context.previous_context.state_id if context.previous_context else None
        )
        next_context_id = (
            context.next_context.state_id if context.next_context else None
        )

        instance = ContextSnapshot(
            state_id=context.state_id,
            workflow_id=context.workflow_id,
            parent_id=parent_id,
            child_ids=child_ids,
            depth=context.get_depth(),
            previous_context_id=previous_context_id,
            next_context_id=next_context_id,
            traversal=traversal,
            pipeline_id=pipeline_id,
            pipeline_class_path=pipeline_class_path,
            pipeline_state=self._build_pipeline_state(context),
            status=context.status.value,
            errors=[self.serializer.serialize_exception(e) for e in context.errors],
            results=[self.serializer.serialize_result(r) for r in context.results],
            aggregated_result=(
                self.serializer.serialize_result(context.aggregated_result)
                if context.aggregated_result
                else None
            ),
            metrics=self._build_metrics(context),
        )

        instance.change_object_id(context.id)
        return instance

    def build_pipeline_task(self, task_profile: "TaskType") -> TaskSnapshot:
        task_data = self.serializer.serialize_task(task_profile)
        snapshot = TaskSnapshot(**task_data)
        snapshot.change_object_id(task_profile.get_id())
        return snapshot

    def _build_traversal(
        self,
        engine: "WorkflowEngine",
    ) -> TraversalSnapshot:
        return TraversalSnapshot(
            engine_class_path=get_obj_klass_import_str(engine),
            task_queue_snapshot=[
                self.serializer.serialise_queue_task(i, task)
                for i, task in enumerate(engine.task_queue)
            ],
            sink_queue_snapshot=[
                self.serializer.serialise_queue_task(i, task)
                for i, task in enumerate(engine.sink_queue)
            ],
            tasks_processed=engine.tasks_processed,
            queue_index=0,
            current_task_queue_size=len(engine.task_queue),
            current_sink_queue_size=len(engine.sink_queue),
            current_task=(
                self.serializer.serialise_queue_task(0, engine.current_task_node)
                if engine.current_task_node
                else None
            ),
            current_task_checkpoint={},
        )

    def _build_pipeline_state(self, context: "ExecutionContext") -> dict:
        pipeline = context.pipeline
        if pipeline:
            return pipeline.__getstate__()
        return {}

    def _build_metrics(self, context: "ExecutionContext") -> dict:
        return {
            "start_time": context.metrics.start_time,
            "end_time": context.metrics.end_time,
            "duration": context.metrics.duration,
        }
