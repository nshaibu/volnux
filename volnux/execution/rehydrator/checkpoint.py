import asyncio
import logging
import typing

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext


class AutoCheckpointer:
    """
    Automatically persists execution state at strategic points.

    Integration points:
    - Before task execution
    - After task completion
    - On status changes
    - On errors
    - Periodic timer
    """

    def __init__(
        self,
        state_store: PersistentStateStore,
        checkpoint_interval: float = 5.0,  # seconds
    ):
        self.state_store = state_store
        self.checkpoint_interval = checkpoint_interval
        self._checkpoint_task: typing.Optional[asyncio.Task] = None
        self._contexts: typing.Set["ExecutionContext"] = set()

    def register_context(self, context: "ExecutionContext") -> None:
        """Add a context to automatic checkpointing"""
        self._contexts.add(context)

    def unregister_context(self, context: "ExecutionContext") -> None:
        """Remove a context from automatic checkpointing"""
        self._contexts.discard(context)

    async def start(self) -> None:
        """Start the periodic checkpointing loop"""
        self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())

    async def stop(self) -> None:
        """Stop the checkpointing loop"""
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

    async def _checkpoint_loop(self) -> None:
        """Periodic checkpoint task"""
        while True:
            try:
                await asyncio.sleep(self.checkpoint_interval)
                await self.checkpoint_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Checkpoint error: {e}")

    async def checkpoint_all(self) -> None:
        """Checkpoint all registered contexts"""
        for context in list(self._contexts):
            try:
                await context.persist(self.state_store)
            except Exception as e:
                logger.error(f"Failed to checkpoint {context.state_id}: {e}")

    async def checkpoint_on_event(
        self, context: "ExecutionContext", event_name: str
    ) -> None:
        """
        Checkpoint triggered by specific events.

        Args:
            context: The context to checkpoint
            event_name: Event that triggered the checkpoint
        """
        logger.debug(f"Checkpointing {context.state_id} on event: {event_name}")
        await context.persist(self.state_store)
