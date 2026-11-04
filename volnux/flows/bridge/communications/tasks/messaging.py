import asyncio
import logging
import typing
import uuid

from volnux.backends.messaging.util import supports_pushpop

from .base import ChannelUnavailable, TaskCommunicationBridge, TaskExecutionHandle
from .channels.messaging import ChannelEnvelope, MessagingCommandChannel

if typing.TYPE_CHECKING:
    from volnux.execution.context import ExecutionContext

logger = logging.getLogger(__name__)

_INBOX_PUMP = "__inbox__"


class MessagingTaskCommunicationBridge(TaskCommunicationBridge):
    """
    Control plane for tasks that run outside this process (process pool,
    Celery): commands and status messages travel through the configured
    messaging backend (Redis, Kafka...), so process boundaries do not matter.

    Needs a backend that several processes can reach (``supports_cross_process``)
    and that has queues. Otherwise ``register_task`` raises ``ChannelUnavailable``
    and the flow runs the task without a channel.

    Every task reports on one shared inbox queue, consumed by a single pump.
    """

    channel_class = MessagingCommandChannel

    def __init__(self, context: "ExecutionContext"):
        super().__init__(context)
        self._inbox = uuid.uuid4().hex

    def _get_or_create_channel(self, task_id: str) -> MessagingCommandChannel:
        if task_id not in self._channels:
            self._channels[task_id] = MessagingCommandChannel(
                task_id, inbox=self._inbox
            )
        return typing.cast(MessagingCommandChannel, self._channels[task_id])

    async def _require_transport(self) -> None:
        try:
            backend = await ChannelEnvelope.get_backend()
        except Exception as exc:
            raise ChannelUnavailable(
                f"no messaging backend is configured: {exc}"
            ) from exc
        if not getattr(backend, "supports_cross_process", False):
            raise ChannelUnavailable(
                f"{type(backend).__name__} is not a cross-process backend: configure "
                "Redis or Kafka as the key-value store to control tasks running "
                "outside this process"
            )
        if not supports_pushpop(backend):
            raise ChannelUnavailable(
                f"{type(backend).__name__} has no queue support, which the task "
                "command channel needs"
            )

    async def register_task(
        self, handle: TaskExecutionHandle
    ) -> MessagingCommandChannel:
        await self._require_transport()
        return typing.cast(MessagingCommandChannel, await super().register_task(handle))

    def _start_pump(self, full_id: str, channel: typing.Any) -> None:
        """One pump serves every task, because they share the inbox."""
        existing = self._pumps.get(_INBOX_PUMP)
        if existing is not None and not existing.done():
            return
        self._pumps[_INBOX_PUMP] = asyncio.create_task(
            self._inbox_pump(channel), name="volnux-bridge-inbox-pump"
        )

    async def _inbox_pump(self, channel: MessagingCommandChannel) -> None:
        try:
            while not self.is_shutdown:
                try:
                    message = await channel.receive_message(timeout=self.pump_timeout)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Inbox pump could not read the backend: %s", exc)
                    await asyncio.sleep(1.0)
                    continue
                if message is not None:
                    await self._process_message(message)
        except asyncio.CancelledError:
            raise

    async def shutdown(self) -> None:
        if self.is_shutdown:
            return
        channels = list(self._channels.values())
        await super().shutdown()  # stops the pump, deletes each command queue
        if channels:
            await typing.cast(MessagingCommandChannel, channels[0]).delete_inbox()
