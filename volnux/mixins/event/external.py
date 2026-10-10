import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Callable

from ..protocols.event import BaseEvent
from .._phase_decorator import EventPhase
from volnux.exceptions import ExternalCommunicationSuspensionRequest


class ExternalCommunicationType(Enum):
    HITL = "hitl"
    EVENT = "event"


class ExternalCommunicationMixin:

    async def communicate(self, *args, **kwargs) -> Optional[Dict[str, Any]]:
        """
        External communication hook. Runs before process().

        Responsible for all interaction with the world outside the
        task — human input requests, external event subscriptions,
        async signal waiting, third-party confirmations.

        Receives the same injected Pipeline fields as process().
        Any values returned from this method are merged into the
        kwargs passed to process() — making human responses and
        external data available to process() via its parameter list.

        This method may suspend the task via request_human_input()
        or wait_for_event(). When it does, the framework checkpoints
        the phase and traps into the coordinator. The method resumes
        when the external event arrives. process() does not start
        until communicate() returns successfully.

        Default implementation is a no-op — events that need no
        external communication do not override this method.

        Returns
        -------
        Optional dict of additional kwargs to merge into process().
        Return None or omit the return to pass no additional data.

        Example — HITL:

            async def communicate(self, order: dict, **kwargs):
                if order["amount"] > 100_000:
                    await self.request_human_input(
                        title="High-value order approval",
                        payload={"order": order},
                        options=["approve", "reject"],
                        timeout_hours=4,
                    )

        Example — waiting for an external async event::

            async def communicate(self, job_id: str, **kwargs):
                await self.wait_for_event(
                    event_type="ml.training.completed",
                    filter={"job_id": job_id},
                    timeout_hours=12,
                )

        Then in process():

            async def process(
                self,
                order: dict,
                approval_decision: str,   # injected from communicate()
                **kwargs
            ):
                result = await self.previous_result.filter(type="hitl").first()
                if result.content["approval_decision"] == "reject":
                    return False, {"reason": "human_rejected"}
                return True, finalise(order)
        """
        return None

    async def wait_for_condition(
        self: "BaseEvent",
        condition_fn: Callable[[], bool],
        title: str,
        description: str = "",
        timeout_hours: Optional[int] = None,
        poll_interval_seconds: float = 30.0,
    ) -> None:
        """
        Suspend this event until an arbitrary condition is met.

        The condition is polled every poll_interval_seconds until it
        returns True or the timeout expires. The event checkpoints,
        suspends, and resumes at process() when the condition is met.

        This is the Volnux equivalent of Airflow's sensor — it waits
        for an external condition (file exists, API healthy, database
        ready) without consuming a worker slot.

        Example:

            async def communicate(self, batch_id: str, **kwargs):
                await self.wait_for_condition(
                    condition_fn=lambda: s3_object_exists(
                        "data-lake", f"batches/{batch_id}.csv"
                    ),
                    title="Waiting for batch file",
                    description=f"Awaiting s3://data-lake/batches/{batch_id}.csv",
                    timeout_hours=4,
                    poll_interval_seconds=60.0,
                )
                # When we reach process(), the file exists
        """
        if self._phase != EventPhase.COMMUNICATING:
            raise RuntimeError(
                "wait_for_condition() can only be called from communicate() hook"
            )

        await self.enqueue_checkpoint()

        raise ExternalCommunicationSuspensionRequest(
            task=self,
            task_id=self._task_id,
            request_id=str(uuid.uuid4()),
            request_type=ExternalCommunicationType.CONDITION,
            title=title,
            description=description,
            payload={},
            options=[],
            timeout_hours=timeout_hours,
            condition_fn=condition_fn,
            poll_interval_seconds=poll_interval_seconds,
        )

    async def wait_for_event(
        self: BaseEvent,
        event_type: str,
        title: str,
        description: str,
        event_filter: Optional[Dict[str, Any]] = None,
        timeout_hours: Optional[int] = None,
    ) -> None:
        """
        Suspend this event until an external event of event_type arrives.

        The event bus delivers the matching event as an EventResult in
        previous_result. communicate() reads it on resumption via:

            result = self.previous_result.filter(
                type=event_type
            ).first()

        This is the generic form of request_human_input() — it works
        for any external async event: ML job completion, payment
        confirmation, third-party webhook, IoT sensor reading, etc.

        Example:

            async def communicate(self, job_id: str, **kwargs):
                await self.wait_for_event(
                    event_type="payment.confirmed",
                    filter={"job_id": job_id},
                    timeout_hours=1,
                )
        """
        if self._phase != EventPhase.COMMUNICATING:
            raise RuntimeError(
                "wait_for_event() can only be called from communicate() hook"
            )

        await self.enqueue_checkpoint()

        raise ExternalCommunicationSuspensionRequest(
            task=self,
            task_id=self._task_id,
            request_id=str(uuid.uuid4()),
            request_type=ExternalCommunicationType.EVENT,
            title=title,
            description=description,
            payload={},
            options=[],
            event_type=event_type,
            event_filter=event_filter or {},
            timeout_hours=timeout_hours,
        )

    async def request_human_input(
        self: BaseEvent,
        title: str,
        description: str,
        payload: Dict[str, Any],
        options: Optional[List[str]] = None,
        timeout_hours: Optional[int] = 24,
    ) -> None:
        """
        Suspend this event and request input from a human.

        The human response is available after resumption via:

            response = self.previous_result.filter(
                type="human_response"
            ).first()

        Usage::

            async def process(self, order: dict, **kwargs):
                if order["amount"] > 100_000:
                    await self.request_human_input(
                        title="High-value order requires approval",
                        description=f"Order {order['id']} exceeds threshold.",
                        payload={"order": order},
                        options=["approve", "reject"],
                        timeout_hours=4,
                    )
                    # Resumes here after human responds
                    response = self.previous_result.filter(
                        type="human_response"
                    ).first()
                    if response.content["decision"] == "reject":
                        return False, {"reason": "human_rejected"}

                return True, {"order": order}
        """
        if self._phase != EventPhase.COMMUNICATING:
            raise RuntimeError(
                "request_human_input() can only be called from communicate() hook"
            )

        await self.enqueue_checkpoint()

        raise ExternalCommunicationSuspensionRequest(
            task=self,
            task_id=self._task_id,
            request_id=str(uuid.uuid4()),
            request_type=ExternalCommunicationType.HITL,
            title=title,
            description=description,
            payload=payload,
            options=options,
            timeout_hours=timeout_hours,
        )
