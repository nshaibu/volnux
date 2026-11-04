import uuid
from enum import Enum
from typing import Any, Dict, List, Optional, Callable

from ..protocols.event import BaseEvent
from .._phase_decorator import EventPhase
from volnux.constants import EMPTY
from volnux.exceptions import ExternalCommunicationSuspensionRequest
from .communication.datastructures import RESPONSE_EVENT_NAMES


class ExternalCommunicationType(Enum):
    HITL = "hitl"
    EVENT = "event"
    CONDITION = "condition"


class ExternalCommunicationMixin:

    def get_response(
        self: BaseEvent, event_name: Optional[str] = None
    ) -> Optional[Any]:
        """The answer injected when this task was rehydrated, or ``None``.

        A task that suspended is brought back with the answer added to its
        ``previous_result`` as an ``EventResult`` whose ``content`` holds
        ``decision``, ``data``, ``reviewer``, ``source`` and ``responded_at``
        (``source == "timeout"`` when nothing answered in time). Before a task
        has been suspended there is nothing: ``previous_result`` is unset, and this
        returns ``None`` instead of failing on the sentinel.

        :param event_name: Only consider results with this ``event_name``. By
            default any answer qualifies (``human_response``, ``external_event``
            or ``condition_met``).
        """
        results = self.previous_result
        if results is None or results is EMPTY:
            return None
        names = [event_name] if event_name else list(RESPONSE_EVENT_NAMES.values())
        return results.filter(event_name__in=names).first()

    async def communicate(self, *args, **kwargs) -> Optional[Dict[str, Any]]:
        """
        External communication hook. Runs before process().

        Responsible for all interaction with the world outside the
        task — human input requests, external event subscriptions,
        async signal waiting, third-party confirmations.

        Receives the same injected Pipeline fields as process().

        This method may suspend the task via request_human_input(),
        wait_for_event() or wait_for_condition(). When it does, the task is
        checkpointed and every resource it holds is released; nothing runs
        until the answer arrives. The answer is then injected into
        ``self.previous_result`` as an ``EventResult`` and communicate() runs
        again from the top, so it should look for the answer first (see
        ``get_response()``) and only ask when there is none. process() does
        not start until communicate() returns normally.

        Default implementation is a no-op — events that need no
        external communication do not override this method.

        Returns
        -------
        Nothing. communicate() asks for something through request_human_input(),
        wait_for_event() or wait_for_condition(); each of these suspends the task:
        it is checkpointed and every resource it holds is released. There is
        therefore no value to return. The answer is injected into
        ``self.previous_result`` when the task is rehydrated, and communicate()
        runs again from the top, reading it from there.

        Example — HITL:

            async def communicate(self, order: dict, **kwargs):
                if order["amount"] > 100_000:
                    await self.request_human_input(
                        title="High-value order approval",
                        description="Orders above 100,000 need sign-off",
                        payload={"order": order},
                        options=["approve", "reject"],
                        timeout_hours=4,
                    )

        Example — waiting for an external async event::

            async def communicate(self, job_id: str, **kwargs):
                await self.wait_for_event(
                    event_type="ml.training.completed",
                    title="Wait for training job",
                    description=f"Training job {job_id} must finish first",
                    event_filter={"job_id": job_id},
                    timeout_hours=12,
                )

        Then in process(), the response is available through get_response():

            async def process(self, order: dict, **kwargs):
                result = self.get_response()
                if result.content["decision"] == "reject":
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
        if self._active_phase != EventPhase.COMMUNICATING:
            raise RuntimeError(
                "wait_for_condition() can only be called from communicate() hook"
            )

        snapshot = await self.checkpoint_for_suspension()

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
            snapshot=snapshot,
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

        The matching event arrives as an EventResult in previous_result when the
        task is rehydrated. communicate() reads it on resumption via:

            result = self.get_response()

        This is the generic form of request_human_input() — it works
        for any external async event: ML job completion, payment
        confirmation, third-party webhook, IoT sensor reading, etc.

        Example:

            async def communicate(self, job_id: str, **kwargs):
                await self.wait_for_event(
                    event_type="payment.confirmed",
                    title="Wait for payment",
                    description="Payment confirmation must arrive first",
                    event_filter={"job_id": job_id},
                    timeout_hours=1,
                )
        """
        if self._active_phase != EventPhase.COMMUNICATING:
            raise RuntimeError(
                "wait_for_event() can only be called from communicate() hook"
            )

        snapshot = await self.checkpoint_for_suspension()

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
            snapshot=snapshot,
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

            response = self.get_response()

        Must be called from the ``communicate()`` hook. On resumption
        ``communicate()`` runs again from the top and finds the human's answer
        in ``previous_result``, so ``process()`` only ever sees the decision.

        Usage::

            async def communicate(self, order: dict, **kwargs):
                if self.get_response() is None and order["amount"] > 100_000:
                    await self.request_human_input(
                        title="High-value order requires approval",
                        description=f"Order {order['id']} exceeds threshold.",
                        payload={"order": order},
                        options=["approve", "reject"],
                        timeout_hours=4,
                    )

            async def process(self, order: dict, **kwargs):
                response = self.get_response()
                if response and response.content["decision"] == "reject":
                    return False, {"reason": "human_rejected"}
                return True, {"order": order}
        """
        if self._active_phase != EventPhase.COMMUNICATING:
            raise RuntimeError(
                "request_human_input() can only be called from communicate() hook"
            )

        snapshot = await self.checkpoint_for_suspension()

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
            snapshot=snapshot,
        )
