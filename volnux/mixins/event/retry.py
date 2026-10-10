import logging

import asyncio
from typing import (
    List,
    Dict,
    Tuple,
    Any,
    Callable,
    cast,
    Union,
    TypedDict,
    Type,
    Optional,
    Sequence,
    Awaitable,
)
from dataclasses import dataclass, field

from volnux.config import VolnuxConfig
from volnux.constants import MAX_BACKOFF, MAX_BACKOFF_FACTOR, MAX_RETRIES
from volnux.exceptions import MaxRetryError
from volnux.signal.signals import event_execution_retry, event_execution_retry_done
from volnux.mixins.protocols.event import BaseEvent as _BaseEvent


logger = logging.getLogger(__name__)

conf = VolnuxConfig.get_instance()


class RetryConfigDict(TypedDict, total=False):
    max_attempts: int
    backoff_factor: float
    max_backoff: float
    retry_on_exceptions: List[Type[Exception]]


@dataclass
class RetryPolicy:
    max_attempts: int = field(
        init=True, default=conf.get("MAX_EVENT_RETRIES", default=MAX_RETRIES)
    )
    backoff_factor: float = field(
        init=True,
        default=conf.get("MAX_EVENT_BACKOFF_FACTOR", default=MAX_BACKOFF_FACTOR),
    )
    max_backoff: float = field(
        init=True, default=conf.get("MAX_EVENT_BACKOFF", default=MAX_BACKOFF)
    )
    retry_on_exceptions: List[Type[Exception]] = field(default_factory=list)


class RetryMixin:
    """
    Provides retry handling capabilities for operations.

    This mixin allows defining and managing retry policies for operations that might fail
    and need to be retried. It includes support for exponential backoff, retryable
    exception checking, and maximum retry attempts. The mixin is intended to be combined
    with other classes to provide retry functionality for their methods.

    :ivar retry_policy: The retry policy configuration used to control retry behavior.
                        Can either be a dictionary or an instance of RetryPolicy.
    :type retry_policy: Optional[Union[RetryPolicy, Dict[str, Any], None]]
    """

    retry_policy: Union[Optional[RetryPolicy], Dict[str, Any], None] = None

    def init_retry(self) -> Union[RetryPolicy, None]:
        if isinstance(self.retry_policy, dict):
            retry_policy = cast(dict, self.retry_policy)
            self.retry_policy = RetryPolicy(**retry_policy)
        return self.retry_policy

    def config_retry_policy(
        self,
        max_attempts: int,
        backoff_factor: float = MAX_BACKOFF_FACTOR,
        max_backoff: float = MAX_BACKOFF,
        retry_on_exceptions: Union[List[Type[Exception]], Type[Exception], None] = None,
    ) -> None:
        """
        Configures the retry policy for the event.
        Args:
            max_attempts (int): Maximum number of retry attempts.
            backoff_factor (float): Factor for calculating backoff time.
            max_backoff (float): Maximum backoff time.
            retry_on_exceptions (Union[Tuple[Type[Exception]], Type[Exception], None]): Exceptions that trigger a retry.
        Returns:
            None
        """
        config: RetryConfigDict = {
            "max_attempts": max_attempts,
            "backoff_factor": backoff_factor,
            "max_backoff": max_backoff,
            "retry_on_exceptions": [],
        }
        if retry_on_exceptions:
            retry_exceptions: Sequence[Type[Exception]] = (
                retry_on_exceptions
                if isinstance(retry_on_exceptions, (tuple, list))
                else [retry_on_exceptions]
            )
            config["retry_on_exceptions"].extend(retry_exceptions)

        self.retry_policy = RetryPolicy(**config)

    def get_backoff_time(self: _BaseEvent) -> float:
        if self.retry_policy is None or self._retry_count <= 1:
            return 0

        backoff_value = self.retry_policy.backoff_factor * (
            2 ** (self._retry_count - 1)
        )

        return min(backoff_value, self.retry_policy.max_backoff)

    async def _sleep_for_backoff(self) -> float:
        backoff = self.get_backoff_time()
        if backoff <= 0:
            return 0
        await asyncio.sleep(backoff)
        return backoff

    def is_retryable(self, exception: Exception) -> bool:
        if self.retry_policy is None:
            return False
        exception_evaluation = not self.retry_policy.retry_on_exceptions or any(
            [
                isinstance(exception, exc)
                and exception.__class__.__name__ == exc.__name__
                for exc in self.retry_policy.retry_on_exceptions
                if exc
            ]
        )
        return isinstance(exception, Exception) and exception_evaluation

    def is_exhausted(self: _BaseEvent) -> bool:
        return (
            self.retry_policy is None
            or self._retry_count >= self.retry_policy.max_attempts
        )

    async def _retry(
        self: _BaseEvent,
        func: Callable[[Any], Awaitable[Tuple[bool, Any]]],
        /,
        *args: Tuple[Any],
        **kwargs: Dict[str, Any],
    ) -> Tuple[bool, Any]:
        """
        Retries the execution of a given callable function according to the retry policy defined
        in the instance. The method ensures retries occur only when certain conditions are met,
        and emits specific events during retry-related activities.

        :param func: The callable to be executed. It must return a tuple where the first element is
            a boolean indicating success or failure and the second element is the result or error object.
        :type func: Callable[[typing.Any], typing.Tuple[bool, typing.Any]]

        :param args: The positional arguments to be passed to the callable.
        :type args: typing.Tuple[typing.Any]

        :param kwargs: The keyword arguments to be passed to the callable.
        :type kwargs: typing.Dict[str, typing.Any]

        :return: A tuple where the first element is a boolean indicating success or failure,
            and the second element is the result or error object.
        :rtype: typing.Tuple[bool, typing.Any]
        """
        if self.retry_policy is None:
            return await func(*args, **kwargs)

        exception_causing_retry = None

        while True:
            if self.is_exhausted():
                await event_execution_retry_done.emit_async(
                    sender=self._execution_context.__class__,
                    event=self,
                    execution_context=self._execution_context,
                    task_id=self._task_id,
                    max_attempts=self.retry_policy.max_attempts,
                )

                raise MaxRetryError(
                    attempt=self._retry_count,
                    exception=exception_causing_retry,
                    reason="Retryable event is already exhausted: actual error:{reason}".format(
                        reason=str(exception_causing_retry)
                    ),
                )

            logger.info(
                "Retrying event {}, attempt {}...".format(
                    self.__class__.__name__, self._retry_count
                )
            )

            try:
                self._retry_count += 1
                return await func(*args, **kwargs)
            except MaxRetryError:
                # ignore this
                break
            except Exception as exc:
                if self.is_retryable(exc):
                    if exception_causing_retry is None:
                        exception_causing_retry = exc
                    back_off = await self._sleep_for_backoff()

                    await event_execution_retry.emit_async(
                        sender=self._execution_context.__class__,
                        event=self,
                        backoff=back_off,
                        retry_count=self._retry_count,
                        max_attempts=self.retry_policy.max_attempts,
                        execution_context=self._execution_context,
                        task_id=self._task_id,
                    )
                    continue
                raise

        return False, None
