import functools
import logging
import inspect

from volnux.execution.rehydrator.event.snapshot import EventPhase


logger = logging.getLogger(__name__)


def phase_step(phase: EventPhase):
    """
    A decorator for marking a method as a step of a specific event phase, providing
    automated logging and handling of the phase transition.

    :param phase: The target phase that will be assigned to the instance
        upon the completion of the wrapped method.
    :type phase: EventPhase
    :return: The wrapped function with phase transition handling, designed to work
        with either synchronous or asynchronous methods.
    :rtype: Callable
    """
    from volnux.signal.signals import event_phase_changed

    def decorator(fn):
        @functools.wraps(fn)
        async def async_wrapper(self, *args, **kwargs):
            logger.debug(
                "Entering phase step %s for %s.%s",
                phase.name,
                self.__class__.__name__,
                fn.__name__,
            )
            try:
                result = await fn(self, *args, **kwargs)
            except Exception:
                logger.exception(
                    "Phase step %s failed for %s.%s; phase not advanced",
                    phase.name,
                    self.__class__.__name__,
                    fn.__name__,
                )
                raise

            self._phase = phase
            logger.debug(
                "Phase advanced to %s for %s.%s",
                phase.name,
                self.__class__.__name__,
                fn.__name__,
            )
            await event_phase_changed.emit_async(
                sender=self.__class__, event=self, phase=phase
            )
            return result

        @functools.wraps(fn)
        def sync_wrapper(self, *args, **kwargs):
            logger.debug(
                "Entering phase step %s for %s.%s",
                phase.name,
                self.__class__.__name__,
                fn.__name__,
            )
            try:
                result = fn(self, *args, **kwargs)
            except Exception:
                logger.exception(
                    "Phase step %s failed for %s.%s; phase not advanced",
                    phase.name,
                    self.__class__.__name__,
                    fn.__name__,
                )
                raise

            self._phase = phase
            logger.debug(
                "Phase advanced to %s for %s.%s",
                phase.name,
                self.__class__.__name__,
                fn.__name__,
            )
            event_phase_changed.emit(sender=self.__class__, event=self, phase=phase)
            return result

        wrapper = async_wrapper if inspect.iscoroutinefunction(fn) else sync_wrapper
        wrapper._phase = phase
        wrapper._is_step = True
        return wrapper

    return decorator
