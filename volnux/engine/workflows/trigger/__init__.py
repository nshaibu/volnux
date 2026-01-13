from .engine import TriggerEngine


_trigger_engine = TriggerEngine()


def get_trigger_engine() -> TriggerEngine:
    return _trigger_engine


__all__ = ["get_trigger_engine"]
