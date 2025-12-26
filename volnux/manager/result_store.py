import threading
import time
import typing
import logging
from volnux.conf import ConfigLoader


logger = logging.getLogger(__name__)

CONF = ConfigLoader.get_lazily_loaded_config()


class ResultStore:
    """
    Singleton in-memory store for task results.
    Used for polling or when clients disconnect before receiving results.
    """
    _instance = None
    _lock = threading.RLock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ResultStore, cls).__new__(cls)
                    cls._instance._minit()
        return cls._instance

    def _minit(self):
        self.results: typing.Dict[str, typing.Dict] = {}
        self.lock = threading.RLock()
        self.ttl = getattr(CONF, "TASK_RESULT_TTL", 600)  # 10 minutes default

    def store(self, correlation_id: str, result: typing.Dict[str, typing.Any]):
        """Store a result with current timestamp"""
        with self.lock:
            self.results[correlation_id] = {
                "data": result,
                "timestamp": time.time()
            }
            logger.debug(f"Stored result for {correlation_id} in ResultStore")

    def get(self, correlation_id: str) -> typing.Optional[typing.Dict[str, typing.Any]]:
        """Retrieve and remove a result (pop)"""
        with self.lock:
            entry = self.results.pop(correlation_id, None)
            if entry:
                return entry["data"]
            return None

    def cleanup(self):
        """Remove expired results"""
        now = time.time()
        expired = []
        with self.lock:
            for cid, entry in self.results.items():
                if now - entry["timestamp"] > self.ttl:
                    expired.append(cid)

            for cid in expired:
                del self.results[cid]

        if expired:
            logger.info(f"Cleaned up {len(expired)} expired results from ResultStore")


def get_result_store():
    return ResultStore()
