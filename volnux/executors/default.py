import logging
from concurrent.futures import Executor, Future

logger = logging.getLogger(__name__)


class DefaultExecutor(Executor):

    def submit(self, fn, /, *args, **kwargs):
        f = Future()
        try:
            f.set_result(fn(*args, **kwargs))
        except Exception as e:
            logger.error(str(e))
            f.set_exception(e)
        return f
