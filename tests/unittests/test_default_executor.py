from concurrent.futures import Future

import pytest

from volnux.executors import default


def test_default_executor():
    def func(school):
        return school

    def exception_func():
        raise ValueError

    executor = default.DefaultExecutor()
    future = executor.submit(func, "knust")
    assert isinstance(future, Future)
    assert future.result() == "knust"

    with pytest.raises(ValueError):
        executor = default.DefaultExecutor()
        future = executor.submit(exception_func)
        raise future.exception()
