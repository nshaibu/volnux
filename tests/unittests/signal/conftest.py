import itertools

import pytest

from volnux.signal.signals import SoftSignal

_counter = itertools.count()


@pytest.fixture
def make_signal():
    """Factory for uniquely named signals so registry entries never collide."""

    def factory(*provide_args):
        return SoftSignal(
            "test_signal_%d" % next(_counter), provide_args=list(provide_args)
        )

    return factory


@pytest.fixture
def signal(make_signal):
    return make_signal("value")


class Sender:
    pass


class OtherSender:
    pass
