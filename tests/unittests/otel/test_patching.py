"""patch_attr / unpatch_all: applying instrumentation once, and undoing it."""

import asyncio

import pytest

from volnux.otel.instrumentations import _patching
from volnux.otel.instrumentations._patching import (
    is_patched,
    patch_attr,
    patched_count,
    unpatch_all,
)


def doubled(function):
    def wrapper(*args, **kwargs):
        return function(*args, **kwargs) * 2

    wrapper.__wrapped__ = function
    return wrapper


class Base:
    def method(self):
        return 1

    @classmethod
    def klass(cls):
        return cls.__name__

    @staticmethod
    def static():
        return 3

    async def coro(self):
        return 5


class Child(Base):
    pass


@pytest.fixture(autouse=True)
def clean():
    unpatch_all()
    yield
    unpatch_all()


class TestPatchAttr:
    def test_it_replaces_the_attribute(self):
        assert patch_attr(Base, "method", doubled) is True
        assert Base().method() == 2

    def test_patching_twice_wraps_once(self):
        assert patch_attr(Base, "method", doubled) is True
        assert patch_attr(Base, "method", doubled) is False
        assert Base().method() == 2  # not 4
        assert patched_count() == 1

    def test_is_patched(self):
        assert not is_patched(Base, "method")
        patch_attr(Base, "method", doubled)
        assert is_patched(Base, "method")

    def test_a_missing_attribute(self):
        with pytest.raises(AttributeError, match="nope"):
            patch_attr(Base, "nope", doubled)
        assert patched_count() == 0

    def test_classmethods_stay_classmethods(self):
        patch_attr(Base, "klass", lambda f: lambda cls: f(cls) + "!")
        assert Base.klass() == "Base!"
        assert Child.klass() == "Child!"
        assert isinstance(Base.__dict__["klass"], classmethod)

    def test_staticmethods_stay_staticmethods(self):
        patch_attr(Base, "static", doubled)
        assert Base.static() == 6
        assert Base().static() == 6
        assert isinstance(Base.__dict__["static"], staticmethod)

    def test_a_coroutine_wrapper_is_awaited(self):
        def make(function):
            async def wrapper(*args, **kwargs):
                return await function(*args, **kwargs) + 1

            return wrapper

        patch_attr(Base, "coro", make)
        assert asyncio.run(Base().coro()) == 6
        assert asyncio.iscoroutinefunction(Base.coro)


class TestSubclasses:
    def test_a_subclass_is_not_taken_for_patched_because_its_base_is(self):
        patch_attr(Base, "method", doubled)
        # (the old hasattr(cls, "_original_run") check said "already patched" here)
        assert patch_attr(Child, "method", doubled) is True
        assert Child().method() == 4  # the base's and its own wrapper

    def test_an_inherited_method_is_patched_on_the_subclass_only(self):
        patch_attr(Child, "method", doubled)
        assert Child().method() == 2
        assert Base().method() == 1
        assert "method" in Child.__dict__

    def test_unpatching_an_inherited_one_removes_the_attribute_again(self):
        patch_attr(Child, "method", doubled)
        unpatch_all()
        assert "method" not in Child.__dict__
        assert Child().method() == 1


class TestUnpatch:
    def test_it_restores_everything(self):
        patch_attr(Base, "method", doubled)
        patch_attr(Base, "klass", lambda f: lambda cls: "x")
        patch_attr(Base, "static", doubled)
        assert unpatch_all() == 3
        assert Base().method() == 1
        assert Base.klass() == "Base"
        assert Base.static() == 3
        assert patched_count() == 0

    def test_it_returns_the_original_function_object(self):
        original = Base.__dict__["method"]
        patch_attr(Base, "method", doubled)
        unpatch_all()
        assert Base.__dict__["method"] is original

    def test_after_unpatching_it_can_be_patched_again(self):
        patch_attr(Base, "method", doubled)
        unpatch_all()
        assert patch_attr(Base, "method", doubled) is True
        assert Base().method() == 2

    def test_unpatching_nothing(self):
        assert unpatch_all() == 0

    def test_something_that_replaced_it_since_is_left_alone(self):
        patch_attr(Base, "method", doubled)
        Base.method = lambda self: 99  # someone else
        assert unpatch_all() == 0
        assert Base().method() == 99
        Base.method = lambda self: 1


class TestModuleFunctions:
    def test_a_function_of_a_module(self):
        import types

        module = types.ModuleType("fake")
        module.compute = lambda: 21
        patch_attr(module, "compute", doubled)
        assert module.compute() == 42
        unpatch_all()
        assert module.compute() == 21
