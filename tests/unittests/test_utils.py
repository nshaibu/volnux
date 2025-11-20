import inspect
from unittest import mock

from volnux.parser.options import StopCondition
from volnux.utils import (_extend_recursion_depth,
                          build_event_arguments_from_pipeline,
                          generate_unique_id, get_expected_args,
                          get_function_call_args, get_obj_klass_import_str,
                          get_obj_state)


def test_generate_unique_id():
    class Klass1:
        pass

    class Klass2:
        pass

    obj1 = Klass1()
    obj2 = Klass1()

    obj3 = Klass2()
    obj4 = Klass1()

    generate_unique_id(obj1)
    generate_unique_id(obj2)
    generate_unique_id(obj3)
    generate_unique_id(obj4)

    assert hasattr(obj1, "_id")
    assert hasattr(obj2, "_id")
    assert hasattr(obj3, "_id")
    assert hasattr(obj4, "_id")

    assert getattr(obj1, "_id") != getattr(obj2, "_id")
    assert getattr(obj3, "_id") != getattr(obj4, "_id")


def test_get_function_call_args():
    params = {"is_config": True, "event": "process"}

    def func_with_no_arguments():
        pass

    def func_with_arguments_but_not_in_params(sh):
        pass

    def func_with_arguments_in_param(is_config, event="event", _complex=True):
        pass

    assert get_function_call_args(func_with_no_arguments, params) == {}
    assert get_function_call_args(func_with_arguments_but_not_in_params, params) == {
        "sh": None
    }
    assert get_function_call_args(func_with_arguments_in_param, params) == {
        "is_config": True,
        "event": "process",
        "_complex": True,
    }


def test_build_event_arguments_from_pipeline():
    from volnux import EventBase, Pipeline
    from volnux.fields import InputDataField

    class A(EventBase):
        def process(self, name, school):
            return True, "Hello world"

    class P(Pipeline):
        name = InputDataField(data_type=str)
        school = InputDataField(data_type=str)

        class Meta:
            pointy = "A"

    p = P(name="nafiu", school="knust")

    assert build_event_arguments_from_pipeline(A, pipeline=p) == (
        {
            "execution_context": None,
            "task_id": None,
            "previous_result": None,
            "stop_condition": StopCondition.NEVER,
            "run_bypass_event_checks": False,
            "options": None,
        },
        {"name": "nafiu", "school": "knust"},
    )


def test__extend_recursion_depth():
    with mock.patch("sys.getrecursionlimit", return_value=1048576):
        assert _extend_recursion_depth() == None

    with mock.patch("sys.getrecursionlimit", return_value=1048576):
        with mock.patch("resource.setrlimit"):
            with mock.patch("sys.setrecursionlimit"):
                assert _extend_recursion_depth(20) == 20


def test_get_get_expected_args():
    def func_with_no_arguments():
        pass

    def func_with_arguments_but_no_keyword_args(sh, val):
        pass

    def func_with_arguments_and_keyword_args(is_config, event="event", _complex=True):
        pass

    def func_with_with_annotated(is_config: bool, event: str, value: int):
        pass

    assert get_expected_args(func_with_no_arguments) == {}
    assert get_expected_args(func_with_arguments_but_no_keyword_args) == {
        "sh": "_empty",
        "val": "_empty",
    }
    assert get_expected_args(
        func_with_arguments_but_no_keyword_args, include_type=True
    ) == {"sh": inspect.Parameter.empty, "val": inspect.Parameter.empty}
    assert get_expected_args(func_with_with_annotated) == {
        "is_config": "_empty",
        "event": "_empty",
        "value": "_empty",
    }
    assert get_expected_args(func_with_with_annotated, include_type=True) == {
        "is_config": bool,
        "event": str,
        "value": int,
    }
    assert get_expected_args(func_with_arguments_and_keyword_args) == {
        "is_config": "_empty",
        "event": "event",
        "_complex": True,
    }
    assert get_expected_args(
        func_with_arguments_and_keyword_args, include_type=True
    ) == {
        "is_config": inspect.Parameter.empty,
        "event": inspect.Parameter.empty,
        "_complex": inspect.Parameter.empty,
    }


def test_get_obj_state():
    class KlassWithGetState:
        def get_state(self):
            return {"key": "value"}

    class KlassWithGetStateFallback:
        def __getstate__(self):
            return {"fallback_key": "fallback_value"}

    obj1 = KlassWithGetState()
    obj4 = KlassWithGetStateFallback()

    assert get_obj_state(obj1) == {"key": "value"}
    assert get_obj_state(obj4) == {"fallback_key": "fallback_value"}


def test_get_obj_klass_import_str():
    class Klass:
        pass

    obj = Klass()
    assert get_obj_klass_import_str(obj) == f"{obj.__module__}.{Klass.__qualname__}"
