"""PipelineMeta (compiling the workflow when the class is defined) and construction."""

import inspect

import pytest

from volnux import Pipeline
from volnux.constants import PIPELINE_AST, PIPELINE_FIELDS, PIPELINE_STATE
from volnux.exceptions import BadPipelineError, ImproperlyConfigured
from volnux.execution.pipeline.fields import InputDataField
from volnux.execution.pipeline.pipeline import PipelineState

from .conftest import unique


# --- where the workflow comes from ---------------------------------------------------------


def test_an_inline_workflow_is_compiled_at_class_definition(
    pipeline_class, event_class
):
    a, b = event_class(name="A"), event_class(name="B")

    cls = pipeline_class(f"{a.__name__} -> {b.__name__}")

    state = cls.get_pipeline_state()
    assert isinstance(state, PipelineState) and getattr(cls, PIPELINE_STATE) is state
    assert state.start.get_event_name() == a.__name__
    assert state.start.condition_node.on_success_event.get_event_name() == b.__name__
    assert cls.get_pointy_ast() is getattr(cls, PIPELINE_AST) and cls.get_pointy_ast()


def test_a_workflow_file_named_in_meta_is_read(tmp_path, event_class):
    ev = event_class()
    f = tmp_path / "flow.pty"
    f.write_text(ev.__name__)

    cls = type(
        unique("FromFile"), (Pipeline,), {"Meta": type("Meta", (), {"file": str(f)})}
    )

    assert cls.get_pipeline_state().start.get_event_name() == ev.__name__


def test_meta_may_be_a_dict_and_lowercase(event_class):
    ev = event_class()

    upper = type(unique("DictMeta"), (Pipeline,), {"Meta": {"pointy": ev.__name__}})
    lower = type(unique("LowerMeta"), (Pipeline,), {"meta": {"pointy": ev.__name__}})

    assert upper.get_pipeline_state().start.get_event_name() == ev.__name__
    assert lower.get_pipeline_state().start.get_event_name() == ev.__name__


def test_inline_pointy_wins_over_a_file(tmp_path, event_class):
    inline, filed = event_class(name="Inline"), event_class(name="Filed")
    f = tmp_path / "flow.pty"
    f.write_text(filed.__name__)

    cls = type(
        unique("Both"),
        (Pipeline,),
        {"Meta": type("Meta", (), {"pointy": inline.__name__, "file": str(f)})},
    )

    assert cls.get_pipeline_state().start.get_event_name() == inline.__name__


def test_a_pty_file_named_after_the_class_is_found_in_the_working_directory(
    tmp_path, monkeypatch, event_class
):
    ev = event_class()
    name = unique("AutoFound")
    (tmp_path / f"{name}.pty").write_text(ev.__name__)
    monkeypatch.chdir(tmp_path)

    cls = type(name, (Pipeline,), {})

    assert cls.get_pipeline_state().start.get_event_name() == ev.__name__


def test_no_workflow_anywhere_is_a_configuration_error(tmp_path):
    with pytest.raises(ImproperlyConfigured, match="No Pointy-Lang source"):
        type(
            unique("Nowhere"),
            (Pipeline,),
            {"Meta": type("Meta", (), {"file": str(tmp_path)})},
        )


def test_a_workflow_that_does_not_parse_is_reported(event_class):
    with pytest.raises((SyntaxError, BadPipelineError)):
        type(
            unique("Broken"),
            (Pipeline,),
            {"Meta": type("Meta", (), {"pointy": "A -> -> B ((("})},
        )


def test_the_base_class_itself_is_not_compiled():
    assert not hasattr(Pipeline, PIPELINE_STATE)


# --- fields -----------------------------------------------------------------------------------


def test_input_fields_are_collected_in_declaration_order(pipeline_class, event_class):
    cls = pipeline_class(
        event_class().__name__,
        first=InputDataField(data_type=str, required=False),
        second=InputDataField(data_type=int, required=False),
    )

    assert [n for n, _ in cls.get_fields()] == ["first", "second"]
    assert list(getattr(cls, PIPELINE_FIELDS)) == ["first", "second"]


def test_a_pipeline_without_fields_takes_no_arguments(pipeline_class, event_class):
    cls = pipeline_class(event_class().__name__)

    assert list(cls.get_fields()) == []
    cls()
    with pytest.raises(TypeError):
        cls(unexpected=1)


def test_fields_of_a_parent_pipeline_are_inherited_and_can_be_replaced(event_class):
    ev = event_class()
    Meta = type("Meta", (), {"pointy": ev.__name__})

    Parent = type(
        unique("Parent"),
        (Pipeline,),
        {
            "Meta": Meta,
            "keep": InputDataField(data_type=str, required=False),
            "replace": InputDataField(data_type=str, required=False),
        },
    )
    new = InputDataField(data_type=int, required=False)
    Child = type(unique("Child"), (Parent,), {"Meta": Meta, "replace": new})

    fields = dict(Child.get_fields())
    assert set(fields) == {"keep", "replace"} and fields["replace"] is new
    assert dict(Parent.get_fields())["replace"] is not new


# --- the call signature ------------------------------------------------------------------------


@pytest.fixture
def signed(pipeline_class, event_class):
    return pipeline_class(
        event_class().__name__,
        needed=InputDataField(data_type=str, required=True),
        with_default=InputDataField(data_type=int, default=7),
        optional=InputDataField(data_type=str, required=False),
    )


def test_a_required_field_is_a_required_parameter(signed):
    signed.construct_call_signature()
    params = signed.__signature__.parameters

    assert params["needed"].default is inspect.Parameter.empty
    assert params["with_default"].default == 7
    assert params["optional"].default is None
    assert params["needed"].annotation == (str,)  # the field's accepted types


def test_required_parameters_come_before_optional_ones(signed):
    signed.construct_call_signature()
    assert list(signed.__signature__.parameters) == [
        "needed",
        "with_default",
        "optional",
    ]


def test_construction_binds_values_by_name_or_position(signed):
    by_name = signed(needed="a", optional="z")
    by_position = signed("a", 3)

    assert (by_name.needed, by_name.with_default, by_name.optional) == ("a", 7, "z")
    assert (by_position.needed, by_position.with_default) == ("a", 3)


def test_a_missing_required_field_or_an_unknown_one_is_rejected(signed):
    with pytest.raises(TypeError):
        signed()
    with pytest.raises(TypeError):
        signed(needed="a", nope=1)


def test_the_signature_is_built_once_per_class_and_not_inherited(event_class):
    ev = event_class()
    Meta = type("Meta", (), {"pointy": ev.__name__})
    Parent = type(
        unique("SigParent"),
        (Pipeline,),
        {"Meta": Meta, "a": InputDataField(data_type=str, required=False)},
    )
    Child = type(
        unique("SigChild"),
        (Parent,),
        {"Meta": Meta, "b": InputDataField(data_type=str, required=False)},
    )

    Parent.construct_call_signature()
    first = Parent.__signature__
    Parent.construct_call_signature()
    Child.construct_call_signature()

    assert Parent.__signature__ is first
    assert list(Child.__signature__.parameters) == ["a", "b"]  # not the parent's


def test_non_batch_fields_are_those_without_a_batch_operation(
    pipeline_class, event_class
):
    cls = pipeline_class(
        event_class().__name__,
        items=InputDataField(data_type=list, batch_size=2),
        label=InputDataField(data_type=str, required=False),
    )

    assert [n for n, _ in cls.get_non_batch_fields()] == ["label"]
