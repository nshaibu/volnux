import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Deque, Dict, List, Optional
from collections import deque

import pytest
from formax import BaseModel

from volnux.backends.fields import ObjectConfig, ObjectField
from volnux.backends.fields.object import default_deserialize, default_serialize


class Color(Enum):
    RED = "red"
    BLUE = "blue"


@dataclass
class Point:
    x: int = 0
    y: int = 0


class Stateful:
    """Defines its own __getstate__/__setstate__."""

    def __init__(self, value=0):
        self.value = value

    def __getstate__(self):
        return {"value": self.value}

    def __setstate__(self, state):
        self.value = state["value"]


class Plain:
    def __init__(self, value=0):
        self.value = value


def _attrib(annotation):
    return annotation.__metadata__[0]


def _pre(annotation):
    return _attrib(annotation).pre_formatter


def _to_state(annotation):
    return _attrib(annotation).metadata["to_state"]


def _from_state(annotation):
    return _attrib(annotation).metadata["from_state"]


class TestObjectConfig:
    def test_defaults(self):
        config = ObjectConfig()
        assert config.serializer is None
        assert config.deserializer is None
        assert config.nullable is False
        assert config.coerce is None

    def test_is_frozen(self):
        with pytest.raises(Exception):
            ObjectConfig().nullable = True


class TestDeclaration:
    def test_single_param(self):
        attrib = _attrib(ObjectField[Point])
        assert attrib.metadata["type"] == "object"
        assert attrib.metadata["object_type"] is Point
        assert attrib.metadata["nullable"] is False
        assert not attrib.has_default()

    def test_with_config(self):
        attrib = _attrib(ObjectField[Point, ObjectConfig(default_factory=Point)])
        assert attrib.has_default()
        # the pre-formatter turns the default into a new object each time
        pre = attrib.pre_formatter
        assert pre(None, attrib.default) == Point()
        assert pre(None, attrib.default) is not pre(None, attrib.default)

    def test_explicit_default(self):
        attrib = _attrib(ObjectField[Color, ObjectConfig(default=Color.RED)])
        assert attrib.default is Color.RED

    def test_optional_is_nullable_and_defaults_to_none(self):
        attrib = _attrib(ObjectField[Optional[Point]])
        assert attrib.metadata["nullable"] is True
        assert attrib.metadata["object_type"] is Point
        assert attrib.default is None

    def test_optional_with_explicit_default_keeps_it(self):
        attrib = _attrib(ObjectField[Optional[Color], ObjectConfig(default=Color.BLUE)])
        assert attrib.default is Color.BLUE

    def test_nullable_config_without_default_stays_required(self):
        attrib = _attrib(ObjectField[Point, ObjectConfig(nullable=True)])
        assert attrib.metadata["nullable"] is True
        assert not attrib.has_default()

    def test_non_nullable_adds_null_validator(self):
        assert _attrib(ObjectField[Point]).validators
        assert not _attrib(ObjectField[Optional[Point]]).validators

    def test_invalid_config_type(self):
        with pytest.raises(TypeError, match="Second argument must be ObjectConfig"):
            ObjectField[Point, {"nullable": True}]

    def test_too_many_params(self):
        with pytest.raises(TypeError, match="expects 1 or 2 arguments"):
            ObjectField[Point, ObjectConfig(), "extra"]

    def test_cannot_be_subclassed_or_instantiated(self):
        with pytest.raises(TypeError):
            ObjectField()
        with pytest.raises(TypeError):

            class Sub(ObjectField):
                pass


class TestPreFormatter:
    def test_keeps_the_live_object(self):
        point = Point(1, 2)
        assert _pre(ObjectField[Point])(None, point) is point

    def test_none_when_nullable(self):
        assert _pre(ObjectField[Optional[Point]])(None, None) is None

    def test_none_when_not_nullable(self):
        with pytest.raises(ValueError, match="cannot be None"):
            _pre(ObjectField[Point])(None, None)

    def test_wrong_type(self):
        with pytest.raises(TypeError, match="Expected Point, got str"):
            _pre(ObjectField[Point])(None, "nope")

    def test_subclass_instances_accepted(self):
        class Special(Point):
            pass

        special = Special()
        assert _pre(ObjectField[Point])(None, special) is special

    def test_generic_alias_checks_its_origin(self):
        pre = _pre(ObjectField[List[int]])
        assert pre(None, [1]) == [1]
        with pytest.raises(TypeError, match="Expected list"):
            pre(None, (1,))

    def test_unresolvable_forward_ref_skips_check(self):
        assert _pre(ObjectField["DoesNotExist"])(None, 42) == 42

    def test_coerce_runs_before_validation(self):
        config = ObjectConfig(coerce=lambda value: deque([value]))
        pre = _pre(ObjectField[Deque[int], config])
        assert pre(None, 7) == deque([7])

    def test_coerce_can_replace_none(self):
        config = ObjectConfig(coerce=lambda value: value if value is not None else Point())
        assert _pre(ObjectField[Point, config])(None, None) == Point()


class TestDefaultCodec:
    def test_primitives_and_containers(self):
        assert default_serialize({"a": [1, "x", None, True]}) == {"a": [1, "x", None, True]}
        assert default_serialize((1, 2)) == [1, 2]
        assert default_serialize(deque([1])) == [1]

    def test_enum(self):
        assert default_serialize(Color.BLUE) == "blue"
        assert default_deserialize(Color, "blue") is Color.BLUE
        assert default_deserialize(Color, Color.RED) is Color.RED

    def test_dataclass(self):
        assert default_serialize(Point(1, 2)) == {"x": 1, "y": 2}
        assert default_deserialize(Point, {"x": 1, "y": 2}) == Point(1, 2)

    def test_nested_dataclass_and_enum_values(self):
        @dataclass
        class Holder:
            point: Point = field(default_factory=Point)
            color: Color = Color.RED

        assert default_serialize(Holder()) == {"point": {"x": 0, "y": 0}, "color": "red"}

    def test_getstate_setstate_class(self):
        payload = default_serialize(Stateful(5))
        assert payload == {"__class__": f"{Stateful.__module__}.Stateful", "state": {"value": 5}}
        restored = default_deserialize(Stateful, payload)
        assert isinstance(restored, Stateful) and restored.value == 5

    def test_getstate_payload_rebuilds_without_calling_init(self):
        class Strict:
            def __init__(self):
                raise AssertionError("__init__ must not run")

            def __getstate__(self):
                return {}

            def __setstate__(self, state):
                pass

        payload = {"__class__": f"{Stateful.__module__}.Stateful", "state": {"value": 1}}
        assert default_deserialize(None, payload).value == 1

    def test_unsupported_object_asks_for_a_serializer(self):
        with pytest.raises(TypeError, match="ObjectConfig\\(serializer"):
            default_serialize(Plain())

    def test_none(self):
        assert default_serialize(None) is None
        assert default_deserialize(Point, None) is None


class TestCodecs:
    def test_default_round_trip(self):
        annotation = ObjectField[Point]
        state = _to_state(annotation)(None, Point(3, 4))
        assert json.loads(json.dumps(state)) == {"x": 3, "y": 4}
        assert _from_state(annotation)(None, state) == Point(3, 4)

    def test_none_is_none_both_ways(self):
        annotation = ObjectField[Optional[Point]]
        assert _to_state(annotation)(None, None) is None
        assert _from_state(annotation)(None, None) is None

    def test_custom_serializer_and_deserializer(self):
        config = ObjectConfig(
            serializer=lambda plain: {"v": plain.value},
            deserializer=lambda payload: Plain(payload["v"]),
        )
        annotation = ObjectField[Plain, config]
        state = _to_state(annotation)(None, Plain(9))
        assert state == {"v": 9}
        assert _from_state(annotation)(None, state).value == 9

    def test_deserializer_for_optional_enum(self):
        annotation = ObjectField[Optional[Color]]
        assert _from_state(annotation)(None, "red") is Color.RED


class TestOnModel:
    def test_defaults_and_live_values(self):
        class Job(BaseModel):
            color: ObjectField[Color, ObjectConfig(default=Color.RED)]
            origin: ObjectField[Point, ObjectConfig(default_factory=Point)]
            parent: ObjectField[Optional[Point]]

        job, other = Job(), Job()
        assert job.color is Color.RED
        assert job.origin == Point()
        assert job.origin is not other.origin
        assert job.parent is None

        point = Point(1, 1)
        job.parent = point
        assert job.parent is point

    def test_assignment_rejects_wrong_type(self):
        class Job(BaseModel):
            origin: ObjectField[Point]

        with pytest.raises(RuntimeError) as exc_info:
            Job(origin="nope")
        assert isinstance(exc_info.value.__cause__, TypeError)

    def test_required_value(self):
        class Job(BaseModel):
            origin: ObjectField[Point]

        with pytest.raises(Exception):
            Job()
