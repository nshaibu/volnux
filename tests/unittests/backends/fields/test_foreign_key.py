import logging
from typing import ForwardRef, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from formax import Attrib, BaseModel

from volnux.backends.fields import FKConfig, FKConstraint, ForeignKeyField, OnDelete
from volnux.backends.fields import foreign_key as fk_module
from volnux.backends.fields.foreign_key import (
    ForeignKey,
    _build_cascade_step,
    _build_protect_step,
    _build_set_default_step,
    _build_set_null_step,
    _check_native_fk_for_instance,
)
from volnux.exceptions import ObjectProtectedError
from volnux.utils import get_obj_klass_import_str


class Target:
    def __init__(self, id, name=""):
        self.id = id
        self.name = name


class Other:
    def __init__(self, id):
        self.id = id


class FakeRecord:
    def __init__(self, id, **fields):
        self.id = id
        for key, value in fields.items():
            setattr(self, key, value)
        self.save = AsyncMock()
        self.delete = AsyncMock()


class FakeQuerySet:
    """Async-iterable stand-in for the result of ``Model.filter()``."""

    def __init__(self, records):
        self.records = list(records)
        self.bulk_delete = AsyncMock()
        self.bulk_update = AsyncMock()

    async def count(self):
        return len(self.records)

    def __aiter__(self):
        async def gen():
            for record in self.records:
                yield record

        return gen()

    def __bool__(self):
        return bool(self.records)


def make_model(records=(), name="Referencing"):
    """Build a fake referencing model whose ``filter`` returns ``records``."""
    queryset = FakeQuerySet(records)
    model = MagicMock()
    model.__name__ = name
    model.filter = AsyncMock(return_value=queryset)
    return model, queryset


def _attrib(annotation):
    return annotation.__metadata__[0]


def _to_state(annotation, instance=None):
    return _attrib(annotation).metadata["to_state"]


def _from_state(annotation):
    return _attrib(annotation).metadata["from_state"]


class TestFKConfig:
    def test_defaults(self):
        config = FKConfig()
        assert config.reverse_name is None
        assert config.on_delete == OnDelete.PROTECT
        assert config.nullable is True
        assert config.constraint == FKConstraint.AUTO

    def test_is_frozen(self):
        with pytest.raises(Exception):
            FKConfig().nullable = False


class TestEnums:
    def test_on_delete_values(self):
        assert OnDelete("cascade") is OnDelete.CASCADE
        assert OnDelete.SET_NULL == "set_null"
        assert {m.value for m in OnDelete} == {
            "cascade",
            "set_null",
            "protect",
            "set_default",
            "do_nothing",
        }

    def test_constraint_values(self):
        assert {m.value for m in FKConstraint} == {
            "auto",
            "native",
            "software",
            "both",
        }


class TestForeignKey:
    def test_unresolved_init(self):
        fk = ForeignKey(object_id="1", model_path="app.Target")
        assert fk.object_id == "1"
        assert fk.model_path == "app.Target"
        assert fk.backend_alias == "default"
        assert fk.id == "1"
        assert not fk.is_resolved
        assert fk._is_native_fk is False

    def test_resolved_init(self):
        target = Target("1")
        fk = ForeignKey(
            object_id="1",
            model_path="app.Target",
            backend_alias="replica",
            is_native_fk=True,
            resolved_instance=target,
        )
        assert fk.is_resolved
        assert fk.backend_alias == "replica"
        assert fk._is_native_fk is True

    def test_attribute_proxy_when_resolved(self):
        fk = ForeignKey("1", "app.Target", resolved_instance=Target("1", name="x"))
        assert fk.name == "x"

    def test_attribute_access_when_unresolved(self):
        fk = ForeignKey("1", "app.Target")
        with pytest.raises(AttributeError, match="not yet resolved"):
            fk.name

    def test_attribute_access_when_resolved_to_nothing(self):
        fk = ForeignKey("1", "app.Target")
        fk._resolved = True
        with pytest.raises(ValueError, match="unresolved ForeignKey"):
            fk.name

    def test_private_attribute_access(self):
        fk = ForeignKey("1", "app.Target", resolved_instance=Target("1"))
        with pytest.raises(AttributeError):
            fk._missing

    def test_repr_and_str_unresolved(self):
        fk = ForeignKey("1", "app.Target")
        assert repr(fk) == "<ForeignKey: app.Target#1 [lazy]>"
        assert str(fk) == "app.Target#1"

    def test_repr_resolved_to_nothing(self):
        fk = ForeignKey("1", "app.Target")
        fk._resolved = True
        assert repr(fk) == "<ForeignKey: app.Target#1 [resolved]>"

    def test_repr_and_str_resolved(self):
        target = Target("1")
        fk = ForeignKey("1", "app.Target", resolved_instance=target)
        assert repr(fk) == repr(target)
        assert str(fk) == str(target)

    def test_equality(self):
        assert ForeignKey("1", "a.T") == ForeignKey("1", "a.T")
        assert ForeignKey("1", "a.T") != ForeignKey("2", "a.T")
        assert ForeignKey("1", "a.T") != ForeignKey("1", "b.T")
        assert ForeignKey("1", "a.T") == Target(1)
        assert ForeignKey("1", "a.T") != Target(2)
        assert ForeignKey("1", "a.T") != "1"

    def test_hash(self):
        assert hash(ForeignKey("1", "a.T")) == hash(ForeignKey("1", "a.T"))
        assert len({ForeignKey("1", "a.T"), ForeignKey("1", "a.T")}) == 1

    def test_bool(self):
        assert ForeignKey("1", "a.T")
        assert not ForeignKey("", "a.T")


class TestForeignKeySerialization:
    def test_serialize_none(self):
        assert ForeignKey.serialize(None) is None

    def test_serialize_foreign_key(self):
        assert ForeignKey.serialize(ForeignKey("1", "a.T")) == {
            "type": "foreignkey",
            "object_id": "1",
        }

    def test_serialize_model_instance(self):
        assert ForeignKey.serialize(Target(7)) == {
            "type": "foreignkey",
            "object_id": "7",
        }

    def test_serialize_rejects_objects_without_id(self):
        with pytest.raises(TypeError, match="Cannot serialize str"):
            ForeignKey.serialize("nope")

    def test_deserialize_none(self):
        assert ForeignKey.deserialize(None) is None

    def test_deserialize_foreign_key_passthrough(self):
        fk = ForeignKey("1", "a.T")
        assert ForeignKey.deserialize(fk) is fk

    def test_deserialize_string(self):
        target = Target("1")
        fk = ForeignKey.deserialize(
            "1",
            model_path="a.T",
            backend_alias="replica",
            is_native_fk=True,
            resolved_instance=target,
        )
        assert fk.object_id == "1"
        assert fk.model_path == "a.T"
        assert fk.backend_alias == "replica"
        assert fk._is_native_fk is True
        assert fk.is_resolved

    def test_deserialize_unexpected_type(self, caplog):
        with caplog.at_level(logging.WARNING, logger=fk_module.__name__):
            assert ForeignKey.deserialize(123) is None
        assert "Unexpected ForeignKey value type" in caplog.text

    def test_deserialize_serialized_payload(self):
        fk = ForeignKey.deserialize(
            {"type": "foreignkey", "object_id": "9"},
            model_path="a.T",
            backend_alias="replica",
            is_native_fk=True,
        )
        assert fk.object_id == "9"
        assert fk.model_path == "a.T"
        assert fk.backend_alias == "replica"
        assert fk._is_native_fk is True

    def test_deserialize_payload_coerces_object_id_to_str(self):
        fk = ForeignKey.deserialize({"type": "foreignkey", "object_id": 9})
        assert fk.object_id == "9"

    def test_deserialize_payload_missing_object_id(self, caplog):
        with caplog.at_level(logging.WARNING, logger=fk_module.__name__):
            assert ForeignKey.deserialize({"type": "foreignkey"}) is None
        assert "missing 'object_id'" in caplog.text

    @pytest.mark.parametrize("value", [Target("1"), ForeignKey("1", "a.T")])
    def test_serialize_deserialize_round_trip(self, value):
        serialized = ForeignKey.serialize(value)
        restored = ForeignKey.deserialize(serialized, model_path="a.T")
        assert restored == ForeignKey("1", "a.T")
        assert ForeignKey.serialize(restored) == serialized


class TestForeignKeyResolveAsync:
    @pytest.mark.asyncio
    async def test_already_resolved(self):
        target = Target("1")
        fk = ForeignKey("1", "a.T", resolved_instance=target)
        assert await fk.resolve_async() is target

    @pytest.mark.asyncio
    async def test_empty_model_path(self):
        fk = ForeignKey("1", "")
        assert await fk.resolve_async() is None
        assert fk._resolved

    @pytest.mark.asyncio
    async def test_import_failure(self):
        fk = ForeignKey("1", "does.not.Exist")
        assert await fk.resolve_async() is None
        assert fk._resolved
        assert not fk.is_resolved

    @pytest.mark.asyncio
    async def test_successful_resolution_is_cached(self):
        target = Target("1")
        model_class = MagicMock()
        model_class.get = AsyncMock(return_value=target)
        fk = ForeignKey("1", "a.T")
        with patch.object(fk_module, "import_string", return_value=model_class):
            assert await fk.resolve_async() is target
            assert await fk.resolve_async() is target
        model_class.get.assert_awaited_once_with("1")
        assert fk.is_resolved
        assert fk.name == ""

    @pytest.mark.asyncio
    async def test_lookup_failure(self):
        model_class = MagicMock()
        model_class.get = AsyncMock(side_effect=LookupError("missing"))
        fk = ForeignKey("1", "a.T")
        with patch.object(fk_module, "import_string", return_value=model_class):
            assert await fk.resolve_async() is None
        assert fk._resolved
        assert not fk.is_resolved


class TestForeignKeyFieldDeclaration:
    def test_cannot_instantiate(self):
        with pytest.raises(TypeError, match="cannot be instantiated"):
            ForeignKeyField()

    def test_cannot_subclass(self):
        with pytest.raises(TypeError, match="Cannot subclass"):

            class Sub(ForeignKeyField):
                pass

    def test_single_param_uses_default_config(self):
        metadata = dict(_attrib(ForeignKeyField[Target]).metadata)
        assert metadata.pop("to_state") and metadata.pop("from_state")
        assert metadata == {
            "type": "foreignkey",
            "target_model": Target,
            "reverse_name": None,
            "on_delete": OnDelete.PROTECT,
            "nullable": True,
            "constraint": FKConstraint.AUTO,
        }

    def test_required_by_default(self):
        assert not _attrib(ForeignKeyField[Target]).has_default()

    def test_optional_target_defaults_to_none(self):
        attrib = _attrib(ForeignKeyField[Optional[Target]])
        assert attrib.default is None
        assert attrib.metadata["target_model"] is Target
        assert attrib.metadata["nullable"] is True

    def test_optional_forward_ref_target(self):
        attrib = _attrib(ForeignKeyField[Optional["Target"]])
        assert attrib.default is None
        assert isinstance(attrib.metadata["target_model"], ForwardRef)

    def test_optional_makes_set_null_valid(self):
        ForeignKeyField[
            Optional[Target], FKConfig(on_delete=OnDelete.SET_NULL, nullable=False)
        ]

    def test_explicit_default(self):
        fallback = Target(9)
        attrib = _attrib(ForeignKeyField[Target, FKConfig(default=fallback)])
        assert attrib.default is fallback

    def test_config_reflected_in_metadata(self):
        config = FKConfig(
            reverse_name="children",
            on_delete=OnDelete.CASCADE,
            nullable=False,
            constraint=FKConstraint.NATIVE,
        )
        metadata = _attrib(ForeignKeyField[Target, config]).metadata
        assert metadata["reverse_name"] == "children"
        assert metadata["on_delete"] == OnDelete.CASCADE
        assert metadata["nullable"] is False
        assert metadata["constraint"] == FKConstraint.NATIVE

    def test_string_target_becomes_forward_ref(self):
        target = _attrib(ForeignKeyField["Target"]).metadata["target_model"]
        assert isinstance(target, ForwardRef)
        assert target.__forward_arg__ == "Target"

    def test_dotted_string_target_keeps_module(self):
        target = _attrib(ForeignKeyField["app.models.Target"]).metadata["target_model"]
        assert target.__forward_arg__ == "Target"
        assert target.__forward_module__ == "app.models"

    def test_invalid_config_type(self):
        with pytest.raises(TypeError, match="Second argument must be FKConfig"):
            ForeignKeyField[Target, {"nullable": False}]

    def test_too_many_params(self):
        with pytest.raises(TypeError, match="expects 1 or 2 arguments"):
            ForeignKeyField[Target, FKConfig(), "extra"]

    def test_set_null_requires_nullable(self):
        with pytest.raises(ValueError, match="SET_NULL requires nullable=True"):
            ForeignKeyField[
                Target, FKConfig(on_delete=OnDelete.SET_NULL, nullable=False)
            ]

    def test_non_nullable_adds_null_validator(self):
        assert _attrib(ForeignKeyField[Target, FKConfig(nullable=False)]).validators
        assert not _attrib(ForeignKeyField[Target]).validators


class TestForeignKeyFieldPreFormatter:
    """The field keeps what was assigned; serialization happens in to_state."""

    def test_none(self):
        assert _attrib(ForeignKeyField[Target]).pre_formatter(None, None) is None

    def test_model_instance_stays_live(self):
        pre = _attrib(ForeignKeyField[Target]).pre_formatter
        target = Target(3)
        assert pre(None, target) is target

    def test_foreign_key_stays_a_reference(self):
        pre = _attrib(ForeignKeyField[Target]).pre_formatter
        reference = ForeignKey("3", "a.T")
        assert pre(None, reference) is reference

    def test_payload_dict_accepted(self):
        pre = _attrib(ForeignKeyField[Target]).pre_formatter
        payload = {"type": "foreignkey", "object_id": "3"}
        assert pre(None, payload) is payload

    def test_wrong_type(self):
        pre = _attrib(ForeignKeyField[Target]).pre_formatter
        with pytest.raises(TypeError, match="Expected Target or ForeignKey"):
            pre(None, Other(1))

    def test_unresolvable_forward_ref_skips_type_check(self):
        pre = _attrib(ForeignKeyField["DoesNotExist"]).pre_formatter
        other = Other(1)
        assert pre(None, other) is other


class TestForeignKeyFieldToState:
    def test_none(self):
        assert _to_state(ForeignKeyField[Target])(None, None) is None

    def test_model_instance(self):
        to_state = _to_state(ForeignKeyField[Target])
        assert to_state(None, Target(3)) == {"type": "foreignkey", "object_id": "3"}

    def test_foreign_key(self):
        to_state = _to_state(ForeignKeyField[Target])
        assert to_state(None, ForeignKey("3", "a.T")) == {
            "type": "foreignkey",
            "object_id": "3",
        }

    def test_payload_passes_through(self):
        to_state = _to_state(ForeignKeyField[Target])
        payload = {"type": "foreignkey", "object_id": "3"}
        assert to_state(None, payload) == payload

    def test_object_without_id_is_rejected(self):
        to_state = _to_state(ForeignKeyField["DoesNotExist"])
        with pytest.raises(TypeError, match="Cannot serialize"):
            to_state(None, "no-id")


class TestForeignKeyFieldFromState:
    def test_none(self):
        assert _from_state(ForeignKeyField[Target])(None, None) is None

    def test_plain_id(self):
        from_state = _from_state(ForeignKeyField[Target])
        fk = from_state(object(), "5")
        assert isinstance(fk, ForeignKey)
        assert fk.object_id == "5"
        assert fk.model_path == get_obj_klass_import_str(Target)
        assert fk._is_native_fk is False
        assert not fk.is_resolved

    def test_serialized_payload(self):
        annotation = ForeignKeyField[Target]
        payload = _to_state(annotation)(None, Target(5))
        fk = _from_state(annotation)(object(), payload)
        assert isinstance(fk, ForeignKey)
        assert fk.object_id == "5"
        assert fk.model_path == get_obj_klass_import_str(Target)

    def test_forward_ref_model_path(self):
        from_state = _from_state(ForeignKeyField["app.models.Target"])
        assert from_state(object(), "5").model_path == "app.models.Target"

    def test_module_less_forward_ref_uses_the_owning_models_module(self):
        from_state = _from_state(ForeignKeyField["Target"])
        owner = Target(1)  # defined in this module
        assert from_state(owner, "5").model_path == f"{Target.__module__}.Target"

    def test_join_pushdown_tuple(self):
        from_state = _from_state(ForeignKeyField[Target])
        hydrated = Target("5", name="joined")
        with patch.object(fk_module, "hydrate_model", return_value=hydrated) as hydrate:
            fk = from_state(object(), ("5", {"id": "5", "name": "joined"}))
        hydrate.assert_called_once_with(Target, {"id": "5", "name": "joined"})
        assert fk.is_resolved
        assert fk._is_native_fk is True
        assert fk.name == "joined"

    def test_join_pushdown_without_data(self):
        from_state = _from_state(ForeignKeyField[Target])
        fk = from_state(object(), ("5", None))
        assert fk.object_id == "5"
        assert fk._is_native_fk is True
        assert not fk.is_resolved

    def test_join_pushdown_hydration_failure(self):
        from_state = _from_state(ForeignKeyField[Target])
        with patch.object(fk_module, "hydrate_model", side_effect=ValueError("bad")):
            fk = from_state(object(), ("5", {"id": "5"}))
        assert fk.object_id == "5"
        assert not fk.is_resolved

    def test_join_pushdown_unresolvable_target(self):
        from_state = _from_state(ForeignKeyField["DoesNotExist"])
        with patch.object(fk_module, "hydrate_model") as hydrate:
            fk = from_state(object(), ("5", {"id": "5"}))
        hydrate.assert_not_called()
        assert not fk.is_resolved


class TestCheckNativeFk:
    def _instance(self, backend):
        model_class = type("NativeCheckModel", (), {"_backend_store": backend})
        return model_class()

    def test_no_field_name(self):
        assert _check_native_fk_for_instance(object(), None) is False

    def test_no_instance(self):
        assert _check_native_fk_for_instance(None, "parent") is False

    def test_no_backend(self):
        assert _check_native_fk_for_instance(self._instance(None), "parent") is False

    def test_backend_without_verifier(self):
        instance = self._instance(object())
        assert _check_native_fk_for_instance(instance, "parent") is False

    def test_backend_verifier_result_is_cached(self):
        backend = MagicMock()
        backend.verify_native_fk_exists.return_value = True
        instance = self._instance(backend)
        assert _check_native_fk_for_instance(instance, "parent") is True
        assert _check_native_fk_for_instance(instance, "parent") is True
        backend.verify_native_fk_exists.assert_called_once_with(
            type(instance), "parent"
        )
        assert type(instance)._fk_native_cache_parent is True

    def test_backend_verifier_error(self):
        backend = MagicMock()
        backend.verify_native_fk_exists.side_effect = RuntimeError("boom")
        assert _check_native_fk_for_instance(self._instance(backend), "p") is False

    def test_missing_backend_attribute(self):
        instance = type("NoBackendModel", (), {})()
        assert _check_native_fk_for_instance(instance, "parent") is False
        assert type(instance)._fk_native_cache_parent is False


class TestOperationHandler:
    async def _run(self, action, model, attrib=None):
        await action.operation_handler(
            obj_id="1",
            obj_class_name="Target",
            referencing_model=model,
            field_name="target",
            attrib=attrib or Attrib(),
        )

    @pytest.mark.asyncio
    async def test_filters_on_object_id_column(self):
        model, _ = make_model()
        await self._run(OnDelete.CASCADE, model)
        model.filter.assert_awaited_once_with(target_object_id="1")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("action", list(OnDelete))
    async def test_no_referring_objects_is_noop(self, action):
        model, queryset = make_model()
        await self._run(action, model)
        queryset.bulk_delete.assert_not_awaited()
        queryset.bulk_update.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_protect(self):
        model, _ = make_model([FakeRecord("a"), FakeRecord("b")])
        with pytest.raises(ObjectProtectedError, match="referenced by 2"):
            await self._run(OnDelete.PROTECT, model)

    @pytest.mark.asyncio
    async def test_cascade_bulk(self):
        records = [FakeRecord("a")]
        model, queryset = make_model(records)
        await self._run(OnDelete.CASCADE, model)
        queryset.bulk_delete.assert_awaited_once()
        records[0].delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cascade_falls_back_to_per_object_delete(self):
        records = [FakeRecord("a"), FakeRecord("b")]
        model, queryset = make_model(records)
        queryset.bulk_delete.side_effect = NotImplementedError
        await self._run(OnDelete.CASCADE, model)
        for record in records:
            record.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_null_bulk(self):
        model, queryset = make_model([FakeRecord("a", target="1")])
        await self._run(OnDelete.SET_NULL, model)
        queryset.bulk_update.assert_awaited_once_with(target=None)

    @pytest.mark.asyncio
    async def test_set_null_falls_back_to_per_object_save(self):
        records = [FakeRecord("a", target="1")]
        model, queryset = make_model(records)
        queryset.bulk_update.side_effect = NotImplementedError
        await self._run(OnDelete.SET_NULL, model)
        assert records[0].target is None
        records[0].save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_default_with_default(self):
        records = [FakeRecord("a", target="1")]
        model, _ = make_model(records)
        attrib = MagicMock()
        attrib.get_default.return_value = "fallback"
        await self._run(OnDelete.SET_DEFAULT, model, attrib)
        assert records[0].target == "fallback"
        records[0].save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_default_without_default(self, caplog):
        records = [FakeRecord("a", target="1")]
        model, _ = make_model(records)
        with caplog.at_level(logging.WARNING, logger=fk_module.__name__):
            await self._run(OnDelete.SET_DEFAULT, model, Attrib())
        assert "no default defined" in caplog.text
        assert records[0].target == "1"
        records[0].save.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_do_nothing_logs_dangling_reference(self, caplog):
        records = [FakeRecord("a")]
        model, queryset = make_model(records)
        with caplog.at_level(logging.WARNING, logger=fk_module.__name__):
            await self._run(OnDelete.DO_NOTHING, model)
        assert "Dangling reference" in caplog.text
        queryset.bulk_delete.assert_not_awaited()
        records[0].delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_string_model_is_imported(self):
        model, queryset = make_model([FakeRecord("a")])
        with patch.object(fk_module, "import_string", return_value=model) as imp:
            await self._run(OnDelete.CASCADE, "app.models.Referencing")
        imp.assert_called_once_with("app.models.Referencing")
        queryset.bulk_delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_string_model_import_failure(self):
        with pytest.raises(ValueError, match="Failed to import model class"):
            await self._run(OnDelete.CASCADE, "does.not.Exist")


class TestSagaSteps:
    @pytest.mark.asyncio
    async def test_cascade_step_uses_model_bulk_delete(self):
        records = [FakeRecord("a")]
        records[0].__getstate__ = MagicMock(return_value={"id": "a"})
        model, _ = make_model(records)
        model.bulk_delete = AsyncMock()
        action, _ = _build_cascade_step(model, ["a"], "Target(1)")
        await action()
        model.filter.assert_awaited_once_with(id__in=["a"])
        model.bulk_delete.assert_awaited_once_with(["a"])

    @pytest.mark.asyncio
    async def test_cascade_compensation_restores_snapshots(self):
        records = [FakeRecord("a")]
        records[0].__getstate__ = MagicMock(return_value={"id": "a"})
        queryset = FakeQuerySet(records)
        restored = []

        class Referencing:
            filter = AsyncMock(return_value=queryset)
            bulk_delete = AsyncMock()

            def __setstate__(self, state):
                self.state = state
                restored.append(self)

            save = AsyncMock()

        action, compensation = _build_cascade_step(Referencing, ["a"], "Target(1)")
        await action()
        await compensation()
        assert len(restored) == 1
        assert restored[0].state == {"id": "a"}
        Referencing.save.assert_awaited_once_with(force_insert=True)

    @pytest.mark.asyncio
    async def test_cascade_compensation_without_action_is_noop(self):
        model, _ = make_model()
        _, compensation = _build_cascade_step(model, [], "Target(1)")
        await compensation()

    @pytest.mark.asyncio
    async def test_cascade_step_falls_back_to_per_object_delete(self):
        records = [FakeRecord("a"), FakeRecord("b")]
        for record in records:
            record.__getstate__ = MagicMock(return_value={"id": record.id})
        model, _ = make_model(records)
        del model.bulk_delete
        action, _ = _build_cascade_step(model, ["a", "b"], "Target(1)")
        await action()
        for record in records:
            record.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_null_step(self):
        records = [FakeRecord("a", target="1"), FakeRecord("b", target="1")]
        model, _ = make_model(records)
        action, _ = _build_set_null_step(model, ["a", "b"], "target", "Target(1)")
        await action()
        for record in records:
            assert record.target is None
            record.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_null_compensation_restores_values(self):
        records = [FakeRecord("a", target="1"), FakeRecord("b", target="2")]
        model, _ = make_model(records)
        action, compensation = _build_set_null_step(
            model, ["a", "b"], "target", "Target(1)"
        )
        await action()
        await compensation()
        assert [r.target for r in records] == ["1", "2"]
        model.filter.assert_awaited_with(id__in=["a", "b"])
        for record in records:
            assert record.save.await_count == 2

    @pytest.mark.asyncio
    async def test_set_null_compensation_without_action_is_noop(self):
        model, _ = make_model()
        _, compensation = _build_set_null_step(model, [], "target", "Target(1)")
        await compensation()
        model.filter.assert_awaited_once_with(id__in=[])

    @pytest.mark.asyncio
    async def test_set_default_step(self):
        records = [FakeRecord("a", target="1")]
        model, _ = make_model(records)
        action, _ = _build_set_default_step(
            model, ["a"], "target", "fallback", "Target(1)"
        )
        await action()
        assert records[0].target == "fallback"
        records[0].save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_set_default_compensation_restores_values(self):
        records = [FakeRecord("a", target="1"), FakeRecord("b", target=None)]
        model, _ = make_model(records)
        action, compensation = _build_set_default_step(
            model, ["a", "b"], "target", "fallback", "Target(1)"
        )
        await action()
        assert [r.target for r in records] == ["fallback", "fallback"]
        await compensation()
        assert [r.target for r in records] == ["1", None]
        for record in records:
            assert record.save.await_count == 2

    @pytest.mark.asyncio
    async def test_set_default_compensation_without_action_is_noop(self):
        model, _ = make_model()
        _, compensation = _build_set_default_step(
            model, [], "target", "fallback", "Target(1)"
        )
        await compensation()

    @pytest.mark.asyncio
    async def test_protect_step_raises_when_referenced(self):
        model, _ = make_model([FakeRecord("a")])
        action, _ = _build_protect_step(model, "target", "1", "Target")
        with pytest.raises(ObjectProtectedError, match="referenced by 1"):
            await action()
        model.filter.assert_awaited_once_with(target_object_id="1")

    @pytest.mark.asyncio
    async def test_protect_step_passes_when_unreferenced(self):
        model, _ = make_model()
        action, compensation = _build_protect_step(model, "target", "1", "Target")
        await action()
        await compensation()

    @pytest.mark.asyncio
    async def test_protect_step_does_not_double_suffix(self):
        model, _ = make_model()
        action, _ = _build_protect_step(model, "target_object_id", "1", "Target")
        await action()
        model.filter.assert_awaited_once_with(target_object_id="1")


class TestForeignKeyFieldOnModel:
    def test_assignment_keeps_the_live_object(self):
        class Holder(BaseModel):
            target: ForeignKeyField[Target]

        target = Target(5)
        holder = Holder(target=target)
        assert holder.target is target

        holder.target = None
        assert holder.target is None

    def test_optional_target_defaults_to_none(self):
        class Holder(BaseModel):
            target: ForeignKeyField[Optional[Target]]

        assert Holder().target is None

    def test_required_target_must_be_given(self):
        class Holder(BaseModel):
            target: ForeignKeyField[Target]

        with pytest.raises(Exception):
            Holder()

    def test_assignment_rejects_wrong_type(self):
        class Holder(BaseModel):
            target: ForeignKeyField[Target]

        with pytest.raises(RuntimeError) as exc_info:
            Holder(target=Other(1))
        assert isinstance(exc_info.value.__cause__, TypeError)
