import sys
import logging
import asyncio
from enum import Enum
from dataclasses import dataclass
from typing import (
    Any,
    Dict,
    Optional,
    Type,
    TYPE_CHECKING,
    Union,
    ForwardRef,
    cast,
    Callable,
    Coroutine,
    List,
    Tuple,
)

from formax import Attrib, MiniAnnotated

from volnux.import_utils import import_string
from volnux.utils import get_obj_klass_import_str
from volnux.exceptions import ObjectProtectedError
from .utils import formax_null_validator
from volnux.utils import hydrate_model
from volnux.backends.db_utils import ModelResolverMixin

if TYPE_CHECKING:
    from volnux.mixins import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)

_MISSING = object()

SagaAction = Callable[[], Coroutine[Any, Any, None]]
SagaCompensation = Callable[[], Coroutine[Any, Any, None]]


def _build_cascade_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    snapshots: Dict[str, Dict[str, Any]] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            snapshots[obj.id] = obj.__getstate__()
        if hasattr(referencing_model, "bulk_delete"):
            await referencing_model.bulk_delete(record_ids)
        else:
            await asyncio.gather(*(obj.delete() async for obj in objects))
        logger.info(
            "Saga CASCADE: deleted %d %s (%s)",
            len(record_ids),
            referencing_model.__name__,
            parent_info,
        )

    async def compensation() -> None:
        for record_id, state in snapshots.items():
            try:
                obj = referencing_model.__new__(referencing_model)
                obj.__setstate__(state)
                await obj.save(force_insert=True)
            except Exception as e:
                logger.error(
                    "Saga CASCADE compensate failed %s(%s): %s",
                    referencing_model.__name__,
                    record_id,
                    e,
                )
        logger.info(
            "Saga CASCADE compensated: restored %d %s",
            len(snapshots),
            referencing_model.__name__,
        )

    return action, compensation


def _build_set_null_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    field_name: str,
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    original_values: Dict[str, Any] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            original_values[obj.id] = getattr(obj, field_name)
            setattr(obj, field_name, None)
        await asyncio.gather(*(obj.save() async for obj in objects))
        logger.info(
            "Saga SET_NULL: cleared %s on %d %s (%s)",
            field_name,
            len(record_ids),
            referencing_model.__name__,
            parent_info,
        )

    async def compensation() -> None:
        objects = await referencing_model.filter(id__in=list(original_values.keys()))
        async for obj in objects:
            original = original_values.get(obj.id)
            if original is not None:
                setattr(obj, field_name, original)
        if objects:
            await asyncio.gather(*(obj.save() async for obj in objects))
        logger.info(
            "Saga SET_NULL compensated: restored %s on %d %s",
            field_name,
            len(original_values),
            referencing_model.__name__,
        )

    return action, compensation


def _build_protect_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    field_name: str,
    obj_id: str,
    obj_class_name: str,
) -> Tuple[SagaAction, SagaCompensation]:
    async def action() -> None:
        filter_key = (
            f"{field_name}_object_id"
            if not field_name.endswith("_object_id")
            else field_name
        )
        referring = await referencing_model.filter(**{filter_key: obj_id})
        count = await referring.count()
        if count > 0:
            raise ObjectProtectedError(
                f"Cannot delete {obj_class_name}({obj_id}): "
                f"referenced by {count} {referencing_model.__name__} objects via '{field_name}'."
            )

    async def compensation() -> None:
        pass

    return action, compensation


def _build_set_default_step(
    referencing_model: Type["KeyValueStoreIntegrationMixin"],
    record_ids: List[str],
    field_name: str,
    default_value: Any,
    parent_info: str,
) -> Tuple[SagaAction, SagaCompensation]:
    original_values: Dict[str, Any] = {}

    async def action() -> None:
        objects = await referencing_model.filter(id__in=record_ids)
        async for obj in objects:
            original_values[obj.id] = getattr(obj, field_name)
            setattr(obj, field_name, default_value)
        await asyncio.gather(*(obj.save() async for obj in objects))
        logger.info(
            "Saga SET_DEFAULT: set %s on %d %s (%s)",
            field_name,
            len(record_ids),
            referencing_model.__name__,
            parent_info,
        )

    async def compensation() -> None:
        objects = await referencing_model.filter(id__in=list(original_values.keys()))
        async for obj in objects:
            original = original_values.get(obj.id)
            setattr(obj, field_name, original)
        if objects:
            await asyncio.gather(*(obj.save() async for obj in objects))
        logger.info(
            "Saga SET_DEFAULT compensated: restored %s on %d %s",
            field_name,
            len(original_values),
            referencing_model.__name__,
        )

    return action, compensation


class OnDelete(str, Enum):
    CASCADE = "cascade"
    SET_NULL = "set_null"
    PROTECT = "protect"
    SET_DEFAULT = "set_default"
    DO_NOTHING = "do_nothing"

    async def operation_handler(
        self,
        obj_id: str,
        obj_class_name: str,
        referencing_model: Union[Type["KeyValueStoreIntegrationMixin"], str],
        field_name: str,
        attrib: Attrib,
    ) -> None:
        if isinstance(referencing_model, str):
            try:
                referencing_model: Type["KeyValueStoreIntegrationMixin"] = import_string(referencing_model)  # type: ignore[assignment]
            except ImportError as e:
                raise ValueError(
                    f"Failed to import model class '{referencing_model}'"
                ) from e

        filter_key = f"{field_name}_object_id"
        referring_objects = await referencing_model.filter(**{filter_key: obj_id})
        count = await referring_objects.count()

        if count == 0:
            return

        if self == OnDelete.PROTECT:
            raise ObjectProtectedError(
                f"Cannot delete {obj_class_name}({obj_id}): "
                f"referenced by {count} {referencing_model.__name__} objects via '{field_name}'. "
                f"Use on_delete=CASCADE or SET_NULL on the ForeignKeyField."
            )

        elif self == OnDelete.CASCADE:
            try:
                await referring_objects.bulk_delete()
            except Exception:
                async for obj in referring_objects:
                    await obj.delete()
            logger.info(
                "Cascaded delete: %d %s objects due to %s(%s)",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.SET_NULL:
            try:
                await referring_objects.bulk_update(**{field_name: None})
            except Exception:
                async for obj in referring_objects:
                    setattr(obj, field_name, None)
                    await obj.save()
            logger.info(
                "Set NULL on %d %s objects due to %s(%s)",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.SET_DEFAULT:
            default = (
                attrib.get_default() if hasattr(attrib, "get_default") else _MISSING
            )
            if default is _MISSING:
                logger.warning(
                    "Cannot SET_DEFAULT for %s.%s: no default defined",
                    referencing_model.__name__,
                    field_name,
                )
                return
            async for obj in referring_objects:
                setattr(obj, field_name, default)
                await obj.save()
            logger.info(
                "Set default on %d %s objects due to %s(%s)",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
            )

        elif self == OnDelete.DO_NOTHING:
            logger.warning(
                "Dangling reference: %d %s objects still reference deleted %s(%s) via '%s'",
                count,
                referencing_model.__name__,
                obj_class_name,
                obj_id,
                field_name,
            )


class FKConstraint(str, Enum):
    AUTO = "auto"
    NATIVE = "native"
    SOFTWARE = "software"
    BOTH = "both"


@dataclass(frozen=True)
class FKConfig:
    reverse_name: Optional[str] = None
    on_delete: OnDelete = OnDelete.PROTECT
    nullable: bool = True
    constraint: FKConstraint = FKConstraint.AUTO


class ForeignKey:

    __slots__ = (
        "object_id",
        "model_path",
        "backend_alias",
        "_resolved",
        "_instance",
        "_is_native_fk",
    )

    def __init__(
        self,
        object_id: str,
        model_path: str,
        backend_alias: str = "default",
        is_native_fk: bool = False,
        resolved_instance: Optional["KeyValueStoreIntegrationMixin"] = None,
    ):
        self.object_id = object_id
        self.model_path = model_path
        self.backend_alias = backend_alias
        self._is_native_fk = is_native_fk

        if resolved_instance is not None:
            self._resolved = True
            self._instance = resolved_instance
        else:
            self._resolved = False
            self._instance = None

    @property
    def id(self) -> str:
        return self.object_id

    @property
    def is_resolved(self) -> bool:
        return self._resolved and self._instance is not None

    @classmethod
    def serialize(cls, value: Any) -> Optional[Dict[str, Any]]:
        if value is None:
            return None
        if isinstance(value, ForeignKey):
            return {"type": "foreignkey", "object_id": value.object_id}
        if hasattr(value, "id"):
            return {"type": "foreignkey", "object_id": str(value.id)}
        raise TypeError(
            f"Cannot serialize {type(value).__name__} as ForeignKey. "
            f"Expected a KeyValueStoreIntegrationMixin instance or ForeignKey."
        )

    @classmethod
    def deserialize(
        cls,
        value: Any,
        model_path: str = "",
        backend_alias: str = "default",
        is_native_fk: bool = False,
        resolved_instance: Optional["KeyValueStoreIntegrationMixin"] = None,
    ) -> Optional["ForeignKey"]:
        if value is None:
            return None
        if isinstance(value, ForeignKey):
            return value
        if isinstance(value, str):
            return cls(
                object_id=value,
                model_path=model_path,
                backend_alias=backend_alias,
                is_native_fk=is_native_fk,
                resolved_instance=resolved_instance,
            )
        logger.warning("Unexpected ForeignKey value type: %s", type(value).__name__)
        return None

    async def resolve_async(self) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Async resolution of the referenced model instance."""
        if self._resolved:
            return self._instance

        if not self.model_path:
            logger.warning("Cannot resolve ForeignKey with empty model_path")
            self._resolved = True
            return None

        try:
            model_class: "KeyValueStoreIntegrationMixin" = import_string(self.model_path)  # type: ignore[assignment]
        except (ImportError, AttributeError) as e:
            logger.error("Failed to import model class '%s': %s", self.model_path, e)
            self._resolved = True
            return None

        try:
            self._instance = await model_class.get(self.object_id)
        except Exception as e:
            logger.warning(
                "Failed to resolve ForeignKey %s -> %s: %s",
                self.model_path,
                self.object_id,
                e,
            )
            self._instance = None

        self._resolved = True
        return self._instance

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)

        if self._resolved and self._instance is not None:
            return getattr(self._instance, name)

        if not self._resolved:
            raise AttributeError(
                f"ForeignKey not yet resolved. Call 'await fk.resolve_async()' before "
                f"accessing attributes, or use resolve_joins() for automatic resolution."
            )

        raise ValueError(
            f"Cannot access '{name}' on unresolved ForeignKey "
            f"({self.model_path}#{self.object_id})"
        )

    def __repr__(self) -> str:
        if self._resolved and self._instance is not None:
            return repr(self._instance)
        status = "resolved" if self._resolved else "lazy"
        return f"<ForeignKey: {self.model_path}#{self.object_id} [{status}]>"

    def __str__(self) -> str:
        if self._resolved and self._instance is not None:
            return str(self._instance)
        return f"{self.model_path}#{self.object_id}"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ForeignKey):
            return (
                self.object_id == other.object_id
                and self.model_path == other.model_path
            )
        if hasattr(other, "id"):
            return self.object_id == str(other.id)
        return False

    def __hash__(self) -> int:
        return hash((self.object_id, self.model_path))

    def __bool__(self) -> bool:
        return bool(self.object_id)


class ForeignKeyField(ModelResolverMixin):
    """Create a MiniAnnotated field definition for a foreign key reference.

    Args:
        target_model: Optional model class for type hinting and validation.
        nullable: Whether the foreign key can be None.
        reverse_name: Optional name for the reverse relation.
        on_delete: Action to perform when the referenced object is deleted.
        constraint: Constraint type for the foreign key relationship.
                    - AUTO: Native if the same database, software otherwise
                    - NATIVE: Database-level constraint only
                    - SOFTWARE: Software-level constraint only
                    - BOTH: Both native and software constraints

    Returns:
        A MiniAnnotated type annotation suitable for use in Formax models.

    Example:
        >>> class Workflow(GovernanceModel):
        ...     created_by: ForeignKeyField[User]
        ...     approved_by: ForeignKeyField[User, FKConfig(nullable=True, reverse_name='approved_workflows')]
    """

    def __init_subclass__(cls, **kwargs):
        raise TypeError("Cannot subclass ForeignKeyField")

    def __new__(cls, *args, **kwargs):
        raise TypeError("ForeignKeyField cannot be instantiated")

    @classmethod
    def __class_getitem__(
        cls, params: Tuple[Union[Type["KeyValueStoreIntegrationMixin"], str], FKConfig]
    ) -> MiniAnnotated:
        if not isinstance(params, tuple):
            params = (params, FKConfig())

        if len(params) == 1:
            target_model = params[0]
            config = FKConfig()
        elif len(params) == 2:
            target_model, config = params
            if not isinstance(config, FKConfig):
                raise TypeError(
                    f"Second argument must be FKConfig, got {type(config).__name__}"
                )
        else:
            raise TypeError(
                f"ForeignKeyField[...] expects 1 or 2 arguments, got {len(params)}"
            )

        if config.on_delete == OnDelete.SET_NULL and not config.nullable:
            raise ValueError("on_delete=SET_NULL requires nullable=True.")

        validators: List[Any] = []
        if not config.nullable:
            validators.append(formax_null_validator)

        if isinstance(target_model, str):
            try:
                module_path, class_name = target_model.rsplit(".", 1)
            except ValueError:
                module_path, class_name = None, target_model
            target_model = ForwardRef(class_name, module=module_path, is_class=True)

        if isinstance(target_model, ForwardRef):
            _model_path = (
                f"{target_model.__forward_module__}.{target_model.__forward_arg__}"
            )
        elif hasattr(target_model, "__module__"):
            _model_path = get_obj_klass_import_str(target_model)
        else:
            _model_path = ""

        # Capture field_name from metadata at descriptor creation time
        # (will be set by Formax when the Attrib is bound to a field)
        _field_name: Optional[str] = None

        def pre_fmt(
            instance: "KeyValueStoreIntegrationMixin", value: Any
        ) -> Optional[Dict[str, Any]]:
            if value is None:
                return None

            if target_model is not None:
                try:
                    model_class = cls._resolve_target_model(target_model)
                    if not isinstance(value, (model_class, ForeignKey)):
                        raise TypeError(
                            f"Expected {model_class.__name__} or ForeignKey, got {type(value).__name__}"
                        )
                except NameError:
                    pass  # ForwardRef not yet resolvable; skip validation

            return ForeignKey.serialize(value)

        def post_fmt(instance: "KeyValueStoreIntegrationMixin", value: Any) -> Any:
            if value is None:
                return None

            # Join pushdown: value is (raw_id, resolved_column_dict) tuple
            if isinstance(value, tuple) and len(value) == 2:
                raw_id, resolved_data = value
                target_cls = None
                if target_model is not None:
                    try:
                        target_cls = cls._resolve_target_model(target_model)
                    except NameError:
                        pass

                resolved_instance = None
                if resolved_data is not None and target_cls is not None:
                    try:
                        resolved_instance = hydrate_model(target_cls, resolved_data)
                    except Exception as e:
                        logger.warning(
                            "Failed to reconstruct join-resolved FK target: %s", e
                        )
                return ForeignKey.deserialize(
                    value=raw_id,
                    model_path=_model_path,
                    backend_alias="default",
                    is_native_fk=True,  # Join pushdown implies native resolution
                    resolved_instance=resolved_instance,
                )

            # introspect backend for enforcement mode
            is_native = _check_native_fk_for_instance(instance, _field_name)

            return ForeignKey.deserialize(
                value=value,
                model_path=_model_path,
                backend_alias="default",
                is_native_fk=is_native,
            )

        return MiniAnnotated[
            Any,
            Attrib(
                pre_formatter=pre_fmt,
                post_formatter=post_fmt,
                validators=validators,
                metadata={
                    "type": "foreignkey",
                    "target_model": target_model,
                    "reverse_name": config.reverse_name,
                    "on_delete": config.on_delete,
                    "nullable": config.nullable,
                    "constraint": config.constraint,
                },
            ),
        ]


def _check_native_fk_for_instance(
    instance: "KeyValueStoreIntegrationMixin",
    field_name: Optional[str],
) -> bool:
    """Introspect the backend to determine if native FK constraint exists.

    Caches result on the class to avoid repeated schema queries.
    Returns False if backend is unavailable or field_name is unknown.
    """
    if field_name is None:
        return False

    if instance is None:
        return False

    model_class = instance.__class__

    cache_attr = f"_fk_native_cache_{field_name}"
    cached = getattr(model_class, cache_attr, None)
    if cached is not None:
        return cached  # type: ignore

    try:
        # Use sync accessor since this runs inside post_formatter
        # which may be called from sync __setstate__ context
        backend = model_class._backend_store  # type: ignore
        if backend is None:
            result = False
        elif hasattr(backend, "verify_native_fk_exists"):
            result = backend.verify_native_fk_exists(model_class, field_name)
        else:
            result = False
    except Exception:
        result = False

    setattr(model_class, cache_attr, result)
    return result
