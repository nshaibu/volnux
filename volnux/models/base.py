import enum
import typing
from typing import Any, ClassVar, Dict, Optional, Tuple, Type

from formax import BaseModel, InitStrategy

from volnux.exceptions import ObjectExistError
from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.backends.fields import DateTimeField, DTConfig
from volnux.backends.fields.foreign_key import ForeignKey

from . import crypto


def _enum_type(annotation: Any) -> Optional[Type[enum.Enum]]:
    """The Enum class an annotation names (through ``Annotated`` and ``Optional``)."""
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        return _enum_type(typing.get_args(annotation)[0])
    if origin is typing.Union:
        found = [
            e for e in map(_enum_type, typing.get_args(annotation)) if e is not None
        ]
        return found[0] if len(found) == 1 else None
    if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
        return annotation
    return None


class GovernanceModel(KeyValueStoreIntegrationMixin, BaseModel):
    """Base model for all governance entities.

    Provides:
        - Self-persistence via KeyValueStoreIntegrationMixin
        - Automatic ForeignKey backreference registration
        - Standard timestamp fields
        - OrJSON serialization support
        - Equality and hashing by type and id (models work in sets and as dict keys)
        - Enum fields that come back from the store as enum members
        - ``unique_together`` checks and a ``validate()`` hook on every save
        - ``encrypted_fields``: stored encrypted, plain in memory

    Timestamps: ``created_at``, ``updated_at`` and the few lifecycle stamps declared
    as ``DateTimeField`` are timezone-aware datetimes. Every other ``*_at`` /
    ``*_time`` / ``*_deadline`` field is a UTC epoch in seconds (``float``), so it
    can be compared and added to without conversion.
    """

    created_at: DateTimeField[DTConfig(auto_now_add=True)]
    updated_at: DateTimeField[DTConfig(auto_now=True)]

    # Field combinations that must be unique, e.g. (("organization", "slug"),).
    # A foreign key is named by its field, not by "<field>_id".
    unique_together: ClassVar[Tuple[Tuple[str, ...], ...]] = ()
    # Fields stored encrypted (see :mod:`volnux.models.crypto`).
    encrypted_fields: ClassVar[Tuple[str, ...]] = ()

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # A dataclass with eq=True and no __hash__ of its own gets __hash__ = None,
        # which would make every model unhashable.
        cls.__hash__ = GovernanceModel.__hash__  # type: ignore[method-assign]
        cls.__eq__ = GovernanceModel.__eq__  # type: ignore[method-assign]

    def __eq__(self, other: object) -> bool:
        # A record is the same record when it has the same type and id: two loads of
        # it are equal even if one is lazier or older than the other.
        if self is other:
            return True
        if type(self) is not type(other):
            return NotImplemented
        return self.id == other.id  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self).__name__, self.id))

    # --- enums ------------------------------------------------------------------

    @classmethod
    def _enum_fields(cls) -> Dict[str, Type[enum.Enum]]:
        cached = cls.__dict__.get("_enum_fields_cache")
        if cached is None:
            cached = {}
            for klass in reversed(cls.__mro__):
                for name, annotation in getattr(klass, "__annotations__", {}).items():
                    enum_cls = _enum_type(annotation)
                    if enum_cls is not None:
                        cached[name] = enum_cls
            cls._enum_fields_cache = cached
        return cached

    # --- state: enums and encryption --------------------------------------------

    def get_state(self) -> Dict[str, Any]:
        state = super().get_state()
        for name in self.encrypted_fields:
            if name in state:
                state[name] = crypto.encrypt_value(state[name])
        return state

    def set_state(self, state: Dict[str, Any]) -> None:
        state = dict(state)
        for name in self.encrypted_fields:
            if name in state:
                state[name] = crypto.decrypt_value(state[name])
        super().set_state(state)
        for name, enum_cls in self._enum_fields().items():
            value = getattr(self, name, None)
            if value is not None and not isinstance(value, enum_cls):
                try:
                    setattr(self, name, enum_cls(value))
                except ValueError:
                    pass

    # --- saving -----------------------------------------------------------------

    def validate(self) -> None:
        """Business rules a record must satisfy before it is saved (raise
        ``ValueError``). Subclasses override this."""

    async def _check_unique(self) -> None:
        for fields in self.unique_together:
            filters: Dict[str, Any] = {}
            for name in fields:
                value = getattr(self, name)
                if isinstance(value, (KeyValueStoreIntegrationMixin, ForeignKey)):
                    filters[f"{name}__object_id"] = value.id
                else:
                    filters[name] = value
            for other in await type(self).filter_async(**filters):
                if other.id != self.id:
                    raise ObjectExistError(
                        f"{type(self).__name__} with the same "
                        f"{', '.join(fields)} already exists"
                    )

    async def _before_save(self) -> None:
        self.validate()
        await self._check_unique()

    async def save(self, *args: Any, **kwargs: Any) -> None:
        await self._before_save()
        await super().save(*args, **kwargs)

    async def update(self) -> None:
        await self._before_save()
        await super().update()
