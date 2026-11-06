import logging
import asyncio
import hashlib
from types import UnionType
from formax import Attrib
from formax.typing import evaluate_forward_ref, get_origin, get_args, is_mini_annotated
from typing import (
    Type,
    cast,
    TYPE_CHECKING,
    Callable,
    Optional,
    Tuple,
    Dict,
    Any,
    ForwardRef,
    Union,
)

from volnux.import_utils import import_string
from volnux.concurrency.async_utils import to_thread

if TYPE_CHECKING:
    from .store import YoyoMigrationsMixin
    from volnux.backends.store import KeyValueStoreBackendBase
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin

logger = logging.getLogger(__name__)


def _safe_name(name: str) -> str:
    """Quote an SQL identifier (table/column/alias) for SQLite and PostgreSQL.

    Quoting lets keywords such as ``order`` be used as column names and keeps
    PostgreSQL from case-folding mixed-case names like ``volnux_Workflow``.
    Embedded double quotes are doubled, so a caller-supplied name (e.g. a
    filter key) can never break out of the identifier.
    """
    return '"' + str(name).replace('"', '""') + '"'


def _generate_constraint_name(name: str, prefix: str, suffix: str) -> str:
    """Generates a valid Postgres constraint name, truncated to 63 bytes max."""
    raw_name = f"{prefix}_{name}_{suffix}"
    # Ensure it fits within Postgres 63-byte limit
    if len(raw_name.encode("utf-8")) > 63:
        # Use a stable digest (builtin hash() is randomised per process) so the
        # same model always yields the same constraint/index name across runs.
        digest = hashlib.sha1(raw_name.encode("utf-8")).hexdigest()[:8]
        budget = 63 - len(f"{prefix}__{digest}_{suffix}".encode("utf-8"))
        truncated = name.encode("utf-8")[: max(budget, 0)].decode("utf-8", "ignore")
        return f"{prefix}_{truncated}_{digest}_{suffix}"
    return raw_name


def is_optional_field(field_type: Any) -> bool:
    """Check if a field type allows None."""
    origin = get_origin(field_type)
    if origin in (Union, UnionType):
        return type(None) in get_args(field_type)
    return False


def _is_collection_type(field_type: Any) -> bool:
    """Check if a (possibly Optional) field type is a list/dict/set/tuple."""
    if get_origin(field_type) in (Union, UnionType):
        non_none = [a for a in get_args(field_type) if a is not type(None)]
        if len(non_none) != 1:
            return False
        field_type = non_none[0]
    return (get_origin(field_type) or field_type) in (list, dict, set, tuple)


def decompose_field_type(field_type: Type[Any]) -> Tuple[Type[Any], Attrib]:
    if is_mini_annotated(field_type):
        args = get_args(field_type)
        attrib: Attrib = (
            field_type.__metadata__[0]
            if hasattr(field_type, "__metadata__")
            else Attrib()
        )
        if args:
            return args[0], attrib
        else:
            raise ValueError(f"Invalid field type: {field_type}")
    return field_type, Attrib()


class ModelResolverMixin:
    """Mixin for resolving model name"""

    @staticmethod
    def _resolve_target_model(
        target_model: Union[ForwardRef, Type["KeyValueStoreIntegrationMixin"]],
        globalns: Optional[Dict[str, Any]] = None,
        localns: Optional[Dict[str, Any]] = None,
    ) -> Type["KeyValueStoreIntegrationMixin"]:
        """
        Resolves the target model to its appropriate type.

        If the provided target model is a forward reference, it evaluates the reference
        using the given global and local namespaces, ensuring the return value is a type
        of `KeyValueStoreIntegrationMixin`.

        :param target_model: The target model to resolve. This can either be a string
            representing a forward reference or a type of `KeyValueStoreIntegrationMixin`.
        :param globalns: A dictionary representing the global namespace used for
            evaluating a forward reference, if applicable. Can be `None`.
        :param localns: A dictionary representing the local namespace used for
            evaluating a forward reference, if applicable. Can be `None`.
        :return: The resolved type of `KeyValueStoreIntegrationMixin`.
        """
        if isinstance(target_model, ForwardRef):
            return cast(
                Type["KeyValueStoreIntegrationMixin"],
                evaluate_forward_ref(target_model, globalns, localns),
            )
        return cast(Type["KeyValueStoreIntegrationMixin"], target_model)


def default_native_fk_check(
    source: Type["KeyValueStoreIntegrationMixin"],
    target: Type["KeyValueStoreIntegrationMixin"],
) -> bool:
    """Default sync callback: compares backend configs without initialization.

    Safe to call at any lifecycle stage. Never triggers backend connect().
    """
    try:
        source_config = source.get_backend_config()
        target_config = target.get_backend_config()

        source_id = _normalize_connection_id(source_config)
        target_id = _normalize_connection_id(target_config)

        if source_id is None or target_id is None:
            return False

        # Same ENGINE + same connection params = same database
        if source_id != target_id:
            return False

        # Both backends must declare FK support in their config or class
        source_engine = source_config.get("ENGINE", "")
        target_engine = target_config.get("ENGINE", "")

        # Check FK support via class-level attribute (no instantiation needed)
        for engine_path in (source_engine, target_engine):
            try:
                backend_cls: Type["KeyValueStoreBackendBase"] = import_string(engine_path)  # type: ignore
                supports: Optional[Callable[[...], bool]] = getattr(
                    backend_cls, "supports_foreign_keys", None
                )
                if supports is None:
                    return False
                result = supports()
                if not result:
                    return False
            except Exception:
                return False

        return True

    except Exception as e:
        logger.debug("Config-based native FK check failed: %s", e)
        return False


def _normalize_connection_id(config: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
    """Extract a canonical identifier from backend config without instantiation.

    Uses only CONNECTOR_CONFIG dict values. No backend object needed.
    Returns None if insufficient metadata to determine equivalence.
    """
    connector_config = config.get("CONNECTOR_CONFIG")
    if not connector_config or not isinstance(connector_config, dict):
        # Backends with no CONNECTOR_CONFIG (e.g., InMemory) are only
        # equivalent to themselves — identified by ENGINE path alone
        engine: Optional[str] = config.get("ENGINE")
        return (engine,) if engine else None

    excluded_keys = {"password", "token", "secret", "credentials"}
    normalized = tuple(
        f"{k}={v}"
        for k, v in sorted(connector_config.items())
        if k.lower() not in excluded_keys
    )

    # Include ENGINE to distinguish different backend types on same host
    engine = config.get("ENGINE", "")
    return (str(engine),) + normalized


async def migrate_models(
    *models: Type["KeyValueStoreIntegrationMixin"], dry_run=False
) -> None:
    """
    Migrates models to the database by creating the necessary tables and constraints.

    Args:
        *models: Variable number of KeyValueStoreIntegrationMixin subclasses to migrate.
        dry_run: If True, print logs

    Raises:
        TypeError: If any model is not a subclass of KeyValueStoreIntegrationMixin.
    """

    # Validate all models before migrating any (fail-fast)
    for model in models:
        if not issubclass(model, KeyValueStoreIntegrationMixin):
            raise TypeError(
                f"Model {model.__name__} is not a subclass of KeyValueStoreIntegrationMixin"
            )

    for model in models:
        try:
            model_backend = cast(object, await model.get_backend())
            model_backend = cast(YoyoMigrationsMixin, model_backend)
        except Exception as e:
            logger.error("Failed to get backend for %s: %s", model.__name__, e)
            continue

        if not hasattr(model_backend, "ensure_schema"):
            logger.warning(
                "Backend %s does not support model migration.",
                model_backend.__class__.__name__,
            )
            continue

        schema_name = await model.get_schema_name()

        num_applied = await to_thread(
            model_backend.ensure_schema, schema_name, model, dry_run=dry_run
        )

        if num_applied > 0:
            logger.info("Applied %d migrations to %s.", num_applied, model.__name__)
        else:
            logger.debug("No migrations to apply for %s.", model.__name__)
