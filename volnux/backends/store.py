"""
Key-Value Store Backend Interface.

This module provides the abstract base class for implementing key-value store backends
with support for CRUD operations, filtering, and record management.
"""

import abc
import dataclasses
import os
import re
import typing
import sys
import time
import orjson as json
import threading
import logging
from enum import Enum
from yoyo import read_migrations, get_backend
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    ForwardRef,
    Dict,
    Iterable,
    Type,
    Union,
    Optional,
    Tuple,
    get_args,
)

from formax import BaseModel
from formax.typing import (
    get_type_hints,
    evaluate_forward_ref,
    is_builtin_type,
)

from .connection import BackendConnectorBase
from volnux.exceptions import SerializationError
from .db_utils import (
    _safe_name,
    _generate_constraint_name,
    is_optional_field,
    decompose_field_type,
    _is_collection_type,
    ModelResolverMixin,
)
from volnux.exceptions import SqlOperationError

if TYPE_CHECKING:
    from .fields import OnDelete
    from .storage_route import StorageRoute
    from volnux.result.stream import Q, ResultStream
    from volnux.mixins.key_value_store_integration import KeyValueStoreIntegrationMixin


logger = logging.getLogger(__name__)


from dataclasses import dataclass, field
from typing import List, Set, Tuple


class SQLDialect(Enum):
    SQLITE = "sqlite"
    POSTGRES = "postgres"


@dataclass
class TableBlueprint:
    schema_name: str
    migration_dir: str
    columns: List[str] = field(default_factory=list)
    fk_constraints: List[str] = field(default_factory=list)
    chk_constraints: List[str] = field(default_factory=list)
    indexes_sql: List[str] = field(default_factory=list)
    # Deferred statements that require other tables to fully exist first
    deferred_alter_statements: List[Tuple[str, str, str]] = field(default_factory=list)

    def compile_base_create_sql(self) -> str:
        """Assembles the primary structural statement."""
        elements = self.columns + self.fk_constraints + self.chk_constraints
        return f'CREATE TABLE IF NOT EXISTS {_safe_name(self.schema_name)} ({", ".join(elements)})'


class YoyoMigrationsMixin:

    connector: BackendConnectorBase[Any]

    dialect: SQLDialect

    _id_field_length: int

    @staticmethod
    def _sqlite_row_and_tuple_to_dict(row: Any, cursor: Any) -> Dict[str, Any]:
        if hasattr(row, "_asdict"):
            row_dict = row._asdict()
        elif hasattr(row, "keys"):
            row_dict = {k: row[k] for k in row.keys()}
        else:
            col_names = [d[0] for d in cursor.description]
            row_dict = dict(zip(col_names, row))
        return row_dict

    def map_python_type_to_sql(self, field_type: Type[Any]) -> str:
        """
        Maps a Python type to its corresponding SQL type.

        :param field_type: The Python type to map.
        :return: The corresponding SQL type as a string.
        """
        raise NotImplementedError(
            "map_python_type_to_sql() must be implemented by subclasses."
        )

    @staticmethod
    def on_delete_constraints_map() -> Dict["OnDelete", str]:
        from .fields import OnDelete

        return {
            OnDelete.CASCADE: "CASCADE",
            OnDelete.SET_NULL: "SET NULL",
            OnDelete.SET_DEFAULT: "SET DEFAULT",
            OnDelete.PROTECT: "NO ACTION",
            OnDelete.DO_NOTHING: "NO ACTION",
        }

    @staticmethod
    def write_yoyo_migration_step(
        migration_dir: str,
        step_name: str,
        steps: List[Tuple[str, Optional[str]]],
    ) -> str:
        """
        Generates a versioned Yoyo migration file in ``migration_dir``.

        Each ``(apply_sql, rollback_sql)`` pair becomes its own yoyo step:
        drivers such as sqlite3 refuse to execute more than one statement
        per call, so statements must not be joined into a single step.
        """
        os.makedirs(migration_dir, exist_ok=True)

        # Create a unique, chronological timestamp prefix
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{step_name}.py"
        full_path = os.path.join(migration_dir, filename)

        # SQL is embedded via repr() so backslashes in regex CHECK patterns
        # and quotes survive verbatim instead of being parsed as escapes.
        step_lines = "".join(
            f"    step({apply_sql!r}, {rollback_sql!r}),\n"
            for apply_sql, rollback_sql in steps
        )
        yoyo_script_content = (
            f'"""\n'
            f"Auto-generated Yoyo migration '{step_name}'.\n"
            f'"""\n'
            f"\n"
            f"from yoyo import step\n"
            f"\n"
            f"__depends__ = {{}}\n"
            f"\n"
            f"steps = [\n"
            f"{step_lines}"
            f"]\n"
        )

        with open(full_path, "w", encoding="utf-8") as f:
            f.write(yoyo_script_content)

        return full_path

    def _invalidate_schema_cache(self, schema_name: Optional[str] = None) -> None:
        """Invalidate the schema cache.

        Args:
            schema_name: Specific schema to invalidate, or None for all.
        """
        pass

    def get_cached_table_columns(self, schema_name: str) -> Dict[str, str]:
        """
        Queries the backend system catalogs to fetch the live columns
        and data types for a given table name.

        Returns:
            A dictionary mapping column name string to data type string.
            Returns an empty dict {} if the table does not exist.
        """
        pass

    def _verify_schema_drift(self, schema_name: str, blueprint: TableBlueprint) -> None:
        """
        Compares the raw compiled model configuration against live database structures.
        Ensures developers don't change Python fields without authoring a corresponding Yoyo file.
        """
        db_columns = self.get_cached_table_columns(schema_name)
        if not db_columns:
            return  # Handled by initial creation loop

        # Extract field names from the compiled blueprint column definitions
        # Quick regex or string split to grab the column identifiers (e.g., "username" from '"username" TEXT')
        compiled_fields = set()
        for col_def in blueprint.columns:
            # Assumes your columns start with double-quoted identifiers like '"field_name"'
            if col_def.startswith('"'):
                field_name = col_def.split('"')[1]
                compiled_fields.add(field_name)

        missing_in_db = compiled_fields - set(db_columns.keys())
        if missing_in_db:
            raise RuntimeError(
                f"Schema Drift Detected in table '{schema_name}'! "
                f"The following fields exist in your Python code models but are missing from the database: {list(missing_in_db)}. "
                f"Please author a new Yoyo migration script in your schema directory to sync this change."
            )

        orphan_in_db = set(db_columns.keys()) - compiled_fields - {"id"}
        if orphan_in_db:
            logger.warning(
                "Database contains column(s) %s for table '%s' that no longer exist in your Python model. "
                "Ensure a rollback or cleanup Yoyo migration script is authored if this was intentional.",
                list(orphan_in_db),
                schema_name,
            )

    def schema_exists(self, schema_name: str) -> bool:
        """
        Checks if a schema exists in the database.

        :param schema_name: The name of the schema to check.
        :return: True if the schema exists, False otherwise.
        """
        raise NotImplementedError

    def compile_schema(
        self,
        schema_name: str,
        record_class: Type["KeyValueStoreIntegrationMixin"],
        graph: Dict[str, TableBlueprint],
        _creating: Set[str],
    ) -> None:
        """
        Pure Data Compiler: Traverses the model dependency graph via DFS
        and populates an in-memory TableBlueprint. No I/O side effects.
        """
        if schema_name in graph:
            return  # Already compiled in this sequence run

        from volnux.backends.fields import OnDelete

        _creating.add(schema_name)

        blueprint = TableBlueprint(
            schema_name=schema_name,
            migration_dir=record_class.get_migration_dir(),
        )

        blueprint.columns.append(
            f"{_safe_name('id')} VARCHAR({self._id_field_length}) PRIMARY KEY"
        )

        try:
            # include_extras keeps MiniAnnotated Attribs (FK/length/pattern metadata)
            record_type_hints = get_type_hints(record_class, include_extras=True)

            for field_name, field_type in record_type_hints.items():
                if field_name.startswith("_"):
                    continue
                # InitVars are constructor-only and ClassVars aren't instance
                # state; neither is persisted, so neither gets a column.
                if isinstance(field_type, dataclasses.InitVar) or (
                    typing.get_origin(field_type) is typing.ClassVar
                ):
                    continue

                field_type, attrib = decompose_field_type(field_type)

                metadata = attrib.metadata or {}
                meta_field_type = metadata.get("type")
                is_unique = metadata.get("unique", False)
                is_optional = is_optional_field(field_type) or metadata.get(
                    "nullable", False
                )

                if meta_field_type == "foreignkey":
                    has_native_fk = metadata.get("has_native_fk", False)
                    col_name = (
                        f"{field_name}_object_id"
                        if not field_name.endswith("_object_id")
                        else field_name
                    )

                    field_def = (
                        f"{_safe_name(col_name)} VARCHAR({self._id_field_length})"
                        + ("" if is_optional else " NOT NULL")
                        # UNIQUE makes the relation one-to-one
                        + (" UNIQUE" if is_unique else "")
                    )
                    blueprint.columns.append(field_def)

                    # Provision performance B-tree index for this foreign key column field name
                    idx_name = _generate_constraint_name(
                        f"{schema_name}_{col_name}", "idx", "fk"
                    )
                    blueprint.indexes_sql.append(
                        f"CREATE INDEX IF NOT EXISTS {_safe_name(idx_name)} ON {_safe_name(schema_name)} ({_safe_name(col_name)})"
                    )

                    if has_native_fk:
                        target_model: Optional[
                            Type["KeyValueStoreIntegrationMixin"]
                        ] = metadata.get("target_model")
                        if target_model is None:
                            raise ValueError(
                                f"Target model missing for FK '{field_name}'"
                            )

                        target_schema = self.resolve_physical_target(  # type: ignore
                            target_model.get_storage_route()
                        )

                        on_delete: "OnDelete" = metadata.get(
                            "on_delete", OnDelete.PROTECT
                        )
                        on_delete_sql = self.on_delete_constraints_map().get(
                            on_delete, "NO ACTION"
                        )

                        # A self-reference can always be declared inline: the
                        # table being created is its own referenced table.
                        is_self_ref = target_schema == schema_name

                        # Cycle Detection Guard Block
                        if not is_self_ref and target_schema in _creating:
                            if self.dialect == SQLDialect.POSTGRES:
                                logger.warning(
                                    "Mutual FK dependency: %s <-> %s. Native FK for "
                                    "'%s' will be added via ALTER TABLE once schemas exist.",
                                    schema_name,
                                    target_schema,
                                    field_name,
                                )
                                blueprint.deferred_alter_statements.append(
                                    (col_name, target_schema, on_delete_sql)
                                )
                                continue
                            # SQLite resolves FK targets at write time, so an inline
                            # reference to a table created later in this run is valid
                            # (and SQLite cannot ADD CONSTRAINT afterwards anyway).
                            logger.debug(
                                "Mutual FK dependency: %s <-> %s. Declaring native FK "
                                "for '%s' inline as a forward reference.",
                                schema_name,
                                target_schema,
                                field_name,
                            )
                        elif not is_self_ref and target_schema not in graph:
                            # Execute downstream recursive compilation path cleanly
                            self.compile_schema(
                                target_schema, target_model, graph, _creating
                            )

                        fk_name = _generate_constraint_name(
                            field_name, "fk", target_schema
                        )
                        blueprint.fk_constraints.append(
                            f"CONSTRAINT {_safe_name(fk_name)} FOREIGN KEY ({_safe_name(col_name)}) "
                            f'REFERENCES {_safe_name(target_schema)}({_safe_name("id")}) ON DELETE {on_delete_sql}'
                        )
                    # Without a native FK the column is software-enforced only
                    continue

                # Type mapping configurations across target backends
                if meta_field_type == "list":
                    sql_type = "TEXT"
                    # Add database native inverted indexing rules for Postgres JSON payloads
                    if self.dialect == SQLDialect.POSTGRES:
                        sql_type = "JSONB"

                        gin_idx = _generate_constraint_name(
                            f"{schema_name}_{field_name}", "idx", "gin"
                        )
                        blueprint.indexes_sql.append(
                            f"CREATE INDEX IF NOT EXISTS {_safe_name(gin_idx)} ON {_safe_name(schema_name)} USING GIN ({_safe_name(field_name)})"
                        )
                elif meta_field_type in ["datetime", "date"]:
                    if self.dialect == SQLDialect.POSTGRES:
                        if meta_field_type == "datetime":
                            is_tz = metadata.get("tz_aware", False)
                            sql_type = "TIMESTAMPTZ" if is_tz else "TIMESTAMP"
                        else:
                            sql_type = "DATE"
                    else:
                        sql_type = "TEXT"
                else:
                    sql_type = self.map_python_type_to_sql(field_type)

                    if sql_type == "TEXT" and not _is_collection_type(field_type):
                        if attrib.min_length and attrib.max_length:
                            sql_type = f"VARCHAR({attrib.max_length})"
                        elif attrib.min_length:
                            sql_type = "TEXT"
                        elif attrib.max_length:
                            sql_type = f"VARCHAR({attrib.max_length})"

                # Build basic string field tokens
                field_def = f"{_safe_name(field_name)} {sql_type}"
                if not is_optional:
                    field_def += " NOT NULL"
                if is_unique:
                    field_def += " UNIQUE"
                blueprint.columns.append(field_def)

                safe_field = _safe_name(field_name)
                # Lists/dicts and SQLite dates are stored as TEXT too, but their
                # length/pattern rules describe the value, not its serialized text.
                is_text_primitive = (
                    sql_type == "TEXT" or sql_type.startswith("VARCHAR")
                ) and not (
                    meta_field_type in ("list", "datetime", "date")
                    or _is_collection_type(field_type)
                )

                if is_text_primitive:
                    if attrib.min_length and attrib.max_length:
                        blueprint.chk_constraints.append(
                            f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'len_range')} CHECK (length({safe_field}) BETWEEN {attrib.min_length} AND {attrib.max_length})"
                        )
                    elif attrib.min_length:
                        blueprint.chk_constraints.append(
                            f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'min_length')} CHECK (length({safe_field}) >= {attrib.min_length})"
                        )
                    elif attrib.max_length:
                        blueprint.chk_constraints.append(
                            f"CONSTRAINT {_generate_constraint_name(field_name, 'chk', 'max_length')} CHECK (length({safe_field}) <= {attrib.max_length})"
                        )

                    # Regex Pattern matching
                    if attrib.pattern is not None:
                        raw_pattern = (
                            attrib.pattern.pattern
                            if hasattr(attrib.pattern, "pattern")
                            else str(attrib.pattern)
                        )
                        raw_pattern = (
                            raw_pattern.replace(r"\A", "^")
                            .replace(r"\Z", "$")
                            .replace("'", "''")
                        )

                        chk_name = _generate_constraint_name(
                            field_name, "chk", "pattern"
                        )
                        if self.dialect == SQLDialect.SQLITE:
                            # The registered REGEXP function has no flags argument,
                            # so carry case-insensitivity inline.
                            if hasattr(attrib.pattern, "flags") and (
                                attrib.pattern.flags & re.IGNORECASE
                            ):
                                raw_pattern = f"(?i){raw_pattern}"
                            blueprint.chk_constraints.append(
                                f"CONSTRAINT {chk_name} CHECK ({safe_field} REGEXP '{raw_pattern}')"
                            )
                        else:
                            is_case_insensitive = hasattr(
                                attrib.pattern, "flags"
                            ) and bool(attrib.pattern.flags & re.IGNORECASE)
                            regex_op = "~*" if is_case_insensitive else "~"
                            blueprint.chk_constraints.append(
                                f"CONSTRAINT {chk_name} CHECK ({safe_field} {regex_op} '{raw_pattern}')"
                            )

                elif sql_type in (
                    "INT",
                    "INTEGER",
                    "BIGINT",
                    "SMALLINT",
                    "NUMERIC",
                    "REAL",
                    "DOUBLE PRECISION",
                ):
                    numeric_checks = []
                    if attrib.gt is not None:
                        numeric_checks.append(f"{safe_field} > {attrib.gt}")
                    if attrib.ge is not None:
                        numeric_checks.append(f"{safe_field} >= {attrib.ge}")
                    if attrib.lt is not None:
                        numeric_checks.append(f"{safe_field} < {attrib.lt}")
                    if attrib.le is not None:
                        numeric_checks.append(f"{safe_field} <= {attrib.le}")
                    if numeric_checks:
                        chk_name = _generate_constraint_name(
                            field_name, "chk", "numeric"
                        )
                        blueprint.chk_constraints.append(
                            f"CONSTRAINT {chk_name} CHECK ({' AND '.join(numeric_checks)})"
                        )

            # Assign completed compilation block to memory dictionary graph
            graph[schema_name] = blueprint
        except Exception as e:
            logger.error(
                f"Error compiling schema layout properties for '{schema_name}': {e}"
            )
            raise SqlOperationError(f"Error compiling schema blueprint: {e}") from e
        finally:
            _creating.discard(schema_name)

    def create_schema(
        self,
        schema_name: str,
        record_class: Type["KeyValueStoreIntegrationMixin"],
        **kwargs,
    ) -> None:
        raise NotImplementedError

    def ensure_schema(
        self,
        schema_name: str,
        record: Type["KeyValueStoreIntegrationMixin"],
        dry_run: bool = False,
    ) -> int:
        """
        Ensure the base schema exists, then apply pending Yoyo migrations.

        Args:
            schema_name: Schema/table name.
            record: Sample model used to derive initial columns.
            dry_run: If True, only report what would happen.

        Returns:
            Number of Yoyo migrations applied, or would be applied in dry_run mode.
        """
        schema_exists = self.schema_exists(schema_name)
        if not schema_exists:
            if dry_run:
                logger.info(
                    "Dry run: would create schema '%s' and apply migrations from %s.",
                    schema_name,
                    record.get_migration_dir(),
                )
            else:
                compiled_graph: Dict[str, TableBlueprint] = {}
                self.compile_schema(
                    schema_name=schema_name,
                    record_class=record,
                    graph=compiled_graph,
                    _creating=set(),
                )

                total_applied = 0

                # Pass 1: create every table in the dependency graph. FK
                # constraints that would form a mutual cycle were deliberately
                # left out of the base CREATE TABLE (see compile_schema), so
                # no table here has to wait on its cyclic partner to exist.
                for s_name, blueprint in compiled_graph.items():

                    # Check if this specific dependency table needs a baseline file written
                    if not os.path.exists(blueprint.migration_dir) or not os.listdir(
                        blueprint.migration_dir
                    ):
                        # Indexes need no rollback of their own: yoyo rolls
                        # steps back in reverse, and dropping the table drops them.
                        baseline_steps: List[Tuple[str, Optional[str]]] = [
                            (
                                blueprint.compile_base_create_sql(),
                                f"DROP TABLE IF EXISTS {_safe_name(s_name)}",
                            )
                        ]
                        baseline_steps.extend(
                            (index_sql, None) for index_sql in blueprint.indexes_sql
                        )

                        # Each table owns its migration history, so the baseline
                        # goes in the directory its migrations are run from.
                        self.write_yoyo_migration_step(
                            blueprint.migration_dir, f"{s_name}_init", baseline_steps
                        )
                        logger.info(
                            "Compiled baseline migration step file for schema '%s'",
                            s_name,
                        )

                    if not self.schema_exists(s_name):
                        self._validate_migrations_dir(blueprint.migration_dir)
                        logger.info(
                            "Executing baseline database setup migration for table '%s'...",
                            s_name,
                        )

                        applied = self.run_migrations(
                            migrations_dir=blueprint.migration_dir, dry_run=False
                        )
                        total_applied += applied
                        self._invalidate_schema_cache(s_name)

                # Pass 2: every table in this dependency graph now exists, so
                # it's safe to add the FK constraints deferred above for
                # mutual cycles via ALTER TABLE. Doing this only after pass 1
                # has created every schema guarantees both sides of a cycle
                # are present before either constraint is applied.
                for s_name, blueprint in compiled_graph.items():
                    if not blueprint.deferred_alter_statements:
                        continue

                    fk_marker = f"_{s_name}_fk.py"
                    already_written = os.path.exists(blueprint.migration_dir) and any(
                        fname.endswith(fk_marker)
                        for fname in os.listdir(blueprint.migration_dir)
                    )

                    if not already_written:
                        fk_steps: List[Tuple[str, Optional[str]]] = []
                        for col, tgt, on_del in blueprint.deferred_alter_statements:
                            fk_name = _safe_name(
                                _generate_constraint_name(col, "fk_def", tgt)
                            )
                            fk_steps.append(
                                (
                                    f"ALTER TABLE {_safe_name(s_name)} ADD CONSTRAINT {fk_name} "
                                    f'FOREIGN KEY ({_safe_name(col)}) REFERENCES {_safe_name(tgt)}("id") ON DELETE {on_del}',
                                    f"ALTER TABLE {_safe_name(s_name)} DROP CONSTRAINT IF EXISTS {fk_name}",
                                )
                            )
                        self.write_yoyo_migration_step(
                            blueprint.migration_dir, f"{s_name}_fk", fk_steps
                        )
                        logger.info(
                            "Compiled deferred FK migration step for schema '%s'",
                            s_name,
                        )

                    self._validate_migrations_dir(blueprint.migration_dir)
                    logger.info(
                        "Applying deferred FK constraints for table '%s'...",
                        s_name,
                    )
                    applied = self.run_migrations(
                        migrations_dir=blueprint.migration_dir, dry_run=False
                    )
                    total_applied += applied
                    self._invalidate_schema_cache(s_name)

                return total_applied

        migrations_dir = record.get_migration_dir()

        self._validate_migrations_dir(migrations_dir)

        applied = 0
        if os.path.exists(migrations_dir) and os.listdir(migrations_dir):
            logger.debug(
                "Scanning and applying incremental updates for table '%s'...",
                schema_name,
            )
            applied = self.run_migrations(
                migrations_dir=migrations_dir, dry_run=dry_run
            )
            self._invalidate_schema_cache(schema_name)

        # Run drift validation check (ensures developers didn't miss a Yoyo step file)
        if not dry_run:
            single_table_graph: Dict[str, TableBlueprint] = {}
            self.compile_schema(
                schema_name=schema_name,
                record_class=record,
                graph=single_table_graph,
                _creating=set(),
            )

            if schema_name in single_table_graph:
                self._verify_schema_drift(schema_name, single_table_graph[schema_name])

        return applied

    def _get_migration_backend(self) -> typing.Any:
        return get_backend(self.connector.get_uri())

    def _validate_migrations_dir(self, migrations_dir: Optional[str] = None) -> None:
        if migrations_dir is None:
            return
        if not os.path.isdir(migrations_dir):
            raise FileNotFoundError(
                f"Migrations directory does not exist: {migrations_dir}"
            )

    def _get_pending_migrations(
        self, migrations_dir: str
    ) -> typing.Tuple[typing.Any, typing.Any, typing.Any]:
        backend = self._get_migration_backend()
        migrations = read_migrations(migrations_dir)
        return backend, migrations, backend.to_apply(migrations)

    def _get_rollback_migrations(
        self, migrations_dir: str, count: typing.Optional[int] = None
    ) -> typing.Tuple[typing.Any, typing.Any, typing.Any]:
        backend = self._get_migration_backend()
        migrations = read_migrations(migrations_dir)

        if not hasattr(backend, "to_rollback"):
            raise NotImplementedError(
                f"{backend.__class__.__name__} does not expose rollback selection support."
            )

        to_rollback = backend.to_rollback(migrations)
        if count is not None:
            to_rollback = to_rollback[:count]

        return backend, migrations, to_rollback

    def _execute_migration_batch(
        self,
        backend: typing.Any,
        migrations: typing.Any,
        migrations_dir: str,
        action: str,
    ) -> int:
        if not migrations:
            if action == "apply":
                logger.debug("No new migration files to apply.")
            else:
                logger.debug("No applied migrations to roll back.")
            return 0

        try:
            with backend.lock():
                if action == "apply":
                    backend.apply_migrations(migrations)
                elif action == "rollback":
                    if hasattr(backend, "rollback_migrations"):
                        backend.rollback_migrations(migrations)
                    elif hasattr(backend, "rollback"):
                        backend.rollback(migrations)
                    else:
                        raise NotImplementedError(
                            f"{backend.__class__.__name__} does not support rollback execution."
                        )
                else:
                    raise ValueError(f"Unknown migration action: {action}")
        except Exception:
            if action == "apply":
                logger.exception("Failed to apply migrations from %s", migrations_dir)
            else:
                logger.exception(
                    "Failed to roll back migrations from %s", migrations_dir
                )
            raise

        return len(migrations)

    @abc.abstractmethod
    def filter_with_joins(
        self,
        schema_name: str,
        model_class: Type["KeyValueStoreIntegrationMixin"],
        join_fields: typing.List[str],
        **filter_kwargs: Any,
    ) -> typing.List[Dict[str, Any]]:
        """Filter records with FK fields resolved in a single query.

        Args:
            schema_name: The schema to query.
            model_class: The model class for deserialization context.
            join_fields: List of FK field names to resolve (e.g., ["created_by", "approved_by"]).
            **filter_kwargs: Standard filter predicates.

        Returns:
            List of dicts where each dict contains the parent record's columns
            PLUS resolved target record data keyed as "{field_name}__resolved".
            If a target cannot be resolved, the key maps to None.

        Raises:
            NotImplementedError: If the backend does not support join pushdown.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support filter_with_joins"
        )

    @abc.abstractmethod
    def has_fk_constraint(self, schema_name: str, column_name: str) -> bool:
        """
        Checks if the specified schema and column utilise a native foreign key constraint.

        :param schema_name: The name of the schema where the column is located.
        :type schema_name: str
        :param column_name: The name of the column for which to verify the foreign key constraint.
        :type column_name: str
        :return: A boolean indicating whether a native foreign key constraint exists.
        :rtype: bool
        """
        raise NotImplementedError("Constraint not implemented")

    def verify_native_fk_exists(
        self,
        record_class: Type["KeyValueStoreIntegrationMixin"],
        field_name: str,
    ) -> bool:
        """
        Check if a native FK constraint actually exists in the schema.

        Returns False if the constraint was omitted due to cycle detection,
        missing target table, or any other reason. This is the authoritative
        check that overrides the pre_formatter's requested mode.
        """
        try:
            schema_name = self.resolve_physical_target(  # type: ignore
                record_class.get_storage_route()
            )
            col_name = (
                f"{field_name}_object_id"
                if not field_name.endswith("_object_id")
                else field_name
            )

            # Query information_schema / sqlite_master for actual FK constraints
            # Implementation varies by backend; returns True only if constraint exists
            return self.has_fk_constraint(schema_name, col_name)
        except Exception:
            return False

    def run_migrations(self, migrations_dir: str, dry_run: bool = False) -> int:
        """
        Apply all pending migrations from the given directory.

        Args:
            migrations_dir: Path to the migrations' directory.
            dry_run: If True, only calculate what would be applied.

        Returns:
            Number of migrations that would be, or were, applied.
        """
        if migrations_dir is None:
            return 0
        self._validate_migrations_dir(migrations_dir)

        backend, _, to_apply = self._get_pending_migrations(migrations_dir)

        if dry_run:
            logger.info(
                "Dry run: would apply %s migration(s) from %s.",
                len(to_apply),
                migrations_dir,
            )
            return len(to_apply)

        applied_count = self._execute_migration_batch(
            backend=backend,
            migrations=to_apply,
            migrations_dir=migrations_dir,
            action="apply",
        )

        if applied_count:
            logger.info("Applied %s new migration(s).", applied_count)

        return applied_count

    def rollback_migrations(
        self,
        migrations_dir: str,
        count: int = 1,
        dry_run: bool = False,
    ) -> int:
        """
        Roll back the last `count` applied migrations.

        Args:
            migrations_dir: Path to the migrations directory.
            count: Number of migrations to roll back.
            dry_run: If True, only calculate what would be rolled back.

        Returns:
            Number of migrations that would be, or were, rolled back.
        """
        self._validate_migrations_dir(migrations_dir)

        if count <= 0:
            logger.debug("Rollback count is <= 0; nothing to do.")
            return 0

        backend, _, to_rollback = self._get_rollback_migrations(
            migrations_dir=migrations_dir,
            count=count,
        )

        if dry_run:
            logger.info(
                "Dry run: would roll back %s migration(s) from %s.",
                len(to_rollback),
                migrations_dir,
            )
            return len(to_rollback)

        rolled_back_count = self._execute_migration_batch(
            backend=backend,
            migrations=to_rollback,
            migrations_dir=migrations_dir,
            action="rollback",
        )

        if rolled_back_count:
            logger.info("Rolled back %s migration(s).", rolled_back_count)

        return rolled_back_count

    def rollback_all_migrations(
        self,
        migrations_dir: str,
        dry_run: bool = False,
    ) -> int:
        """
        Roll back all applied migrations.

        Args:
            migrations_dir: Path to the migrations directory.
            dry_run: If True, only calculate what would be rolled back.

        Returns:
            Number of migrations that would be, or were, rolled back.
        """
        self._validate_migrations_dir(migrations_dir)

        backend, _, to_rollback = self._get_rollback_migrations(
            migrations_dir=migrations_dir,
            count=None,
        )

        if dry_run:
            logger.info(
                "Dry run: would roll back all %s migration(s) from %s.",
                len(to_rollback),
                migrations_dir,
            )
            return len(to_rollback)

        rolled_back_count = self._execute_migration_batch(
            backend=backend,
            migrations=to_rollback,
            migrations_dir=migrations_dir,
            action="rollback",
        )

        if rolled_back_count:
            logger.info("Rolled back all %s migration(s).", rolled_back_count)

        return rolled_back_count


class KeyValueStoreBackendBase(abc.ABC):
    """Abstract base class for key-value store backends.

    This class defines the interface for backend storage implementations,
    providing thread-safe operations for managing records in a schema-based
    key-value store.

    Attributes:
        connector_klass: The connector class to use for backend connections.
        connector: The active backend connector instance.
    """

    connector_klass: Type[BackendConnectorBase]

    NAMESPACE_SEPARATOR = ":"

    RESERVED_FIELDS = {
        "_id",
        "_backend",
        "_backend_class",
        "_objectid_lock",
        "_schema_name",
        "_backend_config",
        "_backend_store",
    }

    def __init__(
        self, namespace_prefix: typing.Optional[str] = None, **connector_config: Any
    ) -> None:
        """Initialize the backend with connector configuration.

        Args:
            **connector_config: Configuration parameters passed to the connector.
        """
        self._namespace_prefix = namespace_prefix
        self.connector = self.connector_klass(**connector_config)
        self._connector_lock = threading.RLock()

    @contextmanager
    def _acquire_lock(self):
        """Context manager for thread-safe operations."""
        self._connector_lock.acquire()
        try:
            yield
        finally:
            self._connector_lock.release()

    def _ensure_connected(self) -> None:
        """Ensure the Redis connection is active.

        Raises:
            ConnectionError: If a connection cannot be established.
        """
        if not self.connector.is_connected():
            logger.warning(
                f"{self.connector.__class__.__name__} connection lost, attempting to reconnect..."
            )
            self.connector.connect()

    def close(self) -> None:
        """Close the backend connection and release resources."""
        with self._acquire_lock():
            if hasattr(self, "connector") and self.connector is not None:
                self.connector.disconnect()

    def __eq__(self, other: Any) -> bool:
        """Equality check for backend instances."""
        return isinstance(other, self.__class__) and self.connector == other.connector

    def __enter__(self) -> "KeyValueStoreBackendBase":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with automatic cleanup."""
        self.close()

    def _prepare_record_data(
        self, record: "KeyValueStoreIntegrationMixin", record_key: str
    ) -> Dict[str, Any]:
        """Prepare record data for database insertion.

        Args:
            record: The record to prepare.
            record_key: The key for the record.

        Returns:
            Dictionary of field names to values.
        """
        record_data: Dict[str, Any] = {"id": record_key}

        for field_name, value in record.__getstate__().items():
            # Private attributes have no column (compile_schema skips them), and
            # the state's "id" must not override the explicit record_key.
            if (
                field_name in self.RESERVED_FIELDS
                or field_name.startswith("_")
                or field_name == "id"
            ):
                continue

            if isinstance(value, (dict, list)):
                if isinstance(value, dict):
                    field_type = value.get("type", "")

                    if field_type == "foreignkey":
                        record_data[f"{field_name}_object_id"] = value.get("object_id")
                    else:
                        record_data[field_name] = value
                else:
                    record_data[field_name] = value
            elif isinstance(value, Enum):
                record_data[field_name] = value.value
            # NOTE: nested objects serialisation should be handled by preformatters
            elif isinstance(value, BaseModel):
                record_data[field_name] = value.__getstate__()
            elif value is not None:
                record_data[field_name] = value
            else:
                record_data[field_name] = None

        return record_data

    @classmethod
    def resolve_target_model_for_field(
        cls, schema_class: Type["KeyValueStoreIntegrationMixin"], field_name: str
    ) -> Type["KeyValueStoreIntegrationMixin"]:
        """
        Resolves the target mode class for the specified field of a schema class.

        This method is used to determine the class type associated with a specific field
        of the given `schema_class`. The field type is validated to ensure it is a
        subclass of `KeyValueStoreIntegrationMixin`, ensuring proper schema integration.

        :param schema_class: The schema class to analyze. It should inherit from
            `KeyValueStoreIntegrationMixin`.
        :type schema_class: Type[KeyValueStoreIntegrationMixin]
        :param field_name: The name of the field in the schema whose target mode type
            needs to be resolved.
        :type field_name: str
        :return: The target mode class for the specified field.
        :rtype: Type[KeyValueStoreIntegrationMixin]
        :raises ValueError: If the field is not found in the schema or its type is invalid
            for resolving as a schema class.
        """
        from volnux.mixins import KeyValueStoreIntegrationMixin

        schema_field_annotation = get_type_hints(schema_class, include_extras=True)
        field_annotation: Type[Any] = schema_field_annotation.get(field_name)  # type: ignore
        if field_annotation is None:
            raise ValueError(
                f"Field '{field_name}' not found in schema '{schema_class.__name__}'"
            )

        field_type, attrib = decompose_field_type(field_annotation)
        metadata = attrib.metadata or {}
        if metadata.get("type") == "foreignkey" and metadata.get("target_model"):
            # ForeignKeyField annotates as MiniAnnotated[Any, ...]; the target
            # model lives in the field metadata.
            field_type = ModelResolverMixin._resolve_target_model(
                metadata["target_model"]
            )
        if field_type is None:
            raise ValueError(f"Invalid field type: {field_annotation}")

        if not (
            isinstance(field_type, type)
            and issubclass(field_type, KeyValueStoreIntegrationMixin)
        ):
            raise ValueError(f"Type '{field_type.__name__}' is not a schema")
        return field_type

    def _serialize_record(self, record: "KeyValueStoreIntegrationMixin") -> bytes:
        """Serialize a record to bytes.

        Args:
            record: The record to serialize.

        Returns:
            Serialized record as bytes.

        Raises:
            SerializationError: If serialization fails.
        """
        try:
            state = record.__getstate__()
            return json.dumps(state, default=str)
        except Exception as e:
            logger.error(f"Failed to serialize record: {e}")
            raise SerializationError(f"Serialization failed: {e}")

    def _deserialize_record(
        self,
        data: Union[bytes, str, Dict[str, Any]],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> "KeyValueStoreIntegrationMixin":
        """Deserialize bytes to a record object.

        Args:
            data: Serialized record data.
            record_klass: The class to instantiate.

        Returns:
            Deserialized record instance.

        Raises:
            SerializationError: If deserialization fails.
        """
        try:
            if isinstance(data, dict):
                state = data
            else:
                state = json.loads(data)

            for key, value in state.items():
                if key == "id":
                    continue
                # Deserialize JSON columns back to Python objects
                if isinstance(value, (str, bytes)):
                    try:
                        parsed = json.loads(value)
                        if isinstance(parsed, (dict, list)):
                            state[key] = parsed
                            continue
                    except (json.JSONDecodeError, TypeError):
                        pass
                state[key] = value

            if hasattr(record_klass, "hydrate_formax_model"):
                copied = state.copy()
                record_id = copied.pop("id")
                record = record_klass.hydrate_formax_model(copied)
                record.change_object_id(record_id)
            else:
                record = record_klass.__new__(record_klass)
                record.__setstate__(state)
            return record
        except Exception as e:
            logger.error(f"Failed to deserialize record: {e}")
            raise SerializationError(f"Deserialization failed: {e}")

    def _create_result_stream(
        self,
        record_keys: typing.List[str],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        chunk_size: int = 1000,
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        """Create a result stream for querying records."""
        from volnux.result.stream import ResultStream

        return ResultStream._make(
            model_klass=record_klass,
            keys=record_keys,
            chunk_size=chunk_size,
            memory_capacity=10,
            persisted_backend=self,
            transaction_id=f"{self.__class__.__name__}",
            predicates=[],
        )

    def supports_foreign_keys(self) -> bool:
        """Check if the backend supports foreign key constraints.

        Returns:
            True if foreign key constraints are supported, False otherwise.
        """
        return False

    def execute_query(self, query: str, *args, **kwargs) -> Any:
        """Execute a query against the backend."""
        raise NotImplementedError("Backend does not support execute_query.")

    def resolve_physical_target(
        self, info: "StorageRoute", overrides: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Resolves the physical schema.
        Accepts optional overrides for administrative cross-tenant reads.
        """
        context = {}
        if info.routing_keys:
            if callable(info.routing_keys):
                context = info.routing_keys()
            else:
                context = info.routing_keys.copy()  # Prevent mutation of static dicts

            if overrides:
                context.update(overrides)

        resolved_components = []
        for comp in info.components:
            if comp.startswith("{") and comp.endswith("}"):
                key = comp[1:-1]
                val = context.get(key)
                if val is not None:
                    resolved_components.append(str(val))
                # If val is None, the component is dropped (e.g., standalone saga)
            else:
                resolved_components.append(comp)

        return self.NAMESPACE_SEPARATOR.join(resolved_components)

    @abc.abstractmethod
    def exists(self, schema_name: str, record_key: str) -> bool:
        """Check if a record exists in the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The unique key identifying the record.

        Returns:
            True if the record exists, False otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def insert(
        self,
        schema_name: str,
        record_key: str,
        record: "KeyValueStoreIntegrationMixin",
        ttl: Optional[int] = None,
    ) -> None:
        """Insert a new record into the store.

        Args:
            schema_name: The schema/namespace to insert into.
            record_key: The unique key for the new record.
            record: The record object to insert.
            ttl: Optional TTL for the new record.

        Raises:
            KeyError: If a record with the same key already exists.
        """
        raise NotImplementedError

    def bulk_insert(
        self,
        schema_name: str,
        records: Dict[str, "KeyValueStoreIntegrationMixin"],
        ttl: Optional[int] = None,
    ) -> int:
        """Insert multiple records in a single operation.

        Args:
            schema_name: The schema to insert into.
            records: Dictionary mapping record keys to record objects.
            ttl: Optional TTL for all inserted records.

        Raises:
            SerializationError: If serialization fails.
            ConnectionError: If Redis operation fails.
        """
        pass

    @abc.abstractmethod
    def update(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> Optional[int]:
        """Update an existing record in the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to update.
            record: The updated record object.

        Raises:
            KeyError: If the record does not exist.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, schema_name: str, record_key: str) -> Optional[int]:
        """Delete a record from the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record is to delete.

        Raises:
            KeyError: If the record does not exist.
        """
        raise NotImplementedError

    def bulk_delete(self, schema_name: str, record_keys: typing.List[str]) -> int:
        """Delete multiple records in a single operation.

        Args:
            schema_name: The schema containing the records.
            record_keys: List of record keys to delete.

        Raises:
            ConnectionError: If Redis operation fails.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(
        self,
        schema_name: str,
        record_key: Union[str, int],
        record_klass: Type["KeyValueStoreIntegrationMixin"],
    ) -> Optional["KeyValueStoreIntegrationMixin"]:
        """Retrieve a single record from the store.

        Args:
            schema_name: The schema/namespace containing the record.
            record_key: The key of the record to retrieve.
            record_klass: The class to instantiate the record with.

        Returns:
            The record instance if found, None otherwise.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def filter(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        order_by: Optional[str] = None,
        **filter_kwargs: Any,
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        """Filter records matching the specified criteria.

        Args:
            schema_name: The schema/namespace to filter within.
            record_klass: The class to instantiate records with.
            limit: Maximum number of records to return (default: None, no limit).
            offset: Number of records to skip (default: None, no offset).
            order_by: Attribute to order results by (default: None, no order).
            **filter_kwargs: Attribute-value pairs to filter by.

        Returns:
            An iterable of matching record instances.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def count(
        self,
        schema_name: str,
        record_klass: Type["KeyValueStoreIntegrationMixin"],
        **filter_kwargs: Any,
    ) -> int:
        """Count records in a schema, optionally filtered.

        Args:
            schema_name: The schema/namespace to count within.
            record_klass: The class to instantiate records with.
            **filter_kwargs: Optional attribute-value pairs to filter by.

        Returns:
            The number of matching records.
        """
        raise NotImplementedError

    @staticmethod
    def load_record(
        record_state: bytes, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> "KeyValueStoreIntegrationMixin":
        """Load a record from its serialized state.

        Args:
            record_state: The serialized record data.
            record_klass: The class to instantiate the record with.

        Returns:
            The instantiated record object.

        Raises:
            SerializationError: If deserialization fails.
        """
        try:
            state = json.loads(record_state)
            record = record_klass.__new__(record_klass)
            record.__setstate__(state)
            return record
        except Exception as e:
            raise SerializationError(f"Failed to load record: {e}")

    @abc.abstractmethod
    def reload(
        self, schema_name: str, record: "KeyValueStoreIntegrationMixin"
    ) -> "KeyValueStoreIntegrationMixin":
        """Reload a record's data from the backend.

        Args:
            schema_name: The schema/namespace containing the record.
            record: The record to reload.

        Returns:
            The reloaded record instance.

        Raises:
            KeyError: If the record no longer exists.
        """
        raise NotImplementedError

    def upsert(
        self, schema_name: str, record_key: str, record: "KeyValueStoreIntegrationMixin"
    ) -> Optional[int]:
        """Insert or update a record (upsert operation).

        Args:
            schema_name: The schema/namespace for the operation.
            record_key: The key of the record.
            record: The record object to upsert.
        """
        if self.exists(schema_name, record_key):
            self.update(schema_name, record_key, record)
        else:
            self.insert(schema_name, record_key, record)

    def list_all(
        self, schema_name: str, record_klass: Type["KeyValueStoreIntegrationMixin"]
    ) -> "ResultStream[KeyValueStoreIntegrationMixin]":
        """List all records in a schema.

        Args:
            schema_name: The schema/namespace to list from.
            record_klass: The class to instantiate records with.

        Returns:
            An iterable of all records in the schema.
        """
        return self.filter(schema_name, record_klass)

    # Custom Performance Ops
    async def atomic_update_field(
        self, key: str, field: str, value: Union[bytes, str]
    ) -> bool:
        """Atomically update a single JSON field within a stored object.

        Returns False if the key does not exist. The backend must guarantee
        this operation is atomic (no read-modify-write race).

        Implementations:
          - Redis: Lua script with GET + SET + KEEPTTL
          - Postgres: UPDATE ... SET jsonb_col = jsonb_set(...) WHERE key = ...
          - InMemory: asyncio.Lock-protected dict mutation
        """
        raise NotImplementedError("Not supported")

    async def atomic_append_to_field(
        self, key: str, field: str, value: Union[bytes, str]
    ) -> bool:
        """Atomically append an element to a JSON array field within a stored object.

        Returns False if the key does not exist. Creates the array if the
        field doesn't yet exist. Backend must guarantee atomicity.

        Implementations:
          - Redis: Lua script with table.insert
          - Postgres: UPDATE ... SET jsonb_col = jsonb_col || jsonb_build_array(...)
          - InMemory: asyncio.Lock-protected list.append
        """
        raise NotImplementedError("Not supported")
