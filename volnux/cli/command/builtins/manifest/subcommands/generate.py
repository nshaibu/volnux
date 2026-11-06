import jsonschema
import importlib
import importlib.util
import inspect
import json
import types
import typing
from pathlib import Path

from volnux.event.base import EventBase
from volnux.cli.command.base import SubCommand, CommandCategory, CommandError
from volnux.cli.command.builtins.manifest.utils import _load_schema, _MANIFEST_FILENAME


class GenerateManifestSubCommand(SubCommand):
    """
    Introspect all EventBase subclasses reachable from a Python package and
    generate a volnux.manifest.json scaffold.

    Usage
    -----
        volnux generate_manifest --package-name my-package --package-version 1.0.0
        volnux generate_manifest --source-module myapp.events
        volnux generate_manifest --output ./volnux.manifest.json --dry-run

    The command scans every subclass of EventBase found in the target module
    (and its submodules), reads the class-level attributes that map to
    EventBase fields, and builds a manifest dict. When --dry-run is set the
    manifest is printed but not written to disk.
    """

    name = "generate_manifest"
    help = (
        "Introspect EventBase subclasses in a Python module and generate a "
        "volnux.manifest.json scaffold. Review and complete the output before "
        "submitting to EventHub."
    )
    category = CommandCategory.DEVELOPMENT

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--source-module",
            metavar="MODULE",
            required=True,
            help=(
                "Fully-qualified Python module to scan for EventBase subclasses. "
                "e.g. 'myapp.events' or 'volnux_postgres_connectors'."
            ),
        )
        parser.add_argument(
            "--package-name",
            metavar="NAME",
            default=None,
            help=(
                "PyPI-style package name for the manifest package.name field. "
                "Defaults to the source module name with underscores replaced by hyphens."
            ),
        )
        parser.add_argument(
            "--package-version",
            metavar="VERSION",
            default="1.0.0",
            help="Semver package version. Defaults to '1.0.0'.",
        )
        parser.add_argument(
            "--author",
            metavar="AUTHOR",
            default="unknown",
            help="Publisher name or organisation slug.",
        )
        parser.add_argument(
            "--license",
            metavar="LICENSE",
            dest="license_id",
            default="MIT",
            help="SPDX licence identifier. Defaults to 'MIT'.",
        )
        parser.add_argument(
            "--source-type",
            metavar="TYPE",
            choices=["pypi", "git", "hub", "local"],
            default="pypi",
            help="Distribution channel for package.source.type. Defaults to 'pypi'.",
        )
        parser.add_argument(
            "--sha256",
            metavar="HEX",
            default=None,
            help=(
                "SHA-256 of the package file (the wheel). Required in the manifest of a "
                "package whose --source-type is 'hub': the loader refuses a download "
                "that does not match."
            ),
        )
        parser.add_argument(
            "--volnux-requires",
            metavar="SPEC",
            default=">=0.8.0",
            help="Minimum volnux-core version specifier. Defaults to '>=0.8.0'.",
        )
        parser.add_argument(
            "--python-requires",
            metavar="SPEC",
            default=">=3.11",
            help="Python version specifier. Defaults to '>=3.11'.",
        )
        parser.add_argument(
            "--output",
            metavar="PATH",
            default=None,
            help=(
                f"Output file path. Defaults to ./{_MANIFEST_FILENAME} "
                "in the current directory."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Print the generated manifest to stdout without writing to disk.",
        )
        parser.add_argument(
            "--skip-validate",
            action="store_true",
            default=False,
            help="Skip running the manifest through the JSON Schema validator after generation.",
        )

    def handle(self, *args, **options) -> typing.Optional[str]:
        source_module: str = options["source_module"]
        pkg_version: str = options["package_version"]
        pkg_name: str = options.get("package_name") or self._default_package_name(
            source_module
        )
        author: str = options.get("author", "unknown")
        license_id: str = options.get("license_id", "MIT")
        source_type: str = options.get("source_type", "pypi")
        volnux_req: str = options.get("volnux_requires", ">=0.8.0")
        python_req: str = options.get("python_requires", ">=3.11")
        sha256: typing.Optional[str] = options.get("sha256")
        dry_run: bool = options.get("dry_run", False)
        skip_validate: bool = options.get("skip_validate", False)
        output_path: Path = (
            Path(options["output"]).expanduser().resolve()
            if options.get("output")
            else Path.cwd() / _MANIFEST_FILENAME
        )

        self.success(f"Scanning module '{source_module}' for EventBase subclasses …\n")

        try:
            module = importlib.import_module(source_module)
        except ImportError as exc:
            raise CommandError(
                f"Cannot import module '{source_module}': {exc}. "
                "Make sure the package is installed in the active Python environment."
            ) from exc

        event_classes = self._collect_event_classes(module, source_module)

        if not event_classes:
            raise CommandError(
                f"No EventBase subclasses found in '{source_module}'. "
                "Ensure the module contains classes that inherit from EventBase."
            )

        self.success(f"Found {len(event_classes)} event class(es).\n")

        manifest = self._build_manifest(
            event_classes=event_classes,
            pkg_name=pkg_name,
            pkg_version=pkg_version,
            author=author,
            license_id=license_id,
            source_type=source_type,
            source_module=source_module,
            volnux_req=volnux_req,
            python_req=python_req,
            sha256=sha256,
        )

        manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)

        if not skip_validate:
            self._validate_generated(manifest)

        if dry_run:
            self.stdout.write(manifest_json + "\n")
            self.warning("Dry run — manifest was NOT written to disk.\n")
            return None

        if output_path.exists():
            self.warning(f"Overwriting existing manifest at {output_path}\n")

        try:
            output_path.write_text(manifest_json + "\n", encoding="utf-8")
        except OSError as exc:
            raise CommandError(
                f"Failed to write manifest to {output_path}: {exc}"
            ) from exc

        self.success(f"✓ Manifest written to {output_path}\n")
        self.warning(
            "Review the generated manifest before publishing. "
            "Fields marked '(TODO)' require manual completion.\n"
        )
        return None

    @staticmethod
    def _default_package_name(source_module: str) -> str:
        """A PyPI-style name (lowercase, hyphens only) from a module path:
        ``my_app.events`` is ``my-app-events``. (The dot was kept, which the schema
        rejects.)"""
        import re

        return re.sub(r"[^a-z0-9]+", "-", source_module.lower()).strip("-")

    def _collect_event_classes(
        self, module: typing.Any, source_module: str
    ) -> typing.List[typing.Type[EventBase]]:
        """
        Walk *module* and all sub-modules whose __name__ starts with
        *source_module*, collecting every concrete EventBase subclass.

        Concrete means: not abstract (no unimplemented abstractmethods),
        not EventBase itself.
        """
        found: typing.Dict[str, typing.Type[EventBase]] = {}

        def _scan(mod: typing.Any) -> None:
            for _attr_name, obj in inspect.getmembers(mod, inspect.isclass):
                if (
                    obj is EventBase
                    or not issubclass(obj, EventBase)
                    or inspect.isabstract(obj)
                ):
                    continue
                # Only collect classes whose defining module belongs to the
                # requested package — avoids pulling in upstream EventBase
                # subclasses from volnux-core itself.
                defining_module: str = getattr(obj, "__module__", "") or ""
                if not defining_module.startswith(source_module):
                    continue
                key = f"{defining_module}.{obj.__qualname__}"
                if key not in found:
                    found[key] = obj

        _scan(module)

        # Recursively scan sub-modules if the module is a package
        if hasattr(module, "__path__"):
            import pkgutil

            for _finder, submod_name, _ispkg in pkgutil.walk_packages(
                module.__path__, prefix=module.__name__ + "."
            ):
                try:
                    submod = importlib.import_module(submod_name)
                    _scan(submod)
                except Exception as exc:
                    self.warning(f"Could not import sub-module {submod_name}: {exc}\n")

        return list(found.values())

    def _build_manifest(
        self,
        event_classes: typing.List[typing.Type[EventBase]],
        pkg_name: str,
        pkg_version: str,
        author: str,
        license_id: str,
        source_type: str,
        source_module: str,
        volnux_req: str,
        python_req: str,
        sha256: typing.Optional[str] = None,
    ) -> typing.Dict:
        """Assemble the full manifest dict from introspected data."""

        return {
            "$schema": "https://eventhub.volnux.dev/manifest/v1.json",
            "manifest_version": "1",
            "package": self._build_package_block(
                pkg_name=pkg_name,
                pkg_version=pkg_version,
                author=author,
                license_id=license_id,
                source_type=source_type,
                source_module=source_module,
                volnux_req=volnux_req,
                python_req=python_req,
                sha256=sha256,
            ),
            "events": [self._build_event_entry(cls) for cls in event_classes],
            "changelog": self._build_initial_changelog(pkg_version),
            "dependencies": {
                "volnux-core": volnux_req,
            },
            "compatibility": {
                "volnux": [self._runtime_series()],
                "python": ["3.11", "3.12"],
            },
        }

    @staticmethod
    def _runtime_series() -> str:
        """``major.minor.x`` of the Volnux running now (what the package was tested on)."""
        import re

        from volnux import __version__

        match = re.match(r"(\d+)\.(\d+)", str(__version__))
        return f"{match.group(1)}.{match.group(2)}.x" if match else "0.9.x"

    def _build_package_block(
        self,
        pkg_name: str,
        pkg_version: str,
        author: str,
        license_id: str,
        source_type: str,
        source_module: str,
        volnux_req: str,
        python_req: str,
        sha256: typing.Optional[str] = None,
    ) -> typing.Dict:
        source: typing.Dict[str, typing.Any] = {
            "type": source_type,
            "package": pkg_name if source_type == "pypi" else source_module,
            "version": pkg_version,
        }
        if sha256:
            source["sha256"] = sha256.lower()
        return {
            "name": pkg_name,
            "version": pkg_version,
            "package_type": "event",
            "description": "(TODO) Short description of this package.",
            "author": author,
            "license": license_id,
            "python_requires": python_req,
            "volnux_requires": volnux_req,
            "keywords": [],
            "source": source,
        }

    # ------------------------------------------------------------------ events

    _MAX_CATEGORIES = 5

    def _build_event_entry(self, cls: typing.Type[EventBase]) -> typing.Dict:
        """
        Read the EventBase class attributes and produce one ``events[]`` entry in the
        shape of ``event_entry`` in schema/event/v1.json (the same shape the runtime
        loader expects):

            identity     class, module, namespace, name, version
            lifecycle    deprecated, deprecation_info, changelog
            discovery    categories, event_type
            execution    executor, executor_config, result_evaluation_strategy, streaming
            parameters   init_params (INIT_PARAMS_SCHEMA), call_params (process())
        """
        class_name: str = cls.__name__
        deprecated: bool = bool(getattr(cls, "deprecated", False))

        return {
            "class": class_name,
            "module": cls.__module__,
            "namespace": getattr(cls, "namespace", None) or "local",
            "name": getattr(cls, "name", None) or class_name,
            "version": getattr(cls, "version", "1.0.0"),
            "categories": self._categories(cls),
            "event_type": self._event_type(cls),
            "deprecated": deprecated,
            "deprecation_info": (
                self._deprecation_info(getattr(cls, "deprecation_info", None))
                if deprecated
                else None
            ),
            "changelog": getattr(cls, "changelog", None),
            "result_evaluation_strategy": self._strategy_name(
                getattr(cls, "result_evaluation_strategy", None)
            ),
            "executor": self._executor_name(getattr(cls, "executor", None)),
            "executor_config": self._jsonable_dict(
                getattr(cls, "executor_config", None)
            ),
            "streaming": bool(getattr(cls, "_volnux_streaming", False)),
            "init_params": self._init_params(cls),
            "call_params": self._call_params(cls),
        }

    def _categories(self, cls: typing.Type[EventBase]) -> typing.List[str]:
        from enum import Enum

        raw = getattr(cls, "categories", None) or ()
        if isinstance(raw, (str, Enum)):
            raw = [raw]
        names = []
        for category in raw:
            name = category.name if isinstance(category, Enum) else str(category)
            name = name.upper()
            if name not in names:
                names.append(name)
        names.sort()
        return names[: self._MAX_CATEGORIES] or ["OTHER"]

    @staticmethod
    def _event_type(cls: typing.Type[EventBase]) -> str:
        raw = getattr(cls, "event_type", None)
        value = str(getattr(raw, "value", raw) or "other").lower()
        return value if value in ("system", "meta", "other") else "other"

    @staticmethod
    def _strategy_name(strategy: typing.Any) -> str:
        from volnux.result_evaluators import (
            AllTasksMustSucceedStrategy,
            AnyTaskMustSucceedStrategy,
            MajorityTasksMustSucceedStrategy,
            NoFailuresAllowedStrategy,
        )

        known = (
            (AllTasksMustSucceedStrategy, "ALL_MUST_SUCCEED"),
            (AnyTaskMustSucceedStrategy, "ANY_MUST_SUCCEED"),
            (MajorityTasksMustSucceedStrategy, "MAJORITY_MUST_SUCCEED"),
            (NoFailuresAllowedStrategy, "NO_FAILURES_ALLOWED"),
        )
        if strategy is None:
            return "ALL_MUST_SUCCEED"
        for strategy_class, name in known:
            if type(strategy) is strategy_class:
                return name
        return "CUSTOM"  # a threshold strategy or one of the author's own

    @staticmethod
    def _executor_name(executor: typing.Any) -> str:
        if executor is None:
            return "default"
        if isinstance(executor, str):
            return executor
        if inspect.isclass(executor):
            return executor.__name__
        return type(executor).__name__

    @staticmethod
    def _jsonable(value: typing.Any) -> typing.Any:
        """``value`` as plain JSON data (anything else becomes its text)."""
        try:
            return json.loads(json.dumps(value, default=str))
        except (TypeError, ValueError):
            return str(value)

    def _jsonable_dict(self, value: typing.Any) -> typing.Optional[typing.Dict]:
        if value is None:
            return None
        if hasattr(value, "to_dict"):
            try:
                value = value.to_dict()
            except Exception:
                value = getattr(value, "__dict__", None)
        elif not isinstance(value, dict):
            value = getattr(value, "__dict__", None)
        return self._jsonable(value) if isinstance(value, dict) else None

    def _deprecation_info(self, info: typing.Any) -> typing.Dict:
        """The class's ``DeprecationInfo`` (a dict) in the manifest's shape. What the
        class does not say is left as a TODO to complete by hand."""
        info = info if isinstance(info, dict) else {}
        return {
            "since_version": info.get("since_version", "(TODO)"),
            "reason": info.get(
                "reason", "(TODO) Describe why this event is deprecated."
            ),
            "replacement": info.get("replacement"),
            "removal_version": info.get("removal_version", "(TODO)"),
        }

    # ------------------------------------------------------------- parameters

    def _init_params(self, cls: typing.Type[EventBase]) -> typing.Dict:
        """Parameters from ``INIT_PARAMS_SCHEMA`` (the event's extra init arguments).

        Validators are Python callables and cannot be written to JSON; a parameter's
        allowed values go to ``enum`` (from ``choices``).
        """
        raw = getattr(cls, "INIT_PARAMS_SCHEMA", None) or {}
        result: typing.Dict[str, typing.Any] = {}
        for param_name, spec in raw.items():
            spec = spec if isinstance(spec, dict) else getattr(spec, "__dict__", {})
            required = bool(spec.get("required", False))
            entry: typing.Dict[str, typing.Any] = {
                "type": self._param_type(spec.get("type")),
                "required": required,
                "description": spec.get("description")
                or f"(TODO) Describe the '{param_name}' parameter.",
            }
            if not required:
                entry["default"] = self._default_of(spec)
            allowed = spec.get("choices") or spec.get("enum")
            if allowed and len(set(map(str, allowed))) >= 2:
                entry["enum"] = [str(v) for v in dict.fromkeys(allowed)]
            if spec.get("examples"):
                entry["examples"] = list(spec["examples"])[:5]
            result[param_name] = entry
        return result

    def _default_of(self, spec: typing.Dict) -> typing.Any:
        from volnux.constants import EMPTY

        default = spec.get("default", EMPTY)
        if default is EMPTY or default is None:
            factory = spec.get("default_factory")
            if callable(factory):
                try:
                    default = factory()
                except Exception:
                    default = None
            else:
                default = None
        return self._jsonable(default)

    def _call_params(self, cls: typing.Type[EventBase]) -> typing.Dict:
        """Arguments of ``process()``, from its signature (``self``, ``*args`` and
        ``**kwargs`` are not parameters a workflow supplies)."""
        process = getattr(cls, "process", None)
        if process is None:
            return {}
        try:
            signature = inspect.signature(process)
        except (ValueError, TypeError):
            return {}
        try:
            hints = typing.get_type_hints(process)
        except Exception:
            hints = dict(getattr(process, "__annotations__", {}) or {})

        result: typing.Dict[str, typing.Any] = {}
        for param_name, param in signature.parameters.items():
            if param_name == "self" or param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue
            has_default = param.default is not inspect.Parameter.empty
            entry: typing.Dict[str, typing.Any] = {
                "type": self._param_type(hints.get(param_name)),
                "required": not has_default,
                "description": f"(TODO) Describe the '{param_name}' argument.",
            }
            if has_default:
                entry["default"] = self._jsonable(param.default)
            result[param_name] = entry
        return result

    def _param_type(self, declared: typing.Any) -> str:
        """The manifest type name (``param_type_enum``) for a declared type."""
        if declared is None:
            return "any"
        if isinstance(declared, str):
            return self._normalise_type_str(declared)
        if isinstance(declared, (tuple, list)):
            members = [t for t in declared if t is not type(None)]
            if len(members) == 1:
                base = self._param_type(members[0])
                nullable = f"{base} | None" if len(members) != len(declared) else base
                return nullable if self._is_valid_type(nullable) else "any"
            return "any"
        return self._type_hint_to_str(declared)

    @staticmethod
    def _is_valid_type(name: str) -> bool:
        from volnux.manifest.schemas import load_schema

        return name in load_schema("event")["$defs"]["param_type_enum"]["enum"]

    def _type_hint_to_str(self, hint: typing.Any) -> str:
        """Convert a runtime type hint to the nearest manifest param_type_enum string."""
        if hint is None:
            return "any"

        _MAP: typing.Dict[typing.Any, str] = {
            str: "str",
            int: "int",
            float: "float",
            bool: "bool",
            dict: "dict",
            list: "list",
        }
        if hint in _MAP:
            return _MAP[hint]

        origin = typing.get_origin(hint)
        args = typing.get_args(hint)

        # Optional[X] / X | None
        if origin is typing.Union or origin is types.UnionType:
            if type(None) in args:
                inner = [a for a in args if a is not type(None)]
                if len(inner) == 1:
                    nullable = self._type_hint_to_str(inner[0]) + " | None"
                    return nullable if self._is_valid_type(nullable) else "any"
            return "any"
        if origin is list:
            element = {str: "str", int: "int", float: "float"}.get(
                args[0] if args else None
            )
            return f"list[{element}]" if element else "list"

        if origin is dict:
            if len(args) == 2 and args[0] is str and args[1] is str:
                return "dict[str, str]"
            if len(args) == 2 and args[0] is str:
                return "dict[str, any]"
            return "dict"

        return "any"

    @staticmethod
    def _normalise_type_str(raw: str) -> str:
        """Best-effort normalisation of a freeform type string to a schema enum value."""
        from volnux.manifest.schemas import load_schema

        valid = set(load_schema("event")["$defs"]["param_type_enum"]["enum"])
        clean = raw.strip()
        if clean in valid:
            return clean
        _ALIAS = {
            "Optional[str]": "str | None",
            "Optional[int]": "int | None",
            "Optional[float]": "float | None",
            "Optional[bool]": "bool | None",
            "Optional[list[str]]": "list[str] | None",
            "Optional[dict]": "dict | None",
            "Optional[list]": "list | None",
            "List[str]": "list[str]",
            "List[int]": "list[int]",
            "List[float]": "list[float]",
            "List": "list",
            "Dict": "dict",
            "Dict[str, str]": "dict[str, str]",
            "Dict[str, Any]": "dict[str, any]",
            "typing.Optional[str]": "str | None",
            "<class 'str'>": "str",
            "<class 'int'>": "int",
            "<class 'float'>": "float",
            "<class 'bool'>": "bool",
            "<class 'dict'>": "dict",
            "<class 'list'>": "list",
            "Any": "any",
        }
        return _ALIAS.get(clean, "any")

    def _build_initial_changelog(self, version: str) -> typing.Dict:
        import datetime

        today = datetime.date.today().isoformat()
        return {
            version: {
                "date": today,
                "type": "initial",
                "entries": [
                    "(TODO) Describe what this version includes.",
                ],
            }
        }

    def _validate_generated(self, manifest: typing.Dict) -> None:
        """Run the schema validator on the freshly built manifest and emit warnings."""
        try:
            schema = _load_schema("event")
        except CommandError:
            self.warning("Could not load schema for post-generation validation.\n")
            return

        validator = jsonschema.Draft202012Validator(schema)
        errors = list(validator.iter_errors(manifest))
        if errors:
            self.warning(
                f"{len(errors)} schema issue(s) found in generated manifest "
                "(likely TODO placeholders). Run 'volnux validate_manifest' "
                "after completing the file.\n"
            )
        else:
            self.success("Generated manifest passes schema validation.\n")
