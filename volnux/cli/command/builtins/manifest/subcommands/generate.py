import jsonschema
import importlib
import importlib.util
import inspect
import json
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
        pkg_name: str = options.get("package_name") or source_module.replace("_", "-")
        author: str = options.get("author", "unknown")
        license_id: str = options.get("license_id", "MIT")
        source_type: str = options.get("source_type", "pypi")
        volnux_req: str = options.get("volnux_requires", ">=0.8.0")
        python_req: str = options.get("python_requires", ">=3.11")
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
            ),
            "events": [self._build_event_entry(cls) for cls in event_classes],
            "changelog": self._build_initial_changelog(pkg_version),
            "dependencies": {
                "volnux-core": volnux_req,
            },
            "compatibility": {
                "volnux": ["0.9.x"],
                "python": ["3.11", "3.12"],
            },
        }

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
    ) -> typing.Dict:
        return {
            "name": pkg_name,
            "version": pkg_version,
            "description": "(TODO) Short description of this package.",
            "author": author,
            "license": license_id,
            "homepage": "(TODO) https://github.com/your-org/" + pkg_name,
            "repository": "(TODO) https://github.com/your-org/" + pkg_name,
            "python_requires": python_req,
            "volnux_requires": volnux_req,
            "keywords": [],
            "source": {
                "type": source_type,
                "package": pkg_name if source_type == "pypi" else source_module,
                "version": pkg_version,
            },
        }

    def _build_event_entry(self, cls: typing.Type[EventBase]) -> typing.Dict:
        """
        Read EventBase class attributes and produce one events[] entry.

        Attributes read directly from the class:
            version, changelog, deprecated, deprecation_info,
            namespace, name, event_type, result_evaluation_strategy,
            executor, executor_config, EXTRA_INIT_PARAMS_SCHEMA
        """
        module_path: str = cls.__module__
        class_name: str = cls.__name__

        version: str = getattr(cls, "version", "1.0.0")
        changelog_val = getattr(cls, "changelog", None)
        deprecated: bool = bool(getattr(cls, "deprecated", False))
        dep_info = getattr(cls, "deprecation_info", None)
        namespace: str = getattr(cls, "namespace", "local")
        name_val = getattr(cls, "name", None)

        # EventType enum → string
        event_type_raw = getattr(cls, "event_type", None)
        event_type_str: str = (
            event_type_raw.value
            if hasattr(event_type_raw, "value")
            else str(event_type_raw) if event_type_raw is not None else "OTHER"
        )
        # Normalise to the schema enum (strip module prefix if present)
        event_type_str = event_type_str.split(".")[-1].upper()

        # result_evaluation_strategy → string
        res_strat_raw = getattr(cls, "result_evaluation_strategy", None)
        res_strat_str: str = (
            res_strat_raw.__class__.__name__
            if res_strat_raw is not None
            else "ALL_MUST_SUCCEED"
        )
        # Map common class names to the enum values used in the schema
        _strat_map = {
            "AllTasksMustSucceedStrategy": "ALL_MUST_SUCCEED",
            "NoFailuresAllowedStrategy": "NO_FAILURES_ALLOWED",
            "AnyTaskMustSucceedStrategy": "ANY_MUST_SUCCEED",
            "MajorityMustSucceedStrategy": "MAJORITY_MUST_SUCCEED",
        }
        res_strat_str = _strat_map.get(res_strat_str, "ALL_MUST_SUCCEED")

        # executor class name
        executor_cls = getattr(cls, "executor", None)
        executor_name: str = (
            executor_cls.__name__
            if inspect.isclass(executor_cls)
            else "DefaultExecutor"
        )

        executor_config = getattr(cls, "executor_config", None)
        executor_config_dict: typing.Optional[typing.Dict] = (
            executor_config.__dict__ if executor_config is not None else None
        )

        # DeprecationInfo → dict
        dep_info_dict: typing.Optional[typing.Dict] = None
        if deprecated and dep_info is not None:
            dep_info_dict = {
                "since_version": getattr(dep_info, "since_version", "(TODO)"),
                "reason": getattr(dep_info, "reason", "(TODO)"),
                "replacement": getattr(dep_info, "replacement", None),
                "removal_version": getattr(dep_info, "removal_version", "(TODO)"),
            }
        elif deprecated:
            # deprecated=True but no DeprecationInfo object — scaffold it
            dep_info_dict = {
                "since_version": "(TODO)",
                "reason": "(TODO) Describe why this event is deprecated.",
                "replacement": None,
                "removal_version": "(TODO)",
            }

        # EXTRA_INIT_PARAMS_SCHEMA → extra_init_params_schema dict
        raw_schema: typing.Dict = getattr(cls, "EXTRA_INIT_PARAMS_SCHEMA", {}) or {}
        extra_init = self._build_extra_init_params(raw_schema, cls)

        entry: typing.Dict[str, typing.Any] = {
            "class": class_name,
            "module": module_path,
            "namespace": namespace,
            "name": name_val or class_name,
            "version": version,
            "event_type": (
                event_type_str
                if event_type_str
                in (
                    "EXTRACT",
                    "TRANSFORM",
                    "LOAD",
                    "VALIDATE",
                    "NOTIFY",
                    "AI",
                    "CONTROL",
                    "CHECKPOINT",
                    "OTHER",
                )
                else "OTHER"
            ),
            "deprecated": deprecated,
            "deprecation_info": dep_info_dict,
            "changelog": changelog_val,
            "result_evaluation_strategy": res_strat_str,
            "executor": executor_name,
            "executor_config": executor_config_dict,
            "extra_init_params_schema": extra_init,
        }

        return entry

    def _build_extra_init_params(
        self,
        raw_schema: typing.Dict,
        cls: typing.Type[EventBase],
    ) -> typing.Dict:
        """
        Convert EXTRA_INIT_PARAMS_SCHEMA entries into the manifest's
        extra_init_params_schema format.

        Falls back to inspecting __init__ type annotations when the
        schema dict is empty or absent.
        """
        if raw_schema:
            return self._schema_from_extra_init_dict(raw_schema)
        return self._schema_from_init_signature(cls)

    def _schema_from_extra_init_dict(self, raw: typing.Dict) -> typing.Dict:
        """
        Translate EXTRA_INIT_PARAMS_SCHEMA values (ExtraEventInitKwargs)
        into manifest param entries.
        """
        result: typing.Dict = {}
        for param_name, spec in raw.items():
            # spec may be a dataclass, TypedDict, or plain dict
            if isinstance(spec, dict):
                field = spec
            else:
                field = spec.__dict__ if hasattr(spec, "__dict__") else {}

            p_type = field.get("type", "any")
            required = bool(field.get("required", True))
            desc = field.get("description", "(TODO) Describe this parameter.")
            default = field.get("default", None)
            enum_vals = field.get("enum", None)
            examples = field.get("examples", None)

            entry: typing.Dict[str, typing.Any] = {
                "type": self._normalise_type_str(str(p_type)),
                "required": required,
                "description": desc,
            }
            if not required:
                entry["default"] = default
            if enum_vals:
                entry["enum"] = list(enum_vals)
            if examples:
                entry["examples"] = list(examples)

            result[param_name] = entry

        return result

    def _schema_from_init_signature(self, cls: typing.Type[EventBase]) -> typing.Dict:
        """
        Fall back to inspecting __init__ annotations when EXTRA_INIT_PARAMS_SCHEMA
        is not declared, skipping 'self' and 'execution_context'.
        """
        result: typing.Dict = {}
        _SKIP = {"self", "execution_context", "args", "kwargs"}

        try:
            sig = inspect.signature(cls.__init__)
        except (ValueError, TypeError):
            return result

        hints = (
            typing.get_type_hints(cls.__init__)
            if hasattr(cls.__init__, "__annotations__")
            else {}
        )

        for param_name, param in sig.parameters.items():
            if param_name in _SKIP:
                continue
            if param.kind in (
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            ):
                continue

            has_default = param.default is not inspect.Parameter.empty
            annotation = hints.get(param_name, None)
            type_str = self._type_hint_to_str(annotation)
            default_val = param.default if has_default else None

            entry: typing.Dict[str, typing.Any] = {
                "type": type_str,
                "required": not has_default,
                "description": f"(TODO) Describe the '{param_name}' parameter.",
            }
            if has_default:
                entry["default"] = (
                    default_val
                    if isinstance(
                        default_val, (str, int, float, bool, list, dict, type(None))
                    )
                    else str(default_val)
                )

            result[param_name] = entry

        return result

    def _type_hint_to_str(self, hint: typing.Any) -> str:
        """Convert a runtime type hint to the nearest manifest param_type_enum string."""
        if hint is None:
            return "any"

        _MAP: typing.Dict[typing.Any, str] = {
            str: "str",
            int: "int",
            float: "float",
            bool: "bool",
        }
        if hint in _MAP:
            return _MAP[hint]

        # Handle Optional[X] — typing.Union[X, None]
        origin = getattr(hint, "__origin__", None)
        args = getattr(hint, "__args__", ())

        if origin is typing.Union and type(None) in args:
            inner = [a for a in args if a is not type(None)]
            if len(inner) == 1:
                base = self._type_hint_to_str(inner[0])
                nullable = base + " | None"
                _VALID = {
                    "str | None",
                    "int | None",
                    "float | None",
                    "bool | None",
                    "list[str] | None",
                    "list[int] | None",
                    "dict | None",
                }
                return nullable if nullable in _VALID else "any"

        if origin is list:
            if args and args[0] is str:
                return "list[str]"
            if args and args[0] is int:
                return "list[int]"
            if args and args[0] is float:
                return "list[float]"
            return "any"

        if origin is dict:
            if len(args) == 2 and args[0] is str and args[1] is str:
                return "dict[str, str]"
            return "dict"

        return "any"

    @staticmethod
    def _normalise_type_str(raw: str) -> str:
        """Best-effort normalisation of a freeform type string to a schema enum value."""
        _DIRECT = {
            "str",
            "int",
            "float",
            "bool",
            "list[str]",
            "list[int]",
            "list[float]",
            "dict",
            "dict[str, str]",
            "dict[str, any]",
            "str | None",
            "int | None",
            "float | None",
            "bool | None",
            "list[str] | None",
            "list[int] | None",
            "dict | None",
            "any",
        }
        clean = raw.strip()
        if clean in _DIRECT:
            return clean
        # Common aliases
        _ALIAS = {
            "Optional[str]": "str | None",
            "Optional[int]": "int | None",
            "Optional[float]": "float | None",
            "Optional[bool]": "bool | None",
            "Optional[list[str]]": "list[str] | None",
            "Optional[dict]": "dict | None",
            "List[str]": "list[str]",
            "List[int]": "list[int]",
            "Dict[str, str]": "dict[str, str]",
            "typing.Optional[str]": "str | None",
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
            schema = _load_schema()
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
