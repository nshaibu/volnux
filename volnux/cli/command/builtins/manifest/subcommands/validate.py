import json
import sys
import typing
import jsonschema

from volnux.cli.command.base import SubCommand, CommandCategory
from volnux.cli.command.builtins.manifest.utils import (
    _load_manifest,
    _load_schema,
    _resolve_manifest_path,
    _MANIFEST_FILENAME,
)


class ValidateManifestSubCommand(SubCommand):
    """
    Validate a volnux.manifest.json file against the EventHub JSON Schema.

    Usage
    -----
        volnux validate_manifest
        volnux validate_manifest --manifest path/to/volnux.manifest.json
        volnux validate_manifest --strict

    Exit codes
    ----------
        0 — manifest is valid (or --strict warnings are absent)
        1 — validation errors found, or a file cannot be read
    """

    name = "validate_manifest"
    help = (
        "Validate a volnux.manifest.json file against the official EventHub "
        "JSON Schema. Prints every validation error with its JSON path."
    )
    category = CommandCategory.DEVELOPMENT

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--manifest",
            metavar="PATH",
            default=None,
            help=(
                "Path to the manifest file to validate. "
                f"Defaults to ./{_MANIFEST_FILENAME} in the current directory."
            ),
        )
        parser.add_argument(
            "--strict",
            action="store_true",
            default=False,
            help=(
                "Treat warnings (deprecated events without replacement, "
                "missing optional recommended fields) as errors."
            ),
        )
        parser.add_argument(
            "--json",
            dest="json_output",
            action="store_true",
            default=False,
            help="Emit results as a JSON object instead of human-readable text.",
        )

    def handle(self, *args, **options) -> typing.Optional[str]:
        manifest_path = _resolve_manifest_path(options.get("manifest"))
        strict: bool = options.get("strict", False)
        json_output: bool = options.get("json_output", False)

        self.success(f"Validating {manifest_path} …\n") if not json_output else None

        schema = _load_schema()
        manifest = _load_manifest(manifest_path)

        validator = jsonschema.Draft202012Validator(schema)
        schema_errors = sorted(
            validator.iter_errors(manifest),
            key=lambda e: list(e.absolute_path),
        )

        semantic_warnings = self._semantic_checks(manifest)

        cross_errors = self._cross_reference_checks(manifest)

        all_errors = [
            {
                "path": " > ".join(str(p) for p in e.absolute_path) or "(root)",
                "message": e.message,
                "level": "error",
            }
            for e in schema_errors
        ] + cross_errors

        all_warnings = [
            {"path": w["path"], "message": w["message"], "level": "warning"}
            for w in semantic_warnings
        ]

        if strict:
            all_errors += [
                {"path": w["path"], "message": w["message"], "level": "error"}
                for w in all_warnings
            ]
            all_warnings = []

        if json_output:
            result = {
                "valid": len(all_errors) == 0,
                "manifest": str(manifest_path),
                "errors": all_errors,
                "warnings": all_warnings,
            }
            self.stdout.write(json.dumps(result, indent=2))
            if all_errors:
                sys.exit(1)
            return None

        if all_errors:
            self.error(f"Found {len(all_errors)} error(s):\n")
            for item in all_errors:
                self.stderr.write(f"  ✗  [{item['path']}]\n     {item['message']}\n")
        else:
            self.success("No schema errors found.\n")

        if all_warnings:
            self.warning(f"Found {len(all_warnings)} warning(s):\n")
            for item in all_warnings:
                self.stdout.write(f"  ⚠  [{item['path']}]\n     {item['message']}\n")

        if not all_errors and not all_warnings:
            self.success("✓ Manifest is valid.\n")
            return None

        if all_errors:
            sys.exit(1)

        self.success("✓ Manifest passed with warnings.\n")
        return None

    def _semantic_checks(
        self, manifest: typing.Dict
    ) -> typing.List[typing.Dict[str, str]]:
        """
        Checks that require understanding of the domain but are not expressible
        in JSON Schema alone.
        """
        warnings: typing.List[typing.Dict[str, str]] = []
        events: typing.List[typing.Dict] = manifest.get("events", [])

        for i, event in enumerate(events):
            path_prefix = f"events > {i} ({event.get('class', '?')})"

            # Deprecated events should name a replacement
            if event.get("deprecated") and not event.get("deprecation_info", {}).get(
                "replacement"
            ):
                warnings.append(
                    {
                        "path": f"{path_prefix} > deprecation_info > replacement",
                        "message": (
                            "Deprecated event has no replacement class. "
                            "Consider providing one so users can migrate."
                        ),
                    }
                )

            # Events with EXTRA_INIT_PARAMS_SCHEMA entries should have descriptions
            schema = event.get("extra_init_params_schema", {})
            for param_name, param in schema.items():
                if not param.get("description", "").strip():
                    warnings.append(
                        {
                            "path": f"{path_prefix} > extra_init_params_schema > {param_name} > description",
                            "message": f"Parameter '{param_name}' has no description.",
                        }
                    )

            # event_type OTHER is allowed but worth flagging
            if event.get("event_type") == "OTHER":
                warnings.append(
                    {
                        "path": f"{path_prefix} > event_type",
                        "message": (
                            "event_type is 'OTHER'. Consider using a more specific type "
                            "(EXTRACT, TRANSFORM, LOAD, VALIDATE, NOTIFY, AI, CONTROL, CHECKPOINT)."
                        ),
                    }
                )

            # the changelog on the event should be non-empty if version > 1.0.0
            ver = event.get("version", "1.0.0")
            if ver != "1.0.0" and not event.get("changelog"):
                warnings.append(
                    {
                        "path": f"{path_prefix} > changelog",
                        "message": (
                            f"Event is at version {ver} but has no changelog. "
                            "Describe what changed so users can understand the history."
                        ),
                    }
                )

        return warnings

    def _cross_reference_checks(
        self, manifest: typing.Dict
    ) -> typing.List[typing.Dict[str, str]]:
        """
        Errors that require reading across multiple top-level sections.
        """
        errors: typing.List[typing.Dict[str, str]] = []

        pkg_version: str = manifest.get("package", {}).get("version", "")
        changelog: typing.Dict = manifest.get("changelog", {})

        if pkg_version and pkg_version not in changelog:
            errors.append(
                {
                    "path": f"changelog > {pkg_version}",
                    "message": (
                        f"package.version '{pkg_version}' has no corresponding entry in "
                        "the top-level changelog. Every release must be documented."
                    ),
                    "level": "error",
                }
            )

        # Every event class listed must be unique (class + module)
        seen: typing.Set[typing.Tuple[str, str]] = set()
        for i, event in enumerate(manifest.get("events", [])):
            key = (event.get("class", ""), event.get("module", ""))
            if key in seen:
                errors.append(
                    {
                        "path": f"events > {i}",
                        "message": (
                            f"Duplicate event entry: class '{key[0]}' from module '{key[1]}' "
                            "appears more than once."
                        ),
                        "level": "error",
                    }
                )
            seen.add(key)

        return errors
