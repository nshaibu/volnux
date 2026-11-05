import json
import typing
from pathlib import Path

from volnux.cli.command.base import CommandError
from volnux.manifest.schemas import PACKAGE_TYPES, load_schema, schema_for_manifest

# Conventional manifest filename.
_MANIFEST_FILENAME = "volnux.manifest.json"


def _load_schema(package_type: str = "event") -> typing.Dict:
    """Load the bundled JSON Schema for ``package_type`` (``event`` or ``workflow``)."""
    if package_type not in PACKAGE_TYPES:
        raise CommandError(
            f"Unknown package type {package_type!r}; expected one of {list(PACKAGE_TYPES)}."
        )
    try:
        return load_schema(package_type)
    except FileNotFoundError as exc:
        raise CommandError(
            f"{exc} Your Volnux installation may be incomplete."
        ) from exc
    except json.JSONDecodeError as exc:
        raise CommandError(f"Bundled manifest schema is not valid JSON: {exc}") from exc


def _schema_for(manifest: typing.Dict) -> typing.Dict:
    """The schema a manifest has to satisfy, chosen by its ``package.package_type``."""
    try:
        return schema_for_manifest(manifest)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        raise CommandError(f"Cannot load the manifest schema: {exc}") from exc


def _load_manifest(path: Path) -> typing.Dict:
    """Read and JSON-parse a manifest file, raising CommandError on failure."""
    if not path.exists():
        raise CommandError(f"Manifest file not found: {path}")
    if not path.is_file():
        raise CommandError(f"Manifest path is not a file: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CommandError(f"Manifest is not valid JSON ({path}): {exc}") from exc


def _resolve_manifest_path(raw: typing.Optional[str]) -> Path:
    """
    If *raw* is provided, use it, otherwise look for volnux.manifest.json in
    the current working directory.
    """
    if raw:
        return Path(raw).expanduser().resolve()
    candidate = Path.cwd() / _MANIFEST_FILENAME
    return candidate
