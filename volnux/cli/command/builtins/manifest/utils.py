import json
import typing
from pathlib import Path

from volnux.cli.command.base import CommandError

_SCHEMA_PATH = Path(__file__).parent.parent / "schemas" / "volnux.manifest.schema.json"

# Conventional manifest filename.
_MANIFEST_FILENAME = "volnux.manifest.json"


def _load_schema() -> typing.Dict:
    """Load the bundled JSON Schema from the disk."""
    if not _SCHEMA_PATH.exists():
        raise CommandError(
            f"Bundled manifest schema not found at {_SCHEMA_PATH}. "
            "Your Volnux installation may be incomplete."
        )
    try:
        return json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CommandError(f"Bundled manifest schema is not valid JSON: {exc}") from exc


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
