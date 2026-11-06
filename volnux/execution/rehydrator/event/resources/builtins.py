"""
Built-in Resource Providers

This module provides common resource providers for typical use cases.
"""

import io
import logging
import typing
from pathlib import Path
from typing import Any, Dict

from .base import ResourceProvider, register_provider

logger = logging.getLogger(__name__)


@register_provider(resource_type=io.IOBase, name="FileHandleProvider")
class FileHandleProvider(ResourceProvider[typing.IO]):
    """
    Provider for file handles (text and binary modes).

    Supports:
    - Text and binary file modes
    - Position restoration
    - Automatic reopening on resume

    Limitations:
    - Does not support network file systems with session affinity
    - Does not preserve file locks
    """

    @classmethod
    def save_state(cls, resource: typing.IO) -> Dict[str, Any]:
        """Save file handle state."""
        if resource.closed:
            raise ValueError("Cannot save state of closed file handle")

        return {
            "path": resource.name,
            "mode": resource.mode,
            "position": resource.tell(),
            "encoding": getattr(resource, "encoding", None),
            "newline": getattr(resource, "newlines", None),
        }

    @classmethod
    def restore_state(cls, data: Dict[str, Any]) -> typing.IO:
        """Restore file handle from state."""
        cls.validate_state_data(data, ["path", "mode"])

        # Validate file still exists
        if not Path(data["path"]).exists():
            raise FileNotFoundError(f"File not found: {data['path']}")

        # Reopen a file
        kwargs = {}
        if data.get("encoding"):
            kwargs["encoding"] = data["encoding"]
        if data.get("newline"):
            kwargs["newline"] = data["newline"]

        file_handle = open(data["path"], data["mode"], **kwargs)

        # Restore position
        if "position" in data:
            file_handle.seek(data["position"])

        logger.info(
            f"Restored file handle: {data['path']} at position {data.get('position', 0)}"
        )
        return file_handle

    @classmethod
    def cleanup(cls, resource: typing.IO) -> None:
        """Close the file handle."""
        try:
            if not resource.closed:
                resource.close()
                logger.debug(f"Closed file handle: {resource.name}")
        except Exception as e:
            logger.warning(f"Failed to close file handle: {e}")


@register_provider(name="SimpleStateProvider")
class SimpleStateProvider(ResourceProvider[Dict[str, Any]]):
    """
    Provider for a simple dictionary-based state.

    Use this for a custom state that's already serializable.
    The resource itself is just a dictionary.
    """

    @classmethod
    def save_state(cls, resource: Dict[str, Any]) -> Dict[str, Any]:
        """Simply return the dictionary."""
        if not isinstance(resource, dict):
            raise TypeError("SimpleStateProvider requires a dictionary")
        return resource.copy()

    @classmethod
    def restore_state(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of the data."""
        return data.copy()


@register_provider(name="PostgresCursorProvider")
class PostgresCursorProvider(ResourceProvider):
    """
    Provider for PostgreSQL database cursors.

    Note: Requires psycopg2 library.
    """

    @classmethod
    def save_state(cls, resource) -> Dict[str, Any]:
        import psycopg2

        if not isinstance(resource, psycopg2.extensions.cursor):
            raise TypeError("Expected psycopg2 cursor")

        return {
            "dsn": resource.connection.dsn,
            "query": resource.query.decode() if resource.query else None,
            "rowcount": resource.rowcount,
        }

    @classmethod
    def restore_state(cls, data: Dict[str, Any]) -> Any:
        import psycopg2

        cls.validate_state_data(data, ["dsn"])

        # Reconnect
        conn = psycopg2.connect(data["dsn"])
        cursor = conn.cursor()

        if data.get("query"):
            cursor.execute(data["query"])

            if data.get("rowcount", 0) > 0:
                cursor.fetchmany(data["rowcount"])

        return cursor

    @classmethod
    def cleanup(cls, resource) -> None:
        try:
            resource.close()
            resource.connection.close()
        except Exception as e:
            logger.warning(f"Failed to close database cursor: {e}")
