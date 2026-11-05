"""
The ``DATABASES`` setting, as Django has it.

::

    DATABASES = {
        "default": {"ENGINE": "volnux.backends.stores.inmemory.InMemoryKeyValueStoreBackend"},
        "triggers": {
            "ENGINE": "volnux.backends.stores.sqlite.SqliteStoreBackend",
            "NAME": "volnux.db",
        },
    }

Each entry names a backend class (``ENGINE``) and says how to reach it with the
keys Django uses; :func:`get_database_config` maps them onto the arguments the
backend's connector takes. A model chooses its entry with its ``get_database_alias()``
(``"default"`` unless it says otherwise).
"""

import copy
import os
import typing

from volnux.exceptions import ImproperlyConfigured

__all__ = [
    "DEFAULT_DB_ALIAS",
    "get_database_config",
    "get_databases",
]

DEFAULT_DB_ALIAS = "default"

#: Django key -> the connector argument it sets.
_CONNECTOR_ARGUMENTS = {
    "NAME": "database",
    "HOST": "host",
    "PORT": "port",
    "USER": "username",
    "PASSWORD": "password",
}
_KEYS = frozenset({"ENGINE", "OPTIONS", *_CONNECTOR_ARGUMENTS})


def get_databases() -> typing.Dict[str, typing.Dict[str, typing.Any]]:
    """The ``DATABASES`` setting, checked.

    :raises ImproperlyConfigured: it is not a dict of dicts, or has no ``default``.
    """
    from volnux.config import setting

    databases = setting("DATABASES")
    if not isinstance(databases, dict) or not all(
        isinstance(alias, str) and isinstance(entry, dict)
        for alias, entry in databases.items()
    ):
        raise ImproperlyConfigured(
            "DATABASES must be a dict of database aliases to their settings, for "
            'example {"default": {"ENGINE": "...", "NAME": "..."}}.'
        )
    if DEFAULT_DB_ALIAS not in databases:
        raise ImproperlyConfigured(
            f"DATABASES must define a '{DEFAULT_DB_ALIAS}' database."
        )
    return databases


def _check(alias: str, entry: typing.Dict[str, typing.Any]) -> None:
    # (messages name keys, never values: a PASSWORD must not end up in a log)
    if "CONNECTOR_CONFIG" in entry:
        raise ImproperlyConfigured(
            f"DATABASES['{alias}'] uses CONNECTOR_CONFIG, which was replaced by the "
            "Django keys NAME, HOST, PORT, USER, PASSWORD and OPTIONS."
        )
    unknown = sorted(set(entry) - _KEYS)
    if unknown:
        raise ImproperlyConfigured(
            f"DATABASES['{alias}'] has unknown key(s) {unknown}. Valid keys: "
            f"{', '.join(sorted(_KEYS))}."
        )
    engine = entry.get("ENGINE")
    if not engine or not isinstance(engine, str):
        raise ImproperlyConfigured(
            f"DATABASES['{alias}'] needs an ENGINE: the dotted path of a backend class."
        )
    options = entry.get("OPTIONS", {})
    if not isinstance(options, dict):
        raise ImproperlyConfigured(f"DATABASES['{alias}']['OPTIONS'] must be a dict.")
    for key, argument in _CONNECTOR_ARGUMENTS.items():
        if entry.get(key) is not None and argument in options:
            raise ImproperlyConfigured(
                f"DATABASES['{alias}'] sets {key} and OPTIONS['{argument}']: use one."
            )


def get_database_config(alias: str = DEFAULT_DB_ALIAS) -> typing.Dict[str, typing.Any]:
    """The configuration of a database in the form the backends are built from:
    ``{"ENGINE": <dotted path>, "CONNECTOR_CONFIG": {<connector arguments>}}``
    (``CONNECTOR_CONFIG`` is left out when there is nothing to pass).

    An alias that ``DATABASES`` does not define uses ``'default'``.

    :raises ImproperlyConfigured: ``DATABASES`` or the entry is invalid.
    """
    databases = get_databases()
    chosen = alias if alias in databases else DEFAULT_DB_ALIAS
    entry = databases[chosen]
    _check(chosen, entry)

    connector: typing.Dict[str, typing.Any] = {}
    for key, argument in _CONNECTOR_ARGUMENTS.items():
        value = entry.get(key)
        if value is not None:
            # (a Path is fine in a settings file: NAME = BASE_DIR / "volnux.db")
            connector[argument] = (
                os.fspath(value) if isinstance(value, os.PathLike) else value
            )
    connector.update(copy.deepcopy(entry.get("OPTIONS", {})))

    config: typing.Dict[str, typing.Any] = {"ENGINE": entry["ENGINE"]}
    if connector:
        config["CONNECTOR_CONFIG"] = connector
    return config
