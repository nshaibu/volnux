import contextlib
import importlib
import importlib.util
import logging
import os
import base64
import threading
import typing
import orjson as json
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, asdict, field
from types import ModuleType
from typing import Any, Callable, Optional, Union
from cryptography.exceptions import InvalidSignature

from volnux.conf import (
    ENVIRONMENT_VARIABLE as ENV_SETTINGS_MODULE,
    REMOVED_SETTINGS,
    global_settings as default_settings,
)
from volnux.exceptions import ImproperlyConfigured
from volnux.concurrency.async_utils import to_thread
from volnux.constants import UNKNOWN
from volnux.crypto.signer import Signer, KeyLoader

if typing.TYPE_CHECKING:
    from volnux.result.result import ResultSet

__all__ = ["VolnuxConfig", "ConfigEntry", "setting", "is_sensitive_setting"]

ENV_CONFIG = "VOLNUX_CONFIG"
ENV_CONFIG_DIR = "VOLNUX_CONFIG_DIR"
CONFIG_FILE = "settings.py"

# Mesh Config
ENV_MESH_NODE_ID = "VOLNUX_MESH_NODE_ID"
ENV_MESH_PRIVATE_KEY = "VOLNUX_MESH_PRIVATE_KEY"
ENV_MESH_PUBLIC_KEY = "VOLNUX_MESH_PUBLIC_KEY"

logger = logging.getLogger(__name__)

_MISSING = UNKNOWN


#: Settings that hold secrets. They are never put in the carriers (``to_dict`` ...)
#: that are sent to workers: a worker has its own environment.
SENSITIVE_SETTINGS = frozenset({"SECRET_KEY", "MESH_PRIVATE_KEY"})
_SENSITIVE_SUFFIXES = ("_SECRET", "_PASSWORD", "_TOKEN", "_KEY", "_PRIVATE_KEY")


def is_sensitive_setting(name: str) -> bool:
    """Whether a setting holds a secret (by its name)."""
    name = name.upper()
    return name in SENSITIVE_SETTINGS or name.endswith(_SENSITIVE_SUFFIXES)


def _generate_node_id() -> str:
    """
    Generates a unique node ID for the mesh network.
    """
    return f"node_{os.urandom(8).hex()}"


_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off", ""})


def _coerce_env(name: str, raw: str, current: Any) -> Any:
    """``raw`` (text from the environment) as the type of the setting it overrides.

    With nothing to go by (a setting that has no value yet) the text is kept. A
    value that does not fit the type is kept as text too, and reported: failing to
    start because of one variable is worse than a setting with the wrong type that
    the code using it will complain about by name.
    """
    if current is _MISSING or current is None:
        # No type to go by. A JSON object or array is still recognised, so a
        # setting that is None until configured (a backend config) can be given
        # as JSON in the environment.
        text = raw.strip()
        if text[:1] in ("{", "["):
            try:
                return json.loads(text)
            except ValueError:
                pass
        return raw
    if isinstance(current, str):
        return raw
    try:
        if isinstance(current, bool):  # (before int: bool is an int)
            text = raw.strip().lower()
            if text in _TRUE:
                return True
            if text in _FALSE:
                return False
            raise ValueError(f"not a boolean: {raw!r}")
        if isinstance(current, int):
            return int(raw.strip())
        if isinstance(current, float):
            return float(raw.strip())
        if isinstance(current, Path):
            return Path(raw)
        if isinstance(current, (list, dict, tuple)):
            value = json.loads(raw)
            if isinstance(current, tuple) and isinstance(value, list):
                return tuple(value)
            if not isinstance(value, type(current)):
                raise ValueError(f"expected JSON {type(current).__name__}")
            return value
    except (ValueError, TypeError) as exc:
        logger.warning(
            "VOLNUX_%s=%r does not fit the setting's type (%s): %s; keeping the text",
            name,
            raw,
            type(current).__name__,
            exc,
        )
    return raw


def _jsonable(value: Any) -> Any:
    """``value`` as something JSON can hold: a ``Path`` (``BASE_DIR``...) or any
    other object JSON does not know becomes its text, containers are converted
    item by item."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    return str(value)


@dataclass
class ConfigEntry:
    value: Any
    name: Optional[str] = None

    # Lamport Logical Clock
    timestamp: int = 0

    origin_mesh_node: str = field(
        default_factory=lambda: os.environ.get(ENV_MESH_NODE_ID, "local")
    )

    # Ed25519 signature for security
    signature: Optional[bytes] = None

    def __post_init__(self) -> None:
        # Keys are case-insensitive (get() upper-cases the key it is given), so an
        # entry named "lower" was stored but could never be found.
        if isinstance(self.name, str):
            self.name = self.name.upper()

    @property
    def key(self) -> Optional[str]:
        return self.name

    @key.setter
    def key(self, value: str) -> None:
        self.name = value.upper()

    def __hash__(self) -> int:
        return hash(self.key) if self.key is not None else -1

    def to_dict(self) -> typing.Dict[str, Any]:
        """Serializes ConfigEntry into a JSON-compatible dictionary."""
        return {
            "name": self.name,
            "value": _jsonable(self.value),
            "timestamp": self.timestamp,
            "origin_mesh_node": self.origin_mesh_node,
            "signature": self.signature.hex() if self.signature else None,
        }

    @classmethod
    def from_dict(cls, data: typing.Dict[str, Any]) -> "ConfigEntry":
        """Reconstructs ConfigEntry from a dictionary."""
        sig: Optional[str] = data.get("signature")
        signature_bytes = bytes.fromhex(sig) if sig else None
        return cls(
            name=data.get("name"),
            value=data.get("value"),
            timestamp=data.get("timestamp", 0),
            origin_mesh_node=data.get("origin_mesh_node", "local"),
            signature=signature_bytes,
        )

    def is_local(self) -> bool:
        """
        Check if the configuration entry is local to the current mesh node.

        :return: True if the configuration entry is local, otherwise False.
        :rtype: bool
        """
        if self.origin_mesh_node == "local":
            return True
        node_id = os.environ.get(ENV_MESH_NODE_ID)
        return node_id is None or self.origin_mesh_node == node_id

    def is_remote(self) -> bool:
        """
        Determines if the current mesh node is remote.

        :return: True if the current node is determined to be remote, otherwise False.
        :rtype: bool
        """
        if self.origin_mesh_node == "local":
            return False
        node_id = os.environ.get(ENV_MESH_NODE_ID)
        if node_id is None:
            return False
        return self.origin_mesh_node != node_id

    def _signing_payload(self) -> bytes:
        """
        Canonical payload used for Ed25519 signing and verification.
        Must match exactly on both sender and receiver.
        """
        payload = {
            "name": self.name,
            "value": self.value,
            "origin_mesh_node": self.origin_mesh_node,
            "timestamp": self.timestamp,
        }
        # default=str: a Path (or other value JSON does not know) signs as its text,
        # which is what the receiver sees after the entry crossed the wire as JSON
        return json.dumps(payload, option=json.OPT_SORT_KEYS, default=str)

    def to_gossip(self) -> typing.Dict[str, Any]:
        """The wire form of an entry sent to the other nodes of the mesh."""
        return {
            "key": self.name,
            "value": self.value,
            "timestamp": self.timestamp,
            "origin_mesh_node": self.origin_mesh_node,
            "signature": self.signature.hex() if self.signature else None,
        }

    @classmethod
    def from_gossip(cls, payload: typing.Dict[str, Any]) -> "ConfigEntry":
        """Rebuild an entry from :meth:`to_gossip` (what arrives from a peer).

        :raises ValueError: the payload is malformed. It comes from the network,
            so every field is checked.
        """
        if not isinstance(payload, dict):
            raise ValueError("config gossip must be a mapping")
        name = payload.get("key") or payload.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("config gossip has no key")
        if "value" not in payload:
            raise ValueError(f"config gossip for '{name}' has no value")
        timestamp = payload.get("timestamp")
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise ValueError(f"config gossip for '{name}' has no integer timestamp")
        if timestamp < 0:
            raise ValueError(f"config gossip for '{name}' has a negative timestamp")
        origin = payload.get("origin_mesh_node") or payload.get("origin_node")
        if not isinstance(origin, str) or not origin:
            raise ValueError(f"config gossip for '{name}' names no origin node")
        raw_signature = payload.get("signature")
        try:
            signature = bytes.fromhex(raw_signature) if raw_signature else None
        except (TypeError, ValueError) as exc:
            raise ValueError(f"config gossip for '{name}': bad signature") from exc
        return cls(
            name=name,
            value=payload["value"],
            timestamp=timestamp,
            origin_mesh_node=origin,
            signature=signature,
        )

    def _create_signer(self, key_pem: Union[bytes, str]) -> Signer:
        """
        Creates a signer instance using the provided private key in PEM format.

        :param key_pem: The private key in PEM format. Can be provided as bytes or
                        a string. If provided as a string, it will be encoded into
                        bytes.
        :return: An initialized `Signer` object containing the private and public
                 keys extracted from the provided PEM data.
        :rtype: Signer
        """
        key_data = key_pem.encode("utf-8") if isinstance(key_pem, str) else key_pem
        loader = KeyLoader(key_data)
        private_k, public_k = loader.key_pairs()
        return Signer(private_key=private_k, public_key=public_k)

    def sign(self, signer: Optional[Signer] = None) -> bytes:
        """
        Signs a payload using the provided signer or a default signer.

        :param signer: An optional `Signer` instance to be used for signing
            the payload. If not provided, a signer will be created using a
            private key retrieved from the environment.
        :return: A byte string representing the computed signature.
        :rtype: bytes
        :raises ValueError: If the `name` attribute is not set or if no signer
            is provided, and a private key cannot be retrieved from the
            environment.
        """
        if self.name is None:
            raise ValueError("ConfigEntry.name must be set before signing")

        if signer is None:
            private_key_pem = os.environ.get(ENV_MESH_PRIVATE_KEY)
            if not private_key_pem:
                raise ValueError("Private key is required to sign the message")
            signer = self._create_signer(key_pem=private_key_pem)

        self.signature = signer.sign(self._signing_payload())
        return self.signature  # type: ignore

    def validate(self, signer: Optional[Signer] = None) -> bool:
        """
        Validates the current object's signature and name using the provided or an
        automatically created signer. For local instances, the validation always
        succeeds. Validation involves verifying the signature with the signing
        payload and ensuring the presence of required attributes.

        :param signer: Optional signer to be used for validation. If not provided,
            a signer will be created from the environment's public key data.
        :type signer: Optional[Signer]
        :return: True if the validation is successful, otherwise False.
        :rtype: bool
        """
        if self.is_local():
            return True
        return self.verify_signature(signer)

    def verify_signature(self, signer: Optional[Signer] = None) -> bool:
        """
        Check the signature against the entry, whoever claims to have made it.

        Unlike :meth:`validate` there is no shortcut for "local" entries: an entry
        that arrives from the network can name any origin, including this node, so
        it must not be trusted for what it says about itself.
        """
        if not self.signature or not self.name:
            return False

        try:
            if signer is None:
                public_key_data = os.environ.get(ENV_MESH_PUBLIC_KEY)
                if not public_key_data:
                    return False
                signer = self._create_signer(key_pem=public_key_data)

            return bool(signer.verify(self._signing_payload(), self.signature))
        except (ValueError, TypeError, InvalidSignature):
            return False


class VolnuxConfig:
    _instance = None
    _lock = threading.RLock()

    def __init__(
        self, config_file: Optional[str] = None, signer: Optional[Signer] = None
    ) -> None:
        """
        Initializes the configuration loader and sets up the configuration environment.

        The initialization process includes loading default settings and attempting to load
        configuration from various specified files. If any file cannot be loaded due to issues
        such as missing files, import errors, or syntax issues, these errors are logged but
        do not stop the process.

        :param config_file: Optional path to a specific configuration file to be loaded. If not
            provided, the default configuration file paths will be used.
        :type config_file: Optional[str]
        :param signer: Optional signing utility to be used for configuration verification or
            related signing operations.
        :type signer: Optional[Signer]
        """
        from volnux.result.result import ResultSet

        self._lock = threading.RLock()
        self._overrides: typing.List[typing.Dict[str, Any]] = []
        self._store: ResultSet[ConfigEntry] = ResultSet[ConfigEntry]()
        self._namespace_store: typing.DefaultDict[str, ResultSet[ConfigEntry]] = (
            defaultdict(ResultSet)
        )
        self._lamport_clock = 0
        self._signer = signer

        self._node_id = os.environ.get(ENV_MESH_NODE_ID)
        if not self._node_id:
            self._node_id = _generate_node_id()
            os.environ[ENV_MESH_NODE_ID] = self._node_id

        # Later sources win: the defaults (volnux.conf.global_settings), the
        # project's settings module (VOLNUX_SETTINGS_MODULE), the config files, then
        # the environment (the way to override one value for one deployment).
        self._load_module(default_settings, source="volnux.conf.global_settings")

        settings_module = os.environ.get(ENV_SETTINGS_MODULE)
        if settings_module:
            self.load_settings_module(settings_module)

        # (a named settings module replaces the search for a `settings.py`)
        for file_path in self._get_config_files(
            config_file, search=not settings_module
        ):
            try:
                self.load_from_file(file_path)
            except ImproperlyConfigured:
                raise  # a renamed setting must not be skipped like a missing file
            except Exception as exc:
                # one bad file must not hide another's failure, or stop the rest
                logger.error(
                    "Failed to load config file %s: %s", file_path, exc, exc_info=exc
                )

        self.load_from_environ()

    @classmethod
    def get_instance(cls) -> "VolnuxConfig":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

    def get_node_id(self) -> str:
        return self._node_id

    def to_dict(self) -> typing.Dict[str, Any]:
        """
        Serializes the complete VolnuxConfig instance into a dictionary
        for transportation across context carriers (Celery, K8s, Ray).
        """
        with self._lock:
            main_store_entries = [
                entry.to_dict()
                for entry in self._store
                if not is_sensitive_setting(entry.name or "")
            ]

            namespace_entries = {
                ns: [
                    entry.to_dict()
                    for entry in store
                    if not is_sensitive_setting(entry.name or "")
                ]
                for ns, store in self._namespace_store.items()
            }

            return {
                "node_id": self._node_id,
                "lamport_clock": self._lamport_clock,
                "store": main_store_entries,
                "namespace_store": namespace_entries,
            }

    @classmethod
    def from_dict(cls, data: typing.Dict[str, Any]) -> "VolnuxConfig":
        """
        Reconstructs a VolnuxConfig instance from a serialized dictionary carrier.
        """
        from volnux.result.result import ResultSet

        instance = cls.__new__(cls)
        instance._lock = threading.RLock()
        instance._overrides = []
        instance._store = ResultSet[ConfigEntry]()
        instance._namespace_store = defaultdict(ResultSet[ConfigEntry])
        instance._signer = None

        instance._node_id = data.get("node_id", _generate_node_id())
        instance._lamport_clock = data.get("lamport_clock", 0)

        # Rehydrate main store
        for entry_dict in data.get("store", []):
            entry = ConfigEntry.from_dict(entry_dict)
            instance._store.add(entry)

        # Rehydrate namespace stores
        for ns, entries in data.get("namespace_store", {}).items():
            for entry_dict in entries:
                entry = ConfigEntry.from_dict(entry_dict)
                instance._namespace_store[ns].add(entry)

        return instance

    def to_json(self) -> str:
        """Serializes the active configuration state to a JSON string.

        A value JSON does not know (the ``Path`` settings such as
        ``BASE_DIR``) is written as its text; it used to make this raise.
        """
        return json.dumps(self.to_dict(), default=str).decode("utf-8")

    @classmethod
    def from_json(cls, json_str: str) -> "VolnuxConfig":
        """Deserializes JSON string into a VolnuxConfig instance."""
        return cls.from_dict(json.loads(json_str))

    def to_base64(self) -> str:
        """Encodes JSON configuration into Base64 string for headers or env vars."""
        return base64.b64encode(self.to_json().encode("utf-8")).decode("utf-8")

    @classmethod
    def from_base64(cls, b64_str: str) -> "VolnuxConfig":
        """Decodes Base64 string back into a VolnuxConfig instance."""
        json_str = base64.b64decode(b64_str.encode("utf-8")).decode("utf-8")
        return cls.from_json(json_str)

    def _get_config_files(
        self, config_file: typing.Optional[str] = None, search: bool = True
    ) -> typing.Iterator[str]:
        """
        Load order:
        1. File discovered in ENV_CONFIG_DIR/current directory (unless ``search``
           is False, because a settings module was named)
        2. File from ENV_CONFIG
        3. Explicit config_file argument
        """
        file_path = self._search_for_config_file_in_dir() if search else None
        if file_path:
            yield file_path

        env_file = os.environ.get(ENV_CONFIG)
        if env_file:
            yield env_file

        if config_file:
            yield config_file

    @staticmethod
    def _search_for_config_file_in_dir() -> typing.Optional[str]:
        dir_path = os.environ.get(ENV_CONFIG_DIR, ".")
        config_path = os.path.join(dir_path, CONFIG_FILE)

        if os.path.isfile(config_path):
            return config_path

        try:
            for item in os.listdir(dir_path):
                subdir = os.path.join(dir_path, item)
                if os.path.isdir(subdir):
                    config_path = os.path.join(subdir, CONFIG_FILE)
                    if os.path.isfile(config_path):
                        return config_path
        except (PermissionError, FileNotFoundError) as exc:
            logger.debug("Error scanning directories: %s", exc)

        return None

    @staticmethod
    def _check_not_removed(name: str, source: str) -> None:
        replacement = REMOVED_SETTINGS.get(name.upper())
        if replacement:
            raise ImproperlyConfigured(
                f"The setting {name.upper()} (set in {source}) was renamed or "
                f"removed. Use {replacement} instead."
            )

    def _load_module(self, config_module: ModuleType, source: str = "") -> None:
        """Load the settings of a module: its UPPER_CASE names, as in Django.

        :raises ImproperlyConfigured: the module sets a setting that was renamed
            or removed.
        """
        if not isinstance(config_module, ModuleType):
            raise TypeError("config_module must be of type ModuleType")

        source = (
            source or getattr(config_module, "__file__", None) or "a settings module"
        )
        valid_types = (int, float, str, bool, list, dict, tuple, Path)
        for field_name, value in vars(config_module).items():
            if field_name.startswith("__") or not field_name.isupper():
                continue
            self._check_not_removed(field_name, source)
            # (A setting set to None is deliberately unset, such as PROJECT_ID;
            # without it `get("PROJECT_ID")` raised.)
            if isinstance(value, valid_types) or value is None:
                self.add_entry_config(
                    ConfigEntry(
                        value=value,
                        origin_mesh_node=self._node_id,
                        name=field_name.upper(),
                        timestamp=0,
                    )
                )

    def load_from_file(self, config_file: typing.Union[str, os.PathLike]) -> ModuleType:
        """Run a settings file, load its settings, and return the module it made."""
        if not os.path.exists(config_file):
            logger.info("Config file %s does not exist. Skipping.", config_file)
            raise FileNotFoundError("Config file does not exist")

        try:
            spec = importlib.util.spec_from_file_location("settings", config_file)
            if spec is None:
                raise FileNotFoundError(
                    "Could not find module specification for config file."
                )

            if spec.loader is None:
                raise ImportError("No loader found for module specification.")

            config_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(config_module)
            self._load_module(config_module, source=str(config_file))
            return config_module

        except ModuleNotFoundError as exc:
            logger.error("Config file %s could not be loaded.", config_file)
            raise ImportError(str(exc)) from exc

    #: Environment variables with the prefix that are not settings: the private key
    #: must never become one (settings are serialised for workers), and the others
    #: only say where the settings are.
    _ENV_NOT_CONFIG = frozenset(
        {"MESH_PRIVATE_KEY", "SETTINGS_MODULE", "CONFIG", "CONFIG_DIR"}
    )

    def load_settings_module(self, name: str) -> None:
        """Import the project's settings module (``myproject.settings``) and load
        its settings on top of what is loaded.

        :raises ImproperlyConfigured: it cannot be imported, or sets a renamed
            setting.
        """
        try:
            module = importlib.import_module(name)
        except ImproperlyConfigured:
            raise
        except Exception as exc:
            raise ImproperlyConfigured(
                f"{ENV_SETTINGS_MODULE}={name!r} could not be imported: "
                f"{type(exc).__name__}: {exc}"
            ) from exc
        self._load_module(module, source=name)

    def load_from_environ(self) -> None:
        """
        Load configurations from environment variables starting with ``VOLNUX_``.

        A value is converted to the type of the setting it overrides (``"9"`` for
        an integer setting is 9); for a setting nothing defines it stays text.
        """
        for key, raw in os.environ.items():
            if not key.startswith("VOLNUX_"):
                continue
            name = key[7:]
            if not name or name in self._ENV_NOT_CONFIG:
                continue
            self._check_not_removed(name, f"the environment variable {key}")
            existing = self.get_config_entry(name)
            # (timestamp 0 like the defaults and files: reading the environment is
            # not a mesh event and must not advance the Lamport clock)
            self.add(
                name,
                _coerce_env(name, raw, existing.value if existing else _MISSING),
                timestamp=0,
            )

    async def load_from_file_async(
        self, config_file: typing.Union[str, os.PathLike]
    ) -> ModuleType:
        return await to_thread(self.load_from_file, config_file)

    def get_config_entry(
        self, key: str, namespace: Optional[str] = None
    ) -> Optional[ConfigEntry]:
        """
        Retrieve a configuration entry by key within an optional namespace.

        This function attempts to fetch a configuration entry using the provided key
        and optional namespace. The key is first resolved in its uppercase form. It
        utilizes two approaches to locate the entry: by hash and by name. If the entry
        cannot be found, it returns None.

        :param key: The key of the configuration entry to fetch.
        :type key: str
        :param namespace: The optional namespace of the configuration entry. Default is None.
        :type namespace: Optional[str]
        :return: The configuration entry object if found, else None.
        :rtype: Optional[ConfigEntry]
        """
        key = key.upper()

        with self._lock:
            if namespace is not None:
                # (looking must not create the namespace)
                store = self._namespace_store.get(namespace)
                if store is None:
                    return None
            else:
                store = self._store

            try:
                return store.get_entry_by_identity(f"hash:{hash(key)}")
            except KeyError:
                pass

            try:
                return store.get(name=key)
            except KeyError:
                return None

    def get(
        self, key: str, default: Any = _MISSING, namespace: Optional[str] = None
    ) -> Any:
        """
        Retrieve the value associated with a configuration key.

        :param key: The configuration key to retrieve.
        :type key: str
        :param default: The default value to return if the key is not found in the environment or internal configuration.
        :type default: Any
        :return: The value corresponding to the configuration key, or the default value if the key is not found.
        :rtype: Any
        :param namespace: Optional. Specifies the namespace for the configuration entry.
        :type namespace: Optional[str]
        :raises AttributeError: If the key is missing and no default value is provided.
        """
        if namespace is None:
            found, value = self._overridden(key)
            if found:
                return value
        entry = self.get_config_entry(key, namespace)
        if entry is not None:
            return entry.value

        if default is _MISSING:
            raise AttributeError(f"Missing configuration key '{key}'")
        return default

    def _overridden(self, key: str) -> typing.Tuple[bool, Any]:
        """(found, value) of a setting changed by :meth:`override`, innermost first."""
        key = key.upper()
        for options in reversed(self._overrides):
            if key in options:
                return True, options[key]
        return False, None

    @contextlib.contextmanager
    def override(self, **options: Any) -> typing.Iterator[None]:
        """Change settings for a block (``volnux.conf.override_settings``). Changes
        nest and are undone on exit, whatever happens inside."""
        names = {}
        for name, value in options.items():
            self._check_not_removed(name, "override_settings")
            names[name.upper()] = value
        with self._lock:
            self._overrides.append(names)
        try:
            yield
        finally:
            with self._lock:
                self._overrides.remove(names)

    def set_setting(self, name: str, value: Any) -> None:
        """Set a setting from code (``settings.configure``), as a settings module would."""
        self._check_not_removed(name, "settings.configure()")
        self.add(name, value, timestamp=0)

    def resolve(self, name: str, default: Any = _MISSING) -> Any:
        """
        The current value of a setting: the environment variable ``VOLNUX_<NAME>``
        (read now, not when this object was built), else the loaded value (a config
        file, else ``settings.py``), else ``default``.

        Use it for values read when something starts (a server, a pool) rather than
        ``get``, which only sees the environment as it was when the configuration
        was built: ``volnux dev`` sets variables after that. An environment value is
        converted to the type of the setting it overrides.

        :raises AttributeError: nothing defines the setting and there is no default.
        """
        key = name.upper()
        found, value = self._overridden(key)
        if found:
            return value
        entry = self.get_config_entry(key)
        current = entry.value if entry is not None else _MISSING

        raw = None if key in self._ENV_NOT_CONFIG else os.environ.get(f"VOLNUX_{key}")
        if raw is not None:
            model = current if current is not _MISSING else default
            return _coerce_env(key, raw, model)

        if entry is not None:
            return current
        if default is _MISSING:
            raise AttributeError(f"Missing configuration key '{name}'")
        return default

    def add_entry_config(
        self, config_entry: ConfigEntry, namespace: Optional[str] = None
    ) -> None:
        with self._lock:
            if config_entry.is_remote():
                # What another node says goes through the mesh rules (signature,
                # Lamport order), never straight into the store.
                status = self.update_from_mesh(config_entry, namespace)
                logger.info("Update from mesh: %s", status)
                return

            if self._signer and config_entry.is_local() and not config_entry.signature:
                config_entry.sign(self._signer)

            self._store_for(namespace).add(config_entry)

    def _store_for(self, namespace: Optional[str]) -> "ResultSet[ConfigEntry]":
        """The store of a namespace (created on first write), or the main one."""
        if namespace is None:
            return self._store
        return self._namespace_store[namespace]

    def add(
        self,
        key: str,
        value: Any,
        node_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        namespace: Optional[str] = None,
    ) -> None:
        """
        Adds a configuration entry to the internal storage.

        :param key: The name of the configuration entry to add. It must be a string.
        :param value: The value associated with the configuration entry.
        :param node_id: Optional. Specifies the origin mesh node identifier. If not
            provided, the current node identifier is used.
        :param timestamp: Optional. Sets the time for the entry. If not provided,
            the Lamport clock is advanced and its new value used, so a local change
            is newer than everything this node has seen (it used the clock without
            advancing it, and a peer's write at the same time could replace it).
        :param namespace: Optional. Specifies the namespace for the configuration entry.
        :return: None
        """
        with self._lock:
            if timestamp is None:
                self._lamport_clock += 1
                timestamp = self._lamport_clock
            entry = ConfigEntry(
                value=value,
                origin_mesh_node=node_id or self.get_node_id(),
                timestamp=int(timestamp),
                name=key.upper(),
            )

            self.add_entry_config(entry, namespace)

    def make_mesh_entry(self, key: str, value: Any) -> ConfigEntry:
        """A new local entry, with the next Lamport timestamp, ready to send to the
        other nodes: signed if this node has a signer or a private key."""
        with self._lock:
            self._lamport_clock += 1
            entry = ConfigEntry(
                value=value,
                name=key.upper(),
                timestamp=self._lamport_clock,
                origin_mesh_node=self.get_node_id(),
            )
        if self._signer is not None or os.environ.get(ENV_MESH_PRIVATE_KEY):
            entry.sign(self._signer)
        return entry

    def _signatures_required(self) -> bool:
        """Whether entries from other nodes must be signed: they must as soon as
        this node can verify (a signer, or the mesh public key, is configured)."""
        return self._signer is not None or bool(os.environ.get(ENV_MESH_PUBLIC_KEY))

    def verify_mesh_entry(self, entry: ConfigEntry) -> bool:
        """Whether an entry received from the mesh may be applied (and relayed):
        well formed and, when verification is configured, correctly signed. The
        origin it claims is not trusted, not even "local" or this node's own id."""
        if not isinstance(entry.name, str) or not entry.name.strip():
            return False
        if isinstance(entry.timestamp, bool) or not isinstance(entry.timestamp, int):
            return False
        if self._signatures_required():
            return entry.verify_signature(self._signer)
        return True

    def update_from_mesh(
        self, incoming_entry: ConfigEntry, namespace: Optional[str] = None
    ) -> bool:
        """
        Apply an entry received from the mesh if it is valid and newer than ours.

        The entry must pass :meth:`verify_mesh_entry`. It replaces the local one
        when its Lamport timestamp is higher, or equal with a greater origin node id
        (a total order, so every node ends with the same value). The clock moves
        past the timestamp of every valid entry received, applied or not.

        :param incoming_entry: The entry received.
        :param namespace: The namespace the entry belongs to, if any.
        :return: True if the local store was updated.
        """
        if not self.verify_mesh_entry(incoming_entry):
            logger.warning(
                "Rejected config update '%s' from '%s': malformed or not correctly "
                "signed",
                incoming_entry.name,
                incoming_entry.origin_mesh_node,
            )
            return False

        with self._lock:
            self._lamport_clock = max(self._lamport_clock, incoming_entry.timestamp) + 1
            local_entry = self.get_config_entry(incoming_entry.name, namespace)

            if local_entry is not None:
                incoming_node = incoming_entry.origin_mesh_node or ""
                local_node = local_entry.origin_mesh_node or ""
                should_update = incoming_entry.timestamp > local_entry.timestamp or (
                    incoming_entry.timestamp == local_entry.timestamp
                    and incoming_node > local_node
                )
                if not should_update:
                    return False

            self._store_for(namespace).add(incoming_entry)
            return True

    async def update_from_mesh_async(
        self, incoming_entry: ConfigEntry, namespace: Optional[str] = None
    ) -> bool:
        """
        Updates the current configuration entry using data from the mesh network asynchronously.

        :param incoming_entry: The configuration entry containing data to replace or
            update the current entry.
        :type incoming_entry: ConfigEntry
        :return: Indicates whether the update operation was successful.
        :rtype: bool
        """
        return await to_thread(self.update_from_mesh, incoming_entry, namespace)

    async def get_async(
        self, key: str, default: Any = _MISSING, namespace: Optional[str] = None
    ) -> Any:
        if namespace is None:
            return await to_thread(self.get, key, default)
        return await to_thread(self.get, key, default, namespace)

    async def add_async(
        self,
        key: str,
        value: Any,
        node_id: Optional[str] = None,
        timestamp: Optional[float] = None,
        namespace: Optional[str] = None,
    ) -> None:
        await to_thread(self.add, key, value, node_id, timestamp, namespace)

    def __getattr__(self, item: str) -> Any:
        if item.startswith("_"):
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{item}'"
            )
        return self.get(item.upper())

    def __repr__(self) -> str:
        return f"VolnuxConfig <len={len(self._store)}>"


# Set by ``volnux.context``: returns the configuration of the active context (the
# one a Celery worker or Kubernetes pod rebuilt from the dispatcher's carrier), or
# None when no context carries one.
_active_context_config: Optional[Callable[[], Optional["VolnuxConfig"]]] = None


def setting(name: str, default: Any = _MISSING) -> Any:
    """The current value of a setting (environment, then config file, then
    ``settings.py``, then ``default``).

    Inside a ``volnux.context`` that carries a configuration (a workflow running on
    a worker, in a pod, or as a task) the value is looked up there first, so the
    workflow sees the settings of the process that started it. A setting the carried
    configuration does not have (the secrets, which are never carried) comes from
    this process's own configuration, as does everything outside a context.
    """
    instance = VolnuxConfig.get_instance()
    if _active_context_config is not None:
        active = _active_context_config()
        if active is not None and active is not instance:
            if not instance._overridden(name)[0]:
                try:
                    return active.resolve(name)
                except AttributeError:
                    pass
    return instance.resolve(name, default)
