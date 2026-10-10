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
from typing import Any, Optional, Union
from cryptography.exceptions import InvalidSignature

from volnux import settings as default_settings
from volnux.concurrency.async_utils import to_thread
from volnux.constants import UNKNOWN
from volnux.crypto.signer import Signer, KeyLoader

if typing.TYPE_CHECKING:
    from volnux.result.result import ResultSet

__all__ = ["VolnuxConfig", "ConfigEntry"]

ENV_CONFIG = "VOLNUX_CONFIG"
ENV_CONFIG_DIR = "VOLNUX_CONFIG_DIR"
CONFIG_FILE = "settings.py"

# Mesh Config
ENV_MESH_NODE_ID = "VOLNUX_MESH_NODE_ID"
ENV_MESH_PRIVATE_KEY = "VOLNUX_MESH_PRIVATE_KEY"
ENV_MESH_PUBLIC_KEY = "VOLNUX_MESH_PUBLIC_KEY"

logger = logging.getLogger(__name__)

_MISSING = UNKNOWN


def _generate_node_id() -> str:
    """
    Generates a unique node ID for the mesh network.
    """
    return f"node_{os.urandom(8).hex()}"


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
            "value": self.value,
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
            "timestamp": self.timestamp
        }
        return json.dumps(payload, option=json.OPT_SORT_KEYS)

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

        if not self.signature or not self.name:
            return False

        try:
            if signer is None:
                public_key_data = os.environ.get(ENV_MESH_PUBLIC_KEY)
                if not public_key_data:
                    return False
                signer = self._create_signer(key_pem=public_key_data)

            return signer.verify(self._signing_payload(), self.signature)
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

        self.load_from_environ()

        self._load_module(default_settings)

        last_exception: Optional[Exception] = None
        for file_path in self._get_config_files(config_file):
            try:
                self.load_from_file(file_path)
                last_exception = None
            except (FileNotFoundError, ImportError, SyntaxError) as exc:
                last_exception = exc
                continue

        if last_exception:
            logger.error(
                "Failed to load config files: %s",
                last_exception,
                exc_info=last_exception,
            )

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
            main_store_entries = [entry.to_dict() for entry in self._store]

            namespace_entries = {
                ns: [entry.to_dict() for entry in store]
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
        instance = cls.__new__(cls)
        instance._lock = threading.RLock()
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
        """Serializes the active configuration state to a JSON string."""
        return json.dumps(self.to_dict()).decode("utf-8")

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
        self, config_file: typing.Optional[str] = None
    ) -> typing.Iterator[str]:
        """
        Load order:
        1. File discovered in ENV_CONFIG_DIR/current directory
        2. File from ENV_CONFIG
        3. Explicit config_file argument
        """
        file_path = self._search_for_config_file_in_dir()
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

    def _load_module(self, config_module: ModuleType) -> None:
        if not isinstance(config_module, ModuleType):
            raise TypeError("config_module must be of type ModuleType")

        valid_types = (int, float, str, bool, list, dict, tuple, Path)
        for field_name, value in vars(config_module).items():
            if field_name.startswith("__"):
                continue
            if isinstance(value, valid_types):
                self.add_entry_config(
                    ConfigEntry(
                        value=value,
                        origin_mesh_node=self._node_id,
                        name=field_name.upper(),
                        timestamp=0,
                    )
                )

    def load_from_file(self, config_file: typing.Union[str, os.PathLike]) -> None:
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
            self._load_module(config_module)

        except ModuleNotFoundError as exc:
            logger.error("Config file %s could not be loaded.", config_file)
            raise ImportError(str(exc)) from exc

    def load_from_environ(self):
        """
        Load configurations from environment variables starting with a specific prefix.

        :return: None
        """
        for key, value in os.environ.items():
            if key.startswith("VOLNUX_"):
                self.add(key[7:], value)

    async def load_from_file_async(
        self, config_file: typing.Union[str, os.PathLike]
    ) -> None:
        await to_thread(self.load_from_file, config_file)

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

        def get_store(ne: Optional[str] = None) -> "ResultSet[ConfigEntry]":
            if ne is not None:
                return self._namespace_store[ne]
            return self._store

        try:
            return get_store(namespace).get_entry_by_identity(f"hash:{hash(key)}")
        except KeyError:
            pass

        try:
            return get_store(namespace).get(name=key)
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
        entry = self.get_config_entry(key, namespace)
        if entry is not None:
            return entry.value

        if default is _MISSING:
            raise AttributeError(f"Missing configuration key '{key}'")
        return default

    def add_entry_config(
        self, config_entry: ConfigEntry, namespace: Optional[str] = None
    ) -> None:
        with self._lock:
            if self._signer and config_entry.is_local() and not config_entry.signature:
                config_entry.sign(self._signer)

            if config_entry.is_remote() and config_entry in self._store:
                status = self.update_from_mesh(config_entry)
                logger.info("Update from mesh: %s", status)
                return

            if namespace is not None:
                qs = self._namespace_store.get(namespace)
                qs.add(config_entry)
                self._namespace_store[namespace] = qs
                return

            self._store.add(config_entry)

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
            uses the current Lamport clock value.
        :param namespace: Optional. Specifies the namespace for the configuration entry.
        :return: None
        """
        entry = ConfigEntry(
            value=value,
            origin_mesh_node=node_id or self.get_node_id(),
            timestamp=int(timestamp or self._lamport_clock),
            name=key.upper(),
        )

        self.add_entry_config(entry, namespace)

    def update_from_mesh(self, incoming_entry: ConfigEntry) -> bool:
        """
        Update the local store with a new entry from the mesh network if certain conditions
        are met. This method ensures that only newer or higher-priority entries are used
        to replace existing entries in the local store. It also updates the Lamport clock
        to maintain consistency in the distributed system.

        :param incoming_entry: The new entry to be considered for update, represented as a
            `ConfigEntry`. This entry is compared with local entries in the store.

        :return: A boolean value indicating whether the local store was updated
            (True if updated, False otherwise).
        """
        with self._lock:
            # local_entry = self._store.get_entry_by_identity(hash(incoming_entry))
            local_entry = self.get_config_entry(key=incoming_entry.name)

            if not local_entry:
                self._store.add(incoming_entry)
                self._lamport_clock = (
                    max(self._lamport_clock, incoming_entry.timestamp) + 1
                )
                return True

            incoming_node = incoming_entry.origin_mesh_node or ""
            local_node = local_entry.origin_mesh_node or ""

            should_update = incoming_entry.timestamp > local_entry.timestamp or (
                incoming_entry.timestamp == local_entry.timestamp
                and incoming_node > local_node
            )

            if not should_update:
                return False

            self._store.add(incoming_entry)
            self._lamport_clock = max(self._lamport_clock, incoming_entry.timestamp) + 1
            return True

    async def update_from_mesh_async(self, incoming_entry: ConfigEntry) -> bool:
        """
        Updates the current configuration entry using data from the mesh network asynchronously.

        :param incoming_entry: The configuration entry containing data to replace or
            update the current entry.
        :type incoming_entry: ConfigEntry
        :return: Indicates whether the update operation was successful.
        :rtype: bool
        """
        return await to_thread(self.update_from_mesh, incoming_entry)

    async def get_async(self, key: str, default: Any = _MISSING) -> Any:
        return await to_thread(self.get, key, default)

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
