from __future__ import annotations

import importlib.util
import logging
import os
import threading
import typing

from volnux import settings as default_settings
from volnux.concurrency.async_utils import to_thread

__all__ = ["ConfigLoader"]


ENV_CONFIG = "EVENT_PIPELINE_CONFIG"

ENV_CONFIG_DIR = "EVENT_PIPELINE_CONFIG_DIR"

CONFIG_FILE = "settings.py"

logger = logging.getLogger(__name__)

_default_config = None
_config_lock = threading.Lock()


class ConfigLoader:
    def __init__(self, config_file: typing.Optional[str] = None) -> None:
        self._config: typing.Dict[str, typing.Any] = {}

        self._load_module(default_settings)

        last_exception: typing.Optional[Exception] = None

        for file in self._get_config_files(config_file):
            try:
                self.load_from_file(file)
                last_exception = None  # Reset last_exception on success
            except (FileNotFoundError, ImportError) as e:
                last_exception = e
                continue

        if last_exception:
            logger.error(f"Failed to load config files: {last_exception}", exc_info=last_exception)

    def _get_config_files(
        self, config_file: typing.Optional[str] = None
    ) -> typing.Iterator[str]:
        """
        Get the list of config files to load, in order of precedence.
        1. Config file in current directory or specified directory
        2. Config file specified in environment variable
        3. Config file passed as argument
        Yields:
            Paths to config files to load
        """
        file = self._search_for_config_file_in_current_directory()
        if file:
            yield file

        if ENV_CONFIG in os.environ:
            yield os.environ[ENV_CONFIG]

        if config_file:
            yield config_file

    @staticmethod
    def _search_for_config_file_in_current_directory() -> typing.Optional[str]:
        """
        Search for the config file in the current directory.
        Returns:
            Path to config file if found, else None
        """
        dir_path = os.environ.get(ENV_CONFIG_DIR, ".")
        # Check the specified directory first
        config_path = os.path.join(dir_path, CONFIG_FILE)
        if os.path.isfile(config_path):
            return config_path

        # Check immediate subdirectories only (one level deep)
        try:
            for item in os.listdir(dir_path):
                subdir = os.path.join(dir_path, item)
                if os.path.isdir(subdir):
                    config_path = os.path.join(subdir, CONFIG_FILE)
                    if os.path.isfile(config_path):
                        return config_path
        except (PermissionError, FileNotFoundError) as e:
            logger.debug(f"Error scanning directories: {e}")

        return None

    def _load_module(self, config_module: typing.Any) -> None:
        """
        Load configurations from a Python module.
        Args:
            config_module: Python module containing configuration attributes.
        """
        for field_name in dir(config_module):
            if not field_name.startswith("__") and not callable(
                getattr(config_module, field_name)
            ):
                self._config[field_name.upper()] = getattr(config_module, field_name)

    def load_from_file(self, config_file: typing.Union[str, os.PathLike]) -> None:
        """
        Load configurations from a Python config file.
        Args:
            config_file: Path to the Python config file.
        Raises:
            FileNotFoundError: If the config file does not exist.
            ImportError: If the config file cannot be imported.
        """
        if not os.path.exists(config_file):
            logger.info(
                f"Config file {config_file} does not exist. Skipping loading from file."
            )
            raise FileNotFoundError("Config file does not exist")

        try:
            # Load the config file as a Python module
            spec = importlib.util.spec_from_file_location("settings", config_file)
            if spec is None:
                logger.warning(f"Could not load spec for {config_file}")
                raise FileNotFoundError(
                    "Could not find module specification for config file."
                )

            config_module = importlib.util.module_from_spec(spec)
            if spec.loader is None:
                logger.warning(f"No loader found for spec of {config_file}")
                raise ImportError("No loader found for module specification.")

            spec.loader.exec_module(config_module)

            self._load_module(config_module)
        except ModuleNotFoundError:
            logger.error(f"Config file {config_file} could not be loaded.")
            raise

    async def aload_from_file(
        self, config_file: typing.Union[str, os.PathLike]
    ) -> None:
        await to_thread(self.load_from_file, config_file)

    def get(self, key: str, default: typing.Optional[typing.Any] = None) -> typing.Any:
        """Get the configuration value, with an optional default."""
        value = self._config.get(key, default)
        if value is None:
            value = os.environ.get(key)
        if value is None:
            raise AttributeError(f"Missing configuration key '{key}'")
        return value

    def add(self, key, value: typing.Any) -> None:
        with _config_lock:
            self._config[key] = value

    async def aget(
        self, key: str, default: typing.Optional[typing.Any] = None
    ) -> typing.Any:
        return await to_thread(self.get, key, default)

    async def aadd(self, key: str, value: typing.Any) -> None:
        await to_thread(self.add, key, value)

    def __getattr__(self, item: str) -> typing.Any:
        """Handle attribute access for configuration keys."""
        if item.startswith("_"):
            # Let Python handle private attributes normally
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{item}'"
            )

        # Look up using an uppercase key
        return self.get(item.upper())

    def __repr__(self) -> str:
        return f"ConfigLoader <len={len(self._config)}>"

    @classmethod
    def get_lazily_loaded_config(
        cls, config_file: typing.Optional[str] = None
    ) -> "ConfigLoader":
        global _default_config

        if _default_config is not None:
            return _default_config

        with _config_lock:
            if _default_config is None:
                _default_config = cls(config_file=config_file)
        return _default_config
