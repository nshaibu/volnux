import os
import sys
import types
import typing
import logging
import inspect
from pathlib import Path
from importlib import import_module
from abc import ABC, abstractmethod
from volnux.config import VolnuxConfig
from volnux import EventBase


logger = logging.getLogger(__name__)

CONF = VolnuxConfig.get_instance()


class BaseManager(ABC):
    """
    Abstract base class for event managers.
    """

    def __init__(self, host, port):
        self._host = host
        self._port = port

    def __repr__(self):
        return f"{self.__class__.__name__}(host={self._host}, port={self._port})"

    def __str__(self):
        return self.__repr__()

    @abstractmethod
    def start(self, *args, **kwargs):
        """Start the task manager server"""

    @abstractmethod
    def shutdown(self):
        """Shutdown the task manager server"""

    @classmethod
    def auto_load_all_task_modules(cls):
        """
        Auto-discover and load all modules containing event classes that inherit from EventBase.
        Searches through the project directory for Python modules and registers event classes.
        """
        project_root = CONF.PROJECT_ROOT_DIR
        logger.info(f"Searching for event modules in: {project_root}")

        discovered_events: typing.Set[typing.Type] = set()
        discovered_modules: typing.Set[types.ModuleType] = set()

        def is_event_class(obj: typing.Type) -> bool:
            """Check if an object is a class inheriting from EventBase"""
            return (
                inspect.isclass(obj)
                and hasattr(obj, "__module__")
                and EventBase in [base for base in inspect.getmro(obj)[1:]]
            )

        def explore_module(module_name: str) -> None:
            """Explore a module for event classes"""
            try:
                module = import_module(module_name)
                for name, obj in inspect.getmembers(module):
                    if is_event_class(obj):
                        discovered_events.add(obj)
                        discovered_modules.add(module)
                        logger.debug(
                            f"Discovered event class: {obj.__module__}.{obj.__name__}"
                        )
            except Exception as e:
                logger.warning(f"Failed to load module {module_name}: {e}")

        # Walk through all Python packages in the project
        for root, dirs, files in os.walk(str(project_root)):
            # Skip __pycache__ and virtual environment directories
            dirs[:] = [d for d in dirs if not d.startswith(("__", "."))]

            if "__init__.py" in files:
                # Calculate the module path relative to project root
                rel_path = Path(root).relative_to(project_root)
                module_path = str(rel_path).replace(os.sep, ".")
                if module_path:
                    explore_module(module_path)

        # Register discovered event modules
        for event_class in discovered_events:
            module = sys.modules.get(event_class.__module__)
            if module:
                cls.register_task_module(event_class.__module__, module)
                logger.info(f"Registered event module: {event_class.__module__}")

        # Register discovered modules
        for module in discovered_modules:
            module_name = module.__name__
            if module_name not in discovered_events:
                cls.register_task_module(module_name, module)
            logger.info(f"Discovered module: {module_name}")

        logger.info(
            f"Discovered {len(discovered_events)} event classes and {len(discovered_modules)} modules"
        )

    @staticmethod
    def register_task_module(module_name: str, module: types.ModuleType) -> None:
        """
        Register a module containing event classes for task execution.

        Args:
            module_name: The fully qualified module name
            module: The module object to register
        """
        if module_name not in sys.modules:
            sys.modules[module_name] = module
            logger.debug(f"Registered module in sys.modules: {module_name}")

    def __del__(self):
        self.shutdown()

    def __enter__(self) -> "BaseManager":
        return self

    def __exit__(self, *args, **kwargs) -> None:
        self.shutdown()
