import sys
import typing
from importlib import import_module


def cached_import(module_path: str, class_name: str) -> typing.Type[typing.Any]:
    """
    Import a module and cache it in sys.modules
    Args:
        module_path (str): The module path to import
        class_name (str): The class name to retrieve from the module
    Returns:
        typing.Any: The imported class from the module
    """
    modules = sys.modules
    if module_path not in modules or (
        # Module is not fully initialized.
        getattr(modules[module_path], "__spec__", None) is not None
        and getattr(modules[module_path].__spec__, "_initializing", False) is True
    ):
        import_module(module_path)
    return getattr(modules[module_path], class_name)  # type: ignore


def import_string(dotted_path: str) -> typing.Type[typing.Any]:
    """
    Import a dotted module path and return the attribute/class designated by the
    last name in the path. Raise ImportError if the import failed.
    Args:
        dotted_path (str): The dotted module path
    Returns:
        typing.Type: The imported attribute/class
    Raises:
        ImportError: If the module or attribute/class cannot be imported
        AttributeError: If the attribute/class does not exist in the module
    """
    try:
        module_path, class_name = dotted_path.rsplit(".", 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    try:
        return cached_import(module_path, class_name)
    except AttributeError as err:
        raise ImportError(
            'Module "%s" does not define a "%s" attribute/class'
            % (module_path, class_name)
        ) from err
