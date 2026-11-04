import importlib
import pkgutil

import pytest

import volnux.app


def _modules():
    yield "volnux.app"
    for info in pkgutil.walk_packages(volnux.app.__path__, "volnux.app."):
        yield info.name


@pytest.mark.parametrize("name", sorted(_modules()))
def test_app_module_imports(name):
    importlib.import_module(name)
