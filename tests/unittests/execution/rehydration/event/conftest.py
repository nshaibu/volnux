""" "Shared test configuration and fixtures for volnux checkpoint tests."""

import sys
from unittest.mock import MagicMock

import pytest


# Auto-mock formax if not installed (common in test environments)
# This allows tests to run without the full Volnux dependency tree.
@pytest.fixture(autouse=True)
def _mock_formax_if_missing():
    """Provide formax.BaseModel, MiniAnnotated, Attrib, ValidationFlags
    if the real module is not available."""
    if "formax" not in sys.modules:
        try:
            import formax  # noqa: F401
        except ImportError:
            # Create a minimal mock
            formax_mock = MagicMock()
            formax_mock.BaseModel = type(
                "BaseModel",
                (),
                {
                    "__init__": lambda self, **kwargs: None,
                    "__setattr__": lambda self, k, v: object.__setattr__(self, k, v),
                },
            )
            formax_mock.MiniAnnotated = lambda *a, **kw: a[0] if a else None
            formax_mock.Attrib = lambda **kw: kw
            formax_mock.ValidationFlags = MagicMock()
            formax_mock.ValidationFlags.NONE = 0
            sys.modules["formax"] = formax_mock


@pytest.fixture(autouse=True)
def _mock_volnux_constants():
    """Provide volnux.constants.MAX_RETRIES if not available."""
    if "volnux.constants" not in sys.modules:
        constants_mock = MagicMock()
        constants_mock.MAX_RETRIES = 3
        sys.modules["volnux.constants"] = constants_mock
