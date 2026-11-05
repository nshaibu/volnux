"""
Point Volnux at the project's settings before anything reads them, the way
Django's ``manage.py`` sets ``DJANGO_SETTINGS_MODULE``.

Run from inside a project (a directory with ``init.py`` and ``settings.py``, or
below one), the CLI puts the project directory on ``sys.path`` and sets
``VOLNUX_SETTINGS_MODULE=settings`` unless the environment already names a module.
It has to happen before the first import of ``volnux.config``, which reads the
settings once, when it is first used.
"""

import os
import sys
from pathlib import Path
from typing import Optional

ENVIRONMENT_VARIABLE = "VOLNUX_SETTINGS_MODULE"


def find_project_dir(start: Optional[Path] = None) -> Optional[Path]:
    """The nearest directory at or above ``start`` (default: the current one) that
    holds both ``init.py`` and ``settings.py``."""
    start = start or Path.cwd()
    for directory in [start, *start.parents]:
        if (directory / "init.py").is_file() and (directory / "settings.py").is_file():
            return directory
    return None


def configure_settings_module(start: Optional[Path] = None) -> Optional[str]:
    """Set ``VOLNUX_SETTINGS_MODULE`` for the project containing ``start``.

    :return: the module name set, or None when it was already set or there is no
        project.
    """
    if os.environ.get(ENVIRONMENT_VARIABLE):
        return None
    project_dir = find_project_dir(start)
    if project_dir is None:
        return None
    if str(project_dir) not in sys.path:
        sys.path.insert(0, str(project_dir))
    os.environ[ENVIRONMENT_VARIABLE] = "settings"
    return "settings"
