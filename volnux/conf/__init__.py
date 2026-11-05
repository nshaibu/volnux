"""
Volnux settings, the way Django does them.

* ``volnux.conf.global_settings`` holds the default of every setting.
* A project overrides the ones it changes in its own settings module, named by
  the ``VOLNUX_SETTINGS_MODULE`` environment variable (``myproject.settings``).
* ``VOLNUX_<NAME>`` environment variables override a single setting.
* Code reads settings through :data:`settings`::

      from volnux.conf import settings

      settings.DATABASES
      settings.API_PORT

* Tests change settings with :func:`override_settings`.

Only UPPER_CASE names are settings.
"""

import functools
import typing

__all__ = [
    "ENVIRONMENT_VARIABLE",
    "REMOVED_SETTINGS",
    "LazySettings",
    "override_settings",
    "settings",
]

#: Names the project's settings module from the environment, like Django's
#: ``DJANGO_SETTINGS_MODULE``.
ENVIRONMENT_VARIABLE = "VOLNUX_SETTINGS_MODULE"

#: Settings that were renamed or removed, and what replaces them. A settings
#: source that still sets one fails at load time with this message: ignoring it
#: would run with the default instead of what the project asked for.
REMOVED_SETTINGS: typing.Dict[str, str] = {
    "KEY_VALUE_STORE_CONFIG": "DATABASES['default'] (Django-style keys: ENGINE, NAME, HOST, PORT, USER, PASSWORD, OPTIONS)",
    "STREAM_BACKEND_CONFIG": "DATABASES['streams']",
    "TRIGGER_STATE_BACKEND_CONFIG": "DATABASES['triggers']",
    "SAGA_DLQ_BACKEND_CONFIG": "DATABASES['saga']",
    "BACKENDS": "DATABASES",
    "ENV": "DEBUG (a boolean: DEBUG = True for development, False for production)",
    "JWT_SECRET": "SECRET_KEY",
    "LOGGING_CONFIG": "LOGGING",
    "PROJECT_ROOT_DIR": "BASE_DIR",
    "API_CORS_ORIGINS": "CORS_ALLOWED_ORIGINS",
    "DEFAULT_EVENT_VERSIONING": "DEFAULT_EVENT_NAMESPACE (events use semantic versioning only)",
    "REDIS_CHECKPOINT_URL": "DATABASES['messaging'] (the Kubernetes executor and remote batches use the messaging backend)",
}


def _config():
    from volnux.config import VolnuxConfig

    return VolnuxConfig.get_instance()


class LazySettings:
    """The settings of the running project: ``settings.NAME``.

    Nothing is read until a setting is used. A setting that does not exist raises
    ``AttributeError``. The value is the current one: the environment variable
    ``VOLNUX_<NAME>`` if set, else the project's, else the default.
    """

    def __getattr__(self, name: str) -> typing.Any:
        if not name.isupper():
            raise AttributeError(
                f"{name!r} is not a setting: settings are UPPER_CASE names"
            )
        return _config().resolve(name)

    def __setattr__(self, name: str, value: typing.Any) -> None:
        raise AttributeError(
            "settings are read-only; use override_settings() in tests or "
            "settings.configure() once at start-up"
        )

    def __contains__(self, name: str) -> bool:
        try:
            _config().resolve(name)
        except AttributeError:
            return False
        return True

    def __dir__(self) -> typing.List[str]:
        return sorted(e.name for e in _config()._store if e.name)

    def get(self, name: str, default: typing.Any = None) -> typing.Any:
        """``settings.NAME`` or ``default`` if there is no such setting."""
        return _config().resolve(name, default)

    def configure(self, **options: typing.Any) -> None:
        """Set settings from code, for a program that has no settings module::

        settings.configure(DEBUG=True, DATABASES={...})
        """
        config = _config()
        for name, value in options.items():
            if not name.isupper():
                raise AttributeError(f"{name!r} is not a setting: use UPPER_CASE")
            config.set_setting(name, value)


settings = LazySettings()


class override_settings:
    """Change settings for a block or a test function, then put them back::

    with override_settings(DEBUG=True):
        ...

    @override_settings(API_PORT=9000)
    def test_something(): ...
    """

    def __init__(self, **options: typing.Any) -> None:
        self.options = options
        self._context: typing.Optional[typing.ContextManager] = None

    def __enter__(self) -> "override_settings":
        self._context = _config().override(**self.options)
        self._context.__enter__()
        return self

    def __exit__(self, *exc: typing.Any) -> None:
        assert self._context is not None
        self._context.__exit__(*exc)

    def __call__(self, function: typing.Callable) -> typing.Callable:
        @functools.wraps(function)
        def wrapper(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            with type(self)(**self.options):
                return function(*args, **kwargs)

        return wrapper
