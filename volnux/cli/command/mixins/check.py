import asyncio
import importlib
import inspect
from typing import TextIO

from ..style import Style
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.exceptions import CommandError


async def _check_backends(engine) -> list[tuple[str, bool, str]]:
    """
    Check every database of the ``DATABASES`` setting by instantiating its backend
    and calling a lightweight health check.

    Uses the same configuration the models use (``get_database_config``), so no
    connection strings or credentials are duplicated here.
    """
    from volnux.backends.databases import get_database_config, get_databases
    from volnux.exceptions import ImproperlyConfigured

    try:
        aliases = list(get_databases())
    except ImproperlyConfigured as exc:
        return [("DATABASES", False, str(exc))]

    results = []
    for alias in aliases:
        try:
            config = get_database_config(alias)
        except ImproperlyConfigured as exc:
            results.append((alias, False, str(exc)))
            continue
        ok, msg = await _check_single_backend(alias, config)
        results.append((alias, ok, msg))

    return results


async def _check_single_backend(
    name: str,
    cfg: dict,
) -> tuple[bool, str]:
    """
    Instantiate a backend from its configuration and verify it is
    reachable. The backend class is imported from the ENGINE path
    and instantiated with the CONNECTOR_CONFIG.

    A successful check means the backend accepted a connection and
    responded to a lightweight query (e.g., SELECT 1, PING).
    """

    engine_path = cfg.get("ENGINE")
    connector_config = cfg.get("CONNECTOR_CONFIG", {})

    if not engine_path:
        return False, f"No ENGINE configured for backend '{name}'"

    try:
        # Import the backend class
        module_path, class_name = engine_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        backend_class = getattr(module, class_name)

        # Instantiate (a backend connects here) and check. The backends are
        # synchronous, so their calls run off the event loop.
        backend = await _call(backend_class, **connector_config)
        try:
            # (a store keeps its connection in `connector`, which does the pinging)
            target = getattr(backend, "connector", None) or backend
            ping = getattr(target, "ping", None)
            if ping is not None and await _call(ping) is False:
                return False, "the backend did not answer a ping"
            get_version = getattr(target, "get_version", None)
            version = await _call(get_version) if get_version else None
            return True, version if version else "reachable"
        finally:
            close = getattr(backend, "close", None)
            if close is not None:
                await _call(close)

    except ImportError as exc:
        return False, f"Could not import backend class: {exc}"
    except Exception as exc:
        return False, str(exc)


async def _call(function, *args, **kwargs):
    """Call a backend function that may be blocking or a coroutine function."""
    from volnux.concurrency.async_utils import to_thread

    result = await to_thread(function, *args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


class CheckMixin:
    """
    Mixin class providing methods for running pre-flight backend checks.

    :ivar stdout: The output stream used for writing messages.
    :type stdout: TextIO
    :ivar style: The style formatting utility used for styling output messages.
    :type style: Style
    """

    # attributes for intellisense
    stdout: TextIO
    style: Style

    async def run_checks(self, engine: TriggerEngine) -> None:
        """Run pre-flight backend checks and print results."""
        self.stdout.write(self.style.NOTICE("  Running checks:\n"))

        try:
            workflow_registry = engine.get_workflow_registry()

            for config in workflow_registry.get_workflow_configs():
                alerts = config.check()
                for alert in alerts:
                    self.stdout.write(self.style.ERROR(f"  ✘: {alert}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"  ✘: {e}"))

        self.stdout.write(self.style.NOTICE("  Checking backends:\n"))

        results = await _check_backends(engine)

        for name, ok, msg in results:
            icon = self.style.SUCCESS("  ✔") if ok else self.style.ERROR("  ✘")
            status = self.style.SUCCESS(msg) if ok else self.style.ERROR(msg)
            self.stdout.write(f"{icon}  {name:<22} {status}")

        failures = [name for name, ok, _ in results if not ok]
        if failures:
            self.stdout.write(
                self.style.WARNING(
                    f"\n  Warning: {len(failures)} backend(s) unreachable "
                    f"({', '.join(failures)})."
                )
            )
        else:
            self.stdout.write(self.style.SUCCESS("\n  All backends reachable.\n"))
