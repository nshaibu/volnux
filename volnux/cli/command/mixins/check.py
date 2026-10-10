import asyncio
import importlib
from typing import TextIO

from ..style import Style
from volnux.engine.workflows.trigger.engine import TriggerEngine
from volnux.exceptions import CommandError


async def _check_backends(engine) -> list[tuple[str, bool, str]]:
    """
    Check all configured backends by instantiating them from the
    BACKENDS configuration and calling a lightweight health check.

    Uses the same configuration the engine uses — no duplicated
    connection strings or manual credential handling.
    """
    from volnux.config import VolnuxConfig

    config = VolnuxConfig.get_instance()
    backends_config = getattr(config, "BACKENDS", {})

    if not backends_config:
        return [("No backends configured", False, "Add BACKENDS to settings.py")]

    results = []
    for backend_name, backend_cfg in backends_config.items():
        ok, msg = await _check_single_backend(backend_name, backend_cfg)
        results.append((backend_name, ok, msg))

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

        # Instantiate and check
        backend = backend_class(**connector_config)
        try:
            await backend.ping()
            version = await backend.get_version()
            return True, version if version else "reachable"
        finally:
            await backend.close()

    except ImportError as exc:
        return False, f"Could not import backend class: {exc}"
    except Exception as exc:
        return False, str(exc)


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
