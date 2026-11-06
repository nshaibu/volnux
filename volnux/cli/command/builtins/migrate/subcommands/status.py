import argparse
import asyncio
import logging
import time
from typing import Optional, Type

from ....base import CommandCategory, SubCommand
from volnux.backends.model_registry import resolve_models

logger = logging.getLogger(__name__)


class StatusSubCommand(SubCommand):
    """Show migration status for all models without applying changes."""

    help = "Show migration status for all models"
    name = "status"
    category = CommandCategory.OTHER

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        pass  # No arguments needed for status

    def handle(self, *args, **options) -> Optional[str]:
        all_models, skipped = resolve_models()

        self.stdout.write(
            f"\n{'Model':<42} {'Backend':<22} {'Schema':<30} Status\n" + "─" * 110
        )

        for model in all_models:
            try:
                backend = model.get_backend()
                schema_name = model.get_schema_name()
                backend_cls = backend.__class__.__name__

                # Check if schema exists by calling ensure_schema with dry_run
                # if the backend supports it, otherwise query directly.
                if hasattr(backend, "schema_exists"):
                    exists = backend.schema_exists(schema_name)
                    status = (
                        self.style.SUCCESS("up to date")
                        if exists
                        else self.style.WARNING("not migrated")
                    )
                else:
                    # Cannot determine status without querying — mark as unknown
                    status = self.style.NOTICE("unknown")

                self.stdout.write(
                    f"  {model.__name__:<40} {backend_cls:<22} {schema_name:<30} {status}"
                )

            except Exception as exc:
                self.stdout.write(
                    f"  {model.__name__:<40} {'':22} {'':30} "
                    f"{self.style.ERROR(f'error: {exc}')}"
                )

        if skipped:
            self.stdout.write(f"\n{'─' * 110}")
            self.stdout.write(self.style.WARNING("\nSkipped (not installed):"))
            for name in skipped:
                self.stdout.write(f"  • {name}")

        self.stdout.write(
            f"\n{len(all_models)} model(s) registered. "
            f"{len(skipped)} skipped.\n"
            f"Run 'volnux migrate up' to apply pending migrations.\n"
        )

        return None
