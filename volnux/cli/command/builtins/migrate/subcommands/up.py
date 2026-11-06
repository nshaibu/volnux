import argparse
import asyncio
import logging
import time
from typing import Optional, Type

from ....base import CommandCategory, SubCommand
from volnux.exceptions import CommandError
from volnux.backends.db_utils import migrate_models
from volnux.backends.model_registry import resolve_models

logger = logging.getLogger(__name__)


class UpSubCommand(SubCommand):
    """Apply all pending migrations to the database."""

    help = "Apply all pending migrations"
    name = "up"
    category = CommandCategory.OTHER

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            dest="dry_run",
            help="Show what would be migrated without applying anything",
        )
        parser.add_argument(
            "--model",
            action="append",
            default=[],
            dest="models",
            metavar="MODEL",
            help=(
                "Migrate only this model class name. "
                "Can be specified multiple times. "
                "Default: all models."
            ),
        )

    def handle(self, *args, **options) -> Optional[str]:
        dry_run = options.get("dry_run", False)
        filter_models = tuple([m for m in options.get("models", [])])

        if dry_run:
            self.warning("Dry run — no changes will be applied.\n")

        all_models, skipped = resolve_models(filter_models)

        if skipped:
            for name in skipped:
                self.warning(f"  Skipped: {name}")
            self.stdout.write("")

        if not all_models:
            raise CommandError(
                "No models could be imported. "
                "Ensure volnux[postgres] or the appropriate backend is installed."
            )

        # Dry run: show what would be migrated
        if dry_run:
            self.stdout.write("Models that would be migrated:\n")
            for model in all_models:
                self.stdout.write(f"  • {model.__name__}")
            self.stdout.write(f"\n{len(all_models)} model(s) — no changes applied.\n")
            return None

        self.stdout.write(f"Applying migrations for {len(all_models)} model(s)...\n")

        start = time.monotonic()
        applied = 0
        no_change = 0
        errors = []

        for model in all_models:
            model_start = time.monotonic()
            try:
                # migrate_models logs at INFO/DEBUG internally via logger.
                # We capture the outcome by checking applied count difference.
                original_info = logger.info
                applied_count = [0]

                # Temporarily intercept the "Applied N migrations" log
                # to surface it in CLI output without duplicating log setup.
                def _capture_info(msg, *a, **kw):
                    if "Applied" in str(msg) and "migrations to" in str(msg):
                        try:
                            applied_count[0] = int(str(msg).split()[1])
                        except (IndexError, ValueError):
                            applied_count[0] = 1
                    original_info(msg, *a, **kw)

                logger.info = _capture_info  # type: ignore[method-assign]
                try:
                    asyncio.run(migrate_models(model))
                finally:
                    logger.info = original_info  # type: ignore[method-assign]

                elapsed_ms = (time.monotonic() - model_start) * 1000

                if applied_count[0] > 0:
                    self.success(
                        f"  ✔  {model.__name__:<40} "
                        f"{applied_count[0]} migration(s) applied  "
                        f"({elapsed_ms:.0f}ms)"
                    )
                    applied += 1
                else:
                    self.stdout.write(
                        f"  –  {model.__name__:<40} already up to date  "
                        f"({elapsed_ms:.0f}ms)"
                    )
                    no_change += 1

            except TypeError as exc:
                # migrate_models raises TypeError for invalid model classes
                errors.append((model.__name__, str(exc)))
                self.error(f"  ✘  {model.__name__:<40} {exc}")

            except Exception as exc:
                errors.append((model.__name__, str(exc)))
                self.error(f"  ✘  {model.__name__:<40} {exc}")
                logger.exception("Migration failed for %s", model.__name__)

        total_ms = (time.monotonic() - start) * 1000
        self.stdout.write("")

        if errors:
            self.error(
                f"Migration completed with errors in {total_ms:.0f}ms:\n"
                f"  Applied:    {applied}\n"
                f"  No change:  {no_change}\n"
                f"  Errors:     {len(errors)}\n"
            )
            for model_name, msg in errors:
                self.error(f"  • {model_name}: {msg}")
            raise CommandError("Some migrations failed — see errors above.")
        else:
            self.success(
                f"Done in {total_ms:.0f}ms — "
                f"{applied} applied, {no_change} already up to date."
            )

        return None
