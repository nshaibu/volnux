import logging

from ...mixins import SubCommandMixin
from ...base import BaseCommand, CommandCategory
from .subcommands import UpSubCommand, StatusSubCommand


logger = logging.getLogger(__name__)


class MigrateCommand(SubCommandMixin, BaseCommand):
    """
    Manage database schema migrations.

    Supported subcommands:
        up      Apply all pending migrations (default)
        status Show migration status for all models

    Usage:
        volnux migrate # same as migrate up
        volnux migrate up
        volnux migrate up --dry-run
        volnux migrate up --model AuditEntry --model Workflow
        volnux migrate status

    Migration applies schemas for all governance models in dependency order:
        IAM → Workflow → Audit → Approval → Delegation → BreakGlass
        → Assets → HITL → Execution → Triggers → Checkpoints

    Idempotent: running migrate up on an already-migrated database is safe.
    The underlying ensure_schema() only applies changes that are pending.

    In Kubernetes, run migrations as an init container before the API starts:
        command: ["volnux", "migrate", "up"]
    """

    help = "Apply database migrations or show migration status"
    name = "migrate"
    category = CommandCategory.PROJECT_MANAGEMENT

    # SubCommand classes available under this command.
    # The CLI entrypoint and HelpCommand use this dict for routing and help.
    subcommands = {
        "up": UpSubCommand,
        "status": StatusSubCommand,
    }

    # def add_arguments(self, parser: argparse.ArgumentParser) -> None:
    #     parser.add_argument(
    #         "subcommand",
    #         nargs="?",
    #         default="up",
    #         choices=list(self.subcommands.keys()),
    #         help="Subcommand to run: 'up' (default) or 'status'",
    #     )
    #     # Forward --dry-run to UpSubCommand when called without explicit subcommand
    #     parser.add_argument(
    #         "--dry-run",
    #         action="store_true",
    #         default=False,
    #         dest="dry_run",
    #         help="Show what would be migrated without applying (up only)",
    #     )
    #     parser.add_argument(
    #         "--model",
    #         action="append",
    #         default=[],
    #         dest="models",
    #         metavar="MODEL",
    #         help="Migrate only this model. Can be repeated. (up only)",
    #     )
    #
    # def handle(self, *args, **options) -> Optional[str]:
    #     subcommand_name = options.get("subcommand", "up")
    #
    #     subcommand_cls = self.subcommands.get(subcommand_name)
    #     if subcommand_cls is None:
    #         raise CommandError(
    #             f"Unknown subcommand '{subcommand_name}'. "
    #             f"Available: {', '.join(self.subcommands)}"
    #         )
    #
    #     subcommand = subcommand_cls()
    #     # Transfer relevant options to the subcommand handler
    #     subcommand.stdout = self.stdout
    #     subcommand.stderr = self.stderr
    #     subcommand.style = self.style
    #
    #     subcommand.handle(*args, **options)
    #     return None
