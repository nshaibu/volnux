from typing import Optional

from volnux import __version__ as version

from ..base import BaseCommand, CommandCategory


class VersionCommand(BaseCommand):
    help = "Show Volnux version"
    name = "version"
    category = CommandCategory.PROJECT_MANAGEMENT

    def add_arguments(self, parser) -> None:
        return

    def handle(self, *args, **options) -> Optional[str]:
        return f"Volnux {version}"
