"""
Two CLI commands for EventHub manifest management:

    volnux manifest validate [--manifest PATH]
    volnux manifest generate [--output PATH] [--package-name NAME]
                             [--package-version VERSION] [--source-type TYPE]
                             [--dry-run]
"""

from .command import ManifestCommand
