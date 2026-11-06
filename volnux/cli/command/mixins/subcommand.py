import argparse
import asyncio
import typing

from volnux.exceptions import CommandError

if typing.TYPE_CHECKING:
    from volnux.cli.command.base import SubCommand


class SubCommandMixin:
    """
    Mixin class to extend command functionality with subcommand handling.

    This class provides methods for managing subcommands in a command-line
    interface context. By using this mixin, developers can define and register
    subcommands using a dictionary and seamlessly handle argument parsing
    and invocation for these subcommands. The mixin integrates with
    argparse to delegate argument parsing to subcommand-specific handlers.

    :ivar subcommands: Dictionary that maps subcommand names (str) to their
                       respective handler classes, which must implement `add_arguments`
                       and `handle` methods.
    :type subcommands: typing.Dict[str, typing.Type[SubCommand]]
    """

    subcommands: typing.Dict[str, typing.Type["SubCommand"]] = {}

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        """
        Install argparse subparsers for each declared subcommand, then let
        the parent chain add any top-level arguments it needs.
        """
        if self.subcommands:
            subparsers = parser.add_subparsers(
                dest="subcommand",
                metavar="<subcommand>",
            )
            # Mark as required, so argparse emits a clear error when omitted.
            subparsers.required = True

            for sub_name, subcommand_type in self.subcommands.items():
                subcommand = subcommand_type()

                sub_parser = subparsers.add_parser(
                    sub_name,
                    help=subcommand.help,
                    description=subcommand.help,
                )
                # Let the command declare per-subcommand arguments.
                subcommand.add_arguments(sub_parser)

        # Always call super() so BaseCommand (and any other mixin) can add
        # its own top-level arguments if needed.
        super().add_arguments(parser)  # type: ignore[misc]

    def handle(self, *args, **options) -> typing.Optional[str]:
        """
        Dispatch to the appropriate sub-subcommand handler, or fall through
        to `super` if no subcommands are declared.
        """
        if not self.subcommands:
            try:
                return super().handle(*args, **options)  # type: ignore[misc]
            except CommandError:
                raise
            except Exception as e:
                raise CommandError(f"Error: {e}")

        subcommand_name: typing.Optional[str] = options.get("subcommand")

        if not subcommand_name:
            # Argparse should have caught this but be defensive.
            raise CommandError(
                f"A subcommand is required. "
                f"Available: {', '.join(self.subcommands)}"
            )

        subcommand_type = self.subcommands.get(subcommand_name)
        if not subcommand_type:
            raise CommandError(
                f"Unknown subcommand '{subcommand_name}'. "
                f"Available: {', '.join(self.subcommands)}"
            )

        try:
            return subcommand_type().handle(options)
        except CommandError:
            raise
        except Exception as e:
            raise CommandError(f"Error: {e}")
