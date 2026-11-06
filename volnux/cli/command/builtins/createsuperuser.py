import argparse
import asyncio
import getpass
import os
import sys
from typing import Optional

from volnux.exceptions import CommandError

from ..base import BaseCommand, CommandCategory
from ..mixins import ProjectMixin


class CreateSuperuserCommand(ProjectMixin, BaseCommand):
    """
    Create the first (or another) super-admin account.

    A new deployment has no users, so nobody can log in to the REST API. This
    creates an organization (if the slug is new), the ``super-admin`` role (if it
    does not exist) and a user holding it.

    Usage:
        volnux createsuperuser --email admin@example.com --name "Ada Admin" --org-slug acme
        volnux createsuperuser ... --password-env ADMIN_PASSWORD
        echo "$PASSWORD" | volnux createsuperuser ... --password-stdin

    The password is read from a hidden prompt (asked twice), from the environment
    variable named by --password-env, or from standard input. It is never accepted
    as a command-line argument, where other users of the machine could see it.
    """

    help = "Create a super-admin account (and its organization if needed)"
    name = "createsuperuser"
    category = CommandCategory.PROJECT_MANAGEMENT

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--email", required=True, help="Login email")
        parser.add_argument("--name", required=True, help="Display name")
        parser.add_argument(
            "--org-slug",
            required=True,
            dest="org_slug",
            help="Slug of the organization (created if it does not exist)",
        )
        parser.add_argument(
            "--org-name",
            dest="org_name",
            default=None,
            help="Name for a new organization (default: the slug)",
        )
        source = parser.add_mutually_exclusive_group()
        source.add_argument(
            "--password-env",
            dest="password_env",
            metavar="VAR",
            help="Read the password from this environment variable",
        )
        source.add_argument(
            "--password-stdin",
            dest="password_stdin",
            action="store_true",
            help="Read the password from standard input",
        )

    def _read_password(self, options) -> str:
        if options.get("password_env"):
            value = os.environ.get(options["password_env"])
            if not value:
                raise CommandError(
                    f"The environment variable {options['password_env']} is not set"
                )
            return value
        if options.get("password_stdin"):
            return sys.stdin.readline().rstrip("\r\n")
        first = getpass.getpass("Password: ")
        if first != getpass.getpass("Password (again): "):
            raise CommandError("The passwords do not match")
        return first

    def handle(self, *args, **options) -> Optional[str]:
        from volnux.app.bootstrap import BootstrapError, create_superuser

        password = self._read_password(options)
        project_dir = self.resolve_project_dir()
        # Loads the project's settings (DATABASES, SECRET_KEY, ...) and engine.
        self.initialise_workflows(project_dir)
        try:
            user = asyncio.run(
                create_superuser(
                    email=options["email"],
                    name=options["name"],
                    password=password,
                    org_slug=options["org_slug"],
                    org_name=options.get("org_name"),
                )
            )
        except BootstrapError as exc:
            raise CommandError(str(exc)) from exc
        return (
            f"Super-admin {user.email} created in organization "
            f"{options['org_slug']!r} (id {user.id})."
        )
