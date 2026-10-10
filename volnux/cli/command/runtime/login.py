import os
import getpass
import httpx

from volnux.cli.command.base import BaseCommand, CommandCategory
from volnux.cli.output import OutputMixin


class LoginCommand(BaseCommand, OutputMixin):
    """Authenticate with a Volnux engine."""

    name = "login"
    category = CommandCategory.AUTH

    def add_arguments(self, parser):
        parser.add_argument(
            "--host", default=os.environ.get("VOLNUX_HOST", "localhost:8080")
        )
        parser.add_argument("--username", "-u")
        parser.add_argument("--password", "-p")
        parser.add_argument("--insecure", action="store_true")

    async def handle(self, *args, **options):
        host = options["host"]
        username = options["username"] or input("Username: ")
        password = options["password"] or getpass.getpass("Password: ")

        async with httpx.AsyncClient(
            verify=not options.get("insecure", False)
        ) as client:
            response = await client.post(
                f"https://{host}/api/v1/auth/login",
                json={"username": username, "password": password},
            )

            if response.status_code != 200:
                self.error(
                    f"Login failed: {response.json().get('detail', 'Unknown error')}"
                )
                return

            tokens = response.json()
            self._store_tokens(host, tokens)
            self.success(f"Authenticated to {host}")
