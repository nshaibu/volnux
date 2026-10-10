import httpx
import argparse
import json
import base64
import os
import time
from pathlib import Path
from typing import Optional

from volnux.exceptions import CommandError


class APIClientMixin:
    """
    Mixin for runtime CLI commands that communicate with a remote
    Volnux engine via REST API with JWT authentication.
    """

    CREDENTIALS_FILE = Path.home() / ".volnux" / "credentials"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client: Optional[httpx.AsyncClient] = None

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--host",
            default=os.environ.get("VOLNUX_HOST", "localhost:8080"),
            help="Volnux engine host:port",
        )
        parser.add_argument(
            "--insecure",
            action="store_true",
            default=False,
            help="Disable TLS verification",
        )

    async def _get_client(self, host: str, insecure: bool = False) -> httpx.AsyncClient:
        """Get or create an authenticated HTTP client."""
        if self._client is None:
            token = self._load_token(host)
            if token is None:
                raise CommandError(
                    f"Not authenticated for {host}. Run 'volnux login' first."
                )

            self._client = httpx.AsyncClient(
                base_url=f"https://{host}" if not insecure else f"http://{host}",
                headers={"Authorization": f"Bearer {token}"},
                verify=not insecure,
            )
        return self._client

    def _load_token(self, host: str) -> Optional[str]:
        """Load access token for a given host, refreshing if expired."""
        if not self.CREDENTIALS_FILE.exists():
            return None

        try:
            credentials = json.loads(self.CREDENTIALS_FILE.read_text())
            host_creds = credentials.get(host)
            if not host_creds:
                return None

            access_token = host_creds["access_token"]

            # Check expiry (with 60s buffer)
            if self._is_expired(access_token):
                # Synchronous refresh for CLI simplicity
                return self._refresh_token_sync(host, host_creds["refresh_token"])

            return access_token
        except Exception:
            return None

    def _is_expired(self, token: str) -> bool:
        """Check if a JWT is expired."""
        try:
            # JWT payload is the second segment
            payload = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))
            exp = payload.get("exp", 0)
            return time.time() > (exp - 60)  # 60s buffer
        except Exception:
            return True  # Can't decode → treat as expired

    def _refresh_token_sync(self, host: str, refresh_token: str) -> Optional[str]:
        """Refresh tokens synchronously (for CLI use outside async context)."""
        # ... call POST /api/v1/auth/refresh, store new tokens, return access_token

    async def _handle_api_error(self, response: httpx.Response) -> None:
        """Handle common API errors with clear messages."""
        if response.status_code == 401:
            raise CommandError(
                "Authentication failed. Run 'volnux login' to re-authenticate."
            )
        elif response.status_code == 403:
            raise CommandError("You don't have permission to perform this action.")
        elif response.status_code >= 500:
            raise CommandError(
                f"Server error ({response.status_code}). Try again later."
            )

    async def _close_client(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
