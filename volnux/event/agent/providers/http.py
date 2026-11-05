"""Shared HTTP plumbing for adapters that talk to a REST API (everything but Gemini).

``httpx`` is a core dependency of Volnux, so no provider SDK is needed. Tests (and
callers that need a proxy or custom TLS) pass ``transport=...`` in the adapter options.
"""

import json
import os
from typing import Any, Dict, Optional

from volnux.exceptions import LLMProviderError
from .base import LLMProviderAdapterBase
from volnux.config import setting

__all__ = ["HTTPProviderAdapter", "json_arguments"]

_RETRYABLE_STATUS = {408, 409, 425, 429}


def json_arguments(raw: Any) -> Dict[str, Any]:
    """Tool arguments as a dict: providers send them as a JSON string or an object."""
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


class HTTPProviderAdapter(LLMProviderAdapterBase):
    """Base of the REST adapters: URL, key, request, and error mapping.

    Options: ``api_key``, ``base_url``, ``timeout`` (seconds), ``headers`` (extra),
    ``transport`` (an ``httpx`` transport), plus what a subclass reads.
    """

    #: Fallbacks, used when neither an option nor the environment variable is set.
    default_base_url: str = ""
    base_url_env: str = ""
    api_key_env: str = ""
    #: Local servers (Ollama, vLLM) work without a key.
    api_key_required: bool = True
    default_timeout: float = float(setting("AGENT_HTTP_TIMEOUT_SECONDS", 120.0))

    def _base_url(self) -> str:
        url = (
            self.options.get("base_url")
            or (os.environ.get(self.base_url_env) if self.base_url_env else None)
            or self.default_base_url
        )
        return url.rstrip("/")

    def _api_key(self) -> Optional[str]:
        key = self.options.get("api_key") or (
            os.environ.get(self.api_key_env, "").strip() if self.api_key_env else ""
        )
        if not key and self.api_key_required:
            raise LLMProviderError(
                f"No API key for '{self.provider_name}': set the "
                f"{self.api_key_env or 'api_key'} environment variable or pass "
                f"api_key in llm_options. Never hardcode keys in source.",
                retryable=False,
            )
        return key or None

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        headers.update(self.options.get("headers") or {})
        return headers

    async def _post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """POST ``payload`` and return the JSON answer; every failure is an
        :class:`LLMProviderError` that says whether a retry can help."""
        import httpx

        url = f"{self._base_url()}{path}"
        name = self.provider_name
        try:
            async with httpx.AsyncClient(
                timeout=self.options.get("timeout", self.default_timeout),
                transport=self.options.get("transport"),
            ) as client:
                response = await client.post(url, json=payload, headers=self._headers())
        except httpx.TimeoutException as exc:
            raise LLMProviderError(f"{name} request timed out: {exc}") from exc
        except httpx.ConnectError as exc:
            raise LLMProviderError(
                f"Cannot connect to {name} at '{url}': {exc}"
            ) from exc
        except httpx.HTTPError as exc:
            raise LLMProviderError(f"{name} request failed: {exc}") from exc

        if response.status_code >= 400:
            retryable = (
                response.status_code >= 500 or response.status_code in _RETRYABLE_STATUS
            )
            raise LLMProviderError(
                f"{name} HTTP error {response.status_code}: {response.text[:500]}",
                retryable=retryable,
                status_code=response.status_code,
            )
        try:
            return response.json()
        except ValueError as exc:
            raise LLMProviderError(f"{name} returned a body that is not JSON") from exc
