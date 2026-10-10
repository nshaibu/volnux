import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .base import TriggerBase, TriggerType

logger = logging.getLogger(__name__)


class WebhookTrigger(TriggerBase):
    """
    Trigger that activates via an inbound HTTP webhook.

    Integration model
    -----------------
    ``WebhookTrigger`` is *framework-agnostic*. It does not own or start an
    HTTP server itself. The caller is responsible for wiring a route in their
    web framework (FastAPI, Flask, Django, …) and calling
    :meth:`handle_webhook` when a request arrives.

    Example (FastAPI)::

        trigger = WebhookTrigger(
            workflow_name="my_workflow",
            endpoint_path="/webhooks/my-hook",
            secret_token="s3cr3t",
        )

        @app.post(trigger.endpoint_path)
        async def webhook_handler(request: Request):
            headers = dict(request.headers)
            body = await request.body() # raw bytes for HMAC
            await trigger.handle_webhook(body, headers=headers)

    Self-registering subclass
    -------------------------
    If you want the trigger to register its own route, subclass and override
    :meth:`_register_endpoint` and :meth:`_unregister_endpoint`. Both hooks
    are ``async`` so subclasses can perform async work (e.g. registering with
    an async framework or making a network call) without workarounds. The base
    implementations are intentional no-ops, so the external model works without
    subclassing::

        class FastAPIWebhookTrigger(WebhookTrigger):
            async def _register_endpoint(self) -> None:
                @self.web_app.post(self.endpoint_path)
                async def handler(request: Request):
                    headers = dict(request.headers)
                    body = await request.body()
                    await self.handle_webhook(body, headers=headers)

            async def _unregister_endpoint(self) -> None:
                # FastAPI does not support route removal at runtime; handle as needed.
                pass

    Signature verification
    ----------------------
    When ``secret_token`` is set, :meth:`handle_webhook` computes an
    ``HMAC-SHA256`` signature over the **raw request body bytes** using the
    secret, then compares it against the value found in ``headers`` under the
    key supplied as ``token_header`` (default: ``"x-webhook-token"``).

    This matches the model used by GitHub, Stripe, Twilio, and most major
    webhook providers: the secret never travels over the wire and the signature
    covers the payload, so neither replay-without-tampering nor
    man-in-the-middle payload substitution can pass verification.

    Comparison always uses :func:`hmac.compare_digest` to prevent timing
    side-channel attacks, including on the "header missing" path.

    Raises :class:`PermissionError` on mismatch so callers can map it to an
    HTTP 401/403 response cleanly.
    """

    trigger_type = TriggerType.WEBHOOK

    def __init__(
        self,
        workflow_name: str,
        endpoint_path: str,
        secret_token: Optional[str] = None,
        token_header: str = "x-webhook-token",
        web_app: Optional[Any] = None,
        workflow_params: Optional[Dict[str, Any]] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if secret_token is not None and not isinstance(secret_token, str):
            raise TypeError(
                f"'secret_token' must be a str, got {type(secret_token).__name__!r}. "
                "Encode bytes tokens to a hex or base64 string before passing them in."
            )

        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self.endpoint_path = endpoint_path
        self.secret_token = secret_token
        self.token_header = token_header.lower()
        self.web_app = web_app

    async def start(self) -> None:
        """
        Register the webhook endpoint (if a self-registering subclass is used).

        In the default external-integration model this is a no-op; the web
        framework route is wired by the caller before the engine starts.
        """
        await self._register_endpoint()
        logger.info(
            "WebhookTrigger '%s' ready at '%s'", self.trigger_id, self.endpoint_path
        )

    async def stop(self) -> None:
        """
        Unregister the webhook endpoint (if applicable).

        In the default external-integration model this is a no-op.
        """
        await self._unregister_endpoint()
        logger.info("WebhookTrigger '%s' stopped", self.trigger_id)

    async def _register_endpoint(self) -> None:
        """
        Override to register a route with a web framework at ``start()`` time.

        The default implementation is a deliberate no-op.
        """

    async def _unregister_endpoint(self) -> None:
        """
        Override to remove a previously registered route at ``stop()`` time.

        The default implementation is a deliberate no-op.
        """

    async def handle_webhook(
        self,
        body: bytes,
        *,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Process an inbound webhook request.

        Parameters
        ----------
        body:
            Raw request body bytes. Required as bytes (not a parsed dict) so
            the HMAC signature can be computed over the exact bytes the sender
            signed. Parse JSON *after* verification if needed.
        headers:
            HTTP request headers. Required when ``secret_token`` is set.

        Raises
        ------
        PermissionError
            If a ``secret_token`` is configured and the request does not
            supply a valid HMAC-SHA256 signature in the expected header.
        """
        normalised_headers = {k.lower(): v for k, v in (headers or {}).items()}

        if self.secret_token is not None:
            self._verify_signature(body, normalised_headers)

        await self.activate(
            webhook_body=body,
            webhook_headers=normalised_headers,
            received_at=datetime.now(timezone.utc).isoformat(),
        )

    def _verify_signature(
        self, body: bytes, normalised_headers: Dict[str, str]
    ) -> None:
        """
        Verify the HMAC-SHA256 signature supplied in ``normalised_headers``.

        The expected signature is computed as::

            HMAC-SHA256(key=secret_token, msg=body).hexdigest()

        Header lookup uses pre-normalised (lowercase) keys. Comparison always
        runs through :func:`hmac.compare_digest` — including when the header
        is absent — to prevent timing-based side-channel attacks.

        Parameters
        ----------
        body:
            Raw request body bytes; must be the exact bytes that were signed.
        normalised_headers:
            Request headers with keys already lowercased.

        Raises
        ------
        PermissionError
            If the signature is absent or does not match.
        """
        provided_sig = normalised_headers.get(self.token_header, "")

        expected_sig = hmac.new(
            self.secret_token.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()

        # Always call compare_digest regardless of whether the header was
        # present; short-circuiting on absence leaks a timing signal.
        if not hmac.compare_digest(provided_sig, expected_sig):
            logger.warning(
                "WebhookTrigger '%s': rejected request — invalid or missing "
                "HMAC-SHA256 signature (header: %r)",
                self.trigger_id,
                self.token_header,
            )
            raise PermissionError(
                f"Invalid or missing webhook signature "
                f"(expected header: {self.token_header!r})"
            )
