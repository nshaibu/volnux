import hashlib
import hmac
import logging
import time
from collections import OrderedDict
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

    Replay protection
    -----------------
    A correctly signed request can otherwise be captured and sent again for ever.
    Set ``timestamp_header`` (e.g. ``"x-webhook-timestamp"``) to require the
    sender to include the time of sending, in Unix seconds, and to sign it with
    the body: the signed message becomes ``f"{timestamp}.".encode() + body``
    (the scheme Stripe uses). Then:

    * a request whose timestamp is more than ``tolerance_seconds`` from now
      (either direction) is refused, so a captured request goes stale;
    * inside that window an identical signature is accepted only once, so it
      cannot be replayed immediately either.

    The seen-signature memory is per process (bounded, entries expire with the
    window); behind several processes, put the same check in a shared store.
    Requires ``secret_token``, and sender and receiver clocks must roughly agree.
    """

    #: Most signatures remembered for replay detection (oldest dropped first).
    _MAX_SEEN_SIGNATURES = 10_000

    trigger_type = TriggerType.WEBHOOK

    def __init__(
        self,
        workflow_name: str,
        endpoint_path: str,
        secret_token: Optional[str] = None,
        token_header: str = "x-webhook-token",
        timestamp_header: Optional[str] = None,
        tolerance_seconds: float = 300.0,
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
        if secret_token == "":
            # An empty secret would "verify" requests against a publicly known key.
            raise ValueError(
                "'secret_token' must not be empty; pass None to disable verification."
            )
        if not isinstance(endpoint_path, str) or not endpoint_path.startswith("/"):
            raise ValueError(
                f"'endpoint_path' must be a path starting with '/', got {endpoint_path!r}."
            )
        if not isinstance(token_header, str) or not token_header.strip():
            raise ValueError("'token_header' must be a non-empty header name.")

        if timestamp_header is not None:
            if not isinstance(timestamp_header, str) or not timestamp_header.strip():
                raise ValueError("'timestamp_header' must be a non-empty header name.")
            if secret_token is None:
                raise ValueError(
                    "Replay protection ('timestamp_header') needs a 'secret_token': "
                    "an unsigned timestamp proves nothing."
                )
        if tolerance_seconds <= 0:
            raise ValueError("'tolerance_seconds' must be positive.")

        super().__init__(
            workflow_name,
            workflow_params=workflow_params,
            enabled=enabled,
            metadata=metadata,
        )
        self.timestamp_header = timestamp_header.lower() if timestamp_header else None
        self.tolerance_seconds = float(tolerance_seconds)
        self._clock = time.time  # replaceable in tests
        self._seen_signatures: "OrderedDict[str, float]" = OrderedDict()
        self.endpoint_path = endpoint_path
        self.secret_token = secret_token
        self.token_header = token_header.lower()
        self.web_app = web_app
        self._endpoint_registered = False

    async def start(self) -> None:
        """
        Register the webhook endpoint (if a self-registering subclass is used).

        In the default external-integration model this is a no-op; the web
        framework route is wired by the caller before the engine starts.
        """
        if self._endpoint_registered:
            logger.debug("WebhookTrigger '%s' already started", self.trigger_id)
            return
        await self._register_endpoint()
        self._endpoint_registered = True
        logger.info(
            "WebhookTrigger '%s' ready at '%s'", self.trigger_id, self.endpoint_path
        )

    async def stop(self) -> None:
        """
        Unregister the webhook endpoint (if applicable).

        In the default external-integration model this is a no-op.
        """
        if not self._endpoint_registered:
            return
        try:
            await self._unregister_endpoint()
        finally:
            self._endpoint_registered = False
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

    #: Headers never forwarded to the workflow: they carry credentials.
    _SENSITIVE_HEADERS = frozenset(
        {"authorization", "proxy-authorization", "cookie", "set-cookie"}
    )

    async def handle_webhook(
        self,
        body: bytes,
        *,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
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

        Returns
        -------
        bool
            ``True`` if the workflow was activated, ``False`` if the request was
            valid but the trigger is disabled (paused or stopped) and ignored it.
            The signature is checked first, so an unauthenticated caller cannot
            learn whether the trigger is enabled.

        Raises
        ------
        TypeError
            If ``body`` is not bytes-like.
        PermissionError
            If a ``secret_token`` is configured and the request does not
            supply a valid HMAC-SHA256 signature in the expected header.
        """
        if not isinstance(body, (bytes, bytearray, memoryview)):
            raise TypeError(
                f"'body' must be the raw request bytes, got {type(body).__name__!r}."
            )
        body = bytes(body)
        normalised_headers = {k.lower(): v for k, v in (headers or {}).items()}

        if self.secret_token is not None:
            self._verify_signature(body, normalised_headers)

        if not self.enabled:
            logger.info(
                "WebhookTrigger '%s' is disabled; webhook ignored.", self.trigger_id
            )
            return False

        forwarded = {
            name: value
            for name, value in normalised_headers.items()
            if name not in self._SENSITIVE_HEADERS and name != self.token_header
        }
        await self.activate(
            webhook_body=body,
            webhook_headers=forwarded,
            received_at=datetime.now(timezone.utc).isoformat(),
        )
        return True

    def _verify_signature(
        self, body: bytes, normalised_headers: Dict[str, str]
    ) -> None:
        """
        Verify the HMAC-SHA256 signature supplied in ``normalised_headers``.

        The expected signature is computed as::

            HMAC-SHA256(key=secret_token, msg=body).hexdigest()

        or, with ``timestamp_header`` set, over ``f"{timestamp}.".encode() + body``
        followed by the staleness and replay checks.

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
        provided_sig = str(normalised_headers.get(self.token_header, "")).strip()
        # Providers such as GitHub send "sha256=<hex>"; hex digits are case-blind.
        if provided_sig.lower().startswith("sha256="):
            provided_sig = provided_sig[len("sha256=") :]
        provided_sig = provided_sig.lower()

        message = body
        timestamp = None
        if self.timestamp_header is not None:
            raw_timestamp = str(
                normalised_headers.get(self.timestamp_header, "")
            ).strip()
            timestamp = self._parse_timestamp(raw_timestamp)
            # The timestamp is part of what is signed, so it cannot be refreshed
            # on a captured request.
            message = raw_timestamp.encode() + b"." + body

        expected_sig = hmac.new(
            self.secret_token.encode(),
            message,
            hashlib.sha256,
        ).hexdigest()

        # Always call compare_digest regardless of whether the header was
        # present; short-circuiting on absence leaks a timing signal. Compared as
        # bytes: str arguments with non-ASCII characters make it raise TypeError
        # (a 500 for the caller) instead of simply not matching.
        if not hmac.compare_digest(provided_sig.encode(), expected_sig.encode()):
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

        if timestamp is not None:
            self._check_not_replayed(expected_sig, timestamp)

    @staticmethod
    def _parse_timestamp(raw: str) -> float:
        try:
            value = float(raw)
        except ValueError:
            value = float("nan")
        if value != value or value in (float("inf"), float("-inf")):
            raise PermissionError("Missing or invalid webhook timestamp.")
        return value

    def _check_not_replayed(self, signature: str, timestamp: float) -> None:
        """Refuse a stale timestamp or a signature already accepted in the window."""
        now = self._clock()
        if abs(now - timestamp) > self.tolerance_seconds:
            logger.warning(
                "WebhookTrigger '%s': rejected request — timestamp outside the "
                "%.0fs tolerance",
                self.trigger_id,
                self.tolerance_seconds,
            )
            raise PermissionError("Webhook timestamp outside the allowed tolerance.")

        # forget what has aged out (insertion order is expiry order)
        while self._seen_signatures:
            oldest, expires = next(iter(self._seen_signatures.items()))
            if expires > now:
                break
            self._seen_signatures.popitem(last=False)

        if signature in self._seen_signatures:
            logger.warning(
                "WebhookTrigger '%s': rejected request — signature already used",
                self.trigger_id,
            )
            raise PermissionError("Webhook request was already received (replay).")

        self._seen_signatures[signature] = now + self.tolerance_seconds
        while len(self._seen_signatures) > self._MAX_SEEN_SIGNATURES:
            self._seen_signatures.popitem(last=False)
