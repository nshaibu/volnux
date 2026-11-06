import hashlib
import hmac

import pytest

from volnux.engine.workflows.trigger.state import TriggerStateRecord
from volnux.engine.workflows.trigger.triggers import TriggerLifecycle, WebhookTrigger
from volnux.engine.workflows.trigger.triggers.base import TriggerType

SECRET = "s3cr3t"
BODY = b'{"event": "push", "ref": "main"}'


def sign(body=BODY, secret=SECRET):
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def make(**kw):
    kw.setdefault("endpoint_path", "/webhooks/hook")
    t = WebhookTrigger("wf", **kw)
    got = []

    async def cb(activation):
        got.append(activation)

    t.set_activation_callback(cb)
    return t, got


class TestConstruction:
    def test_type_and_defaults(self):
        t = WebhookTrigger("wf", "/hook")
        assert t.trigger_type is TriggerType.WEBHOOK
        assert t.endpoint_path == "/hook" and t.secret_token is None
        assert t.token_header == "x-webhook-token"

    def test_header_name_is_lowercased(self):
        assert WebhookTrigger(
            "wf", "/h", token_header="X-Hub-Signature"
        ).token_header == ("x-hub-signature")

    @pytest.mark.parametrize("bad", [b"bytes", 123, ["x"]])
    def test_secret_must_be_a_string(self, bad):
        with pytest.raises(TypeError):
            WebhookTrigger("wf", "/h", secret_token=bad)

    def test_empty_secret_is_refused(self):
        with pytest.raises(ValueError, match="secret_token"):
            WebhookTrigger("wf", "/h", secret_token="")

    @pytest.mark.parametrize("bad", ["", "hook", "webhooks/x", None, 5])
    def test_endpoint_path_must_start_with_a_slash(self, bad):
        with pytest.raises(ValueError, match="endpoint_path"):
            WebhookTrigger("wf", bad)

    @pytest.mark.parametrize("bad", ["", "   ", None])
    def test_token_header_must_not_be_blank(self, bad):
        with pytest.raises(ValueError, match="token_header"):
            WebhookTrigger("wf", "/h", token_header=bad)


@pytest.mark.asyncio
class TestWithoutSecret:
    async def test_activates_with_body_headers_and_time(self, store):
        t, got = make(workflow_params={"fixed": 1})
        assert await t.handle_webhook(
            BODY, headers={"Content-Type": "application/json"}
        )
        p = got[0].workflow_params
        assert p["fixed"] == 1 and p["webhook_body"] == BODY
        assert p["webhook_headers"] == {"content-type": "application/json"}
        assert p["received_at"]
        assert got[0].activation_source is TriggerType.WEBHOOK

    async def test_headers_are_optional(self, store):
        t, got = make()
        assert await t.handle_webhook(b"")
        assert got[0].workflow_params["webhook_headers"] == {}

    async def test_bytearray_and_memoryview_bodies_are_accepted(self, store):
        t, got = make()
        await t.handle_webhook(bytearray(BODY))
        await t.handle_webhook(memoryview(BODY))
        assert [g.workflow_params["webhook_body"] for g in got] == [BODY, BODY]

    @pytest.mark.parametrize("bad", ["text", {"a": 1}, None, 5])
    async def test_non_bytes_body_is_refused(self, store, bad):
        t, got = make()
        with pytest.raises(TypeError, match="raw request bytes"):
            await t.handle_webhook(bad)
        assert got == []

    async def test_credentials_are_not_forwarded(self, store):
        t, got = make()
        await t.handle_webhook(
            BODY,
            headers={
                "Authorization": "Bearer abc",
                "Cookie": "session=1",
                "Proxy-Authorization": "x",
                "X-Request-Id": "r1",
            },
        )
        assert got[0].workflow_params["webhook_headers"] == {"x-request-id": "r1"}


@pytest.mark.asyncio
class TestSignature:
    async def test_valid_signature_activates(self, store):
        t, got = make(secret_token=SECRET)
        assert await t.handle_webhook(BODY, headers={"x-webhook-token": sign()})
        assert len(got) == 1

    async def test_the_signature_header_is_not_forwarded(self, store):
        t, got = make(secret_token=SECRET)
        await t.handle_webhook(BODY, headers={"X-Webhook-Token": sign(), "X-A": "1"})
        assert got[0].workflow_params["webhook_headers"] == {"x-a": "1"}

    async def test_header_name_is_case_insensitive(self, store):
        t, got = make(secret_token=SECRET)
        await t.handle_webhook(BODY, headers={"X-WEBHOOK-TOKEN": sign()})
        assert len(got) == 1

    async def test_custom_header(self, store):
        t, got = make(secret_token=SECRET, token_header="X-Hub-Signature-256")
        await t.handle_webhook(BODY, headers={"x-hub-signature-256": sign()})
        assert len(got) == 1

    async def test_sha256_prefix_and_uppercase_hex_are_accepted(self, store):
        t, got = make(secret_token=SECRET)
        await t.handle_webhook(BODY, headers={"x-webhook-token": f"sha256={sign()}"})
        await t.handle_webhook(
            BODY, headers={"x-webhook-token": f"SHA256={sign().upper()}"}
        )
        assert len(got) == 2

    @pytest.mark.parametrize(
        "headers",
        [
            None,
            {},
            {"x-webhook-token": ""},
            {"x-webhook-token": "deadbeef"},
            {"x-webhook-token": "sha256="},
            {"x-other": "x"},
            {"x-webhook-token": SECRET},  # the secret itself is not a signature
            {"x-webhook-token": "é" * 64},  # non-ASCII must be a mismatch, not a crash
        ],
    )
    async def test_bad_or_missing_signature_is_refused(self, store, headers):
        t, got = make(secret_token=SECRET)
        with pytest.raises(PermissionError, match="signature"):
            await t.handle_webhook(BODY, headers=headers)
        assert got == [] and t.fire_count == 0

    async def test_signature_covers_the_body(self, store):
        t, got = make(secret_token=SECRET)
        with pytest.raises(PermissionError):
            await t.handle_webhook(BODY + b" ", headers={"x-webhook-token": sign()})
        assert got == []

    async def test_signature_made_with_another_secret_is_refused(self, store):
        t, _ = make(secret_token=SECRET)
        with pytest.raises(PermissionError):
            await t.handle_webhook(
                BODY, headers={"x-webhook-token": sign(secret="other")}
            )

    async def test_comparison_is_constant_time(self, store, monkeypatch):
        calls = []
        real = hmac.compare_digest

        def spy(a, b):
            calls.append((type(a), type(b)))
            return real(a, b)

        monkeypatch.setattr(hmac, "compare_digest", spy)
        t, _ = make(secret_token=SECRET)
        with pytest.raises(PermissionError):
            await t.handle_webhook(BODY)  # header absent: still compared
        assert calls == [(bytes, bytes)]


@pytest.mark.asyncio
class TestEnabledState:
    async def test_disabled_trigger_ignores_the_request(self, store):
        t, got = make(enabled=False)
        assert await t.handle_webhook(BODY) is False
        assert got == []

    async def test_paused_and_stopped_ignore_the_request(self, store):
        t, got = make()
        await t.run()
        await t.pause()
        assert await t.handle_webhook(BODY) is False
        await t.resume()
        assert await t.handle_webhook(BODY) is True
        await t.end()
        assert await t.handle_webhook(BODY) is False
        assert len(got) == 1

    async def test_signature_is_checked_before_enabled(self, store):
        # an unauthenticated caller must not learn that the trigger is off
        t, _ = make(secret_token=SECRET, enabled=False)
        with pytest.raises(PermissionError):
            await t.handle_webhook(BODY, headers={"x-webhook-token": "bad"})
        assert (
            await t.handle_webhook(BODY, headers={"x-webhook-token": sign()}) is False
        )


@pytest.mark.asyncio
class TestActivation:
    async def test_counts_and_persists(self, store):
        t, got = make()
        await t.handle_webhook(BODY)
        await t.handle_webhook(BODY)
        record = await TriggerStateRecord.get(t.trigger_id)
        assert len(got) == 2 and record.fire_count == 2
        assert record.last_fired is not None

    async def test_no_callback_raises_and_counts_error(self, store):
        t = WebhookTrigger("wf", "/h")
        with pytest.raises(RuntimeError, match="no activation callback"):
            await t.handle_webhook(BODY)
        assert t.error_count == 1

    async def test_callback_failure_propagates(self, store):
        t = WebhookTrigger("wf", "/h")

        async def boom(activation):
            raise ValueError("engine busy")

        t.set_activation_callback(boom)
        with pytest.raises(ValueError):
            await t.handle_webhook(BODY)
        assert t.error_count == 1


class Recording(WebhookTrigger):
    def __init__(self, *a, fail_register=False, **kw):
        super().__init__(*a, **kw)
        self.registered = 0
        self.unregistered = 0
        self.fail_register = fail_register

    async def _register_endpoint(self):
        if self.fail_register:
            raise RuntimeError("route table full")
        self.registered += 1

    async def _unregister_endpoint(self):
        self.unregistered += 1


@pytest.mark.asyncio
class TestEndpointHooks:
    async def test_default_hooks_are_no_ops(self):
        t = WebhookTrigger("wf", "/h")
        await t.start()
        await t.stop()

    async def test_start_registers_once(self):
        t = Recording("wf", "/h")
        await t.start()
        await t.start()
        assert t.registered == 1

    async def test_stop_unregisters_once_and_only_after_start(self):
        t = Recording("wf", "/h")
        await t.stop()
        assert t.unregistered == 0
        await t.start()
        await t.stop()
        await t.stop()
        assert t.unregistered == 1

    async def test_restart_registers_again(self):
        t = Recording("wf", "/h")
        await t.start()
        await t.stop()
        await t.start()
        assert (t.registered, t.unregistered) == (2, 1)

    async def test_failed_registration_can_be_retried(self):
        t = Recording("wf", "/h", fail_register=True)
        with pytest.raises(RuntimeError):
            await t.start()
        t.fail_register = False
        await t.start()
        assert t.registered == 1

    async def test_run_and_end_update_the_record(self, store):
        t, _ = make()
        await t.run()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.ACTIVE
        )
        await t.end()
        assert (await TriggerStateRecord.get(t.trigger_id)).lifecycle is (
            TriggerLifecycle.STOPPED
        )


class TestWithAWebFramework:
    """The documented FastAPI wiring, end to end through real HTTP requests."""

    @pytest.fixture
    def client_and_trigger(self, store):
        fastapi = pytest.importorskip("fastapi")
        pytest.importorskip("httpx")
        from fastapi.testclient import TestClient

        trigger, got = make(secret_token=SECRET)
        app = fastapi.FastAPI()

        @app.post(trigger.endpoint_path)
        async def hook(request: fastapi.Request):
            try:
                ok = await trigger.handle_webhook(
                    await request.body(), headers=dict(request.headers)
                )
            except PermissionError:
                raise fastapi.HTTPException(status_code=401)
            return {"accepted": ok}

        return TestClient(app), trigger, got

    def test_signed_request_is_accepted(self, client_and_trigger):
        client, trigger, got = client_and_trigger
        response = client.post(
            trigger.endpoint_path, content=BODY, headers={"X-Webhook-Token": sign()}
        )
        assert response.status_code == 200 and response.json() == {"accepted": True}
        assert got[0].workflow_params["webhook_body"] == BODY

    def test_unsigned_request_is_rejected(self, client_and_trigger):
        client, trigger, got = client_and_trigger
        assert client.post(trigger.endpoint_path, content=BODY).status_code == 401
        assert got == []


TS_HEADER = "x-webhook-timestamp"
NOW = 1_700_000_000.0


def sign_ts(timestamp, body=BODY, secret=SECRET):
    """Sender side: sign ``"<timestamp>." + body``."""
    message = str(timestamp).encode() + b"." + body
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


def headers_for(timestamp=NOW, body=BODY, **extra):
    return {
        "x-webhook-token": sign_ts(int(timestamp), body),
        TS_HEADER: str(int(timestamp)),
        **extra,
    }


def make_protected(**kw):
    t, got = make(secret_token=SECRET, timestamp_header=TS_HEADER, **kw)
    t._clock = lambda: NOW
    return t, got


class TestReplayConfiguration:
    def test_off_by_default(self):
        t = WebhookTrigger("wf", "/h", secret_token=SECRET)
        assert t.timestamp_header is None and t.tolerance_seconds == 300.0

    def test_needs_a_secret(self):
        with pytest.raises(ValueError, match="secret_token"):
            WebhookTrigger("wf", "/h", timestamp_header=TS_HEADER)

    @pytest.mark.parametrize("bad", ["", "  "])
    def test_header_name_must_not_be_blank(self, bad):
        with pytest.raises(ValueError, match="timestamp_header"):
            WebhookTrigger("wf", "/h", secret_token=SECRET, timestamp_header=bad)

    @pytest.mark.parametrize("bad", [0, -5])
    def test_tolerance_must_be_positive(self, bad):
        with pytest.raises(ValueError, match="tolerance_seconds"):
            WebhookTrigger("wf", "/h", secret_token=SECRET, tolerance_seconds=bad)

    def test_header_name_is_lowercased(self):
        t = WebhookTrigger("wf", "/h", secret_token=SECRET, timestamp_header="X-TS")
        assert t.timestamp_header == "x-ts"


@pytest.mark.asyncio
class TestReplayProtection:
    async def test_fresh_signed_request_is_accepted(self, store):
        t, got = make_protected()
        assert await t.handle_webhook(BODY, headers=headers_for())
        assert len(got) == 1

    async def test_the_same_request_cannot_be_replayed(self, store):
        t, got = make_protected()
        await t.handle_webhook(BODY, headers=headers_for())
        with pytest.raises(PermissionError, match="replay"):
            await t.handle_webhook(BODY, headers=headers_for())
        assert len(got) == 1 and t.fire_count == 1

    async def test_a_different_delivery_is_accepted(self, store):
        t, got = make_protected()
        await t.handle_webhook(BODY, headers=headers_for(NOW))
        await t.handle_webhook(BODY, headers=headers_for(NOW + 1))
        await t.handle_webhook(b"other", headers=headers_for(NOW, b"other"))
        assert len(got) == 3

    @pytest.mark.parametrize("offset", [-301, 301, -86400, 10**6])
    async def test_stale_or_future_timestamps_are_refused(self, store, offset):
        t, got = make_protected()
        with pytest.raises(PermissionError, match="tolerance"):
            await t.handle_webhook(BODY, headers=headers_for(NOW + offset))
        assert got == []

    @pytest.mark.parametrize("offset", [-300, 300, -299, 0, 120])
    async def test_timestamps_inside_the_window_are_accepted(self, store, offset):
        t, got = make_protected()
        await t.handle_webhook(BODY, headers=headers_for(NOW + offset))
        assert len(got) == 1

    async def test_custom_tolerance(self, store):
        t, got = make_protected(tolerance_seconds=10)
        with pytest.raises(PermissionError, match="tolerance"):
            await t.handle_webhook(BODY, headers=headers_for(NOW - 11))
        await t.handle_webhook(BODY, headers=headers_for(NOW - 9))
        assert len(got) == 1

    @pytest.mark.parametrize("raw", [None, "", "yesterday", "nan", "inf", "-inf"])
    async def test_missing_or_invalid_timestamp_is_refused(self, store, raw):
        t, got = make_protected()
        headers = {"x-webhook-token": sign_ts(int(NOW))}
        if raw is not None:
            headers[TS_HEADER] = raw
        with pytest.raises(PermissionError, match="timestamp"):
            await t.handle_webhook(BODY, headers=headers)
        assert got == []

    async def test_refreshing_the_timestamp_of_a_captured_request_fails(self, store):
        # the attacker keeps the old signature but sends a current timestamp
        t, got = make_protected()
        old = headers_for(NOW - 3600)
        forged = {**old, TS_HEADER: str(int(NOW))}
        with pytest.raises(PermissionError, match="signature"):
            await t.handle_webhook(BODY, headers=forged)
        assert got == []

    async def test_a_signature_over_the_body_alone_is_not_enough(self, store):
        t, got = make_protected()
        headers = {"x-webhook-token": sign(), TS_HEADER: str(int(NOW))}
        with pytest.raises(PermissionError, match="signature"):
            await t.handle_webhook(BODY, headers=headers)
        assert got == []

    async def test_a_rejected_request_does_not_use_up_the_signature(self, store):
        t, got = make_protected()
        bad = {**headers_for(), "x-webhook-token": "0" * 64}
        with pytest.raises(PermissionError):
            await t.handle_webhook(BODY, headers=bad)
        await t.handle_webhook(BODY, headers=headers_for())
        assert len(got) == 1

    async def test_a_replay_after_the_window_is_refused_as_stale(self, store):
        t, got = make_protected(tolerance_seconds=60)
        await t.handle_webhook(BODY, headers=headers_for(NOW))
        t._clock = lambda: NOW + 61  # the remembered entry has expired too
        with pytest.raises(PermissionError, match="tolerance"):
            await t.handle_webhook(BODY, headers=headers_for(NOW))
        assert len(got) == 1

    async def test_expired_entries_are_forgotten(self, store):
        t, _ = make_protected(tolerance_seconds=60)
        for i in range(5):
            await t.handle_webhook(BODY, headers=headers_for(NOW + i))
        t._clock = lambda: NOW + 1000
        await t.handle_webhook(BODY, headers=headers_for(NOW + 1000))
        assert len(t._seen_signatures) == 1

    async def test_memory_is_bounded(self, store, monkeypatch):
        monkeypatch.setattr(WebhookTrigger, "_MAX_SEEN_SIGNATURES", 5)
        t, _ = make_protected()
        for i in range(20):
            await t.handle_webhook(BODY, headers=headers_for(NOW + (i % 100)))
        assert len(t._seen_signatures) <= 5

    async def test_disabled_trigger_still_checks_authenticity(self, store):
        t, got = make_protected(enabled=False)
        with pytest.raises(PermissionError):
            await t.handle_webhook(BODY, headers=headers_for(NOW - 10**6))
        assert await t.handle_webhook(BODY, headers=headers_for()) is False
        assert got == []

    async def test_works_without_replay_protection_for_plain_signing(self, store):
        t, got = make(secret_token=SECRET)  # unchanged behaviour
        await t.handle_webhook(BODY, headers={"x-webhook-token": sign()})
        await t.handle_webhook(BODY, headers={"x-webhook-token": sign()})
        assert len(got) == 2

    async def test_real_clock_is_used_by_default(self, store):
        import time

        t, got = make(secret_token=SECRET, timestamp_header=TS_HEADER)
        stamp = int(time.time())
        await t.handle_webhook(BODY, headers=headers_for(stamp))
        assert len(got) == 1
