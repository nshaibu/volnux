"""The shared pieces: permission dependencies, the audit chain, serialisation, wiring."""

import asyncio
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from tests.unittests.app.conftest import run
from volnux.app import utils
from volnux.app.dependencies import (
    REDACTED,
    _serialize_model,
    get_org_id,
    page_args,
)
from volnux.app.enums import Permission
from volnux.app.security import hash_password, verify_password
from volnux.models.enums import AuditEventType


def ctx(*roles):
    return utils.RequestContext(
        user=SimpleNamespace(id="u"),
        organization=SimpleNamespace(id="o"),
        roles=list(roles),
        client_type="cli",
    )


class TestPermissionDependencies:
    """These used to call ``user.get(...)`` on the RequestContext dataclass."""

    def test_require_permission(self):
        check = utils.require_permission(Permission.WORKFLOW_CREATE)
        asyncio.run(check(ctx("workflow-author")))
        asyncio.run(check(ctx("super-admin")))
        with pytest.raises(HTTPException) as e:
            asyncio.run(check(ctx("workflow-consumer")))
        assert e.value.status_code == 403
        with pytest.raises(HTTPException):
            asyncio.run(check(ctx()))

    def test_require_any_permission(self):
        check = utils.require_any_permission(
            Permission.WORKFLOW_CREATE, Permission.AUDIT_READ
        )
        asyncio.run(check(ctx("governance-auditor")))
        with pytest.raises(HTTPException):
            asyncio.run(check(ctx("workflow-consumer")))

    def test_require_role(self):
        check = utils.require_role("org-admin")
        asyncio.run(check(ctx("org-admin")))
        asyncio.run(check(ctx("super-admin")))
        with pytest.raises(HTTPException):
            asyncio.run(check(ctx("workflow-author")))

    def test_org_id_comes_from_the_context(self):
        assert get_org_id(ctx()) == "o"

    def test_paging(self):
        assert page_args({"page": 3, "page_size": 10}) == {"limit": 10, "offset": 20}


class TestAuditChain:
    def seed(self, world):
        user = world.seed("a", "acme", [])
        return user, user.organization

    def test_entries_are_chained_and_verifiable(self, world):
        user, org = self.seed(world)

        async def go():
            first = await utils.audit_log(
                org, AuditEventType.ROLE_ASSIGNED, user, "r", "user", "1", "one"
            )
            second = await utils.audit_log(
                org, AuditEventType.ROLE_ASSIGNED, user, "r", "user", "2", "two"
            )
            assert first.previous_hash is None and first.hash
            assert second.previous_hash == first.hash
            assert await utils.verify_audit_chain(org.id)
            return first

        run(go())

    def test_tampering_breaks_the_chain(self, world):
        user, org = self.seed(world)
        from volnux.models import AuditEntry

        async def go():
            for n in range(3):
                await utils.audit_log(
                    org, AuditEventType.ROLE_ASSIGNED, user, "r", "user", str(n), "x"
                )
            assert await utils.verify_audit_chain(org.id)
            entries = await AuditEntry.filter_async(
                organization__object_id=org.id, order_by="created_at"
            )
            entries[1].action = "rewritten"
            # (the model refuses this; write around it as a database admin could)
            from volnux.models.base import GovernanceModel

            await GovernanceModel.update(entries[1])
            assert not await utils.verify_audit_chain(org.id)

        run(go())

    def test_chains_are_per_organization(self, world):
        a = world.seed("a", "acme", [])
        b = world.seed("b", "globex", [])

        async def go():
            await utils.audit_log(
                a.organization, AuditEventType.ROLE_ASSIGNED, a, "r", "t", "1", "x"
            )
            entry = await utils.audit_log(
                b.organization, AuditEventType.ROLE_ASSIGNED, b, "r", "t", "1", "x"
            )
            assert entry.previous_hash is None

        run(go())


class TestSerialisation:
    def test_foreign_keys_become_ids_and_secrets_are_hidden(self, world):
        user = world.seed("a", "acme", [])
        data = _serialize_model(user)
        assert data["organization"] == user.organization.id
        assert "password_hash" not in data
        assert "_backend_class" not in data

    def test_secret_looking_config_values_are_masked(self):
        class Fake:
            def __getstate__(self):
                return {"config": {"api_key": "k", "nested": {"token": "t", "ok": 1}}}

        out = _serialize_model(Fake())
        assert out["config"]["api_key"] == REDACTED
        assert out["config"]["nested"] == {"token": REDACTED, "ok": 1}


class TestPasswords:
    def test_hash_and_verify(self):
        h = hash_password("a long password")
        assert verify_password("a long password", h)
        assert not verify_password("another", h)
        assert h != hash_password("a long password")  # salted

    @pytest.mark.parametrize("stored", [None, "", "plain", "scrypt$x", "md5$a$b"])
    def test_bad_hashes_never_verify(self, stored):
        assert not verify_password("x", stored)


class TestWiring:
    def test_routes_and_handlers_register_on_import_of_the_package(self):
        import volnux.app  # noqa: F401
        from volnux.app.app import get_current_app
        from volnux.exceptions import ObjectDoesNotExist

        app = get_current_app()
        assert ObjectDoesNotExist in app.exception_handlers
        paths = {r.path for r in app.routes}
        assert "/api/v1/workflows" in paths and "/api/v1/audit/verify" in paths

    def test_openapi_builds_and_request_schemas_are_not_patched_as_models(self):
        import volnux.app  # noqa: F401
        from volnux.app.app import get_current_app

        app = get_current_app()
        app.openapi_schema = None
        schema = app.openapi()
        # UserCreate merely contains "User": it must not get the User model's constraints
        assert "UserCreate" in schema["components"]["schemas"]
        assert "password" in schema["components"]["schemas"]["UserCreate"]["properties"]

    def test_cors_never_combines_a_wildcard_with_credentials(self):
        from volnux.app.app import get_current_app

        cors = next(
            m
            for m in get_current_app().user_middleware
            if m.cls.__name__ == "CORSMiddleware"
        )
        origins = cors.kwargs["allow_origins"]
        assert not ("*" in origins and cors.kwargs["allow_credentials"])


class TestStartup:
    def test_a_missing_user_is_a_401_not_a_500(self, client, world):
        from volnux.app.utils import create_jwt_token

        token = create_jwt_token("nobody", "nowhere", [])
        r = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 401

    def test_token_for_the_wrong_organization_is_refused(self, client, world):
        user = world.seed("a", "acme", [])
        other = world.seed("b", "globex", [])
        from volnux.app.utils import create_jwt_token

        token = create_jwt_token(user.id, other.organization.id, [])
        r = client.get("/api/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert r.status_code == 401

    def test_deactivated_user_and_organization(self, client, world):
        user = world.seed("a", "acme", [])
        h = world.headers(user)
        assert client.get("/api/v1/auth/me", headers=h).status_code == 200

        async def off(model):
            model.is_active = False
            await model.save()

        run(off(user))
        assert client.get("/api/v1/auth/me", headers=h).status_code == 403

    def test_unknown_ids_are_404(self, client, world):
        user = world.seed("a", "acme", ["org-admin"])
        h = world.headers(user)
        assert client.get("/api/v1/users/doesnotexist", headers=h).status_code == 404
