"""Shared fixtures for the governance API tests: a serialising in-memory backend bound
to every model, a TestClient, two organisations with users, and token helpers."""

import asyncio
import os

import pytest
from fastapi.testclient import TestClient

import volnux.models as models_pkg
from tests.unittests.support import SerializingMemoryBackend

SECRET = "x" * 48


def run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


@pytest.fixture
def backend(monkeypatch):
    b = SerializingMemoryBackend()
    for name in models_pkg.__all__:
        cls = getattr(models_pkg, name)
        monkeypatch.setattr(cls, "_backend_store", b, raising=False)
        monkeypatch.setattr(cls, "_backend_initialized", True, raising=False)
    yield b
    b.close()


@pytest.fixture
def client(backend, monkeypatch):
    monkeypatch.setenv("VOLNUX_SECRET_KEY", SECRET)
    import volnux.app.v1  # noqa: F401  (registers the routes)
    import volnux.app.exception_handlers  # noqa: F401
    from volnux.app.app import get_current_app

    with TestClient(
        get_current_app(), raise_server_exceptions=bool(os.environ.get("TEST_RAISE"))
    ) as c:
        yield c


class World:
    """Two organisations with users in every role, seeded in the backend."""

    PASSWORD = "correct-horse-battery"

    def __init__(self):
        self.orgs = {}
        self.roles = {}
        self.users = {}

    def seed(self, name, org_slug, role_slugs):
        from volnux.app.security import hash_password
        from volnux.models import Organization, Role, RoleAssignment, User

        async def go():
            org = self.orgs.get(org_slug)
            if org is None:
                org = Organization(name=org_slug, slug=org_slug)
                await org.save()
                self.orgs[org_slug] = org
            user = User(
                name=name,
                email=f"{name}@{org_slug}.io",
                organization=org,
                password_hash=hash_password(self.PASSWORD),
            )
            await user.save()
            for slug in role_slugs:
                role = self.roles.get(slug)
                if role is None:
                    role = Role(name=slug, slug=slug)
                    await role.save()
                    self.roles[slug] = role
                await RoleAssignment(
                    user=user, role=role, team=None, granted_by=user, revoked_at=None
                ).save()
            return user

        user = run(go())
        self.users[name] = user
        return user

    def headers(self, user):
        from volnux.app.utils import create_jwt_token

        token = create_jwt_token(user.id, user.organization.id, [])
        return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def world(backend, monkeypatch):
    monkeypatch.setenv("VOLNUX_SECRET_KEY", SECRET)
    return World()
