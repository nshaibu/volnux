import asyncio

import pytest

import volnux.models as models_pkg
from tests.unittests.support import SerializingMemoryBackend


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.setenv("VOLNUX_SECRET_KEY", "k" * 48)
    b = SerializingMemoryBackend()
    for name in models_pkg.__all__:
        cls = getattr(models_pkg, name)
        monkeypatch.setattr(cls, "_backend_store", b, raising=False)
        monkeypatch.setattr(cls, "_backend_initialized", True, raising=False)
    yield b
    b.close()


@pytest.fixture
def base(backend):
    """An organization, a second one, a user, a team and a workflow."""
    from volnux.models import Organization, Team, User, Workflow

    class B:
        pass

    async def go():
        b = B()
        b.org = Organization(name="o", slug="o")
        await b.org.save()
        b.org2 = Organization(name="o2", slug="o2")
        await b.org2.save()
        b.user = User(name="u", email="u@o.io", organization=b.org)
        await b.user.save()
        b.team = Team(name="t", slug="t", organization=b.org)
        await b.team.save()
        b.wf = Workflow(name="w", organization=b.org, team=b.team, created_by=b.user)
        await b.wf.save()
        return b

    return run(go())
