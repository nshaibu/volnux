"""`volnux createsuperuser`: the first account of a deployment."""

import asyncio
import io

import pytest
from fastapi.testclient import TestClient

import volnux.models as models_pkg
from tests.unittests.support import SerializingMemoryBackend
from volnux.cli.command.builtins.createsuperuser import CreateSuperuserCommand
from volnux.cli.command.base import get_command_registry
from volnux.exceptions import CommandError
from volnux.models import AuditEntry, Organization, Role, RoleAssignment, User

SECRET = "s" * 48


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.setenv("VOLNUX_SECRET_KEY", SECRET)
    b = SerializingMemoryBackend()
    for name in models_pkg.__all__:
        cls = getattr(models_pkg, name)
        monkeypatch.setattr(cls, "_backend_store", b, raising=False)
        monkeypatch.setattr(cls, "_backend_initialized", True, raising=False)
    yield b
    b.close()


@pytest.fixture
def command(backend, monkeypatch, tmp_path):
    cmd = CreateSuperuserCommand()
    monkeypatch.setattr(cmd, "resolve_project_dir", lambda: tmp_path)
    monkeypatch.setattr(cmd, "initialise_workflows", lambda *a, **k: None)
    return cmd


def opts(**kw):
    base = dict(
        email="ada@acme.io",
        name="Ada Admin",
        org_slug="acme",
        org_name=None,
        password_env="ADMIN_PASSWORD",
        password_stdin=False,
    )
    base.update(kw)
    return base


class TestCommand:
    def test_it_is_registered(self):
        assert (
            get_command_registry().get_by_name("createsuperuser")
            is CreateSuperuserCommand
        )

    def test_creates_everything_and_the_account_can_log_in(self, command, monkeypatch):
        monkeypatch.setenv("ADMIN_PASSWORD", "a-long-password")
        message = command.handle(**opts())
        assert "ada@acme.io" in message

        user = run(User.filter_async(email="ada@acme.io"))[0]
        assert user.password_hash and "a-long-password" not in user.password_hash
        org = run(Organization.filter_async(slug="acme"))[0]
        assert org.name == "acme"
        role = run(Role.filter_async(slug="super-admin"))[0]
        assignment = run(RoleAssignment.filter_async(user__object_id=user.id))[0]
        assert assignment.role.id == role.id and assignment.revoked_at is None
        assert run(AuditEntry.verify_chain(org.id))

        import volnux.app  # noqa: F401
        from volnux.app.app import get_current_app

        with TestClient(get_current_app()) as client:
            r = client.post(
                "/api/v1/auth/login",
                data={"email": "ada@acme.io", "password": "a-long-password"},
            )
            assert r.status_code == 200, r.text
            assert r.json()["data"]["roles"] == ["super-admin"]
            bad = client.post(
                "/api/v1/auth/login",
                data={"email": "ada@acme.io", "password": "wrong-password"},
            )
            assert bad.status_code == 401

    def test_org_name_and_an_existing_organization_and_role_are_reused(
        self, command, monkeypatch
    ):
        monkeypatch.setenv("ADMIN_PASSWORD", "a-long-password")
        command.handle(**opts(org_name="Acme Inc"))
        command.handle(**opts(email="bob@acme.io", name="Bob"))
        assert len(run(Organization.filter_async())) == 1
        assert run(Organization.filter_async())[0].name == "Acme Inc"
        assert len(run(Role.filter_async(slug="super-admin"))) == 1
        assert len(run(User.filter_async())) == 2

    def test_a_duplicate_email_is_refused(self, command, monkeypatch):
        monkeypatch.setenv("ADMIN_PASSWORD", "a-long-password")
        command.handle(**opts())
        with pytest.raises(CommandError, match="already exists"):
            command.handle(**opts())
        assert len(run(User.filter_async())) == 1

    @pytest.mark.parametrize(
        "kw, match",
        [
            ({"email": "not-an-email"}, "email"),
            ({"org_slug": "Bad Slug"}, "slug"),
            ({"name": "  "}, "name"),
        ],
    )
    def test_bad_input_is_refused(self, command, monkeypatch, kw, match):
        monkeypatch.setenv("ADMIN_PASSWORD", "a-long-password")
        with pytest.raises(CommandError, match=match):
            command.handle(**opts(**kw))
        assert run(User.filter_async()) == []

    def test_a_short_password_is_refused(self, command, monkeypatch):
        monkeypatch.setenv("ADMIN_PASSWORD", "short")
        with pytest.raises(CommandError, match="at least 8"):
            command.handle(**opts())
        assert run(User.filter_async()) == []


class TestPasswordSources:
    def test_stdin(self, command, monkeypatch):
        monkeypatch.setattr("sys.stdin", io.StringIO("from-stdin-password\nignored\n"))
        command.handle(**opts(password_env=None, password_stdin=True))
        from volnux.app.security import verify_password

        user = run(User.filter_async())[0]
        assert verify_password("from-stdin-password", user.password_hash)

    def test_prompt_asks_twice(self, command, monkeypatch):
        answers = iter(["prompted-password", "prompted-password"])
        monkeypatch.setattr("getpass.getpass", lambda prompt="": next(answers))
        command.handle(**opts(password_env=None))
        assert len(run(User.filter_async())) == 1

    def test_prompt_mismatch(self, command, monkeypatch):
        answers = iter(["one-password-1", "another-password"])
        monkeypatch.setattr("getpass.getpass", lambda prompt="": next(answers))
        with pytest.raises(CommandError, match="do not match"):
            command.handle(**opts(password_env=None))
        assert run(User.filter_async()) == []

    def test_unset_environment_variable(self, command, monkeypatch):
        monkeypatch.delenv("ADMIN_PASSWORD", raising=False)
        with pytest.raises(CommandError, match="ADMIN_PASSWORD"):
            command.handle(**opts())

    def test_the_password_is_never_an_argument(self):
        import argparse

        parser = argparse.ArgumentParser()
        CreateSuperuserCommand().add_arguments(parser)
        with pytest.raises(SystemExit):
            parser.parse_args(
                [
                    "--email",
                    "a@b.io",
                    "--name",
                    "n",
                    "--org-slug",
                    "o",
                    "--password",
                    "x",
                ]
            )
