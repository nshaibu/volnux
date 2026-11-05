"""Login tokens: the key is the SECRET_KEY setting, required unless DEBUG, never in the source."""

import asyncio
from datetime import datetime, timezone

import jwt
import pytest
from fastapi import HTTPException

import volnux.app.utils as utils
from volnux.exceptions import ImproperlyConfigured

OLD_HARDCODED_SECRET = "volnux-jwt-secret"


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    for var in (
        "VOLNUX_SECRET_KEY",
        "VOLNUX_DEBUG",
        "VOLNUX_JWT_ALGORITHM",
        "VOLNUX_JWT_EXPIRATION_HOURS",
    ):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(utils, "_development_secret", None)


class TestProduction:
    def test_no_secret_is_an_error_and_says_how_to_fix_it(self):
        with pytest.raises(ImproperlyConfigured, match="VOLNUX_SECRET_KEY"):
            utils.get_jwt_secret()

    def test_production_is_the_default(self):
        # DEBUG is off unless something turns it on
        with pytest.raises(ImproperlyConfigured):
            utils.get_jwt_secret()

    def test_debug_false_is_production(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_DEBUG", "false")
        with pytest.raises(ImproperlyConfigured):
            utils.get_jwt_secret()

    def test_tokens_cannot_be_made_without_a_secret(self):
        with pytest.raises(ImproperlyConfigured):
            utils.create_jwt_token("u", "o", ["admin"])

    def test_the_api_refuses_to_start(self):
        from fastapi import FastAPI

        from volnux.app.app import lifespan

        async def start():
            async with lifespan(FastAPI()):
                pass

        with pytest.raises(ImproperlyConfigured):
            asyncio.run(start())

    def test_the_api_starts_with_a_secret(self, monkeypatch):
        from fastapi import FastAPI

        from volnux.app.app import lifespan

        monkeypatch.setenv("VOLNUX_SECRET_KEY", "s" * 40)
        app = FastAPI()

        async def start():
            async with lifespan(app):
                return app.state.start_time

        assert asyncio.run(start()) > 0

    def test_a_configured_secret_round_trips(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "k" * 40)
        token = utils.create_jwt_token("u1", "o1", ["admin"])
        payload = utils.decode_jwt_token(token)
        assert (payload["sub"], payload["org"], payload["roles"]) == (
            "u1",
            "o1",
            ["admin"],
        )

    def test_a_token_from_another_key_is_rejected(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "a" * 40)
        token = utils.create_jwt_token("u1", "o1", [])
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "b" * 40)
        with pytest.raises(HTTPException) as exc:
            utils.decode_jwt_token(token)
        assert exc.value.status_code == 401


class TestTheOldHardcodedSecret:
    def test_it_is_gone_from_the_source(self):
        source = open(utils.__file__).read()
        assert OLD_HARDCODED_SECRET not in source

    def test_a_token_forged_with_it_is_rejected(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "real-secret-" + "x" * 30)
        forged = jwt.encode(
            {
                "sub": "attacker",
                "org": "o",
                "roles": ["admin"],
                "exp": datetime.now(timezone.utc).timestamp() + 3600,
            },
            OLD_HARDCODED_SECRET,
            algorithm="HS256",
        )
        with pytest.raises(HTTPException) as exc:
            utils.decode_jwt_token(forged)
        assert exc.value.status_code == 401

    def test_nor_in_development(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_DEBUG", "true")
        forged = jwt.encode(
            {"sub": "x", "org": "o", "roles": [], "exp": 9999999999},
            OLD_HARDCODED_SECRET,
            algorithm="HS256",
        )
        with pytest.raises(HTTPException):
            utils.decode_jwt_token(forged)


class TestDevelopment:
    @pytest.fixture(autouse=True)
    def development(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_DEBUG", "true")

    def test_a_random_secret_is_made_and_kept_for_the_process(self, caplog):
        first = utils.get_jwt_secret()
        assert len(first) >= 32 and utils.get_jwt_secret() == first

    def test_it_differs_between_processes(self, monkeypatch):
        first = utils.get_jwt_secret()
        monkeypatch.setattr(utils, "_development_secret", None)
        assert utils.get_jwt_secret() != first

    def test_tokens_work(self):
        token = utils.create_jwt_token("u", "o", ["viewer"])
        assert utils.decode_jwt_token(token)["roles"] == ["viewer"]

    def test_it_is_announced_once(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger=utils.logger.name):
            utils.get_jwt_secret()
            utils.get_jwt_secret()
        assert caplog.text.count("SECRET_KEY is not set") == 1

    def test_a_configured_secret_still_wins(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "configured-" + "x" * 30)
        assert utils.get_jwt_secret().startswith("configured-")


class TestAlgorithmAndExpiry:
    def setup_method(self):
        pass

    @pytest.fixture(autouse=True)
    def secret(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_SECRET_KEY", "k" * 40)

    def test_defaults(self):
        assert utils.get_jwt_algorithm() == "HS256"
        assert utils.get_jwt_expiration_hours() == 24

    def test_expiry_follows_the_setting(self, monkeypatch):
        monkeypatch.setenv("VOLNUX_JWT_EXPIRATION_HOURS", "2")
        payload = jwt.decode(
            utils.create_jwt_token("u", "o", []),
            options={"verify_signature": False},
        )
        assert payload["exp"] - payload["iat"] == 2 * 3600

    @pytest.mark.parametrize("algorithm", ["none", "NONE", "RS256", "ES256", ""])
    def test_only_symmetric_algorithms_are_allowed(self, monkeypatch, algorithm):
        # "none" would accept unsigned tokens; an asymmetric one needs a key pair
        monkeypatch.setenv("VOLNUX_JWT_ALGORITHM", algorithm)
        with pytest.raises(ImproperlyConfigured):
            utils.get_jwt_algorithm()

    @pytest.mark.parametrize("algorithm", ["hs256", "HS384", "HS512"])
    def test_hmac_algorithms_work(self, monkeypatch, algorithm):
        monkeypatch.setenv("VOLNUX_JWT_ALGORITHM", algorithm)
        token = utils.create_jwt_token("u", "o", [])
        assert utils.decode_jwt_token(token)["sub"] == "u"

    def test_an_unsigned_token_is_rejected(self):
        unsigned = jwt.encode(
            {"sub": "x", "org": "o", "roles": [], "exp": 9999999999},
            key=None,
            algorithm="none",
        )
        with pytest.raises(HTTPException):
            utils.decode_jwt_token(unsigned)
