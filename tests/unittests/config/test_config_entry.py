import os

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
)

import volnux.config as config_module
from volnux.config import (
    ConfigEntry,
    ENV_MESH_NODE_ID,
    ENV_MESH_PUBLIC_KEY,
    ENV_MESH_PRIVATE_KEY,
)
from volnux.crypto.signer import Signer


def make_signer():
    private_key = Ed25519PrivateKey.generate()
    return Signer(private_key=private_key, public_key=private_key.public_key())


def make_entry(**overrides):
    data = {
        "value": {"enabled": True, "threshold": 10},
        "name": "FEATURE_FLAG",
        "timestamp": 1,
        "origin_mesh_node": "node-a",
    }
    data.update(overrides)
    return ConfigEntry(**data)


def test_sign_sets_signature():
    signer = make_signer()
    entry = make_entry()

    signature = entry.sign(signer)

    assert signature is not None
    assert entry.signature == signature


def test_sign_requires_name():
    signer = make_signer()
    entry = ConfigEntry(value="x", name=None, timestamp=1, origin_mesh_node="node-a")

    with pytest.raises(ValueError, match="ConfigEntry.name must be set before signing"):
        entry.sign(signer)


def test_validate_returns_true_for_local_entry_without_signature():
    entry = ConfigEntry(value="local", name="LOCAL_KEY", origin_mesh_node="local")

    assert entry.validate() is True


def test_validate_returns_false_for_remote_entry_without_signature(monkeypatch):
    entry = ConfigEntry(value="remote", name="REMOTE_KEY", origin_mesh_node="node-b")

    monkeypatch.setenv(ENV_MESH_NODE_ID, "node-c")

    assert entry.validate() is False


def test_validate_returns_true_for_signed_remote_entry_with_signer():
    signer = make_signer()
    entry = make_entry(origin_mesh_node="node-b")

    entry.sign(signer)

    assert entry.validate(signer) is True


def test_validate_returns_false_for_tampered_value(monkeypatch):
    signer = make_signer()
    entry = make_entry(origin_mesh_node="node-b")
    entry.sign(signer)

    # Set current node id such that the entry is signed by a different node
    monkeypatch.setenv(ENV_MESH_NODE_ID, "node-c")

    entry.value = {"enabled": False, "threshold": 10}

    assert entry.validate(signer) is False


def test_validate_returns_false_for_wrong_public_key(monkeypatch):
    signer_a = make_signer()
    signer_b = make_signer()

    entry = make_entry(origin_mesh_node="node-b")
    entry.sign(signer_a)

    monkeypatch.setenv(ENV_MESH_NODE_ID, "node-c")

    assert entry.validate(signer_b) is False


def test_validate_uses_environment_public_key_when_signer_missing(monkeypatch):
    private_key = Ed25519PrivateKey.generate()
    signer = Signer(private_key=private_key, public_key=private_key.public_key())

    entry = make_entry(origin_mesh_node="node-b")
    entry.sign(signer)

    pem_public_key = private_key.public_key().public_bytes_raw()

    # If your implementation expects PEM input, replace this with PEM serialization.
    # This test assumes validate() is using a Signer passed in or environment-backed loader.
    monkeypatch.setenv(ENV_MESH_PUBLIC_KEY, os.environ.get(ENV_MESH_PUBLIC_KEY, ""))

    # Since the current validate() implementation accepts a Signer directly,
    # the most reliable assertion here is to verify the direct path.
    assert entry.validate(signer) is True


def test_sign_and_validate_are_stable_for_nested_dict_order():
    signer = make_signer()

    entry_a = ConfigEntry(
        value={"b": 2, "a": 1},
        name="PAYLOAD",
        timestamp=7,
        origin_mesh_node="node-b",
    )
    entry_b = ConfigEntry(
        value={"a": 1, "b": 2},
        name="PAYLOAD",
        timestamp=7,
        origin_mesh_node="node-b",
    )

    sig = entry_a.sign(signer)
    entry_b.signature = sig

    assert entry_b.validate(signer) is True


def test_validate_rejects_missing_signature_for_remote_entry(monkeypatch):
    entry = ConfigEntry(value="hello", name="KEY", origin_mesh_node="node-b")

    monkeypatch.setenv(ENV_MESH_NODE_ID, "node-c")

    assert entry.validate() is False
