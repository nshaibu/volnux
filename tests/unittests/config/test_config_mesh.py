import asyncio
import logging
from unittest.mock import AsyncMock

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from volnux.config import ConfigEntry, ENV_MESH_NODE_ID, VolnuxConfig
from volnux.crypto.signer import Signer


def make_signer():
    key = Ed25519PrivateKey.generate()
    return Signer(private_key=key, public_key=key.public_key())


def remote(name="K", value="remote", ts=1, node="node-z", signer=None):
    entry = ConfigEntry(value=value, name=name, timestamp=ts, origin_mesh_node=node)
    if signer is not None:
        entry.sign(signer)
    return entry


class TestLamportClock:
    def test_a_local_write_advances_the_clock(self, isolated):
        cfg = isolated({})
        cfg.add("k", 1)
        cfg.add("k", 2)
        assert cfg._lamport_clock == 2
        assert cfg.get_config_entry("k").timestamp == 2

    def test_a_local_write_is_newer_than_what_was_received(self, isolated):
        cfg = isolated({})
        cfg.update_from_mesh(remote("K", ts=10, node="node-a"))
        cfg.add("k", "mine")
        assert cfg.get_config_entry("k").timestamp > 10

    def test_a_later_local_write_beats_an_older_remote_one(self, isolated):
        cfg = isolated({})
        cfg.add("k", "mine")  # ts 1
        cfg.update_from_mesh(remote("K", "theirs", ts=1, node="zzz"))
        assert cfg.get("k") == "theirs"  # same instant: the greater node id wins
        cfg.add("k", "mine again")  # a later local write (ts 3: the clock moved on)
        cfg.update_from_mesh(remote("K", "stale", ts=1, node="zzz"))
        assert cfg.get("k") == "mine again"

    def test_explicit_zero_timestamp_is_kept(self, isolated):
        cfg = isolated({})
        cfg.add("k", 1, timestamp=0)
        assert cfg.get_config_entry("k").timestamp == 0

    def test_receiving_moves_the_clock_past_the_entry(self, isolated):
        cfg = isolated({})
        cfg.update_from_mesh(remote(ts=40))
        assert cfg._lamport_clock == 41

    def test_a_rejected_stale_entry_still_moves_the_clock(self, isolated):
        cfg = isolated({})
        cfg.update_from_mesh(remote(ts=50, node="b"))
        before = cfg._lamport_clock
        assert cfg.update_from_mesh(remote(ts=2, node="b")) is False
        assert cfg._lamport_clock > before

    def test_defaults_and_the_environment_keep_timestamp_zero(
        self, isolated, monkeypatch
    ):
        monkeypatch.setenv("VOLNUX_FROM_ENV", "x")
        cfg = isolated({"A": 1})
        assert cfg.get_config_entry("A").timestamp == 0
        assert cfg.get_config_entry("FROM_ENV").timestamp == 0
        assert cfg._lamport_clock == 0


class TestRemoteEntriesTakeTheMeshPath:
    def test_a_new_remote_entry_is_accepted_when_nothing_is_verified(self, isolated):
        cfg = isolated({})
        cfg.add_entry_config(remote("K", "v", node="other-node"))
        assert cfg.get("k") == "v"

    def test_a_stale_remote_entry_is_ignored(self, isolated):
        cfg = isolated({})
        cfg.update_from_mesh(remote("K", "new", ts=5, node="b"))
        cfg.add_entry_config(remote("K", "old", ts=1, node="b"))
        assert cfg.get("k") == "new"

    def test_into_a_namespace(self, isolated):
        cfg = isolated({})
        assert cfg.update_from_mesh(remote("K", "v"), namespace="ns")
        assert cfg.get("k", namespace="ns") == "v"
        assert cfg.get("k", None) is None
        assert not cfg.update_from_mesh(remote("K", "older", ts=0), namespace="ns")

    @pytest.mark.asyncio
    async def test_async_form(self, isolated):
        cfg = isolated({})
        assert await cfg.update_from_mesh_async(remote("K", "v"), namespace="ns")
        assert cfg.get("k", namespace="ns") == "v"

    @pytest.mark.parametrize(
        "entry",
        [
            ConfigEntry(value=1, name=None, timestamp=1, origin_mesh_node="b"),
            ConfigEntry(value=1, name="  ", timestamp=1, origin_mesh_node="b"),
            ConfigEntry(value=1, name="K", timestamp="1", origin_mesh_node="b"),
            ConfigEntry(value=1, name="K", timestamp=True, origin_mesh_node="b"),
        ],
    )
    def test_malformed_entries_are_rejected(self, isolated, entry):
        cfg = isolated({})
        before = len(cfg._store)
        assert cfg.update_from_mesh(entry) is False
        assert len(cfg._store) == before


class TestSignedUpdates:
    def configured(self, isolated):
        signer = make_signer()
        return isolated({}, signer=signer), signer

    def test_a_correctly_signed_entry_is_applied(self, isolated):
        cfg, signer = self.configured(isolated)
        assert cfg.update_from_mesh(remote("K", "v", signer=signer))
        assert cfg.get("k") == "v"

    def test_an_unsigned_entry_is_refused_once_verification_is_configured(
        self, isolated, caplog
    ):
        cfg, _ = self.configured(isolated)
        with caplog.at_level(logging.WARNING, logger="volnux.config"):
            assert cfg.update_from_mesh(remote("K", "v")) is False
        assert cfg.get("k", None) is None
        assert "Rejected config update" in caplog.text

    def test_a_tampered_entry_is_refused(self, isolated):
        cfg, signer = self.configured(isolated)
        entry = remote("K", "v", signer=signer)
        entry.value = "evil"
        assert cfg.update_from_mesh(entry) is False

    def test_a_different_key_is_refused(self, isolated):
        cfg, _ = self.configured(isolated)
        assert cfg.update_from_mesh(remote("K", "v", signer=make_signer())) is False

    @pytest.mark.parametrize("origin", ["local", "self"])
    def test_claiming_to_be_local_or_this_node_is_no_shortcut(self, isolated, origin):
        # validate() trusts is_local(); a peer can simply say it is "local" or
        # use this node's own id, so the mesh path never relies on it
        cfg, _ = self.configured(isolated)
        node = cfg.get_node_id() if origin == "self" else origin
        assert cfg.update_from_mesh(remote("K", "forged", node=node)) is False
        assert cfg.get("k", None) is None

    def test_through_add_entry_config_too(self, isolated):
        cfg, _ = self.configured(isolated)
        cfg.add_entry_config(remote("K", "forged"))
        assert cfg.get("k", None) is None

    def test_local_writes_are_signed_and_unaffected(self, isolated):
        cfg, signer = self.configured(isolated)
        cfg.add("mine", 1)
        entry = cfg.get_config_entry("mine")
        assert entry.signature and entry.verify_signature(signer)

    def test_verification_by_the_public_key_in_the_environment(
        self, isolated, monkeypatch
    ):
        from cryptography.hazmat.primitives import serialization

        key = Ed25519PrivateKey.generate()
        signer = Signer(private_key=key, public_key=key.public_key())
        pem = key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        cfg = isolated({})
        assert cfg.update_from_mesh(remote("K", "open")) is True  # nothing configured
        monkeypatch.setenv("VOLNUX_MESH_PUBLIC_KEY", pem.decode())
        assert cfg.update_from_mesh(remote("K2", "unsigned")) is False
        assert cfg.update_from_mesh(remote("K2", "signed", signer=signer))
        assert cfg.get("k2") == "signed"

    def test_verify_signature_has_no_local_shortcut(self):
        entry = ConfigEntry(value=1, name="K", origin_mesh_node="local")
        assert entry.validate() is True
        assert entry.verify_signature(make_signer()) is False

    def test_a_path_value_signs_and_verifies(self, isolated):
        from pathlib import Path

        cfg, signer = self.configured(isolated)
        entry = remote("K", Path("/a/b"), signer=signer)
        assert entry.verify_signature(signer)

    def test_make_mesh_entry_signs_with_the_signer_and_ticks(self, isolated):
        cfg, signer = self.configured(isolated)
        entry = cfg.make_mesh_entry("k", 1)
        assert entry.name == "K" and entry.timestamp == 1
        assert entry.origin_mesh_node == cfg.get_node_id()
        assert entry.verify_signature(signer)

    def test_make_mesh_entry_without_keys_is_unsigned(self, isolated):
        entry = isolated({}).make_mesh_entry("k", 1)
        assert entry.signature is None


class FakeMesh:
    """Just what MeshClient's config-gossip methods use."""

    def __init__(self, config):
        from volnux.manager.mesh.client import MeshClient

        self.config = config
        self._seen_gossip = {}
        self._seen_gossip_lock = asyncio.Lock()
        self._epidemic_send = AsyncMock()
        self.node_id = "mesh-node"
        self._relay = MeshClient._relay_config_gossip.__get__(self)
        self._broadcast = MeshClient.broadcast_config_change.__get__(self)


class TestMeshClientGossip:
    @pytest.mark.asyncio
    async def test_broadcast_reaches_another_node(self, isolated):
        sender = isolated({})
        sender_mesh = FakeMesh(sender)
        await sender_mesh._broadcast("limit", 25)
        (name, payload), _ = sender_mesh._epidemic_send.call_args
        assert name == "CONFIG_UPDATE" and payload["key"] == "LIMIT"

        receiver = isolated({})
        receiver_mesh = FakeMesh(receiver)
        await receiver_mesh._relay(payload["gossip_id"], payload)
        # this used to raise KeyError ("origin_mesh_node") in the relay task
        assert receiver.get("limit") == 25
        receiver_mesh._epidemic_send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_signed_end_to_end(self, isolated):
        signer = make_signer()
        sender_mesh = FakeMesh(isolated({}, signer=signer))
        await sender_mesh._broadcast("limit", 7)
        payload = sender_mesh._epidemic_send.call_args[0][1]
        assert payload["signature"]

        receiver = isolated({}, signer=signer)
        await FakeMesh(receiver)._relay(payload["gossip_id"], payload)
        assert receiver.get("limit") == 7

    @pytest.mark.asyncio
    async def test_a_forged_update_is_neither_applied_nor_relayed(self, isolated):
        receiver = isolated({}, signer=make_signer())
        mesh = FakeMesh(receiver)
        forged = remote("LIMIT", 999, node="local").to_gossip()
        await mesh._relay("g1", {"gossip_id": "g1", **forged})
        assert receiver.get("limit", None) is None
        mesh._epidemic_send.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"key": "K"},
            {"key": "K", "value": 1, "timestamp": "x", "origin_node": "n"},
        ],
    )
    async def test_malformed_gossip_is_dropped_not_raised(self, isolated, payload):
        mesh = FakeMesh(isolated({}))
        await mesh._relay("g1", {"gossip_id": "g1", **payload})
        mesh._epidemic_send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_same_gossip_is_handled_once(self, isolated):
        receiver = isolated({})
        mesh = FakeMesh(receiver)
        payload = {"gossip_id": "g1", **remote("K", "v").to_gossip()}
        await mesh._relay("g1", payload)
        await mesh._relay("g1", payload)
        mesh._epidemic_send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_stale_update_is_still_passed_on(self, isolated):
        receiver = isolated({})
        receiver.update_from_mesh(remote("K", "new", ts=9, node="b"))
        mesh = FakeMesh(receiver)
        await mesh._relay(
            "g2", {"gossip_id": "g2", **remote("K", "old", ts=1, node="b").to_gossip()}
        )
        assert receiver.get("k") == "new"
        mesh._epidemic_send.assert_awaited_once()
