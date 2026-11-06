import json
from pathlib import Path

import pytest

from volnux.config import ConfigEntry, VolnuxConfig


def populated(isolated):
    cfg = isolated({"RETRIES": 5, "ROOT": Path("/srv/app"), "FLAGS": ["a", "b"]})
    cfg.add("custom", {"x": 1})
    cfg.add("scoped", "v", namespace="team")
    return cfg


class TestRoundTrip:
    def test_from_dict_rebuilds_the_config(self, isolated):
        # it raised NameError (ResultSet was only imported for type checking)
        cfg = populated(isolated)
        again = VolnuxConfig.from_dict(cfg.to_dict())
        assert again.get("RETRIES") == 5
        assert again.get("CUSTOM") == {"x": 1}
        assert again.get("scoped", namespace="team") == "v"
        assert again.get_node_id() == cfg.get_node_id()
        assert again._lamport_clock == cfg._lamport_clock

    def test_json_and_base64_carriers(self, isolated):
        cfg = populated(isolated)
        for again in (
            VolnuxConfig.from_json(cfg.to_json()),
            VolnuxConfig.from_base64(cfg.to_base64()),
        ):
            assert again.get("FLAGS") == ["a", "b"]
            assert again.get("scoped", namespace="team") == "v"

    def test_paths_survive_as_text(self, isolated):
        # to_json raised TypeError for the Path settings (BASE_DIR ...)
        cfg = populated(isolated)
        assert json.loads(cfg.to_json())["store"]
        assert VolnuxConfig.from_json(cfg.to_json()).get("ROOT") == "/srv/app"

    def test_the_real_defaults_serialise(self, monkeypatch):
        monkeypatch.setattr(
            VolnuxConfig, "_search_for_config_file_in_dir", staticmethod(lambda: None)
        )
        cfg = VolnuxConfig()
        again = VolnuxConfig.from_json(cfg.to_json())
        assert again.get("MAX_EVENT_RETRIES") == cfg.get("MAX_EVENT_RETRIES")

    def test_a_rebuilt_config_is_independent(self, isolated):
        cfg = populated(isolated)
        again = VolnuxConfig.from_dict(cfg.to_dict())
        again.add("only_here", 1)
        assert cfg.get("only_here", None) is None
        assert again._lock is not cfg._lock

    def test_signature_survives_the_carrier(self, isolated):
        entry = ConfigEntry(value=1, name="k", timestamp=3, origin_mesh_node="n")
        entry.signature = b"\x01\x02"
        assert ConfigEntry.from_dict(entry.to_dict()).signature == b"\x01\x02"

    def test_an_empty_carrier(self):
        cfg = VolnuxConfig.from_dict({})
        assert cfg.get("anything", None) is None and cfg._lamport_clock == 0


class TestGossipForm:
    def entry(self, **kw):
        data = dict(value={"a": 1}, name="k", timestamp=4, origin_mesh_node="n1")
        data.update(kw)
        return ConfigEntry(**data)

    def test_round_trip(self):
        entry = self.entry()
        entry.signature = b"\xaa\xbb"
        again = ConfigEntry.from_gossip(entry.to_gossip())
        assert again == entry

    def test_the_form_is_json(self):
        entry = self.entry()
        entry.signature = b"\xaa"
        assert json.loads(json.dumps(entry.to_gossip()))["signature"] == "aa"

    def test_old_origin_key_is_accepted(self):
        payload = self.entry().to_gossip()
        payload["origin_node"] = payload.pop("origin_mesh_node")
        assert ConfigEntry.from_gossip(payload).origin_mesh_node == "n1"

    @pytest.mark.parametrize(
        "change",
        [
            {"key": None},
            {"key": ""},
            {"key": 5},
            {"timestamp": None},
            {"timestamp": "4"},
            {"timestamp": 4.5},
            {"timestamp": True},
            {"timestamp": -1},
            {"origin_mesh_node": None},
            {"origin_mesh_node": ""},
            {"signature": "not hex"},
            {"signature": 12},
        ],
    )
    def test_malformed_payloads(self, change):
        payload = {**self.entry().to_gossip(), **change}
        payload.pop("origin_node", None)
        with pytest.raises(ValueError):
            ConfigEntry.from_gossip(payload)

    def test_missing_value(self):
        payload = self.entry().to_gossip()
        del payload["value"]
        with pytest.raises(ValueError, match="no value"):
            ConfigEntry.from_gossip(payload)

    def test_not_a_mapping(self):
        with pytest.raises(ValueError):
            ConfigEntry.from_gossip(["k", 1])  # type: ignore[arg-type]
