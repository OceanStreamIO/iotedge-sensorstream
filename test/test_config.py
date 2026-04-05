"""Tests for config.py."""

from __future__ import annotations

from config import EdgeConfig, _parse_bool, _unwrap


class TestParseHelpers:
    def test_parse_bool_true_values(self):
        assert _parse_bool(True) is True
        assert _parse_bool("true") is True
        assert _parse_bool("1") is True
        assert _parse_bool(1) is True

    def test_parse_bool_false_values(self):
        assert _parse_bool(False) is False
        assert _parse_bool("false") is False
        assert _parse_bool("0") is False
        assert _parse_bool(0) is False

    def test_parse_bool_none_uses_default(self):
        assert _parse_bool(None, default=True) is True
        assert _parse_bool(None, default=False) is False

    def test_unwrap_plain_value(self):
        assert _unwrap("hello") == "hello"
        assert _unwrap(42) == 42

    def test_unwrap_wrapped_value(self):
        assert _unwrap({"value": "hello"}) == "hello"
        assert _unwrap({"value": 42}) == 42


class TestEdgeConfig:
    def test_from_standalone_defaults(self):
        config = EdgeConfig.from_standalone()
        assert config.storage_backend == "local"
        assert config.input_mode == "file"
        assert config.stream_port == 9100

    def test_from_standalone_override(self):
        config = EdgeConfig.from_standalone(
            campaign_id="test_campaign",
            stream_port=9200,
        )
        assert config.campaign_id == "test_campaign"
        assert config.stream_port == 9200

    def test_from_twin(self):
        twin = {
            "input_mode": "both",
            "campaign_id": "cruise_2024",
            "stream_port": {"value": 9200},
            "watch_polling": "true",
        }
        config = EdgeConfig.from_twin_and_env(twin)
        assert config.input_mode == "both"
        assert config.campaign_id == "cruise_2024"
        assert config.stream_port == 9200
        assert config.watch_polling is True

    def test_update_from_twin(self):
        config = EdgeConfig.from_standalone()
        config.update_from_twin({
            "campaign_id": "updated",
            "batch_max_records": "500",
            "$version": 3,
        })
        assert config.campaign_id == "updated"
        assert config.batch_max_records == 500
        assert config._config_version == 1

    def test_update_from_twin_rejects_invalid_literal(self):
        config = EdgeConfig.from_standalone()
        config.update_from_twin({"input_mode": "invalid_mode"})
        assert config.input_mode == "file"  # unchanged
