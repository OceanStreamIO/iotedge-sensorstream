"""Tests for telemetry downsampling (TelemetryThrottle) and config additions."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import EdgeConfig
from exports.throttle import TelemetryThrottle


# -------------------------------------------------------------------
# Config tests for new fields
# -------------------------------------------------------------------


class TestDownsampleConfig:
    def test_default_downsample_seconds(self):
        config = EdgeConfig.from_standalone()
        assert config.telemetry_downsample_seconds == 30

    def test_custom_downsample_seconds(self):
        config = EdgeConfig.from_standalone(telemetry_downsample_seconds=120)
        assert config.telemetry_downsample_seconds == 120

    def test_twin_update_downsample(self):
        config = EdgeConfig.from_standalone()
        config.update_from_twin({"telemetry_downsample_seconds": 60})
        assert config.telemetry_downsample_seconds == 60

    def test_twin_update_send_summaries(self):
        config = EdgeConfig.from_standalone()
        assert config.telemetry_send_summaries is True
        config.update_from_twin({"telemetry_send_summaries": False})
        assert config.telemetry_send_summaries is False

    def test_hex_stream_format_accepted(self):
        config = EdgeConfig.from_standalone(stream_format="hex")
        assert config.stream_format == "hex"

    def test_twin_update_stream_format_hex(self):
        config = EdgeConfig.from_standalone()
        config.update_from_twin({"stream_format": "hex"})
        assert config.stream_format == "hex"


# -------------------------------------------------------------------
# TelemetryThrottle tests
# -------------------------------------------------------------------


class TestTelemetryThrottle:
    def _make_throttle(self, downsample=5, send_records=True, send_summaries=True) -> TelemetryThrottle:
        config = EdgeConfig.from_standalone(
            telemetry_downsample_seconds=downsample,
            telemetry_send_records=send_records,
            telemetry_send_summaries=send_summaries,
            telemetry_interval_seconds=10,
        )
        client = MagicMock()
        return TelemetryThrottle(config, client)

    @patch("exports.telemetry.send_record_telemetry")
    def test_first_record_always_sent(self, mock_send):
        throttle = self._make_throttle(downsample=30)
        result = throttle.maybe_send_record({"temperature": 22.5})
        assert result is True
        assert throttle._records_sent == 1
        mock_send.assert_called_once()

    @patch("exports.telemetry.send_record_telemetry")
    def test_second_record_within_window_suppressed(self, mock_send):
        throttle = self._make_throttle(downsample=30)
        throttle.maybe_send_record({"temperature": 22.5})
        result = throttle.maybe_send_record({"temperature": 22.6})
        assert result is False
        assert throttle._records_suppressed == 1
        assert mock_send.call_count == 1

    @patch("exports.telemetry.send_record_telemetry")
    def test_record_sent_after_window_elapses(self, mock_send):
        throttle = self._make_throttle(downsample=1)
        throttle.maybe_send_record({"temperature": 22.5})
        # Simulate time passing
        throttle._last_record_sent = time.monotonic() - 2
        result = throttle.maybe_send_record({"temperature": 22.6})
        assert result is True
        assert throttle._records_sent == 2

    def test_no_client_never_sends(self):
        config = EdgeConfig.from_standalone(telemetry_downsample_seconds=1)
        throttle = TelemetryThrottle(config, client=None)
        result = throttle.maybe_send_record({"temperature": 22.5})
        assert result is False

    def test_send_records_disabled(self):
        throttle = self._make_throttle(send_records=False)
        result = throttle.maybe_send_record({"temperature": 22.5})
        assert result is False

    @patch("exports.telemetry.send_processing_telemetry")
    def test_summary_sent(self, mock_send):
        throttle = self._make_throttle()
        result = throttle.maybe_send_summary({"status": "ok", "record_count": 100})
        assert result is True
        assert throttle._summaries_sent == 1

    @patch("exports.telemetry.send_processing_telemetry")
    def test_summary_suppressed_within_interval(self, mock_send):
        throttle = self._make_throttle()
        throttle.maybe_send_summary({"status": "ok"})
        result = throttle.maybe_send_summary({"status": "ok"})
        assert result is False

    @patch("exports.telemetry.send_processing_telemetry")
    def test_force_send_summary_ignores_interval(self, mock_send):
        throttle = self._make_throttle()
        throttle.maybe_send_summary({"status": "ok"})
        result = throttle.force_send_summary({"status": "ok2"})
        assert result is True
        assert mock_send.call_count == 2

    def test_stats(self):
        throttle = self._make_throttle()
        stats = throttle.stats()
        assert "records_sent" in stats
        assert "records_suppressed" in stats
        assert "summaries_sent" in stats

    def test_reset_stats(self):
        throttle = self._make_throttle()
        throttle._records_sent = 10
        throttle._records_suppressed = 50
        throttle.reset_stats()
        assert throttle._records_sent == 0
        assert throttle._records_suppressed == 0
