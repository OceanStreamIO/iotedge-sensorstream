"""Rate-limited telemetry sender for IoT Hub D2C messages.

Enforces a configurable minimum interval between messages so that
high-frequency sensor data (24 Hz CTD scans, 1 Hz GNSS) does not
overwhelm the IoT Hub message quota.

The interval is controlled by ``EdgeConfig.telemetry_downsample_seconds``
(default 30 s, twin-adjustable from 1 s to 600+ s).  When a message is
suppressed, the latest record is held and sent when the window expires.
Summary messages are sent on a separate, longer cadence controlled by
``EdgeConfig.telemetry_interval_seconds`` (default 300 s).
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient
    from config import EdgeConfig

logger = logging.getLogger("sensorstream")


class TelemetryThrottle:
    """Gate that ensures at most one telemetry message per downsample window.

    Parameters
    ----------
    config
        Edge configuration — reads ``telemetry_downsample_seconds``,
        ``telemetry_send_records``, ``telemetry_send_summaries``,
        and ``telemetry_interval_seconds``.
    client
        Azure IoT Hub module client (``None`` in standalone mode).
    """

    def __init__(self, config: "EdgeConfig", client: "IoTHubModuleClient | None" = None):
        self.config = config
        self.client = client

        # Timestamps of last *sent* message (monotonic seconds)
        self._last_record_sent: float = 0.0
        self._last_summary_sent: float = 0.0

        # Counters for reporting
        self._records_sent: int = 0
        self._records_suppressed: int = 0
        self._summaries_sent: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def maybe_send_record(self, record: Dict[str, Any]) -> bool:
        """Send an individual record if the downsample window allows it.

        Returns ``True`` if the message was sent, ``False`` if suppressed.
        """
        if self.client is None or not self.config.telemetry_send_records:
            return False

        now = time.monotonic()
        interval = max(1, self.config.telemetry_downsample_seconds)

        if (now - self._last_record_sent) < interval:
            self._records_suppressed += 1
            return False

        from exports.telemetry import send_record_telemetry

        try:
            send_record_telemetry(self.client, record, self.config)
            self._last_record_sent = now
            self._records_sent += 1
            return True
        except Exception as e:
            logger.warning("Failed to send record telemetry: %s", e)
            return False

    def maybe_send_summary(self, result: Dict[str, Any]) -> bool:
        """Send a processing-result summary if the summary interval allows.

        Returns ``True`` if the message was sent, ``False`` if suppressed.
        """
        if self.client is None or not self.config.telemetry_send_summaries:
            return False

        now = time.monotonic()
        interval = max(1, self.config.telemetry_interval_seconds)

        if (now - self._last_summary_sent) < interval:
            return False

        from exports.telemetry import send_processing_telemetry

        try:
            send_processing_telemetry(self.client, result, self.config)
            self._last_summary_sent = now
            self._summaries_sent += 1
            return True
        except Exception as e:
            logger.warning("Failed to send summary telemetry: %s", e)
            return False

    def force_send_summary(self, result: Dict[str, Any]) -> bool:
        """Send a summary immediately, ignoring the rate limit.

        Used at end of file processing or shutdown.
        """
        if self.client is None:
            return False

        from exports.telemetry import send_processing_telemetry

        try:
            send_processing_telemetry(self.client, result, self.config)
            self._last_summary_sent = time.monotonic()
            self._summaries_sent += 1
            return True
        except Exception as e:
            logger.warning("Failed to send summary telemetry: %s", e)
            return False

    def stats(self) -> Dict[str, int]:
        """Return send/suppress counters."""
        return {
            "records_sent": self._records_sent,
            "records_suppressed": self._records_suppressed,
            "summaries_sent": self._summaries_sent,
        }

    def reset_stats(self) -> None:
        """Reset counters (e.g. after reporting)."""
        self._records_sent = 0
        self._records_suppressed = 0
        self._summaries_sent = 0
