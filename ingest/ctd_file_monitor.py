"""Poll-based monitor for a CTD 'latest reading' CSV file.

Polls a file at a configurable interval and delegates parsing to the
observatory-specific ``parse_ctd_latest()`` from the ``oceanstream``
providers package.  The observatory key (e.g. ``"munkholmen"``) is
read from ``EdgeConfig.ctd_observatory`` and determines which column
mapping, fixed coordinates, and metadata are applied.

Configuration (all twin-adjustable):
- ``ctd_file_path``              — path to the CSV file
- ``ctd_poll_interval_seconds``  — poll interval (default 30 s)
- ``ctd_observatory``            — observatory key (default ``"munkholmen"``)

The telemetry send rate is governed by the existing ``TelemetryThrottle``
using ``telemetry_downsample_seconds``.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from config import EdgeConfig

logger = logging.getLogger("sensorstream")

# Lazy-loaded flag for the oceanstream provider
_HAS_OCEANLAB = False

try:
    from oceanstream.providers.oceanlab import parse_ctd_latest as _os_parse_ctd_latest

    _HAS_OCEANLAB = True
except (ImportError, ModuleNotFoundError):
    _os_parse_ctd_latest = None  # type: ignore[assignment]


def _parse_ctd_csv_fallback(file_path: Path) -> dict[str, Any] | None:
    """Minimal CSV parser when oceanstream is not installed."""
    import csv

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
            reader = csv.DictReader(fh)
            row = next(reader, None)
    except (OSError, StopIteration):
        return None

    if row is None:
        return None

    record: dict[str, Any] = {"source": "ctd_latest", "file_type": "ctd_csv"}
    for raw_col, value in row.items():
        if raw_col is None:
            continue
        key = raw_col.strip().lower()
        value = value.strip() if isinstance(value, str) else value
        if key in ("timestamp",):
            record["time"] = value
            continue
        try:
            record[key] = float(value)
        except (ValueError, TypeError):
            record[key] = value

    return record if record.get("time") else None


def parse_ctd_latest(
    file_path: Path, observatory: str = "munkholmen"
) -> dict[str, Any] | None:
    """Parse a CTD latest-reading CSV using the observatory provider.

    Falls back to a minimal CSV reader if oceanstream is not installed.
    """
    if _HAS_OCEANLAB and _os_parse_ctd_latest is not None:
        return _os_parse_ctd_latest(file_path, observatory=observatory)
    logger.debug("oceanstream not available — using fallback CTD parser")
    return _parse_ctd_csv_fallback(file_path)


class CtdFileMonitor:
    """Poll a CTD 'latest reading' CSV file and queue records.

    Parameters
    ----------
    config
        Edge configuration — reads ``ctd_file_path`` and
        ``ctd_poll_interval_seconds``.
    queue
        Async queue to push ``("ctd_reading", record_dict)`` items.
    """

    def __init__(self, config: "EdgeConfig", queue: asyncio.Queue):
        self.config = config
        self.queue = queue
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_timestamp: str | None = None
        self._readings_queued: int = 0

    async def start(self) -> None:
        """Start the polling loop."""
        file_path = Path(self.config.ctd_file_path)
        if not file_path.exists():
            logger.warning(
                "CTD file not found: %s — monitor will retry",
                file_path,
            )

        self._running = True
        self._task = asyncio.ensure_future(self._poll_loop())
        logger.info(
            "CTD file monitor started: file=%s, interval=%ds",
            self.config.ctd_file_path,
            self.config.ctd_poll_interval_seconds,
        )

    async def stop(self) -> None:
        """Stop the polling loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(
            "CTD file monitor stopped (%d readings queued)",
            self._readings_queued,
        )

    async def _poll_loop(self) -> None:
        """Periodically read the CTD file and queue new readings."""
        while self._running:
            try:
                file_path = Path(self.config.ctd_file_path)
                observatory = getattr(self.config, "ctd_observatory", "munkholmen")
                record = parse_ctd_latest(file_path, observatory=observatory)

                if record is not None:
                    ts = record.get("time")
                    if ts != self._last_timestamp:
                        self._last_timestamp = ts
                        await self.queue.put(("ctd_reading", record))
                        self._readings_queued += 1
                        logger.debug(
                            "CTD reading queued: T=%.2f°C, S=%.2f, P=%.1f dbar @ %s",
                            record.get("temperature", 0),
                            record.get("salinity", 0),
                            record.get("pressure", 0),
                            ts,
                        )
                    else:
                        logger.debug("CTD reading unchanged (ts=%s) — skipped", ts)

            except Exception as exc:
                logger.warning("CTD poll error: %s", exc)

            await asyncio.sleep(self.config.ctd_poll_interval_seconds)
