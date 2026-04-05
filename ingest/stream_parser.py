"""Parse NMEA and CSV lines from network streams or files.

Handles two data formats:
- **NMEA**: Lines starting with ``$GP`` / ``$GN`` / ``$GL`` etc.
  Optionally prefixed with ISO8601 timestamp (R2R raw format).
- **CSV**: Comma-delimited records with a header row.

Used by both ``stream_listener`` (real-time) and ``file_watcher`` (batch).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

import re

logger = logging.getLogger("sensorstream")

# NMEA sentence prefixes we recognise
_NMEA_PREFIXES = ("$GP", "$GN", "$GL", "$GA", "$GB", "$GI")

# Hex scan line pattern: only hex digits, at least 40 chars (20+ bytes/scan)
_HEX_SCAN_RE = re.compile(r"^[0-9A-Fa-f]{40,}$")

# Sentence types we extract data from
_POSITION_TYPES = ("GGA", "RMC", "GNS")
_SPEED_TYPES = ("RMC", "VTG")
_TIME_TYPES = ("ZDA",)

# CSV columns we produce (superset of NMEA and CSV fields)
OUTPUT_COLUMNS = [
    "time",
    "latitude",
    "longitude",
    "gps_quality",
    "num_satellites",
    "horizontal_dilution",
    "gps_antenna_height",
    "speed_over_ground",
    "course_over_ground",
    "depth",
    "TEMP",
    "PSAL",
    "CNDC",
]


def detect_format(line: str) -> str:
    """Detect whether a line is NMEA, CSV, or hex.

    Returns ``"nmea"``, ``"csv"``, or ``"hex"``.
    """
    stripped = line.strip()

    # Header / comment lines (hex file preamble, GeoCSV, etc.)
    if stripped.startswith("*") or stripped.startswith("#"):
        return "csv"  # ignore for detection; will be re-checked on data lines

    # Direct NMEA sentence
    if any(stripped.startswith(p) for p in _NMEA_PREFIXES):
        return "nmea"

    # ISO8601 timestamp followed by NMEA sentence (R2R format)
    parts = stripped.split(maxsplit=1)
    if len(parts) == 2 and any(parts[1].startswith(p) for p in _NMEA_PREFIXES):
        return "nmea"

    # SeaBird hex scan line (all hex digits, 40+ chars)
    if _HEX_SCAN_RE.match(stripped):
        return "hex"

    return "csv"


def parse_nmea_line(line: str) -> dict[str, Any] | None:
    """Parse a single NMEA line and extract relevant data.

    Accepts two formats:
    - ``<ISO8601> <NMEA_sentence>`` (R2R file format)
    - ``<NMEA_sentence>`` (raw serial / network stream)

    Returns a dict with parsed fields, or None on failure.
    """
    import pynmea2

    stripped = line.strip()
    if not stripped:
        return None

    timestamp: datetime | None = None
    nmea_part: str

    # Try to split off an ISO8601 prefix
    parts = stripped.split(maxsplit=1)
    if len(parts) == 2 and any(parts[1].startswith(p) for p in _NMEA_PREFIXES):
        try:
            timestamp = datetime.fromisoformat(parts[0].replace("Z", "+00:00"))
        except ValueError:
            pass
        nmea_part = parts[1]
    elif any(stripped.startswith(p) for p in _NMEA_PREFIXES):
        nmea_part = stripped
    else:
        return None

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)

    try:
        msg = pynmea2.parse(nmea_part)
    except pynmea2.ParseError as e:
        logger.debug("NMEA parse error: %s — %s", nmea_part, e)
        return None

    data: dict[str, Any] = {"time": timestamp.isoformat()}

    if isinstance(msg, pynmea2.types.talker.GGA):
        if msg.latitude is not None and msg.longitude is not None:
            data["latitude"] = float(msg.latitude)
            data["longitude"] = float(msg.longitude)
        if msg.gps_qual is not None:
            data["gps_quality"] = int(msg.gps_qual)
        if msg.num_sats:
            data["num_satellites"] = int(msg.num_sats)
        if msg.horizontal_dil:
            data["horizontal_dilution"] = float(msg.horizontal_dil)
        if msg.altitude is not None:
            data["gps_antenna_height"] = float(msg.altitude)

    elif isinstance(msg, pynmea2.types.talker.RMC):
        if msg.latitude is not None and msg.longitude is not None:
            data["latitude"] = float(msg.latitude)
            data["longitude"] = float(msg.longitude)
        if msg.spd_over_grnd is not None:
            data["speed_over_ground"] = float(msg.spd_over_grnd) * 0.514444
        if msg.true_course is not None:
            data["course_over_ground"] = float(msg.true_course)

    elif isinstance(msg, pynmea2.types.talker.GNS):
        if msg.latitude is not None and msg.longitude is not None:
            data["latitude"] = float(msg.latitude)
            data["longitude"] = float(msg.longitude)
        if msg.num_sats:
            data["num_satellites"] = int(msg.num_sats)
        if msg.hdop:
            data["horizontal_dilution"] = float(msg.hdop)
        if msg.altitude is not None:
            data["gps_antenna_height"] = float(msg.altitude)

    elif isinstance(msg, pynmea2.types.talker.VTG):
        if msg.spd_over_grnd_kts is not None:
            data["speed_over_ground"] = float(msg.spd_over_grnd_kts) * 0.514444
        if msg.true_track is not None:
            data["course_over_ground"] = float(msg.true_track)

    elif isinstance(msg, pynmea2.types.talker.ZDA):
        if hasattr(msg, "timestamp") and msg.timestamp:
            try:
                utc_time = msg.timestamp
                day = int(msg.day) if msg.day else 1
                month = int(msg.month) if msg.month else 1
                year = int(msg.year) if msg.year else 1970
                gps_dt = datetime(
                    year=year, month=month, day=day,
                    hour=utc_time.hour, minute=utc_time.minute,
                    second=utc_time.second, microsecond=utc_time.microsecond,
                    tzinfo=timezone.utc,
                )
                data["time"] = gps_dt.isoformat()
                data["gps_utc_time"] = gps_dt.isoformat()
            except (ValueError, AttributeError):
                pass

    # Only return if we extracted something beyond timestamp
    if len(data) > 1:
        return data
    return None


def parse_csv_line(line: str, headers: list[str]) -> dict[str, Any] | None:
    """Parse a single CSV line against known headers.

    Returns a dict keyed by header names, or None on failure.
    """
    stripped = line.strip()
    if not stripped:
        return None

    values = stripped.split(",")
    if len(values) != len(headers):
        logger.debug("CSV column count mismatch: expected %d, got %d", len(headers), len(values))
        return None

    record: dict[str, Any] = {}
    for header, val in zip(headers, values):
        val = val.strip()
        if not val:
            continue
        # Try numeric conversion
        try:
            record[header] = float(val)
            if record[header] == int(record[header]):
                record[header] = int(record[header])
            continue
        except ValueError:
            pass
        record[header] = val

    return record if record else None


def records_to_dataframe(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert a list of parsed records to a DataFrame.

    Coerces the ``time`` column to datetime and sorts by it.
    """
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = df.dropna(subset=["time"])
        df = df.sort_values("time").reset_index(drop=True)

    # Coerce numeric columns
    for col in ("latitude", "longitude", "depth", "TEMP", "PSAL", "CNDC",
                "speed_over_ground", "course_over_ground", "gps_antenna_height",
                "horizontal_dilution"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df
