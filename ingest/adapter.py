"""Adapter layer delegating all parsing to the oceanstream library.

This module provides a unified interface for parsing sensor data files
and stream lines.  All heavy lifting is done by the ``oceanstream``
package; this module wraps those calls with graceful error handling and
format normalization expected by the sensorstream pipeline.

The adapter handles:
- NMEA text files and individual NMEA lines (live streams)
- CSV / GeoCSV files
- Sea-Bird CTD ``.hex`` files (via seabirdscientific)
- Sea-Bird CTD ``.cnv`` files
- RDI ADCP ``.raw`` binary files (via dolfyn)

If specific oceanstream submodules are not installed, the corresponding
functions raise ``RuntimeError`` with an actionable install message.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    pass

logger = logging.getLogger("sensorstream")

# ---------------------------------------------------------------------------
# Availability flags — checked lazily to avoid deep import chains
# ---------------------------------------------------------------------------

HAS_OCEANSTREAM = False
HAS_NMEA = False
HAS_CTD = False
HAS_ADCP = False

try:
    import oceanstream  # noqa: F401

    HAS_OCEANSTREAM = True
except ImportError:
    pass

if HAS_OCEANSTREAM:
    # Check individual submodule availability without triggering heavy imports.
    # The actual functions are imported lazily inside the parse_* functions.
    try:
        import oceanstream.sensors.processors.nmea_gnss  # noqa: F401

        HAS_NMEA = True
    except ImportError:
        pass

    try:
        import oceanstream.sensors.processors.sbe911  # noqa: F401

        HAS_CTD = True
    except ImportError:
        pass

    try:
        import warnings

        import numpy as np
        import scipy.integrate

        # Apply compat patches before dolfyn touches numpy/scipy.
        # Guard with hasattr to handle numpy <1.25 (no np.exceptions)
        # and numpy >=2.0 (no np.NaN, no np.RankWarning).
        if not hasattr(np, "NaN"):
            np.NaN = np.nan  # type: ignore[attr-defined]
        if not hasattr(np, "RankWarning"):
            _rank_warning = getattr(
                getattr(np, "exceptions", None), "RankWarning", None
            )
            if _rank_warning is not None:
                np.RankWarning = _rank_warning  # type: ignore[attr-defined]
        if not hasattr(scipy.integrate, "cumtrapz"):
            if hasattr(scipy.integrate, "cumulative_trapezoid"):
                scipy.integrate.cumtrapz = scipy.integrate.cumulative_trapezoid  # type: ignore[attr-defined]

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, message="pkg_resources")
            import dolfyn  # noqa: F401

        # Verify the oceanstream ADCP module is also importable
        from oceanstream.adcp.rdi_reader import read_rdi as _test_rdi  # noqa: F401

        HAS_ADCP = True
    except (ImportError, AttributeError, Exception):
        pass


# NMEA prefix detection
_NMEA_PREFIXES = ("$GP", "$GN", "$GL", "$GA", "$GB", "$GI")


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------


def detect_format(line: str) -> str:
    """Detect whether a line is NMEA, CSV, or hex.

    Returns ``"nmea"``, ``"csv"``, or ``"hex"``.
    """
    import re

    stripped = line.strip()

    if stripped.startswith("*") or stripped.startswith("#"):
        return "csv"

    # Direct NMEA sentence
    if any(stripped.startswith(p) for p in _NMEA_PREFIXES):
        return "nmea"

    # ISO8601 timestamp + NMEA sentence (R2R format)
    parts = stripped.split(maxsplit=1)
    if len(parts) == 2 and any(parts[1].startswith(p) for p in _NMEA_PREFIXES):
        return "nmea"

    # SeaBird hex scan line (all hex digits, 40+ chars)
    if re.match(r"^[0-9A-Fa-f]{40,}$", stripped):
        return "hex"

    return "csv"


# ---------------------------------------------------------------------------
# NMEA parsing
# ---------------------------------------------------------------------------


def parse_nmea_line(line: str) -> dict[str, Any] | None:
    """Parse a single NMEA line using oceanstream.

    Accepts two formats:
    - ``<ISO8601> <NMEA_sentence>`` (R2R file format)
    - ``<NMEA_sentence>`` (raw serial / network stream — timestamp added automatically)

    Returns a dict with parsed fields, or None on failure.
    """
    if not HAS_NMEA:
        raise RuntimeError(
            "oceanstream NMEA parser not available. "
            "Install with: pip install oceanstream[geotrack]"
        )

    stripped = line.strip()
    if not stripped:
        return None

    # If the line is a raw NMEA sentence (no timestamp prefix),
    # prepend an ISO8601 timestamp to match what oceanstream expects.
    parts = stripped.split(maxsplit=1)
    if len(parts) == 1 or not any(parts[1].startswith(p) for p in _NMEA_PREFIXES):
        # Either single token or the second part isn't NMEA — check if the whole line is NMEA
        if any(stripped.startswith(p) for p in _NMEA_PREFIXES):
            stripped = f"{datetime.now(timezone.utc).isoformat()} {stripped}"
        else:
            return None

    from oceanstream.sensors.processors.nmea_gnss import parse_nmea_line as _os_parse_nmea

    result = _os_parse_nmea(stripped)
    if result is None:
        return None

    # Normalize: oceanstream uses "timestamp" key, sensorstream uses "time" (ISO string)
    data: dict[str, Any] = {}
    if "timestamp" in result:
        data["time"] = result["timestamp"].isoformat()
    for key, value in result.items():
        if key != "timestamp":
            data[key] = value

    return data if len(data) > 1 else None


def parse_nmea_file(file_path: Path) -> pd.DataFrame:
    """Parse an NMEA text file into a DataFrame.

    Parameters
    ----------
    file_path : Path
        Path to a ``.txt`` file with NMEA sentences.

    Returns
    -------
    pd.DataFrame
        DataFrame with columns like ``time``, ``latitude``, ``longitude``, etc.
    """
    records: list[dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            record = parse_nmea_line(line)
            if record is not None:
                records.append(record)

    logger.info("Parsed %d NMEA records from %s", len(records), file_path.name)
    return records_to_dataframe(records)


# ---------------------------------------------------------------------------
# CSV parsing
# ---------------------------------------------------------------------------


def parse_csv_line(line: str, headers: list[str]) -> dict[str, Any] | None:
    """Parse a single CSV line against known headers.

    Returns a dict keyed by header names, or None on failure.
    """
    stripped = line.strip()
    if not stripped:
        return None

    values = stripped.split(",")
    if len(values) != len(headers):
        return None

    record: dict[str, Any] = {}
    for header, val in zip(headers, values):
        val = val.strip()
        if not val:
            continue
        try:
            numeric = float(val)
            record[header] = int(numeric) if numeric == int(numeric) else numeric
            continue
        except ValueError:
            pass
        record[header] = val

    return record if record else None


def parse_csv_file(file_path: Path) -> pd.DataFrame:
    """Parse a CSV file into a DataFrame.

    Parameters
    ----------
    file_path : Path
        Path to a CSV file.

    Returns
    -------
    pd.DataFrame
    """
    headers = _peek_headers(file_path)
    df = pd.read_csv(
        file_path,
        parse_dates=["time"] if "time" in headers else False,
    )

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = df.dropna(subset=["time"])

    for col in ("latitude", "longitude", "depth", "TEMP", "PSAL", "CNDC"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("Parsed %d CSV records from %s", len(df), file_path.name)
    return df


# ---------------------------------------------------------------------------
# CTD hex / cnv parsing
# ---------------------------------------------------------------------------


def parse_hex_file(file_path: Path) -> pd.DataFrame:
    """Parse a Sea-Bird ``.hex`` file via oceanstream.

    Requires companion ``.hdr`` and ``.XMLCON`` files in the same
    directory.

    Parameters
    ----------
    file_path : Path
        Path to the ``.hex`` file.

    Returns
    -------
    pd.DataFrame
    """
    if not HAS_CTD:
        raise RuntimeError(
            "oceanstream CTD parser not available. "
            "Install with: pip install oceanstream[geotrack]"
        )

    from oceanstream.sensors.processors.sbe911 import (
        CTDCast,
        process_ctd_cast as _os_process_ctd_cast,
    )

    cast = CTDCast(
        cast_id=file_path.stem,
        cruise_id="sensorstream",
        hex_file=file_path,
    )

    # Find companion files
    parent = file_path.parent
    stem = file_path.stem.lower()
    for f in parent.iterdir():
        f_stem = f.stem.lower()
        if f_stem == stem and f.suffix.lower() == ".hdr":
            cast.hdr_file = f
        elif f_stem == stem and f.suffix.lower() == ".xmlcon":
            cast.xmlcon_file = f

    df = _os_process_ctd_cast(cast)
    if df is None:
        logger.warning("CTD processing returned no data for %s", file_path.name)
        return pd.DataFrame()

    logger.info("Parsed %d CTD records from %s", len(df), file_path.name)
    return df


def parse_cnv_file(file_path: Path) -> pd.DataFrame:
    """Parse a processed CTD ``.cnv`` file.

    Reads the Sea-Bird header to extract column names, then parses
    the whitespace-delimited data section.

    Parameters
    ----------
    file_path : Path
        Path to the ``.cnv`` file.

    Returns
    -------
    pd.DataFrame
    """
    columns: list[str] = []
    header_lines = 0
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            header_lines += 1
            if line.startswith("# name"):
                parts = line.split("=", 1)
                if len(parts) == 2:
                    col_name = parts[1].strip().split(":")[0].strip()
                    columns.append(col_name)
            if line.startswith("*END*"):
                break

    if not columns:
        logger.warning("No column definitions found in CTD file: %s", file_path)
        return pd.DataFrame()

    df = pd.read_csv(
        file_path,
        skiprows=header_lines,
        sep=r"\s+",
        names=columns,
        engine="python",
    )

    logger.info("Parsed %d CTD records from %s", len(df), file_path.name)
    return df


# ---------------------------------------------------------------------------
# ADCP parsing
# ---------------------------------------------------------------------------


def parse_adcp_file(
    raw_path: Path,
    transducer_depth: float = 7.0,
    ensemble_interval: float = 120.0,
    corr_threshold: int = 64,
) -> pd.DataFrame:
    """Parse an RDI ADCP ``.raw`` file via oceanstream.

    Full pipeline: read → beam→earth → ensemble average → flatten.

    Parameters
    ----------
    raw_path : Path
        Path to the ``.raw`` binary file.
    transducer_depth : float
        Transducer depth below surface in metres.
    ensemble_interval : float
        Averaging interval in seconds.
    corr_threshold : int
        Minimum beam-averaged correlation (0–255).

    Returns
    -------
    pd.DataFrame
        One row per (ensemble, depth cell).
    """
    if not HAS_ADCP:
        raise RuntimeError(
            "oceanstream ADCP parser not available. "
            "Install with: pip install oceanstream[adcp]"
        )

    from oceanstream.adcp.processor import process_file as _os_process_adcp

    return _os_process_adcp(
        raw_path,
        transducer_depth=transducer_depth,
        ensemble_interval=ensemble_interval,
        corr_threshold=corr_threshold,
    )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


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

    for col in (
        "latitude", "longitude", "depth", "TEMP", "PSAL", "CNDC",
        "speed_over_ground", "course_over_ground", "gps_antenna_height",
        "horizontal_dilution",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _peek_headers(file_path: Path) -> list[str]:
    """Read the first line of a file to get CSV headers."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()
            while first_line.startswith("#"):
                first_line = f.readline().strip()
            return [h.strip() for h in first_line.split(",")]
    except Exception:
        return []


def enrich_with_provider(
    df: pd.DataFrame, provider_name: str, file_path: "Path | None" = None
) -> pd.DataFrame:
    """Optionally enrich a DataFrame using an oceanstream provider.

    Parameters
    ----------
    df : pd.DataFrame
        Data to enrich.
    provider_name : str
        Provider name or ``"auto"`` for auto-detection.
    file_path : Path or None
        Source file path, used for provider auto-detection when *provider_name*
        is ``"auto"``.

    Returns
    -------
    pd.DataFrame
        Enriched DataFrame (original if enrichment fails or is unavailable).
    """
    if not HAS_OCEANSTREAM:
        logger.debug("oceanstream not available — skipping provider enrichment")
        return df

    try:
        from oceanstream.providers.factory import detect_or_get_provider

        provider = detect_or_get_provider(
            provider_name if provider_name != "auto" else None,
            file_path=file_path,
        )
        if provider is not None:
            df = provider.enrich_dataframe(df)
            logger.info("Enriched with provider: %s", provider.name)
    except Exception as e:
        logger.warning("Provider enrichment failed: %s", e)

    return df
