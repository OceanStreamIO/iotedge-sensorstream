"""Sensor data processing pipeline.

Two processing paths:
- **File-based**: Reads a sensor data file (CSV, NMEA .txt, CTD .hex/.cnv,
  or .tar.gz archive), parses it, optionally enriches via an oceanstream
  provider, writes GeoParquet, and emits telemetry.
- **Stream batch**: Takes a pre-parsed DataFrame from the stream listener,
  enriches it, appends to a daily GeoParquet partition, and emits telemetry.
"""

from __future__ import annotations

import logging
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict

import pandas as pd

from ingest.adcp_parser import HAS_DOLFYN, parse_adcp_file
from ingest.hex_parser import HAS_SBS, parse_hex_file
from ingest.stream_parser import (
    detect_format,
    parse_csv_line,
    parse_nmea_line,
    records_to_dataframe,
)

if TYPE_CHECKING:
    from azure_handler.storage import StorageBackend
    from config import EdgeConfig

logger = logging.getLogger("sensorstream")

# File extensions we handle
_NMEA_EXTENSIONS = {".txt"}
_CSV_EXTENSIONS = {".csv", ".geocsv"}
_HEX_EXTENSIONS = {".hex"}
_CNV_EXTENSIONS = {".cnv"}
_CTD_EXTENSIONS = _HEX_EXTENSIONS | _CNV_EXTENSIONS
_ADCP_EXTENSIONS = {".raw"}
_ARCHIVE_EXTENSIONS = {".tar.gz", ".tgz"}


def _detect_file_type(path: Path) -> str:
    """Detect file type from extension."""
    name = path.name.lower()
    if name.endswith(".tar.gz") or name.endswith(".tgz"):
        return "archive"
    suffix = path.suffix.lower()
    if suffix in _NMEA_EXTENSIONS:
        return "nmea"
    if suffix in _CSV_EXTENSIONS:
        return "csv"
    if suffix in _HEX_EXTENSIONS:
        return "hex"
    if suffix in _CNV_EXTENSIONS:
        return "cnv"
    if suffix in _ADCP_EXTENSIONS:
        return "adcp"
    return "csv"  # default fallback


def _parse_nmea_file(file_path: Path) -> pd.DataFrame:
    """Parse an NMEA text file into a DataFrame."""
    records = []
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            record = parse_nmea_line(line)
            if record is not None:
                records.append(record)

    logger.info("Parsed %d NMEA records from %s", len(records), file_path.name)
    return records_to_dataframe(records)


def _parse_csv_file(file_path: Path) -> pd.DataFrame:
    """Parse a CSV file into a DataFrame."""
    df = pd.read_csv(file_path, parse_dates=["time"] if "time" in _peek_headers(file_path) else False)

    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
        df = df.dropna(subset=["time"])

    for col in ("latitude", "longitude", "depth", "TEMP", "PSAL", "CNDC"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("Parsed %d CSV records from %s", len(df), file_path.name)
    return df


def _parse_ctd_file(file_path: Path) -> pd.DataFrame:
    """Parse a processed CTD .cnv file into a DataFrame.

    Reads the Sea-Bird header to extract column names, then parses
    the whitespace-delimited data section.
    """
    columns = []
    header_lines = 0
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            header_lines += 1
            if line.startswith("# name"):
                # Format: # name N = colname: description
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


def _parse_hex_or_fallback(file_path: Path) -> pd.DataFrame:
    """Parse a .hex file using seabirdscientific, or skip gracefully."""
    if not HAS_SBS:
        logger.warning(
            "seabirdscientific not installed — cannot parse .hex file: %s. "
            "Install with: pip install seabirdscientific",
            file_path.name,
        )
        return pd.DataFrame()
    return parse_hex_file(file_path)


def _parse_adcp_or_fallback(file_path: Path) -> pd.DataFrame:
    """Parse an ADCP .raw file using dolfyn, or skip gracefully."""
    if not HAS_DOLFYN:
        logger.warning(
            "dolfyn not installed — cannot parse ADCP .raw file: %s. "
            "Install with: pip install dolfyn",
            file_path.name,
        )
        return pd.DataFrame()
    return parse_adcp_file(file_path)


def _peek_headers(file_path: Path) -> list[str]:
    """Read the first line of a file to get CSV headers."""
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().strip()
            # Skip comment lines (GeoCSV metadata)
            while first_line.startswith("#"):
                first_line = f.readline().strip()
            return [h.strip() for h in first_line.split(",")]
    except Exception:
        return []


def _extract_archive(archive_path: Path) -> list[Path]:
    """Extract a .tar.gz archive and return paths to data files."""
    tmpdir = tempfile.mkdtemp(prefix="sensorstream_")
    extracted = []

    with tarfile.open(archive_path, "r:gz") as tar:
        # Security: prevent path traversal
        for member in tar.getmembers():
            if member.name.startswith("/") or ".." in member.name:
                logger.warning("Skipping suspicious archive member: %s", member.name)
                continue
            tar.extract(member, tmpdir)
            if member.isfile():
                p = Path(tmpdir) / member.name
                suffix = p.suffix.lower()
                if suffix in (_NMEA_EXTENSIONS | _CSV_EXTENSIONS | _CTD_EXTENSIONS | {".hdr", ".xmlcon"}):
                    extracted.append(p)

    logger.info("Extracted %d data files from %s", len(extracted), archive_path.name)
    return extracted


def _enrich_with_provider(df: pd.DataFrame, provider_name: str) -> pd.DataFrame:
    """Optionally enrich a DataFrame using an oceanstream provider."""
    try:
        from oceanstream.providers.factory import detect_or_get_provider
        provider = detect_or_get_provider(provider_name if provider_name != "auto" else None, df)
        if provider is not None:
            df = provider.enrich_dataframe(df)
            logger.info("Enriched with provider: %s", provider.name)
    except ImportError:
        logger.debug("oceanstream not available — skipping provider enrichment")
    except Exception as e:
        logger.warning("Provider enrichment failed: %s", e)
    return df


def _make_output_path(config: "EdgeConfig", source_name: str, suffix: str = ".parquet") -> str:
    """Build a storage path for output files.

    Path structure: ``{campaign_container}/{processed_container}/{date}/{stem}{suffix}``
    where campaign_container is the Azure Blob container (from survey_id)
    and processed_container (``sensordata``) is a subfolder within it.
    """
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    stem = Path(source_name).stem
    return f"{config.campaign_container}/{config.processed_container}/{day}/{stem}{suffix}"


async def process_file(
    file_path: str,
    config: "EdgeConfig",
    storage: "StorageBackend",
    client: Any = None,
) -> Dict[str, Any]:
    """Process a single sensor data file through the pipeline.

    Parameters
    ----------
    file_path
        Path to the sensor data file.
    config
        Current edge configuration.
    storage
        Storage backend for writing output.
    client
        Optional IoT Hub module client for telemetry.

    Returns
    -------
    dict
        Processing result summary.
    """
    path = Path(file_path)
    start_time = time.time()
    result: Dict[str, Any] = {"source_file": path.name, "status": "ok"}

    file_type = _detect_file_type(path)
    logger.info("Processing %s file: %s", file_type, path.name)

    try:
        if file_type == "archive":
            extracted = _extract_archive(path)
            all_dfs = []
            for extracted_file in extracted:
                sub_type = _detect_file_type(extracted_file)
                if sub_type == "nmea":
                    all_dfs.append(_parse_nmea_file(extracted_file))
                elif sub_type == "csv":
                    all_dfs.append(_parse_csv_file(extracted_file))
                elif sub_type == "hex":
                    all_dfs.append(_parse_hex_or_fallback(extracted_file))
                elif sub_type == "cnv":
                    all_dfs.append(_parse_ctd_file(extracted_file))
            df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
        elif file_type == "nmea":
            df = _parse_nmea_file(path)
        elif file_type == "hex":
            df = _parse_hex_or_fallback(path)
        elif file_type == "cnv":
            df = _parse_ctd_file(path)
        elif file_type == "adcp":
            df = _parse_adcp_or_fallback(path)
        else:
            df = _parse_csv_file(path)

        if df.empty:
            result["status"] = "skipped"
            result["reason"] = "no records parsed"
            return result

        # Provider enrichment
        df = _enrich_with_provider(df, config.provider)

        # Deduplicate (skip for ADCP — multiple depth cells per timestamp)
        if "time" in df.columns and file_type != "adcp":
            before = len(df)
            df = df.drop_duplicates(subset=["time"], keep="first")
            if len(df) < before:
                logger.info("Removed %d duplicate records", before - len(df))

        # Write GeoParquet
        output_path = _make_output_path(config, path.name)
        storage.save_parquet(df, output_path)

        # Write metadata
        from exports.metadata import write_metadata
        meta_path = _make_output_path(config, path.name, suffix=".metadata.json")
        write_metadata(df, path.name, meta_path, storage)

        # Build result
        elapsed_ms = int((time.time() - start_time) * 1000)
        result.update({
            "record_count": len(df),
            "output_path": output_path,
            "processing_time_ms": elapsed_ms,
            "file_type": file_type,
        })

        if "time" in df.columns:
            result["time_range"] = [
                df["time"].min().isoformat(),
                df["time"].max().isoformat(),
            ]
        if "latitude" in df.columns and "longitude" in df.columns:
            result["lat_range"] = [float(df["latitude"].min()), float(df["latitude"].max())]
            result["lon_range"] = [float(df["longitude"].min()), float(df["longitude"].max())]

        # Send telemetry
        if client is not None:
            from exports.telemetry import send_processing_telemetry
            send_processing_telemetry(client, result, config)

        logger.info(
            "✓ %s — %d records, %dms", path.name, len(df), elapsed_ms
        )
        return result

    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        logger.error("✗ %s — FAILED: %s", path.name, e, exc_info=True)
        result.update({
            "status": "error",
            "error": str(e),
            "processing_time_ms": elapsed_ms,
        })
        return result


async def process_stream_batch(
    df: pd.DataFrame,
    config: "EdgeConfig",
    storage: "StorageBackend",
    client: Any = None,
) -> Dict[str, Any]:
    """Process a batch of records from the network stream.

    Parameters
    ----------
    df
        DataFrame with parsed sensor records.
    config
        Current edge configuration.
    storage
        Storage backend for writing output.
    client
        Optional IoT Hub module client for telemetry.

    Returns
    -------
    dict
        Processing result summary.
    """
    start_time = time.time()
    result: Dict[str, Any] = {"source": "stream", "status": "ok"}

    try:
        if df.empty:
            result["status"] = "skipped"
            result["reason"] = "empty batch"
            return result

        # Provider enrichment
        df = _enrich_with_provider(df, config.provider)

        # Deduplicate
        if "time" in df.columns:
            df = df.drop_duplicates(subset=["time"], keep="first")

        # Write GeoParquet with timestamp-based name
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        output_path = _make_output_path(config, f"stream_{ts}")
        storage.save_parquet(df, output_path)

        elapsed_ms = int((time.time() - start_time) * 1000)
        result.update({
            "record_count": len(df),
            "output_path": output_path,
            "processing_time_ms": elapsed_ms,
        })

        if "time" in df.columns:
            result["time_range"] = [
                df["time"].min().isoformat(),
                df["time"].max().isoformat(),
            ]
        if "latitude" in df.columns and "longitude" in df.columns:
            result["lat_range"] = [float(df["latitude"].min()), float(df["latitude"].max())]
            result["lon_range"] = [float(df["longitude"].min()), float(df["longitude"].max())]

        # Send telemetry
        if client is not None:
            from exports.telemetry import send_batch_summary
            send_batch_summary(client, result, config)

        logger.info("✓ Stream batch — %d records, %dms", len(df), elapsed_ms)
        return result

    except Exception as e:
        elapsed_ms = int((time.time() - start_time) * 1000)
        logger.error("✗ Stream batch FAILED: %s", e, exc_info=True)
        result.update({
            "status": "error",
            "error": str(e),
            "processing_time_ms": elapsed_ms,
        })
        return result
