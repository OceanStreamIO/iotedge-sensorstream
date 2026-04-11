"""Sensor data processing pipeline.

Two processing paths:
- **File-based**: Reads a sensor data file (CSV, NMEA .txt, CTD .hex/.cnv,
  or .tar.gz archive), parses it via the oceanstream adapter, optionally
  enriches via an oceanstream provider, writes GeoParquet, and emits telemetry.
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

from ingest.adapter import (
    HAS_AD2CP,
    HAS_ADCP,
    HAS_CTD,
    enrich_with_provider,
    parse_ad2cp_file,
    parse_adcp_file,
    parse_cnv_file,
    parse_csv_file,
    parse_hex_file,
    parse_nmea_file,
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
_AD2CP_EXTENSIONS = {".ad2cp"}
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
    if suffix in _AD2CP_EXTENSIONS:
        return "ad2cp"
    return "csv"  # default fallback


def _parse_hex_or_fallback(file_path: Path) -> pd.DataFrame:
    """Parse a .hex file via oceanstream, or skip gracefully."""
    if not HAS_CTD:
        logger.warning(
            "oceanstream CTD parser not available — cannot parse .hex file: %s. "
            "Install with: pip install oceanstream[geotrack]",
            file_path.name,
        )
        return pd.DataFrame()
    return parse_hex_file(file_path)


def _parse_adcp_or_fallback(file_path: Path) -> pd.DataFrame:
    """Parse an ADCP .raw file via oceanstream, or skip gracefully."""
    if not HAS_ADCP:
        logger.warning(
            "oceanstream ADCP parser not available — cannot parse .raw file: %s. "
            "Install with: pip install oceanstream[adcp]",
            file_path.name,
        )
        return pd.DataFrame()
    return parse_adcp_file(file_path)


def _parse_ad2cp_or_fallback(file_path: Path) -> pd.DataFrame:
    """Parse a Nortek AD2CP .ad2cp file via oceanstream, or skip gracefully."""
    if not HAS_AD2CP:
        logger.warning(
            "oceanstream AD2CP parser not available — cannot parse .ad2cp file: %s. "
            "Install with: pip install oceanstream[adcp]",
            file_path.name,
        )
        return pd.DataFrame()
    return parse_ad2cp_file(file_path)


def _extract_archive(archive_path: Path) -> tuple[list[Path], str]:
    """Extract a .tar.gz archive and return paths to data files.

    Returns
    -------
    tuple[list[Path], str]
        (extracted file paths, temp directory path to clean up later)
    """
    tmpdir = tempfile.mkdtemp(prefix="sensorstream_")
    extracted = []

    with tarfile.open(archive_path, "r:gz") as tar:
        for member in tar.getmembers():
            # Security: reject absolute paths, traversal, symlinks, hardlinks
            if (
                member.name.startswith("/")
                or ".." in member.name
                or member.issym()
                or member.islnk()
            ):
                logger.warning("Skipping suspicious archive member: %s", member.name)
                continue
            # Ensure resolved path stays inside tmpdir
            target = Path(tmpdir).joinpath(member.name).resolve()
            if not str(target).startswith(str(Path(tmpdir).resolve())):
                logger.warning("Path traversal in archive member: %s", member.name)
                continue
            tar.extract(member, tmpdir)
            if member.isfile():
                suffix = target.suffix.lower()
                if suffix in (_NMEA_EXTENSIONS | _CSV_EXTENSIONS | _CTD_EXTENSIONS | _ADCP_EXTENSIONS | _AD2CP_EXTENSIONS | {".hdr", ".xmlcon"}):
                    extracted.append(target)

    logger.info("Extracted %d data files from %s", len(extracted), archive_path.name)
    return extracted, tmpdir


def _enrich_with_provider(
    df: pd.DataFrame, provider_name: str, file_path: "Path | None" = None
) -> pd.DataFrame:
    """Optionally enrich a DataFrame using an oceanstream provider."""
    return enrich_with_provider(df, provider_name, file_path=file_path)


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
            extracted, tmpdir = _extract_archive(path)
            try:
                all_dfs = []
                for extracted_file in extracted:
                    sub_type = _detect_file_type(extracted_file)
                    if sub_type == "nmea":
                        all_dfs.append(parse_nmea_file(extracted_file))
                    elif sub_type == "csv":
                        all_dfs.append(parse_csv_file(extracted_file))
                    elif sub_type == "hex":
                        all_dfs.append(_parse_hex_or_fallback(extracted_file))
                    elif sub_type == "cnv":
                        all_dfs.append(parse_cnv_file(extracted_file))
                    elif sub_type == "adcp":
                        all_dfs.append(_parse_adcp_or_fallback(extracted_file))
                    elif sub_type == "ad2cp":
                        all_dfs.append(_parse_ad2cp_or_fallback(extracted_file))
                df = pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
            finally:
                import shutil
                shutil.rmtree(tmpdir, ignore_errors=True)
        elif file_type == "nmea":
            df = parse_nmea_file(path)
        elif file_type == "hex":
            df = _parse_hex_or_fallback(path)
        elif file_type == "cnv":
            df = parse_cnv_file(path)
        elif file_type == "adcp":
            df = _parse_adcp_or_fallback(path)
        elif file_type == "ad2cp":
            df = _parse_ad2cp_or_fallback(path)
        else:
            df = parse_csv_file(path)

        if df.empty:
            result["status"] = "skipped"
            result["reason"] = "no records parsed"
            return result

        # Provider enrichment
        df = _enrich_with_provider(df, config.provider, file_path=path)

        # Deduplicate (skip for ADCP/AD2CP — multiple depth cells per timestamp)
        if "time" in df.columns and file_type not in ("adcp", "ad2cp"):
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
