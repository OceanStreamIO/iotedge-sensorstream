"""Write metadata JSON alongside processed output."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd
    from azure_handler.storage import StorageBackend

logger = logging.getLogger("sensorstream")


def build_metadata(
    df: "pd.DataFrame",
    source_name: str,
) -> dict[str, Any]:
    """Build a metadata dict describing a processed dataset."""
    meta: dict[str, Any] = {
        "source_file": source_name,
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(df),
        "columns": list(df.columns),
    }

    if "time" in df.columns and not df["time"].isna().all():
        meta["time_range"] = {
            "start": df["time"].min().isoformat(),
            "end": df["time"].max().isoformat(),
        }

    if "latitude" in df.columns and "longitude" in df.columns:
        valid = df["latitude"].notna() & df["longitude"].notna()
        if valid.any():
            meta["spatial_extent"] = {
                "lat_min": float(df.loc[valid, "latitude"].min()),
                "lat_max": float(df.loc[valid, "latitude"].max()),
                "lon_min": float(df.loc[valid, "longitude"].min()),
                "lon_max": float(df.loc[valid, "longitude"].max()),
            }

    # Summary stats for numeric sensor columns
    sensor_cols = [c for c in ("TEMP", "PSAL", "CNDC", "depth",
                                "speed_over_ground", "gps_antenna_height") if c in df.columns]
    if sensor_cols:
        stats = {}
        for col in sensor_cols:
            series = df[col].dropna()
            if not series.empty:
                stats[col] = {
                    "min": float(series.min()),
                    "max": float(series.max()),
                    "mean": float(series.mean()),
                }
        meta["sensor_stats"] = stats

    return meta


def write_metadata(
    df: "pd.DataFrame",
    source_name: str,
    output_path: str,
    storage: "StorageBackend",
) -> str:
    """Build and persist metadata JSON."""
    meta = build_metadata(df, source_name)
    data = json.dumps(meta, indent=2, default=str).encode("utf-8")
    return storage.save_file(data, output_path)
