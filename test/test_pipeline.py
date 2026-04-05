"""Integration tests for the processing pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from azure_handler.storage import LocalStorage
from config import EdgeConfig
from process.pipeline import process_file


@pytest.fixture
def config(tmp_path: Path) -> EdgeConfig:
    return EdgeConfig.from_standalone(
        campaign_id="test_campaign",
        output_base_path=str(tmp_path),
        provider="auto",
    )


@pytest.fixture
def storage(tmp_path: Path) -> LocalStorage:
    return LocalStorage(base_path=str(tmp_path / "test_campaign"))


# ------------------------------------------------------------------
# CSV file processing
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_csv_file(config, storage, tmp_path):
    """Process a simple CSV file and verify GeoParquet output."""
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(
        "time,latitude,longitude,depth,TEMP,PSAL,CNDC\n"
        "2023-06-01T00:00:00Z,41.18,1.75,20.0,17.5,37.7,4.8\n"
        "2023-06-01T00:30:00Z,41.18,1.75,20.0,17.4,37.7,4.8\n"
        "2023-06-01T01:00:00Z,41.18,1.75,20.0,17.3,37.7,4.8\n"
    )

    result = await process_file(str(csv_file), config, storage)

    assert result["status"] == "ok"
    assert result["record_count"] == 3
    assert "output_path" in result
    assert "time_range" in result


@pytest.mark.asyncio
async def test_process_empty_csv(config, storage, tmp_path):
    """Processing an empty CSV should return skipped status."""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("time,latitude,longitude\n")

    result = await process_file(str(csv_file), config, storage)

    assert result["status"] == "skipped"


# ------------------------------------------------------------------
# NMEA file processing
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_process_nmea_file(config, storage, tmp_path):
    """Process a small NMEA .txt file."""
    nmea_file = tmp_path / "test_nmea.txt"
    nmea_file.write_text(
        "2024-02-17T00:00:00.110Z $GPGGA,235959.00,3242.3912,N,11714.1643,W,1,10,0.8,10.4,M,-34.3,M,,*66\n"
        "2024-02-17T00:00:01.110Z $GPGGA,000000.00,3242.3916,N,11714.1640,W,1,10,0.8,10.2,M,-34.3,M,,*66\n"
        "2024-02-17T00:00:02.068Z $GPGGA,000001.00,3242.3916,N,11714.1639,W,1,10,0.8,10.1,M,-34.3,M,,*6A\n"
    )

    result = await process_file(str(nmea_file), config, storage)

    assert result["status"] == "ok"
    assert result["record_count"] == 3
    assert result["file_type"] == "nmea"
    assert "lat_range" in result


# ------------------------------------------------------------------
# Metadata output
# ------------------------------------------------------------------

@pytest.mark.asyncio
async def test_metadata_written(config, storage, tmp_path):
    """Verify that metadata.json is written alongside parquet."""
    csv_file = tmp_path / "meta_test.csv"
    csv_file.write_text(
        "time,latitude,longitude,TEMP\n"
        "2023-06-01T00:00:00Z,41.18,1.75,17.5\n"
    )

    result = await process_file(str(csv_file), config, storage)
    assert result["status"] == "ok"

    # Find metadata files
    meta_files = list((tmp_path / "test_campaign").rglob("*.metadata.json"))
    assert len(meta_files) >= 1

    meta = json.loads(meta_files[0].read_text())
    assert meta["record_count"] == 1
    assert "sensor_stats" in meta
    assert "TEMP" in meta["sensor_stats"]
