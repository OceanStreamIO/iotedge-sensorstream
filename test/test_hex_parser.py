"""Tests for ingest/hex_parser.py — SeaBird .hex file parsing."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure the project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingest.hex_parser import (
    HAS_SBS,
    find_hex_group,
    parse_hdr_file,
    parse_hex_file,
    parse_xmlcon_file,
)

# Note: hex_dir and hex_file fixtures come from conftest.py
# (downloaded from Azure blob on first run, cached locally)


# -------------------------------------------------------------------
# Tests for find_hex_group()
# -------------------------------------------------------------------


class TestFindHexGroup:
    def test_finds_all_companions(self, hex_file):
        group = find_hex_group(hex_file)
        assert group["hex"] == hex_file
        assert group["hdr"] is not None
        assert group["hdr"].name.lower() == "11901.hdr"
        assert group["xmlcon"] is not None
        assert group["xmlcon"].name.lower() == "11901.xmlcon"

    def test_hex_only_returns_none_for_missing(self, tmp_path):
        fake_hex = tmp_path / "solo.hex"
        fake_hex.write_text("deadbeef")
        group = find_hex_group(fake_hex)
        assert group["hex"] == fake_hex
        assert group["hdr"] is None
        assert group["xmlcon"] is None


# -------------------------------------------------------------------
# Tests for parse_hdr_file()
# -------------------------------------------------------------------


class TestParseHdr:
    @pytest.fixture()
    def hdr_meta(self, hex_dir):
        return parse_hdr_file(hex_dir / "11901.hdr")

    def test_latitude_parsed(self, hdr_meta):
        assert "latitude" in hdr_meta
        assert abs(hdr_meta["latitude"] - 30.0) < 0.1  # 30 00.00 N

    def test_longitude_parsed(self, hdr_meta):
        assert "longitude" in hdr_meta
        assert hdr_meta["longitude"] < 0  # West → negative
        assert abs(hdr_meta["longitude"] - (-160.636)) < 0.01  # 160 38.16 W

    def test_start_time_parsed(self, hdr_meta):
        assert "start_time" in hdr_meta
        assert hdr_meta["start_time"].year == 2022
        assert hdr_meta["start_time"].month == 6

    def test_station_parsed(self, hdr_meta):
        assert hdr_meta.get("station") == "11901"

    def test_bytes_per_scan(self, hdr_meta):
        assert hdr_meta.get("bytes_per_scan") == 41


# -------------------------------------------------------------------
# Tests for parse_xmlcon_file()
# -------------------------------------------------------------------


class TestParseXmlcon:
    @pytest.fixture()
    def xmlcon_config(self, hex_dir):
        return parse_xmlcon_file(hex_dir / "11901.XMLCON")

    def test_sensors_found(self, xmlcon_config):
        assert len(xmlcon_config["sensors"]) > 0

    def test_first_sensor_is_temperature(self, xmlcon_config):
        first = xmlcon_config["sensors"][0]
        assert first["index"] == 0
        assert first["type"] == "TemperatureSensor"

    def test_has_calibration_coefficients(self, xmlcon_config):
        first = xmlcon_config["sensors"][0]
        coefs = first.get("coefficients", {})
        # Should have G, H, I, J for temperature
        for key in ("G", "H", "I", "J", "F0"):
            assert key in coefs, f"Missing coefficient {key}"

    def test_nmea_position_flag(self, xmlcon_config):
        assert xmlcon_config["nmea_position_added"] is True

    def test_scan_time_flag(self, xmlcon_config):
        assert xmlcon_config["scan_time_added"] is True


# -------------------------------------------------------------------
# Tests for parse_hex_file() — full integration
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_SBS, reason="seabirdscientific not installed")
class TestParseHexFile:
    @pytest.fixture(scope="class")
    def hex_df(self, hex_file):
        return parse_hex_file(hex_file)

    def test_returns_dataframe(self, hex_df):
        assert len(hex_df) > 0

    def test_has_scan_column(self, hex_df):
        assert "scan" in hex_df.columns
        assert hex_df["scan"].iloc[0] == 1

    def test_has_raw_frequency_columns(self, hex_df):
        assert "temperature_freq" in hex_df.columns
        assert "conductivity_freq" in hex_df.columns
        assert "pressure_freq" in hex_df.columns

    def test_has_calibrated_temperature(self, hex_df):
        assert "temperature" in hex_df.columns
        # Surface water around 30°N in June — should be in a sane range
        mean_t = hex_df["temperature"].mean()
        assert 0 < mean_t < 35, f"Mean temperature {mean_t} outside expected range"

    def test_has_calibrated_pressure(self, hex_df):
        assert "pressure" in hex_df.columns
        # Downcast — pressure should start near 0 and increase
        assert hex_df["pressure"].iloc[0] < hex_df["pressure"].iloc[-1]

    def test_has_calibrated_conductivity(self, hex_df):
        assert "conductivity" in hex_df.columns
        # Seawater conductivity typically 2-6 S/m
        mean_c = hex_df["conductivity"].mean()
        assert 1 < mean_c < 7, f"Mean conductivity {mean_c} outside expected range"

    def test_has_depth_column(self, hex_df):
        assert "depth" in hex_df.columns
        # Should be positive and increasing for a downcast
        assert hex_df["depth"].iloc[-1] > 0

    def test_has_salinity(self, hex_df):
        assert "salinity" in hex_df.columns
        # Cast includes deck/soak scans so mean can be low;
        # just check that peak salinity is in a sane ocean range
        max_s = hex_df["salinity"].max()
        assert 30 < max_s < 40, f"Max salinity {max_s} outside expected range"

    def test_has_latitude_longitude(self, hex_df):
        assert "latitude" in hex_df.columns
        assert "longitude" in hex_df.columns

    def test_has_station_metadata(self, hex_df):
        assert "station" in hex_df.columns
        assert hex_df["station"].iloc[0] == "11901"

    def test_scan_count_matches_hex_lines(self, hex_df):
        # The hex file has ~13k data lines (after header)
        # Just verify we got a substantial number of scans
        assert len(hex_df) > 10000


# -------------------------------------------------------------------
# Tests for pipeline integration with .hex files
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_SBS, reason="seabirdscientific not installed")
class TestPipelineHexIntegration:
    def test_detect_file_type_hex(self):
        from process.pipeline import _detect_file_type

        assert _detect_file_type(Path("cast.hex")) == "hex"
        assert _detect_file_type(Path("cast.cnv")) == "cnv"

    @pytest.mark.asyncio
    async def test_process_hex_file(self, tmp_path, hex_dir, hex_file):
        from azure_handler.storage import LocalStorage
        from config import EdgeConfig

        config = EdgeConfig.from_standalone(
            output_base_path=str(tmp_path),
        )
        storage = LocalStorage(str(tmp_path))

        from process.pipeline import process_file

        result = await process_file(str(hex_file), config, storage)

        assert result["status"] == "ok"
        assert result["record_count"] > 100
        assert result["file_type"] == "hex"

        # Verify parquet was written
        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify metadata was written
        meta_files = list(tmp_path.rglob("*.metadata.json"))
        assert len(meta_files) == 1
