"""Unit tests for ingest/stream_parser.py."""

from __future__ import annotations

import pytest
import pandas as pd

from ingest.adapter import HAS_NMEA
from ingest.stream_parser import (
    detect_format,
    parse_csv_line,
    parse_nmea_line,
    records_to_dataframe,
)


# ------------------------------------------------------------------
# detect_format
# ------------------------------------------------------------------

class TestDetectFormat:
    def test_direct_nmea_sentence(self):
        assert detect_format("$GPGGA,235959.00,3242.3912,N,11714.1643,W,1,10,0.8,10.4,M,-34.3,M,,*66") == "nmea"

    def test_timestamped_nmea_sentence(self):
        line = "2024-02-17T00:00:00.110545Z $GPGGA,235959.00,3242.3912,N,11714.1643,W,1,10,0.8,10.4,M,-34.3,M,,*66"
        assert detect_format(line) == "nmea"

    def test_csv_with_header(self):
        assert detect_format("time,latitude,longitude,depth,TEMP") == "csv"

    def test_csv_data_line(self):
        assert detect_format("2023-06-01T00:00:00Z,41.18212,1.75257,20.0,17.5326") == "csv"

    def test_gn_prefix(self):
        assert detect_format("$GNGGA,120000.00,5100.0000,N,00100.0000,E,1,8,1.0,50.0,M,45.0,M,,*71") == "nmea"


# ------------------------------------------------------------------
# parse_nmea_line
# ------------------------------------------------------------------

@pytest.mark.skipif(not HAS_NMEA, reason="oceanstream NMEA parser not installed")
class TestParseNmeaLine:
    def test_gga_with_timestamp(self):
        line = "2024-02-17T00:00:01.346168Z $GPGGA,000000.00,3242.3916,N,11714.1640,W,1,10,0.8,10.2,M,-34.3,M,,*66"
        result = parse_nmea_line(line)
        assert result is not None
        assert "latitude" in result
        assert "longitude" in result
        assert result["latitude"] == pytest.approx(32.706527, abs=0.001)
        assert result["longitude"] == pytest.approx(-117.236067, abs=0.001)
        assert result["gps_quality"] == 1
        assert result["num_satellites"] == 10
        assert "2024-02-17" in result["time"]

    def test_rmc_speed_conversion(self):
        line = "2024-02-17T00:00:02.878324Z $GPRMC,000002.00,A,3242.3917,N,11714.1640,W,0.0,314.1,170224,11.0,E*77"
        result = parse_nmea_line(line)
        assert result is not None
        assert "speed_over_ground" in result
        assert result["speed_over_ground"] == pytest.approx(0.0, abs=0.01)
        assert "course_over_ground" in result

    def test_vtg_sentence(self):
        line = "$GPVTG,314.1,T,303.1,M,0.025,N,0.046,K*4D"
        result = parse_nmea_line(line)
        assert result is not None
        assert "speed_over_ground" in result
        # 0.025 knots * 0.514444 ≈ 0.01286 m/s
        assert result["speed_over_ground"] == pytest.approx(0.01286, abs=0.001)
        assert result["course_over_ground"] == pytest.approx(314.1)

    def test_raw_nmea_without_timestamp(self):
        line = "$GPGGA,120000.00,5100.0000,N,00100.0000,E,1,8,1.0,50.0,M,45.0,M,,*67"
        result = parse_nmea_line(line)
        assert result is not None
        assert "time" in result
        assert "latitude" in result

    def test_empty_line_returns_none(self):
        assert parse_nmea_line("") is None
        assert parse_nmea_line("   ") is None

    def test_malformed_nmea_returns_none(self):
        assert parse_nmea_line("not a valid nmea sentence") is None

    def test_zda_extracts_gps_time(self):
        line = "2024-02-17T00:00:00.343499Z $GPZDA,000000.00,17,02,2024,00,00*66"
        result = parse_nmea_line(line)
        assert result is not None
        assert "gps_utc_time" in result
        assert "2024-02-17" in result["time"]


# ------------------------------------------------------------------
# parse_csv_line
# ------------------------------------------------------------------

class TestParseCsvLine:
    def test_basic_csv(self):
        headers = ["time", "latitude", "longitude", "depth", "TEMP", "PSAL", "CNDC"]
        line = "2023-06-01T00:00:00Z,41.18212,1.75257,20.0,17.5326,37.7394,4.8496"
        result = parse_csv_line(line, headers)
        assert result is not None
        assert result["TEMP"] == pytest.approx(17.5326)
        assert result["latitude"] == pytest.approx(41.18212)

    def test_column_mismatch_returns_none(self):
        headers = ["a", "b", "c"]
        assert parse_csv_line("1,2", headers) is None

    def test_empty_line_returns_none(self):
        assert parse_csv_line("", ["a"]) is None

    def test_string_value_preserved(self):
        headers = ["name", "value"]
        result = parse_csv_line("sensor_1,hello", headers)
        assert result is not None
        assert result["name"] == "sensor_1"
        assert result["value"] == "hello"


# ------------------------------------------------------------------
# records_to_dataframe
# ------------------------------------------------------------------

class TestRecordsToDataframe:
    def test_empty_list(self):
        df = records_to_dataframe([])
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_with_records(self):
        records = [
            {"time": "2024-01-01T00:00:00Z", "latitude": 40.0, "longitude": -70.0},
            {"time": "2024-01-01T00:01:00Z", "latitude": 40.1, "longitude": -70.1},
        ]
        df = records_to_dataframe(records)
        assert len(df) == 2
        assert df["latitude"].dtype == float
        assert pd.api.types.is_datetime64_any_dtype(df["time"])

    def test_sorts_by_time(self):
        records = [
            {"time": "2024-01-01T00:01:00Z", "latitude": 40.1},
            {"time": "2024-01-01T00:00:00Z", "latitude": 40.0},
        ]
        df = records_to_dataframe(records)
        assert df.iloc[0]["latitude"] == pytest.approx(40.0)
