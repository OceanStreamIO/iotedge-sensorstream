"""Tests for CTD hex stream simulator and hex stream listener."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingest.stream_parser import detect_format
from simulate.ctd_stream_simulator import CTDStreamSimulator

# Note: hex_dir and hex_file fixtures come from conftest.py
# (downloaded from Azure blob on first run, cached locally)


# -------------------------------------------------------------------
# Format detection
# -------------------------------------------------------------------


class TestHexFormatDetection:
    def test_detect_hex_scan_line(self):
        # Real scan line from the hex file (82 chars = 41 bytes)
        line = "11F0B90B4F8581A7991253E00A5A5AFF30AFBFA9F501AFFFB64FFF16E3607A8E38409122938073AB62"
        assert detect_format(line) == "hex"

    def test_detect_hex_short_line_is_csv(self):
        # Too short to be a hex scan
        assert detect_format("ABCDEF0123") == "csv"

    def test_detect_nmea_not_hex(self):
        assert detect_format("$GPGGA,123456,3000.0,N,16038.16,W,1,12,0.8,0,M,,,,*67") == "nmea"

    def test_detect_header_line(self):
        # Header lines start with * — treated as csv (ignored)
        assert detect_format("* Sea-Bird SBE 9 Data File:") == "csv"

    def test_detect_comment_line(self):
        assert detect_format("# XMLCON_START") == "csv"


# -------------------------------------------------------------------
# CTD Stream Simulator
# -------------------------------------------------------------------


class TestCTDStreamSimulator:
    def test_load_separates_header_and_scans(self, hex_file):
        sim = CTDStreamSimulator(hex_file=hex_file, rate=1)
        sim._load()
        assert len(sim._header_lines) > 0
        assert len(sim._scan_lines) > 10000
        # Header should end with *END*
        assert sim._header_lines[-1].strip() == "*END*"

    def test_finds_xmlcon(self, hex_file):
        sim = CTDStreamSimulator(hex_file=hex_file, rate=1)
        sim._load()
        assert len(sim._xmlcon_content) > 0
        assert "SBE_InstrumentConfiguration" in sim._xmlcon_content

    def test_preamble_contains_header_and_xmlcon(self, hex_file):
        sim = CTDStreamSimulator(hex_file=hex_file, rate=1)
        sim._load()
        preamble = sim._build_preamble()
        assert "Sea-Bird" in preamble
        assert "# XMLCON_START" in preamble
        assert "# XMLCON_END" in preamble

    @pytest.mark.asyncio
    async def test_tcp_sends_scans(self, hex_file):
        """Start simulator, connect a client, and verify we receive hex lines."""
        sim = CTDStreamSimulator(
            hex_file=hex_file,
            host="127.0.0.1",
            port=19200,  # Use high port to avoid conflicts
            rate=100,  # Fast for testing
            loop=False,
            send_header=False,  # Skip header for simpler test
        )

        server_task = asyncio.ensure_future(sim.run())
        await asyncio.sleep(0.3)  # Let server start

        # Connect client
        reader, writer = await asyncio.open_connection("127.0.0.1", 19200)

        # Read a few lines
        lines_received = []
        for _ in range(10):
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line:
                lines_received.append(line.decode("utf-8").strip())

        writer.close()
        await sim.stop()

        assert len(lines_received) == 10
        # Each line should be a valid hex scan
        for line in lines_received:
            assert all(c in "0123456789ABCDEFabcdef" for c in line), f"Non-hex: {line[:40]}"
            assert len(line) >= 40
