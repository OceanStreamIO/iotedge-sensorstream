"""Tests for the stream and file simulators."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from simulate.file_simulator import FileSimulator
from simulate.stream_simulator import StreamSimulator


@pytest.mark.asyncio
async def test_file_simulator_drops_files(tmp_path: Path):
    """File simulator should copy data files to target directory."""
    source = tmp_path / "source"
    source.mkdir()
    (source / "test1.csv").write_text("time,lat\n2024-01-01,40\n")
    (source / "test2.csv").write_text("time,lat\n2024-01-01,41\n")
    (source / "readme.md").write_text("not data")

    target = tmp_path / "target"

    sim = FileSimulator(source_dir=source, target_dir=target, interval=0.1, loop=False)
    await sim.run()

    dropped = list(target.glob("*.csv"))
    assert len(dropped) == 2


@pytest.mark.asyncio
async def test_stream_simulator_loads_lines(tmp_path: Path):
    """Stream simulator should load lines from a file."""
    data_file = tmp_path / "test.txt"
    data_file.write_text(
        "$GPGGA,120000.00,5100.0000,N,00100.0000,E,1,8,1.0,50.0,M,45.0,M,,*5F\n"
        "$GPGGA,120001.00,5100.0001,N,00100.0001,E,1,8,1.0,50.0,M,45.0,M,,*52\n"
    )

    sim = StreamSimulator(
        file_path=data_file,
        protocol="tcp",
        host="127.0.0.1",
        port=19876,
        rate=100,
        loop=False,
    )
    sim._load_lines()
    assert len(sim._lines) == 2


@pytest.mark.asyncio
async def test_file_simulator_handles_no_data(tmp_path: Path):
    """File simulator should handle empty source directory gracefully."""
    source = tmp_path / "empty_source"
    source.mkdir()
    target = tmp_path / "target"

    sim = FileSimulator(source_dir=source, target_dir=target, interval=0.1, loop=False)
    await sim.run()  # Should not raise

    assert not target.exists() or len(list(target.iterdir())) == 0
