"""Unit tests for ingest/file_watcher.py."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from ingest.file_watcher import FileWatcher


class _FakeConfig:
    """Minimal config stub for file watcher tests."""

    def __init__(self, watch_dir: str):
        self.watch_dir = watch_dir
        self.watch_patterns = "*.csv,*.txt"
        self.watch_polling = True
        self.watch_poll_interval = 1


@pytest.mark.asyncio
async def test_scan_existing_finds_files(tmp_path: Path):
    """scan_existing should queue files matching watch_patterns."""
    (tmp_path / "data1.csv").write_text("time,lat\n2024-01-01,40\n")
    (tmp_path / "data2.txt").write_text("$GPGGA,120000.00\n")
    (tmp_path / "readme.md").write_text("not a data file")

    config = _FakeConfig(str(tmp_path))
    queue: asyncio.Queue = asyncio.Queue()
    watcher = FileWatcher(config, queue)

    count = await watcher.scan_existing()
    assert count == 2
    assert queue.qsize() == 2

    items = []
    while not queue.empty():
        items.append(await queue.get())
    assert all(t == "file" for t, _ in items)


@pytest.mark.asyncio
async def test_scan_existing_skips_if_dir_missing():
    """scan_existing should return 0 if the directory doesn't exist."""
    config = _FakeConfig("/nonexistent/path/abc123")
    queue: asyncio.Queue = asyncio.Queue()
    watcher = FileWatcher(config, queue)

    count = await watcher.scan_existing()
    assert count == 0


@pytest.mark.asyncio
async def test_wait_stable(tmp_path: Path):
    """_wait_stable should return True once file stops changing."""
    f = tmp_path / "test.csv"
    f.write_text("header\n")

    result = await FileWatcher._wait_stable(f, timeout=5.0)
    assert result is True


@pytest.mark.asyncio
async def test_wait_stable_empty_file(tmp_path: Path):
    """_wait_stable should not consider an empty file as stable."""
    f = tmp_path / "empty.csv"
    f.write_text("")

    result = await FileWatcher._wait_stable(f, timeout=3.0)
    assert result is False
