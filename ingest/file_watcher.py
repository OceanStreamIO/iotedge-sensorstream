"""File system watcher for sensor data files.

Monitors a directory for new files matching configurable glob patterns.
Supports two observation modes:
- Native OS events (inotify/FSEvents) via watchdog — fast, for local disks
- Polling — reliable for SMB/network-mounted drives

When a new file is detected it waits for the file to stabilise (no size
change for 2 seconds) before queuing it for processing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from config import EdgeConfig

logger = logging.getLogger("sensorstream")

# How long a file must be stable (no size change) before processing
_STABILISE_SECONDS = 2.0


class FileWatcher:
    """Watch a directory for new sensor data files and queue them."""

    def __init__(self, config: "EdgeConfig", queue: asyncio.Queue):
        self.config = config
        self.queue = queue
        self._seen: set[str] = set()
        self._observer = None
        self._running = False

    async def start(self) -> None:
        """Start watching ``config.watch_dir``."""
        from watchdog.observers import Observer
        from watchdog.observers.polling import PollingObserver
        from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

        watch_path = Path(self.config.watch_dir)
        if not watch_path.exists():
            watch_path.mkdir(parents=True, exist_ok=True)
            logger.info("Created watch directory: %s", watch_path)

        patterns = [p.strip() for p in self.config.watch_patterns.split(",")]
        loop = asyncio.get_running_loop()

        class _Handler(FileSystemEventHandler):
            def __init__(self, watcher: FileWatcher):
                self._watcher = watcher

            def on_created(self, event):
                if not event.is_directory:
                    loop.call_soon_threadsafe(
                        asyncio.ensure_future,
                        self._watcher._on_new_file(event.src_path, patterns),
                    )

            def on_moved(self, event):
                if not event.is_directory:
                    loop.call_soon_threadsafe(
                        asyncio.ensure_future,
                        self._watcher._on_new_file(event.dest_path, patterns),
                    )

        handler = _Handler(self)

        if self.config.watch_polling:
            self._observer = PollingObserver(timeout=self.config.watch_poll_interval)
        else:
            self._observer = Observer()

        self._observer.schedule(handler, str(watch_path), recursive=True)
        self._observer.start()
        self._running = True

        logger.info(
            "File watcher started: dir=%s, patterns=%s, polling=%s",
            watch_path, patterns, self.config.watch_polling,
        )

    async def stop(self) -> None:
        """Stop the file watcher."""
        self._running = False
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
        logger.info("File watcher stopped")

    async def scan_existing(self) -> int:
        """Scan watch_dir for existing files and queue them.

        Returns the number of files queued.
        """
        watch_path = Path(self.config.watch_dir)
        if not watch_path.exists():
            return 0

        patterns = [p.strip() for p in self.config.watch_patterns.split(",")]
        count = 0
        for pattern in patterns:
            for file_path in sorted(watch_path.rglob(pattern)):
                if file_path.is_file() and str(file_path) not in self._seen:
                    self._seen.add(str(file_path))
                    await self.queue.put(("file", str(file_path)))
                    count += 1

        if count:
            logger.info("Queued %d existing files from %s", count, watch_path)
        return count

    async def _on_new_file(self, file_path: str, patterns: list[str]) -> None:
        """Handle a newly detected file: wait for stability, then queue."""
        path = Path(file_path)

        # Check against patterns
        if not any(path.match(p) for p in patterns):
            return

        if str(path) in self._seen:
            return

        # Wait for the file to stabilise
        if not await self._wait_stable(path):
            return

        self._seen.add(str(path))
        await self.queue.put(("file", str(path)))
        logger.info("Queued new file: %s", path.name)

    @staticmethod
    async def _wait_stable(path: Path, timeout: float = 30.0) -> bool:
        """Wait until a file's size stops changing."""
        start = time.monotonic()
        prev_size = -1
        stable_since = 0.0

        while time.monotonic() - start < timeout:
            try:
                size = path.stat().st_size
            except OSError:
                return False

            if size == prev_size and size > 0:
                if time.monotonic() - stable_since >= _STABILISE_SECONDS:
                    return True
            else:
                prev_size = size
                stable_since = time.monotonic()

            await asyncio.sleep(0.5)

        logger.warning("File did not stabilise within %ds: %s", timeout, path)
        return False
