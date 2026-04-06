"""File simulator — drops sensor data files into a watch directory.

Copies files from a source directory into a target directory at
configurable intervals, simulating new file arrivals (e.g. from an
instrument writing to a network share).

Usage::

    python -m simulate.file_simulator \\
        --source-dir ../oceanstream-cli/raw_data/emso \\
        --target-dir /tmp/sensorstream-watch \\
        --interval 10

    python -m simulate.file_simulator \\
        --source-dir ../oceanstream-cli/raw_data/r2r \\
        --target-dir /tmp/sensorstream-watch \\
        --interval 5 --loop
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import shutil
from pathlib import Path

logger = logging.getLogger("sensorstream.simulator")

# File types we'll copy
_DATA_EXTENSIONS = {".csv", ".txt", ".hex", ".cnv", ".tar.gz", ".tgz", ".geocsv"}


class FileSimulator:
    """Copy data files into a target directory at timed intervals."""

    def __init__(
        self,
        source_dir: Path,
        target_dir: Path,
        interval: float = 10.0,
        loop: bool = False,
    ):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.interval = interval
        self.loop = loop
        self._running = False

    def _discover_files(self) -> list[Path]:
        """Find data files in the source directory."""
        files = []
        for f in sorted(self.source_dir.rglob("*")):
            if f.is_file():
                name = f.name.lower()
                if any(name.endswith(ext) for ext in _DATA_EXTENSIONS):
                    files.append(f)
        return files

    async def run(self) -> None:
        """Start dropping files."""
        self.target_dir.mkdir(parents=True, exist_ok=True)
        files = self._discover_files()

        if not files:
            logger.error("No data files found in %s", self.source_dir)
            return

        logger.info(
            "File simulator: %d files, interval=%.1fs, target=%s",
            len(files), self.interval, self.target_dir,
        )

        self._running = True
        idx = 0

        while self._running:
            src = files[idx]
            dest = self.target_dir / src.name

            # Avoid overwriting — add counter suffix if needed
            if dest.exists():
                stem = src.stem
                suffix = src.suffix
                counter = 1
                while dest.exists():
                    dest = self.target_dir / f"{stem}_{counter}{suffix}"
                    counter += 1

            shutil.copy2(src, dest)
            logger.info("Dropped file %d/%d: %s → %s", idx + 1, len(files), src.name, dest.name)

            idx += 1
            if idx >= len(files):
                if self.loop:
                    idx = 0
                    logger.info("Looping — restarting from beginning")
                else:
                    logger.info("All files dropped")
                    break

            await asyncio.sleep(self.interval)

    async def stop(self) -> None:
        self._running = False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drop sensor data files into a watch directory")
    parser.add_argument("--source-dir", required=True, type=Path, help="Directory with source data files")
    parser.add_argument("--target-dir", required=True, type=Path, help="Target watch directory")
    parser.add_argument("--interval", default=10.0, type=float, help="Seconds between file drops")
    parser.add_argument("--loop", action="store_true", help="Loop when all files have been dropped")
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    args = parse_args()
    sim = FileSimulator(
        source_dir=args.source_dir,
        target_dir=args.target_dir,
        interval=args.interval,
        loop=args.loop,
    )
    try:
        asyncio.run(sim.run())
    except KeyboardInterrupt:
        logger.info("File simulator stopped")


if __name__ == "__main__":
    main()
