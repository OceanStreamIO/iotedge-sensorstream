"""Stream simulator — replays sensor data files over TCP or UDP.

Reads NMEA (.txt) or CSV files and sends them line by line over a
network socket, simulating a serial-to-network bridge.

Usage::

    python -m simulate.stream_simulator \\
        --file ../sd-data-ingest/raw_data/r2r/RR2401_gnss_gp170_aft-2024-02-17.txt \\
        --protocol tcp --port 9100 --rate 10 --loop

    python -m simulate.stream_simulator \\
        --file ../sd-data-ingest/raw_data/emso/EMSO_OBSEA_CTD_30min.csv \\
        --protocol udp --port 9100 --format csv --rate 5
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

logger = logging.getLogger("sensorstream.simulator")


class StreamSimulator:
    """Replay a data file over TCP or UDP."""

    def __init__(
        self,
        file_path: Path,
        protocol: str = "tcp",
        host: str = "0.0.0.0",
        port: int = 9100,
        rate: float = 10.0,
        loop: bool = False,
        data_format: str = "auto",
    ):
        self.file_path = file_path
        self.protocol = protocol
        self.host = host
        self.port = port
        self.delay = 1.0 / rate if rate > 0 else 0
        self.loop = loop
        self.data_format = data_format
        self._running = False
        self._lines: list[str] = []

    def _load_lines(self) -> None:
        """Load lines from the data file."""
        with open(self.file_path, "r", encoding="utf-8", errors="replace") as f:
            self._lines = [line for line in f if line.strip()]

        if not self._lines:
            raise ValueError(f"No data lines in {self.file_path}")

        logger.info("Loaded %d lines from %s", len(self._lines), self.file_path)

    async def run(self) -> None:
        """Start the simulator."""
        self._load_lines()
        self._running = True

        if self.protocol == "tcp":
            await self._run_tcp()
        elif self.protocol == "udp":
            await self._run_udp()
        else:
            raise ValueError(f"Unsupported protocol: {self.protocol}")

    async def stop(self) -> None:
        self._running = False

    async def _run_tcp(self) -> None:
        """Run as TCP server — sends data to each connected client."""
        clients: list[asyncio.StreamWriter] = []

        async def handle_client(reader, writer):
            addr = writer.get_extra_info("peername")
            logger.info("TCP client connected: %s", addr)
            clients.append(writer)
            try:
                # Keep connection alive until client disconnects
                while self._running:
                    data = await reader.read(1024)
                    if not data:
                        break
            except Exception:
                pass
            finally:
                clients.remove(writer)
                writer.close()
                logger.info("TCP client disconnected: %s", addr)

        server = await asyncio.start_server(handle_client, self.host, self.port)
        logger.info("TCP simulator listening on %s:%d", self.host, self.port)
        logger.info("Waiting for client connection...")

        async with server:
            while self._running:
                # Wait for at least one client
                while not clients and self._running:
                    await asyncio.sleep(0.5)

                idx = 0
                while self._running:
                    line = self._lines[idx]
                    data = (line.rstrip("\n") + "\n").encode("utf-8")

                    disconnected = []
                    for writer in clients:
                        try:
                            writer.write(data)
                            await writer.drain()
                        except Exception:
                            disconnected.append(writer)

                    for w in disconnected:
                        if w in clients:
                            clients.remove(w)

                    await asyncio.sleep(self.delay)
                    idx += 1
                    if idx >= len(self._lines):
                        if self.loop:
                            idx = 0
                            logger.info("Looping — restarting from beginning")
                        else:
                            logger.info("All lines sent")
                            self._running = False
                            break

    async def _run_udp(self) -> None:
        """Run as UDP sender — broadcasts lines to localhost:port."""
        loop = asyncio.get_running_loop()

        class _Protocol(asyncio.DatagramProtocol):
            def connection_made(self, transport):
                pass

        transport, _ = await loop.create_datagram_endpoint(
            _Protocol, remote_addr=(self.host, self.port)
        )
        logger.info("UDP simulator sending to %s:%d", self.host, self.port)

        idx = 0
        while self._running:
            line = self._lines[idx]
            data = (line.rstrip("\n") + "\n").encode("utf-8")
            transport.sendto(data)
            await asyncio.sleep(self.delay)
            idx += 1
            if idx >= len(self._lines):
                if self.loop:
                    idx = 0
                    logger.info("Looping — restarting from beginning")
                else:
                    logger.info("All lines sent")
                    self._running = False

        transport.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay sensor data over TCP/UDP")
    parser.add_argument("--file", required=True, type=Path, help="Data file to replay")
    parser.add_argument("--protocol", default="tcp", choices=["tcp", "udp"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", default=9100, type=int)
    parser.add_argument("--rate", default=10.0, type=float, help="Lines per second")
    parser.add_argument("--loop", action="store_true", help="Loop when file is exhausted")
    parser.add_argument("--format", default="auto", dest="data_format",
                        choices=["auto", "nmea", "csv"])
    return parser.parse_args()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    args = parse_args()
    sim = StreamSimulator(
        file_path=args.file,
        protocol=args.protocol,
        host=args.host,
        port=args.port,
        rate=args.rate,
        loop=args.loop,
        data_format=args.data_format,
    )
    try:
        asyncio.run(sim.run())
    except KeyboardInterrupt:
        logger.info("Simulator stopped")


if __name__ == "__main__":
    main()
