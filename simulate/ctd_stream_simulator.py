"""CTD hex stream simulator — replays .hex scan data over TCP.

Simulates the output of a SeaBird SBE 11plus deck unit connected
via a serial-to-TCP bridge.  Each scan line in the ``.hex`` file is
a fixed-length hexadecimal string (e.g. 82 chars for 41 bytes/scan)
terminated by CR/LF, transmitted at the instrument sampling rate
(24 Hz for an SBE 911plus).

**How SeaBird CTD instruments transmit live data**

The SBE 911plus CTD samples at 24 Hz.  The underwater unit sends raw
frequency counts and voltage readings up the conduction cable to the
SBE 11plus V2 deck unit, which:

1. Digitises the analog telemetry signal.
2. Outputs each scan as a hex-encoded binary string on its RS-232
   serial port (configurable baud: 9600–115200).
3. A serial-to-TCP bridge (Moxa NPort, Digi Connect, or software
   bridge like ``socat``) forwards these lines to the network.
4. Seasave V7 (acquisition software) can also re-broadcast the raw
   stream over TCP for third-party consumers.

Each hex line encodes:
- 3 frequency channels (T, C, P) × 3 bytes each
- 0–2 secondary frequency channels × 3 bytes
- 0–8 voltage channels × 12 bits each (packed)
- Optional: NMEA position (lat/lon packed BCD)
- Optional: system time (4 bytes, seconds since midnight)
- Status/modulo byte

The line length in hex characters = ``Number of Bytes Per Scan × 2``.

This simulator:
- Reads a real ``.hex`` file (skips the header before ``*END*``).
- Sends lines over TCP at a configurable rate (default 24 Hz).
- Supports ``--rate`` to slow down for testing (e.g. 1 Hz).
- Sends the ``.XMLCON`` content as a preamble header so the receiver
  knows the sensor configuration.

Usage::

    python -m simulate.ctd_stream_simulator \\
        --hex test/test_data/ctd/hex/11901.hex \\
        --port 9200 --rate 24 --loop

    # Slow replay for testing (1 scan/sec):
    python -m simulate.ctd_stream_simulator \\
        --hex test/test_data/ctd/hex/11901.hex \\
        --port 9200 --rate 1
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger("sensorstream.simulator.ctd")


class CTDStreamSimulator:
    """Replay ``.hex`` scan lines over TCP, simulating a deck unit stream.

    Parameters
    ----------
    hex_file
        Path to a SeaBird ``.hex`` file.
    host, port
        TCP listen address.
    rate
        Scans per second (default 24.0 = real SBE 911plus rate).
    loop
        Restart from the beginning after reaching the last scan.
    send_header
        If ``True``, send the hex file's header block (up to ``*END*``)
        and any found ``.XMLCON`` content as a preamble to each new
        TCP client connection.  This lets the receiver auto-configure.
    """

    def __init__(
        self,
        hex_file: Path,
        host: str = "0.0.0.0",
        port: int = 9200,
        rate: float = 24.0,
        loop: bool = False,
        send_header: bool = True,
    ):
        self.hex_file = hex_file
        self.host = host
        self.port = port
        self.delay = 1.0 / rate if rate > 0 else 0
        self.loop = loop
        self.send_header = send_header
        self._running = False
        self._header_lines: list[str] = []
        self._scan_lines: list[str] = []
        self._xmlcon_content: str = ""

    def _load(self) -> None:
        """Load hex file, splitting header from scan data."""
        in_header = True
        with open(self.hex_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if in_header:
                    self._header_lines.append(line.rstrip("\n"))
                    if line.strip() == "*END*":
                        in_header = False
                else:
                    stripped = line.strip()
                    if stripped:
                        self._scan_lines.append(stripped)

        if not self._scan_lines:
            raise ValueError(f"No scan lines found in {self.hex_file}")

        logger.info(
            "Loaded %d header lines + %d scan lines from %s",
            len(self._header_lines), len(self._scan_lines), self.hex_file.name,
        )

        # Try to find companion XMLCON
        from ingest.hex_parser import find_hex_group

        group = find_hex_group(self.hex_file)
        if group["xmlcon"] is not None:
            self._xmlcon_content = group["xmlcon"].read_text(encoding="utf-8", errors="replace")
            logger.info("Found XMLCON: %s", group["xmlcon"].name)

    async def run(self) -> None:
        """Start the TCP server and replay scans to connected clients."""
        self._load()
        self._running = True
        clients: list[asyncio.StreamWriter] = []

        async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            addr = writer.get_extra_info("peername")
            logger.info("CTD client connected: %s", addr)
            clients.append(writer)

            # Send preamble header
            if self.send_header:
                preamble = self._build_preamble()
                try:
                    writer.write(preamble.encode("utf-8"))
                    await writer.drain()
                except Exception:
                    pass

            try:
                while self._running:
                    data = await reader.read(1024)
                    if not data:
                        break
            except Exception:
                pass
            finally:
                if writer in clients:
                    clients.remove(writer)
                writer.close()
                logger.info("CTD client disconnected: %s", addr)

        server = await asyncio.start_server(handle_client, self.host, self.port)
        logger.info("CTD hex stream simulator on %s:%d (%.1f Hz)", self.host, self.port, 1 / self.delay if self.delay else 0)
        logger.info("Waiting for client connection…")

        async with server:
            idx = 0
            while self._running:
                # Wait for at least one client
                while not clients and self._running:
                    await asyncio.sleep(0.2)

                while self._running and clients:
                    line = self._scan_lines[idx] + "\r\n"
                    data = line.encode("utf-8")

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
                    if idx >= len(self._scan_lines):
                        if self.loop:
                            idx = 0
                            logger.info("Looping — restarting from scan 0")
                        else:
                            logger.info("All %d scans sent", len(self._scan_lines))
                            self._running = False
                            break

    async def stop(self) -> None:
        self._running = False

    def _build_preamble(self) -> str:
        """Build a preamble string for new TCP connections."""
        parts = []
        # Hex file header (metadata + *END*)
        if self._header_lines:
            parts.extend(self._header_lines)
        # XMLCON as commented block
        if self._xmlcon_content:
            parts.append("# XMLCON_START")
            for xmlline in self._xmlcon_content.splitlines():
                parts.append(f"# {xmlline}")
            parts.append("# XMLCON_END")
        parts.append("")  # trailing newline
        return "\r\n".join(parts) + "\r\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay SeaBird .hex scans over TCP (simulates deck unit serial stream)"
    )
    parser.add_argument("--hex", required=True, type=Path, help=".hex file to replay")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9200)
    parser.add_argument("--rate", type=float, default=24.0, help="Scans per second (default: 24)")
    parser.add_argument("--loop", action="store_true", help="Loop after reaching end of file")
    parser.add_argument("--no-header", action="store_true", help="Don't send preamble header")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    sim = CTDStreamSimulator(
        hex_file=args.hex,
        host=args.host,
        port=args.port,
        rate=args.rate,
        loop=args.loop,
        send_header=not args.no_header,
    )
    try:
        asyncio.run(sim.run())
    except KeyboardInterrupt:
        logger.info("Simulator stopped")


if __name__ == "__main__":
    main()
