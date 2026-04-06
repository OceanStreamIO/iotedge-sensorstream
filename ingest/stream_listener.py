"""Network stream listener for TCP and UDP sensor data.

Listens on a configurable port for incoming sensor data (NMEA sentences
or CSV lines), buffers records, and queues batched DataFrames for
processing when either ``batch_interval_seconds`` or ``batch_max_records``
is reached.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from ingest.adapter import detect_format, parse_csv_line, parse_nmea_line, records_to_dataframe

# Hex stream support
import re

_HEX_SCAN_RE = re.compile(r"^[0-9A-Fa-f]{40,}$")

if TYPE_CHECKING:
    from config import EdgeConfig

logger = logging.getLogger("sensorstream")


class StreamListener:
    """Async TCP/UDP listener that parses sensor data lines and batches them."""

    def __init__(
        self,
        config: "EdgeConfig",
        queue: asyncio.Queue,
    ):
        self.config = config
        self.queue = queue

        self._records: list[dict[str, Any]] = []
        self._csv_headers: list[str] | None = None
        self._detected_format: str | None = None
        self._last_flush = time.monotonic()
        self._running = False
        self._servers: list[Any] = []
        self._transports: list[Any] = []

        # Hex stream state
        self._hex_header_lines: list[str] = []
        self._hex_scan_lines: list[str] = []
        self._hex_header_done = False
        self._hex_xmlcon_lines: list[str] = []
        self._hex_in_xmlcon = False

    async def start(self) -> None:
        """Start listening based on config protocol (tcp, udp, or auto)."""
        self._running = True
        protocol = self.config.stream_protocol
        host = self.config.stream_host
        port = self.config.stream_port

        if protocol in ("tcp", "auto"):
            await self._start_tcp(host, port)

        if protocol in ("udp", "auto"):
            await self._start_udp(host, port)

        # Background flush timer
        asyncio.ensure_future(self._flush_timer())

        logger.info(
            "Stream listener started: protocol=%s, %s:%d, format=%s",
            protocol, host, port, self.config.stream_format,
        )

    async def stop(self) -> None:
        """Stop all listeners and flush remaining records."""
        self._running = False
        for server in self._servers:
            server.close()
        for transport in self._transports:
            transport.close()
        await self._flush()
        await self._flush_hex()
        logger.info("Stream listener stopped")

    # ------------------------------------------------------------------
    # TCP
    # ------------------------------------------------------------------

    async def _start_tcp(self, host: str, port: int) -> None:
        if self.config.stream_connect_mode == "client":
            asyncio.ensure_future(self._tcp_client_loop(host, port))
        else:
            server = await asyncio.start_server(self._handle_tcp_connection, host, port)
            self._servers.append(server)
            logger.info("TCP server listening on %s:%d", host, port)

    async def _handle_tcp_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        logger.info("TCP connection from %s", addr)
        try:
            while self._running:
                line_bytes = await reader.readline()
                if not line_bytes:
                    break
                await self._process_line(line_bytes.decode("utf-8", errors="replace"))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("TCP connection error: %s", e)
        finally:
            writer.close()

    async def _tcp_client_loop(self, host: str, port: int) -> None:
        """Connect to a remote TCP source and read lines."""
        while self._running:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                logger.info("TCP client connected to %s:%d", host, port)
                while self._running:
                    line_bytes = await reader.readline()
                    if not line_bytes:
                        break
                    await self._process_line(line_bytes.decode("utf-8", errors="replace"))
                writer.close()
            except (ConnectionRefusedError, OSError) as e:
                logger.warning("TCP connect failed (%s:%d): %s — retrying in 5s", host, port, e)
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                return

    # ------------------------------------------------------------------
    # UDP
    # ------------------------------------------------------------------

    async def _start_udp(self, host: str, port: int) -> None:
        loop = asyncio.get_running_loop()
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(self._process_line_sync),
            local_addr=(host, port),
        )
        self._transports.append(transport)
        logger.info("UDP listener on %s:%d", host, port)

    def _process_line_sync(self, line: str) -> None:
        """Sync callback for UDP protocol — schedules async processing."""
        loop = asyncio.get_event_loop()
        loop.create_task(self._process_line(line))

    # ------------------------------------------------------------------
    # Line processing
    # ------------------------------------------------------------------

    async def _process_line(self, line: str) -> None:
        """Parse a single line and add to the record buffer."""
        stripped = line.strip()
        if not stripped:
            return

        fmt = self.config.stream_format
        if fmt == "auto":
            if self._detected_format is None:
                self._detected_format = detect_format(stripped)
                logger.info("Auto-detected stream format: %s", self._detected_format)
            fmt = self._detected_format

        if fmt == "hex":
            await self._process_hex_line(stripped)
            return

        record: dict[str, Any] | None = None

        if fmt == "nmea":
            record = parse_nmea_line(stripped)
        elif fmt == "csv":
            if self._csv_headers is None:
                # First line is header
                self._csv_headers = [h.strip() for h in stripped.split(",")]
                logger.info("CSV headers: %s", self._csv_headers)
                return
            record = parse_csv_line(stripped, self._csv_headers)

        if record is not None:
            self._records.append(record)

        # Check batch thresholds
        if len(self._records) >= self.config.batch_max_records:
            await self._flush()

    async def _process_hex_line(self, line: str) -> None:
        """Process a line from a hex scan stream.

        Header lines (``*`` prefix or ``# XMLCON_*`` blocks) are buffered
        for later use.  Hex scan data lines are accumulated and flushed
        as a batch when the count or time threshold is reached.
        """
        # Header / preamble
        if line.startswith("*") or line.startswith("#"):
            if line == "# XMLCON_START":
                self._hex_in_xmlcon = True
                return
            if line == "# XMLCON_END":
                self._hex_in_xmlcon = False
                return
            if self._hex_in_xmlcon:
                # Strip the "# " prefix
                self._hex_xmlcon_lines.append(line[2:] if line.startswith("# ") else line[1:])
                return
            if line.strip() == "*END*":
                self._hex_header_done = True
            if not self._hex_header_done:
                self._hex_header_lines.append(line)
            return

        # Scan data: validate it's actually hex
        if _HEX_SCAN_RE.match(line):
            self._hex_scan_lines.append(line)

            if len(self._hex_scan_lines) >= self.config.batch_max_records:
                await self._flush_hex()
        else:
            logger.debug("Ignoring non-hex line: %s", line[:60])

    async def _flush_timer(self) -> None:
        """Periodically flush buffered records."""
        while self._running:
            await asyncio.sleep(1)
            elapsed = time.monotonic() - self._last_flush
            if elapsed >= self.config.batch_interval_seconds:
                if self._records:
                    await self._flush()
                if self._hex_scan_lines:
                    await self._flush_hex()

    async def _flush(self) -> None:
        """Convert buffered records to a DataFrame and put on the queue."""
        if not self._records:
            return

        records = self._records
        self._records = []
        self._last_flush = time.monotonic()

        df = records_to_dataframe(records)
        if not df.empty:
            await self.queue.put(("stream_batch", df))
            logger.info("Flushed %d stream records to processing queue", len(df))

    async def _flush_hex(self) -> None:
        """Decode buffered hex scan lines and put a DataFrame on the queue.

        Uses seabirdscientific if available; otherwise the raw hex lines
        are stored as-is for later file-based processing.
        """
        if not self._hex_scan_lines:
            return

        scan_lines = self._hex_scan_lines
        self._hex_scan_lines = []
        self._last_flush = time.monotonic()

        try:
            from ingest.adapter import HAS_CTD

            if HAS_CTD:
                df = self._decode_hex_batch(scan_lines)
                if df is not None and not df.empty:
                    await self.queue.put(("stream_batch", df))
                    logger.info(
                        "Flushed %d hex scans (%d records) to processing queue",
                        len(scan_lines), len(df),
                    )
                    return
        except Exception as e:
            logger.warning("Hex decode failed: %s — writing raw lines instead", e)

        # Fallback: queue raw hex text for file-based processing later
        import pandas as pd

        df = pd.DataFrame({"hex_scan": scan_lines})
        await self.queue.put(("stream_batch", df))
        logger.info("Flushed %d raw hex lines to processing queue", len(scan_lines))

    def _decode_hex_batch(self, scan_lines: list[str]) -> "pd.DataFrame | None":
        """Decode a list of hex scan strings into a calibrated DataFrame.

        Writes the scans to a temporary .hex file (with the buffered
        header) so that the existing hex_parser can process them.
        """
        import tempfile
        from pathlib import Path

        import pandas as pd

        from ingest.adapter import parse_hex_file

        # Build a temporary hex file with header + scans
        tmpdir = tempfile.mkdtemp(prefix="sensorstream_hex_")
        tmp_hex = Path(tmpdir) / "stream_batch.hex"

        with open(tmp_hex, "w") as f:
            for h in self._hex_header_lines:
                f.write(h + "\n")
            f.write("*END*\n")
            for scan in scan_lines:
                f.write(scan + "\n")

        # Write XMLCON if we received it in the preamble
        if self._hex_xmlcon_lines:
            tmp_xmlcon = Path(tmpdir) / "stream_batch.XMLCON"
            with open(tmp_xmlcon, "w") as f:
                f.write("\n".join(self._hex_xmlcon_lines))

        try:
            df = parse_hex_file(tmp_hex)
            return df
        finally:
            import shutil

            shutil.rmtree(tmpdir, ignore_errors=True)


class _UDPProtocol(asyncio.DatagramProtocol):
    """Simple UDP protocol that delegates line processing."""

    def __init__(self, callback):
        self._callback = callback
        self._buffer = ""

    def datagram_received(self, data: bytes, addr: tuple) -> None:
        text = data.decode("utf-8", errors="replace")
        self._buffer += text
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.strip():
                self._callback(line)

    def error_received(self, exc: Exception) -> None:
        logger.error("UDP error: %s", exc)
