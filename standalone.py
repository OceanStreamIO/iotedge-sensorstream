#!/usr/bin/env python3
"""Standalone entry point for the sensorstream processing pipeline.

Runs locally without Azure IoT Edge runtime. Processes sensor data
files from a directory and optionally listens for network streams.

Usage::

    # Process files from a directory
    python standalone.py \\
        --input-dir ../sd-data-ingest/raw_data/emso \\
        --output-dir ./output \\
        --campaign-id EMSO_test \\
        --provider emso

    # Watch a directory for new files
    python standalone.py \\
        --watch /tmp/sensorstream-watch \\
        --output-dir ./output \\
        --campaign-id test

    # Listen for network stream data
    python standalone.py \\
        --stream-port 9100 \\
        --output-dir ./output \\
        --campaign-id test

    # Run with built-in stream simulator
    python standalone.py \\
        --simulate-stream ../sd-data-ingest/raw_data/r2r/RR2401_gnss_gp170_aft-2024-02-17.txt \\
        --output-dir ./output \\
        --campaign-id R2R_test
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
logger = logging.getLogger("sensorstream")

for _noisy in ("fsspec", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process sensor data files locally (standalone mode).",
    )

    # Input mode — at least one required
    input_group = parser.add_argument_group("input sources (at least one required)")
    input_group.add_argument(
        "--input-dir", type=Path,
        help="Directory containing sensor data files to process (batch mode).",
    )
    input_group.add_argument(
        "--watch", type=Path, metavar="DIR",
        help="Directory to watch for new files (live mode).",
    )
    input_group.add_argument(
        "--stream-port", type=int,
        help="Listen for TCP/UDP sensor data on this port.",
    )

    # Simulators
    sim_group = parser.add_argument_group("built-in simulators")
    sim_group.add_argument(
        "--simulate-stream", type=Path, metavar="FILE",
        help="Start stream simulator with this data file.",
    )
    sim_group.add_argument(
        "--simulate-files", type=Path, metavar="DIR",
        help="Start file simulator from this source directory.",
    )
    sim_group.add_argument(
        "--simulate-ctd", type=Path, metavar="HEX_FILE",
        help="Start CTD hex stream simulator from a .hex file (TCP).",
    )
    sim_group.add_argument(
        "--simulate-rate", type=float, default=10.0,
        help="Simulator rate: lines/sec (stream) or interval in seconds (file). Default: 10.",
    )

    # Output & metadata
    parser.add_argument("--output-dir", default=Path("./output"), type=Path)
    parser.add_argument("--campaign-id", default="")
    parser.add_argument("--platform-id", default="")
    parser.add_argument("--provider", default="auto")

    # Processing
    parser.add_argument("--stream-format", default="auto", choices=["auto", "nmea", "csv", "hex"])
    parser.add_argument("--stream-protocol", default="auto", choices=["auto", "tcp", "udp"])
    parser.add_argument("--batch-interval", type=int, default=60, help="Stream batch interval (seconds)")
    parser.add_argument("--batch-max-records", type=int, default=1000)
    parser.add_argument("--max-files", type=int, default=None, help="Process at most N files")
    parser.add_argument(
        "--downsample", type=int, default=30,
        help="Telemetry downsample interval in seconds (default: 30)",
    )

    return parser.parse_args()


async def run_pipeline(args: argparse.Namespace) -> None:
    from azure_handler.storage import LocalStorage
    from config import EdgeConfig
    from process.pipeline import process_file, process_stream_batch

    # Build config
    config = EdgeConfig.from_standalone(
        campaign_id=args.campaign_id,
        platform_id=args.platform_id,
        provider=args.provider,
        output_base_path=str(args.output_dir),
        stream_format=args.stream_format,
        stream_protocol=args.stream_protocol,
        batch_interval_seconds=args.batch_interval,
        batch_max_records=args.batch_max_records,
        telemetry_downsample_seconds=args.downsample,
    )
    if args.watch:
        config.watch_dir = str(args.watch)
    if args.stream_port:
        config.stream_port = args.stream_port

    # Storage
    campaign = config.campaign_id or "default"
    campaign_root = args.output_dir / campaign
    storage = LocalStorage(base_path=str(campaign_root))

    queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    # --- Processing worker ---
    all_results = []

    async def worker():
        while True:
            item = await queue.get()
            try:
                msg_type, payload = item
                if msg_type == "file":
                    result = await process_file(payload, config, storage, client=None)
                    all_results.append(result)
                elif msg_type == "stream_batch":
                    result = await process_stream_batch(payload, config, storage, client=None)
                    all_results.append(result)
            except Exception as e:
                logger.error("Worker error: %s", e, exc_info=True)
            finally:
                queue.task_done()

    worker_task = asyncio.ensure_future(worker())

    # --- Start simulators if requested ---
    sim_tasks = []

    if args.simulate_stream:
        from simulate.stream_simulator import StreamSimulator
        # Default to TCP on stream port
        port = args.stream_port or 9100
        config.stream_port = port
        sim = StreamSimulator(
            file_path=args.simulate_stream,
            protocol="tcp",
            host="127.0.0.1",
            port=port,
            rate=args.simulate_rate,
            loop=False,
        )
        sim_tasks.append(asyncio.ensure_future(sim.run()))
        # Also start the stream listener
        args.stream_port = port
        # Give simulator a moment to start
        await asyncio.sleep(0.5)

    if args.simulate_files:
        from simulate.file_simulator import FileSimulator
        watch_dir = args.watch or Path("/tmp/sensorstream-watch")
        config.watch_dir = str(watch_dir)
        sim = FileSimulator(
            source_dir=args.simulate_files,
            target_dir=watch_dir,
            interval=args.simulate_rate,
            loop=False,
        )
        sim_tasks.append(asyncio.ensure_future(sim.run()))
        if not args.watch:
            args.watch = watch_dir

    if args.simulate_ctd:
        from simulate.ctd_stream_simulator import CTDStreamSimulator
        ctd_port = args.stream_port or 9200
        config.stream_port = ctd_port
        config.stream_format = "hex"
        ctd_sim = CTDStreamSimulator(
            hex_file=args.simulate_ctd,
            host="127.0.0.1",
            port=ctd_port,
            rate=args.simulate_rate,
            loop=False,
            send_header=True,
        )
        sim_tasks.append(asyncio.ensure_future(ctd_sim.run()))
        args.stream_port = ctd_port
        await asyncio.sleep(0.5)

    # --- Start ingest sources ---
    stream_listener = None
    file_watcher = None

    if args.stream_port or args.simulate_stream or args.simulate_ctd:
        from ingest.stream_listener import StreamListener
        # In standalone stream mode with simulator, connect as client
        if args.simulate_stream or args.simulate_ctd:
            config.stream_connect_mode = "client"
            config.stream_host = "127.0.0.1"
        stream_listener = StreamListener(config, queue)
        await stream_listener.start()

    if args.watch:
        from ingest.file_watcher import FileWatcher
        file_watcher = FileWatcher(config, queue)
        await file_watcher.start()
        await file_watcher.scan_existing()

    # --- Batch mode: process files from input-dir ---
    if args.input_dir:
        total_start = time.time()
        input_dir = args.input_dir.resolve()
        patterns = ["*.csv", "*.txt", "*.hex", "*.cnv", "*.tar.gz", "*.tgz"]
        raw_files = []
        for pat in patterns:
            raw_files.extend(sorted(input_dir.glob(pat)))

        if not raw_files:
            logger.error("No sensor data files found in %s", input_dir)
        else:
            if args.max_files:
                raw_files = raw_files[:args.max_files]
            logger.info("Found %d files in %s", len(raw_files), input_dir)

            for i, f in enumerate(raw_files, 1):
                logger.info("━━━ File %d/%d: %s ━━━", i, len(raw_files), f.name)
                result = await process_file(str(f), config, storage, client=None)
                all_results.append(result)

            total_time = time.time() - total_start
            ok = sum(1 for r in all_results if r.get("status") == "ok")
            failed = sum(1 for r in all_results if r.get("status") == "error")
            skipped = sum(1 for r in all_results if r.get("status") == "skipped")
            logger.info(
                "━━━ Done: %d ok, %d failed, %d skipped in %.1fs ━━━",
                ok, failed, skipped, total_time,
            )
    elif args.watch or args.stream_port or args.simulate_stream or args.simulate_files or args.simulate_ctd:
        # Live mode — wait for data until Ctrl+C
        logger.info("Live mode — press Ctrl+C to stop")
        try:
            # Wait for simulators to finish, or forever if in watch/stream mode
            if sim_tasks:
                await asyncio.gather(*sim_tasks)
                # Allow processing to catch up
                await asyncio.sleep(2)
                await queue.join()
            else:
                stop = asyncio.Event()
                await stop.wait()
        except asyncio.CancelledError:
            pass
    else:
        logger.error("No input source specified. Use --input-dir, --watch, or --stream-port")
        sys.exit(1)

    # --- Cleanup ---
    if stream_listener:
        await stream_listener.stop()
    if file_watcher:
        await file_watcher.stop()
    worker_task.cancel()

    # Summary
    total_records = sum(r.get("record_count", 0) for r in all_results)
    logger.info("Total: %d results, %d records processed", len(all_results), total_records)


def main():
    args = parse_args()
    try:
        asyncio.run(run_pipeline(args))
    except KeyboardInterrupt:
        logger.info("Interrupted")


if __name__ == "__main__":
    main()
