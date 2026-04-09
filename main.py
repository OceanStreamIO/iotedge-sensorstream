"""IoT Edge sensorstream module — entry point.

Dual-mode processing:
- **stream**: Listens for TCP/UDP sensor data (NMEA or CSV lines),
  buffers records, and writes GeoParquet batches.
- **file**: Receives ``sensorfileadded`` messages from filenotifier
  or watches a directory for new sensor data files.
- **both**: Both modes run concurrently.

Configuration comes from:
- IoT Hub module twin desired properties
- Environment variables (see .env.example)
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys

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

for _noisy in ("azure", "fsspec", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


async def async_main() -> None:
    """Async entry point — sets up IoT Hub client, config, and mode dispatcher."""
    from azure_handler import create_client, create_storage
    from config import EdgeConfig, _TWIN_KEY_MAP
    from ingest.file_trigger import handle_sensor_file_added, parse_input_message
    from ingest.file_watcher import FileWatcher
    from ingest.stream_listener import StreamListener
    from process.pipeline import process_file, process_stream_batch

    # --- IoT Hub client ---
    client = create_client()
    logger.info("IoT Hub module client initialized")

    # --- Configuration from twin ---
    twin = client.get_twin()
    desired = twin.get("desired", {})
    config = EdgeConfig.from_twin_and_env(desired)
    logging.getLogger("sensorstream").setLevel(
        getattr(logging, config.log_level.upper(), logging.INFO)
    )
    logger.info(
        "Config loaded: mode=%s, protocol=%s, format=%s",
        config.input_mode, config.stream_protocol, config.stream_format,
    )

    # --- Storage backend ---
    storage = create_storage(
        backend=config.storage_backend,
        base_path=config.output_base_path,
    )
    logger.info("Storage backend: %s", config.storage_backend)

    # --- Processing queue ---
    queue: asyncio.Queue = asyncio.Queue(maxsize=100)

    # --- Telemetry throttle ---
    from exports.throttle import TelemetryThrottle
    throttle = TelemetryThrottle(config, client)

    loop = asyncio.get_running_loop()

    # --- Twin update handler ---
    def on_twin_update(patch):
        nonlocal config
        logger.info("Twin patch received: %s", list(patch.keys()))
        config.update_from_twin(patch)
        try:
            from dataclasses import fields as dc_fields
            from azure_handler.message_handler import serialize_for_json
            known = {f.name for f in dc_fields(config)}
            reported = {
                k: getattr(config, _TWIN_KEY_MAP.get(k, k))
                for k in patch
                if _TWIN_KEY_MAP.get(k, k) in known
            }
            if reported:
                client.patch_twin_reported_properties(serialize_for_json(reported))
        except Exception as e:
            logger.error("Failed to report twin: %s", e)

    client.on_twin_desired_properties_patch_received = on_twin_update

    # --- Input message handler ---
    def on_message(message):
        if message.input_name == "sensorfileadded":
            data = parse_input_message(message)
            if data.get("event") == "fileadd":
                loop.call_soon_threadsafe(
                    asyncio.ensure_future,
                    handle_sensor_file_added(data, queue),
                )

    client.on_message_received = on_message

    # --- Processing worker ---
    async def worker():
        while True:
            item = await queue.get()
            try:
                msg_type, payload = item
                if msg_type == "file":
                    await process_file(payload, config, storage, client)
                elif msg_type == "stream_batch":
                    await process_stream_batch(payload, config, storage, client)
                elif msg_type == "ctd_reading":
                    throttle.maybe_send_record(payload)
            except Exception as e:
                logger.error("Worker error: %s", e, exc_info=True)
            finally:
                queue.task_done()

    worker_task = asyncio.ensure_future(worker())

    # --- Start ingest sources ---
    stream_listener = None
    file_watcher = None
    ctd_monitor = None

    if config.ctd_enabled:
        from ingest.ctd_file_monitor import CtdFileMonitor
        ctd_monitor = CtdFileMonitor(config, queue)
        await ctd_monitor.start()

    if config.input_mode in ("stream", "both"):
        stream_listener = StreamListener(config, queue)
        await stream_listener.start()

    if config.input_mode in ("file", "both"):
        file_watcher = FileWatcher(config, queue)
        await file_watcher.start()
        await file_watcher.scan_existing()

    # --- Graceful shutdown ---
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info("Sensorstream module running — waiting for data")
    await stop_event.wait()

    logger.info("Shutting down…")
    if ctd_monitor:
        await ctd_monitor.stop()
    if stream_listener:
        await stream_listener.stop()
    if file_watcher:
        await file_watcher.stop()
    worker_task.cancel()
    client.shutdown()
    logger.info("Shutdown complete")


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
