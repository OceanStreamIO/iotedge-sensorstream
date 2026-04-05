"""File-based trigger processing for IoT Edge messages.

Handles ``sensorfileadded`` input messages from the filenotifier module
and queues the file path for processing.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger("sensorstream")


async def handle_sensor_file_added(
    message_data: Dict[str, Any],
    queue,
) -> Dict[str, Any]:
    """Process a sensor file triggered by an IoT Edge message.

    Parameters
    ----------
    message_data
        Decoded JSON from the ``sensorfileadded`` input message.
        Expected keys: ``event``, ``file_added_path``.
    queue
        asyncio.Queue to put the file path onto.
    """
    event = message_data.get("event")
    file_path = message_data.get("file_added_path", "")

    if event != "fileadd":
        logger.info("Ignoring non-fileadd event: %s", event)
        return {"status": "skipped", "reason": f"event={event}"}

    if not file_path or not Path(file_path).exists():
        logger.error("Sensor file not found: %s", file_path)
        return {"status": "error", "reason": f"file not found: {file_path}"}

    await queue.put(("file", file_path))
    logger.info("Queued sensor file from IoT Edge trigger: %s", file_path)
    return {"status": "queued", "file": file_path}


def parse_input_message(message) -> Dict[str, Any]:
    """Parse an IoT Edge input message to a dict."""
    try:
        byte_str = message.data
        return json.loads(byte_str.decode("utf-8"))
    except Exception as e:
        logger.error("Failed to parse input message: %s", e)
        return {}
