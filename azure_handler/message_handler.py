"""Send messages and update twin reported properties via IoT Hub."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient

logger = logging.getLogger("sensorstream")


def _default_serializer(obj):
    """Convert non-JSON-serializable objects."""
    if isinstance(obj, (pd.Timestamp, np.datetime64, datetime)):
        return obj.isoformat()
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, np.generic):
        return obj.item()
    return obj


def serialize_for_json(obj):
    """Recursively make an object JSON-serializable."""
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [serialize_for_json(v) for v in obj]
    if isinstance(obj, (pd.Timestamp, np.datetime64, datetime)):
        return obj.isoformat()
    if isinstance(obj, (np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, set):
        return list(obj)
    return obj


def send_to_hub(
    client: "IoTHubModuleClient",
    data: Dict[str, Any] | None = None,
    properties: Dict[str, Any] | None = None,
    output_name: str = "output1",
) -> None:
    """Send data to Azure IoT Hub via output message or twin patch."""
    from azure.iot.device import Message

    try:
        if properties:
            properties = serialize_for_json(properties)
            client.patch_twin_reported_properties(properties)
        else:
            if data is None:
                data = {}

            payload = json.dumps(data, default=_default_serializer)
            message = Message(payload)
            message.message_id = str(uuid.uuid4())
            client.send_message_to_output(message, output_name)

        logger.info("Sent data to IoT Hub on output '%s'", output_name)
    except Exception as e:
        logger.error("Failed to send data to IoT Hub on output '%s': %s", output_name, e, exc_info=True)
