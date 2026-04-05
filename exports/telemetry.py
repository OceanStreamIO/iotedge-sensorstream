"""Send processing telemetry and results to IoT Hub."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from azure.iot.device import IoTHubModuleClient
    from config import EdgeConfig

logger = logging.getLogger("sensorstream")


def send_processing_telemetry(
    client: "IoTHubModuleClient",
    result: Dict[str, Any],
    config: "EdgeConfig",
) -> None:
    """Send file processing result telemetry via IoT Hub output message."""
    from azure_handler.message_handler import send_to_hub

    payload = {
        **result,
        "campaign_id": config.campaign_id,
        "platform_id": config.platform_id,
        "provider": config.provider,
    }

    send_to_hub(client, data=payload, output_name="output1")

    # Update reported twin properties
    try:
        reported = {
            "last_processed_file": result.get("source_file", ""),
            "last_record_count": result.get("record_count", 0),
            "last_processing_time_ms": result.get("processing_time_ms", 0),
            "input_mode": config.input_mode,
            "config_version": config._config_version,
        }
        client.patch_twin_reported_properties(reported)
    except Exception as e:
        logger.error("Failed to update reported properties: %s", e)


def send_batch_summary(
    client: "IoTHubModuleClient",
    result: Dict[str, Any],
    config: "EdgeConfig",
) -> None:
    """Send stream batch summary telemetry via IoT Hub output message."""
    from azure_handler.message_handler import send_to_hub

    payload = {
        **result,
        "campaign_id": config.campaign_id,
        "platform_id": config.platform_id,
        "source": "stream",
    }

    send_to_hub(client, data=payload, output_name="output1")


def send_record_telemetry(
    client: "IoTHubModuleClient",
    record: Dict[str, Any],
    config: "EdgeConfig",
) -> None:
    """Send an individual parsed record as a D2C telemetry message."""
    from azure_handler.message_handler import send_to_hub

    payload = {
        **record,
        "campaign_id": config.campaign_id,
        "platform_id": config.platform_id,
    }

    send_to_hub(client, data=payload, output_name="output1")
