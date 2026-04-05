"""IoT Hub module client creation for the sensorstream module."""

from __future__ import annotations

import logging
import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("sensorstream")

DEVICE_CONNECTION_STRING: Optional[str] = os.getenv("DEVICE_CONNECTION_STRING")
LOCAL_ENV: bool = os.getenv("LOCAL_ENV", "").lower() in ("true", "1", "yes")


def create_client():
    """Create the IoT Hub module client.

    In IoT Edge runtime: creates from edge environment.
    In local/dev mode: creates from device connection string.
    """
    from azure.iot.device import IoTHubModuleClient

    client = None

    try:
        if LOCAL_ENV:
            logger.info("Running in local environment using device connection string.")
            if not DEVICE_CONNECTION_STRING:
                raise ValueError("Missing DEVICE_CONNECTION_STRING environment variable.")
            client = IoTHubModuleClient.create_from_connection_string(DEVICE_CONNECTION_STRING)
        else:
            client = IoTHubModuleClient.create_from_edge_environment()

        if client is None:
            raise ValueError("Failed to create IoTHubModuleClient.")

        logger.info("IoT Hub module client initialized")
        return client

    except Exception as e:
        logger.error("Could not connect: %s", e, exc_info=True)
        raise
