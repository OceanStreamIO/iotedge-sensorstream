"""Azure IoT Hub handler — client, messaging, storage."""

from __future__ import annotations


def create_client():
    """Lazy import to avoid pulling azure-iot-device at module level."""
    from azure_handler.connect_iothub import create_client as _create
    return _create()


def create_storage(backend: str = "local", base_path: str = "/app/processed"):
    """Factory for storage backends."""
    from azure_handler.storage import LocalStorage, AzureBlobEdgeStorage

    if backend == "azure-blob-edge":
        try:
            return AzureBlobEdgeStorage()
        except Exception:
            import logging
            logging.getLogger("sensorstream").warning(
                "Azure Blob Edge unavailable, falling back to local storage"
            )
            return LocalStorage(base_path)

    return LocalStorage(base_path)
