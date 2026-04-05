"""Storage abstraction for sensor data persistence.

Supports:
- ``local``: Direct filesystem writes
- ``azure-blob-edge``: Azure Blob Storage on IoT Edge (localhost:11002)
"""

from __future__ import annotations

import logging
import os
import shutil
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger("sensorstream")


class StorageBackend(ABC):
    """Protocol for sensor data storage."""

    @abstractmethod
    def save_parquet(self, df: "pd.DataFrame", path: str) -> str:
        """Save a DataFrame as Parquet. Returns the resolved path."""

    @abstractmethod
    def save_file(self, data: bytes, path: str) -> str:
        """Save raw bytes to a file path. Returns the resolved path."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if a path exists in the store."""

    @abstractmethod
    def list_files(self, prefix: str, suffix: str = "") -> list[str]:
        """List files under a prefix, optionally filtered by suffix."""


class LocalStorage(StorageBackend):
    """Direct filesystem storage."""

    def __init__(self, base_path: str = "/app/processed"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve(self, path: str) -> Path:
        full = self.base_path / path
        full.parent.mkdir(parents=True, exist_ok=True)
        return full

    def save_parquet(self, df: "pd.DataFrame", path: str) -> str:
        import geopandas as gpd

        full = self._resolve(path)

        if "latitude" in df.columns and "longitude" in df.columns:
            valid = df["latitude"].notna() & df["longitude"].notna()
            if valid.any():
                geometry = gpd.points_from_xy(
                    df.loc[valid, "longitude"], df.loc[valid, "latitude"]
                )
                gdf = gpd.GeoDataFrame(df.loc[valid], geometry=geometry, crs="EPSG:4326")
                gdf.to_parquet(str(full))
                logger.info("Saved GeoParquet (%d rows) to %s", len(gdf), full)
                return str(full)

        df.to_parquet(str(full), index=False)
        logger.info("Saved Parquet (%d rows) to %s", len(df), full)
        return str(full)

    def save_file(self, data: bytes, path: str) -> str:
        full = self._resolve(path)
        full.write_bytes(data)
        return str(full)

    def exists(self, path: str) -> bool:
        return (self.base_path / path).exists()

    def list_files(self, prefix: str, suffix: str = "") -> list[str]:
        base = self.base_path / prefix
        if not base.exists():
            return []
        pattern = f"*{suffix}" if suffix else "*"
        return [
            str(p.relative_to(self.base_path))
            for p in base.rglob(pattern)
            if p.is_file()
        ]


class AzureBlobEdgeStorage(StorageBackend):
    """Azure Blob Storage on IoT Edge (localhost:11002).

    Uses pinned API version 2019-07-07 compatible with the edge blob module.
    """

    _API_VERSION = "2019-07-07"

    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING", ""
        )
        if not self.connection_string:
            raise EnvironmentError(
                "AZURE_STORAGE_CONNECTION_STRING not set for AzureBlobEdgeStorage"
            )
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from azure.storage.blob import BlobServiceClient
            self._client = BlobServiceClient.from_connection_string(
                self.connection_string, api_version=self._API_VERSION,
            )
        return self._client

    def _ensure_container(self, container: str) -> None:
        cc = self.client.get_container_client(container)
        try:
            cc.get_container_properties()
        except Exception:
            cc.create_container()
            logger.info("Created container: %s", container)

    def save_parquet(self, df: "pd.DataFrame", path: str) -> str:
        import tempfile
        import geopandas as gpd

        container = path.split("/")[0]
        blob_name = "/".join(path.split("/")[1:])
        self._ensure_container(container)

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
            if "latitude" in df.columns and "longitude" in df.columns:
                valid = df["latitude"].notna() & df["longitude"].notna()
                if valid.any():
                    geometry = gpd.points_from_xy(
                        df.loc[valid, "longitude"], df.loc[valid, "latitude"]
                    )
                    gdf = gpd.GeoDataFrame(df.loc[valid], geometry=geometry, crs="EPSG:4326")
                    gdf.to_parquet(tmp.name)
                else:
                    df.to_parquet(tmp.name, index=False)
            else:
                df.to_parquet(tmp.name, index=False)

            tmp.seek(0)
            bc = self.client.get_container_client(container).get_blob_client(blob_name)
            with open(tmp.name, "rb") as f:
                bc.upload_blob(f, overwrite=True)

        logger.info("Saved Parquet to blob: %s", path)
        return path

    def save_file(self, data: bytes, path: str) -> str:
        container = path.split("/")[0]
        blob_name = "/".join(path.split("/")[1:])
        self._ensure_container(container)
        bc = self.client.get_container_client(container).get_blob_client(blob_name)
        bc.upload_blob(data, overwrite=True)
        logger.info("Saved file to blob: %s", path)
        return path

    def exists(self, path: str) -> bool:
        container = path.split("/")[0]
        blob_name = "/".join(path.split("/")[1:])
        try:
            bc = self.client.get_container_client(container).get_blob_client(blob_name)
            bc.get_blob_properties()
            return True
        except Exception:
            return False

    def list_files(self, prefix: str, suffix: str = "") -> list[str]:
        container = prefix.split("/")[0]
        blob_prefix = "/".join(prefix.split("/")[1:])
        cc = self.client.get_container_client(container)
        results = []
        for blob in cc.list_blobs(name_starts_with=blob_prefix):
            if not suffix or blob.name.endswith(suffix):
                results.append(f"{container}/{blob.name}")
        return results
