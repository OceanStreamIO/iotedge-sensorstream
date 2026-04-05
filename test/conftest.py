"""Shared test configuration — blob-backed test data with local caching.

Test data is stored in Azure Blob Storage (``sensorstream-test`` container,
``raw/`` prefix) so that tests run identically on dev machines and edge
devices without shipping raw data around.

On first run the fixture downloads files to a local cache directory
(``test/.test_data_cache/``).  Subsequent runs use the cache unless the
blob is newer.  If no connection string is available, the fixture falls
back to ``test/test_data/`` (if it exists locally).

Environment
-----------
AZURE_CONNECTION_STRING : str
    Connection string for the ``ne1osvmdevtest`` storage account.
    Loaded from ``../sd-data-ingest/.env`` or the local ``.env``.
SENSORSTREAM_TEST_CONTAINER : str
    Container name (default ``sensorstream-test``).
SENSORSTREAM_TEST_CACHE : str
    Local cache dir (default ``test/.test_data_cache``).
"""

from __future__ import annotations

# test_azure_e2e.py is a standalone script (python test/test_azure_e2e.py),
# not a pytest test — exclude it from collection.
collect_ignore = ["test_azure_e2e.py"]

import logging
import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap env
# ---------------------------------------------------------------------------

# Try multiple .env locations
for _env in [
    Path(__file__).resolve().parent.parent / ".env",
    Path(__file__).resolve().parent.parent.parent / "sd-data-ingest" / ".env",
]:
    if _env.exists():
        load_dotenv(_env)

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger("sensorstream.test")

CONN_STR = os.getenv("AZURE_CONNECTION_STRING", "")
CONTAINER_NAME = os.getenv("SENSORSTREAM_TEST_CONTAINER", "sensorstream-test")
BLOB_PREFIX = "raw/"

# Directories
TEST_DIR = Path(__file__).resolve().parent
CACHE_DIR = Path(os.getenv(
    "SENSORSTREAM_TEST_CACHE",
    str(TEST_DIR / ".test_data_cache"),
))
LOCAL_DATA_DIR = TEST_DIR / "test_data"


# ---------------------------------------------------------------------------
# Blob download helpers
# ---------------------------------------------------------------------------


def _blob_service():
    from azure.storage.blob import BlobServiceClient
    return BlobServiceClient.from_connection_string(CONN_STR)


def _sync_blob_to_cache(blob_name: str, local_path: Path) -> bool:
    """Download a single blob to *local_path* if it's newer or missing.

    Returns True if the file was downloaded, False if cache was fresh.
    """
    try:
        svc = _blob_service()
        bc = svc.get_container_client(CONTAINER_NAME).get_blob_client(blob_name)
        props = bc.get_blob_properties()
        blob_size = props.size

        if local_path.exists() and local_path.stat().st_size == blob_size:
            return False  # cache is fresh

        local_path.parent.mkdir(parents=True, exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(bc.download_blob().readall())

        logger.info("Downloaded %s → %s (%d KB)", blob_name, local_path, blob_size // 1024)
        return True
    except Exception as e:
        logger.warning("Failed to download %s: %s", blob_name, e)
        return False


def _sync_all_test_data() -> Path:
    """Ensure all raw test data is available locally.

    Returns the path to the local data directory (cache or fallback).
    """
    if not CONN_STR:
        if LOCAL_DATA_DIR.exists():
            logger.info("No Azure connection — using local test_data/")
            return LOCAL_DATA_DIR
        pytest.skip("No AZURE_CONNECTION_STRING and no local test_data/ — cannot run tests")

    try:
        svc = _blob_service()
        cc = svc.get_container_client(CONTAINER_NAME)
        blobs = list(cc.list_blobs(name_starts_with=BLOB_PREFIX))
    except Exception as e:
        if LOCAL_DATA_DIR.exists():
            logger.warning("Azure unavailable (%s) — using local test_data/", e)
            return LOCAL_DATA_DIR
        pytest.skip(f"Azure unavailable and no local test_data/: {e}")

    downloaded = 0
    for blob in blobs:
        # raw/ctd/hex/11901.hex → ctd/hex/11901.hex
        rel = blob.name[len(BLOB_PREFIX):]
        local = CACHE_DIR / rel
        if _sync_blob_to_cache(blob.name, local):
            downloaded += 1

    if downloaded:
        logger.info("Downloaded %d files from blob to %s", downloaded, CACHE_DIR)

    return CACHE_DIR


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def test_data_dir() -> Path:
    """Root directory with all test data files.

    Returns the cache dir (downloaded from blob) or the local test_data/.
    """
    return _sync_all_test_data()


@pytest.fixture(scope="session")
def ctd_csv_file(test_data_dir: Path) -> Path:
    """Path to a real EMSO CTD CSV file."""
    p = test_data_dir / "ctd" / "EMSO_OBSEA_CTD_7day.csv"
    if not p.exists():
        pytest.skip(f"CTD CSV not found: {p}")
    return p


@pytest.fixture(scope="session")
def ctd_cnv_file(test_data_dir: Path) -> Path:
    """Path to a real Sea-Bird .cnv file."""
    p = test_data_dir / "ctd" / "RR2205_11901_1db.cnv"
    if not p.exists():
        pytest.skip(f"CTD CNV not found: {p}")
    return p


@pytest.fixture(scope="session")
def hex_dir(test_data_dir: Path) -> Path:
    """Directory containing .hex + .hdr + .XMLCON for cast 11901."""
    p = test_data_dir / "ctd" / "hex"
    if not p.exists() or not (p / "11901.hex").exists():
        pytest.skip(f"Hex test data not found: {p}")
    return p


@pytest.fixture(scope="session")
def hex_file(hex_dir: Path) -> Path:
    return hex_dir / "11901.hex"


@pytest.fixture(scope="session")
def gnss_nmea_file(test_data_dir: Path) -> Path:
    """Path to a real GNSS NMEA file (2000 lines)."""
    p = test_data_dir / "gnss" / "RR2401_gnss_2000lines.txt"
    if not p.exists():
        pytest.skip(f"GNSS NMEA not found: {p}")
    return p
