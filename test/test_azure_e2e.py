#!/usr/bin/env python3
"""Azure Blob integration test for the sensorstream module.

1. Creates a test container in Azure Blob Storage
2. Uploads raw test data to ``raw/`` prefix
3. Runs the standalone pipeline, reading from local test_data/ and writing
   processed GeoParquet to Azure blob
4. Verifies the processed files exist in blob storage
5. Lists and summarises everything in the container

Usage::

    # Set connection string in .env or environment
    export AZURE_CONNECTION_STRING="..."

    # Run the full test
    python test/test_azure_e2e.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

# Load .env from the sd-data-ingest project (where the connection string is)
_env_path = Path(__file__).resolve().parent.parent.parent / "sd-data-ingest" / ".env"
if _env_path.exists():
    load_dotenv(_env_path)
# Also try local .env
load_dotenv()

CONN_STR = os.getenv("AZURE_CONNECTION_STRING", "")
CONTAINER_NAME = "sensorstream-test"

# Import the shared data sync helper so we always have test data locally
# (downloaded from blob on first run, cached in .test_data_cache/)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
try:
    from test.conftest import _sync_all_test_data, CACHE_DIR, LOCAL_DATA_DIR
    TEST_DATA_DIR = _sync_all_test_data()
except Exception:
    # Fallback for standalone script execution
    TEST_DATA_DIR = Path(__file__).parent / "test_data"
    if not TEST_DATA_DIR.exists():
        TEST_DATA_DIR = Path(__file__).parent / ".test_data_cache"

if not CONN_STR:
    print("ERROR: AZURE_CONNECTION_STRING not set")
    sys.exit(1)


def get_blob_service():
    from azure.storage.blob import BlobServiceClient
    return BlobServiceClient.from_connection_string(CONN_STR)


def step_create_container():
    """Create the test container (if it doesn't exist)."""
    print("\n=== Step 1: Create container ===")
    svc = get_blob_service()
    cc = svc.get_container_client(CONTAINER_NAME)
    try:
        cc.get_container_properties()
        print(f"Container '{CONTAINER_NAME}' already exists")
    except Exception:
        cc.create_container()
        print(f"Created container '{CONTAINER_NAME}'")


def step_upload_raw_data():
    """Upload all test data files to raw/ prefix in the blob container."""
    print("\n=== Step 2: Upload raw test data ===")
    svc = get_blob_service()
    cc = svc.get_container_client(CONTAINER_NAME)

    count = 0
    for data_file in sorted(TEST_DATA_DIR.rglob("*")):
        if not data_file.is_file():
            continue
        # Preserve subdirectory structure: raw/ctd/file.csv, raw/gnss/file.txt
        rel = data_file.relative_to(TEST_DATA_DIR)
        blob_name = f"raw/{rel}"

        bc = cc.get_blob_client(blob_name)
        with open(data_file, "rb") as f:
            bc.upload_blob(f, overwrite=True)
        size_kb = data_file.stat().st_size / 1024
        print(f"  ↑ {blob_name} ({size_kb:.1f} KB)")
        count += 1

    print(f"Uploaded {count} files to {CONTAINER_NAME}/raw/")
    return count


def step_run_pipeline():
    """Run standalone pipeline on each data subdirectory, writing to Azure blob."""
    print("\n=== Step 3: Run standalone pipeline ===")
    import asyncio
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from azure_handler.storage import LocalStorage
    from config import EdgeConfig
    from process.pipeline import process_file

    results = []
    for subdir in sorted(TEST_DATA_DIR.iterdir()):
        if not subdir.is_dir():
            continue

        campaign_id = f"test_{subdir.name}"
        print(f"\n--- Processing {subdir.name}/ as campaign '{campaign_id}' ---")

        # Use local storage that writes to a temp dir, then upload to blob
        output_dir = Path(__file__).parent / "output_azure" / campaign_id
        output_dir.mkdir(parents=True, exist_ok=True)

        config = EdgeConfig.from_standalone(
            campaign_id=campaign_id,
            output_base_path=str(output_dir),
            provider="auto",
        )
        storage = LocalStorage(base_path=str(output_dir))

        patterns = ["*.csv", "*.txt", "*.cnv"]
        data_files = []
        for pat in patterns:
            data_files.extend(sorted(subdir.glob(pat)))

        for data_file in data_files:
            print(f"  Processing: {data_file.name}")
            result = asyncio.run(process_file(str(data_file), config, storage))
            results.append(result)
            status = result.get("status", "?")
            records = result.get("record_count", 0)
            ms = result.get("processing_time_ms", 0)
            print(f"    → {status}: {records} records, {ms}ms")

    # Upload processed results to blob
    print("\n--- Uploading processed results to blob ---")
    svc = get_blob_service()
    cc = svc.get_container_client(CONTAINER_NAME)
    output_base = Path(__file__).parent / "output_azure"

    upload_count = 0
    for f in sorted(output_base.rglob("*")):
        if not f.is_file():
            continue
        rel = f.relative_to(output_base)
        blob_name = f"processed/{rel}"
        bc = cc.get_blob_client(blob_name)
        with open(f, "rb") as fh:
            bc.upload_blob(fh, overwrite=True)
        size_kb = f.stat().st_size / 1024
        print(f"  ↑ {blob_name} ({size_kb:.1f} KB)")
        upload_count += 1

    print(f"Uploaded {upload_count} processed files")
    return results


def step_verify():
    """List all blobs in the container and verify expected structure."""
    print("\n=== Step 4: Verify blob contents ===")
    svc = get_blob_service()
    cc = svc.get_container_client(CONTAINER_NAME)

    raw_blobs = []
    processed_blobs = []
    parquet_blobs = []
    metadata_blobs = []

    for blob in cc.list_blobs():
        size_kb = (blob.size or 0) / 1024
        if blob.name.startswith("raw/"):
            raw_blobs.append(blob.name)
        elif blob.name.startswith("processed/"):
            processed_blobs.append(blob.name)
            if blob.name.endswith(".parquet"):
                parquet_blobs.append(blob.name)
            elif blob.name.endswith(".metadata.json"):
                metadata_blobs.append(blob.name)

    print(f"\nContainer: {CONTAINER_NAME}")
    print(f"  Raw files:      {len(raw_blobs)}")
    print(f"  Processed files: {len(processed_blobs)}")
    print(f"    GeoParquet:    {len(parquet_blobs)}")
    print(f"    Metadata JSON: {len(metadata_blobs)}")

    print("\n  Raw blobs:")
    for b in raw_blobs:
        print(f"    {b}")

    print("\n  Processed blobs:")
    for b in processed_blobs:
        print(f"    {b}")

    # Verify we have parquet for each input type
    errors = []
    if not parquet_blobs:
        errors.append("No GeoParquet files found!")
    if not metadata_blobs:
        errors.append("No metadata JSON files found!")
    if len(parquet_blobs) < 3:
        errors.append(f"Expected at least 3 parquet files (CTD CSV, NMEA, CNV), got {len(parquet_blobs)}")

    # Read and display a metadata file
    if metadata_blobs:
        print(f"\n  Sample metadata ({metadata_blobs[0]}):")
        bc = cc.get_blob_client(metadata_blobs[0])
        meta = json.loads(bc.download_blob().readall())
        for k, v in meta.items():
            print(f"    {k}: {v}")

    # Read a parquet file to verify it's valid
    if parquet_blobs:
        print(f"\n  Verifying GeoParquet: {parquet_blobs[0]}")
        import tempfile
        import pandas as pd
        bc = cc.get_blob_client(parquet_blobs[0])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            tmp.write(bc.download_blob().readall())
            tmp.flush()
            df = pd.read_parquet(tmp.name)
            print(f"    Rows: {len(df)}, Columns: {list(df.columns)}")
            if "geometry" in df.columns:
                print(f"    Has geometry column (GeoParquet)")
            if "latitude" in df.columns:
                print(f"    Lat range: [{df['latitude'].min():.4f}, {df['latitude'].max():.4f}]")
            if "time" in df.columns or "time" in str(df.dtypes):
                print(f"    Time range: {df.iloc[0].get('time', 'N/A')} → {df.iloc[-1].get('time', 'N/A')}")

    if errors:
        print(f"\n  ERRORS:")
        for e in errors:
            print(f"    ✗ {e}")
        return False

    print(f"\n  ✓ All verifications passed!")
    return True


def step_cleanup_local():
    """Remove local output_azure directory."""
    import shutil
    output_dir = Path(__file__).parent / "output_azure"
    if output_dir.exists():
        shutil.rmtree(output_dir)
        print("\nCleaned up local output_azure/")


def main():
    print(f"Sensorstream Azure E2E Test")
    print(f"Container: {CONTAINER_NAME}")
    print(f"Test data: {TEST_DATA_DIR}")

    start = time.time()

    step_create_container()
    step_upload_raw_data()
    results = step_run_pipeline()
    ok = step_verify()
    step_cleanup_local()

    elapsed = time.time() - start
    print(f"\n{'='*50}")

    ok_count = sum(1 for r in results if r.get("status") == "ok")
    fail_count = sum(1 for r in results if r.get("status") == "error")
    total_records = sum(r.get("record_count", 0) for r in results)

    print(f"Pipeline: {ok_count} ok, {fail_count} failed, {total_records} total records")
    print(f"Time: {elapsed:.1f}s")
    print(f"Result: {'PASS' if ok and fail_count == 0 else 'FAIL'}")

    sys.exit(0 if ok and fail_count == 0 else 1)


if __name__ == "__main__":
    main()
