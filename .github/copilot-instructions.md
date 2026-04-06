# Sensorstream IoT Edge Module — Project Guidelines

Azure IoT Edge module for ingesting and processing oceanographic sensor data (CTD, GNSS, ADCP) from USV and ship-based platforms. Outputs GeoParquet files with per-record metadata. Runs on edge devices (Jetson Orin, x86 Ubuntu) and locally in standalone mode.

## Architecture

| Module | Purpose |
|--------|---------|
| `azure_handler/` | IoT Hub client, message sending, twin sync, storage abstraction (local + Azure Blob) |
| `ingest/adapter.py` | **Adapter layer** — delegates all parsing to the `oceanstream` library (NMEA, CSV, CTD hex, ADCP) |
| `ingest/stream_listener.py` | TCP/UDP stream listener, hex stream buffering, batch flushing |
| `ingest/file_watcher.py` | Watchdog directory monitor for new sensor files |
| `ingest/file_trigger.py` | IoT Edge message handler for file-added events |
| `ingest/hex_parser.py` | Sea-Bird hex file utilities (companion file discovery, header parsing) — used by CTD stream simulator |
| `ingest/stream_parser.py` | Thin re-export from `ingest.adapter` (backwards compatibility) |
| `ingest/adcp_parser.py` | Thin re-export from `oceanstream.adcp` (backwards compatibility) |
| `process/` | Processing pipeline: file → adapter parse → DataFrame → GeoParquet + metadata JSON |
| `exports/` | Telemetry (IoT Hub D2C messages), metadata JSON generation, telemetry throttle/downsampling |
| `simulate/` | Built-in simulators: file dropper, NMEA stream replayer, CTD hex stream (SBE 11plus emulation) |
| `test/` | Pytest suite with blob-backed test data fixtures |

### Data Flow

```
Sensor data (TCP/UDP stream, file drop, IoT Edge message)
  → ingest/adapter.py (delegates to oceanstream: NMEA, CSV, CTD hex, ADCP)
  → process/pipeline.py (DataFrame, provider enrichment, deduplication)
  → exports/ (GeoParquet + metadata JSON → storage, telemetry → IoT Hub)
```

### Oceanstream Dependency

All data parsing is handled by the **oceanstream** library (`oceanstream` package from `oceanstream-cli` repo). Sensorstream installs it with `[geotrack,adcp]` extras:

- **Local dev**: `pip install -e "../oceanstream-cli[geotrack,adcp]"`
- **Production**: `pip install "oceanstream[geotrack,adcp] @ git+https://github.com/OceanStreamIO/oceanstream-cli.git"`

The adapter (`ingest/adapter.py`) provides:
- `parse_nmea_line()` / `parse_nmea_file()` — NMEA via `oceanstream.sensors.processors.nmea_gnss`
- `parse_csv_file()` / `parse_csv_line()` — CSV parsing
- `parse_hex_file()` — CTD hex via `oceanstream.sensors.processors.sbe911`
- `parse_cnv_file()` — CTD cnv files
- `parse_adcp_file()` — ADCP via `oceanstream.adcp.processor.process_file`
- `enrich_with_provider()` — Provider enrichment via `oceanstream.providers`
- `detect_format()` — Line format auto-detection (NMEA/CSV/hex)

### Entry Points

| Entry Point | Purpose |
|-------------|---------|
| `main.py` | IoT Edge module — runs under `aziot-edge` runtime with twin config |
| `standalone.py` | CLI tool — local processing, file watching, stream listening, simulators |
| `simulate/__main__.py` | `python -m simulate` — run simulators directly |

## Configuration

All config flows through the `EdgeConfig` dataclass in `config.py`:

- **IoT Edge mode**: `EdgeConfig.from_twin_and_env(desired_properties)` — twin props + env vars
- **Standalone mode**: `EdgeConfig.from_standalone(**kwargs)` — env vars + CLI args
- **Live updates**: `config.update_from_twin(patch)` — applies twin patches in place, increments `_config_version`

Key config fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `input_mode` | `stream \| file \| both` | `both` | Which ingest sources to activate |
| `stream_format` | `nmea \| csv \| hex \| auto` | `auto` | Expected stream data format |
| `stream_protocol` | `tcp \| udp \| auto` | `auto` | Network protocol |
| `stream_port` | `int` | `9100` | Listen/connect port |
| `stream_connect_mode` | `server \| client` | `server` | TCP server (listen) or client (connect) |
| `watch_dir` | `str` | `/data/sensor` | Directory to watch for new files |
| `watch_patterns` | `str` | `*.csv,*.txt,*.hex,*.cnv,*.tar.gz` | Glob patterns for file watcher |
| `batch_interval_seconds` | `int` | `60` | Stream batch flush interval |
| `batch_max_records` | `int` | `1000` | Stream batch flush size |
| `telemetry_downsample_seconds` | `int` | `30` | Min interval between telemetry messages |
| `storage_backend` | `azure-blob-edge \| local` | `azure-blob-edge` | Output storage |
| `output_base_path` | `str` | `/app/processed` | Local output root |

Twin property names map 1:1 to config field names, except `Log_Level` → `log_level` (via `_TWIN_KEY_MAP`).

## Supported Data Formats

| Format | Extension | Parser | Notes |
|--------|-----------|--------|-------|
| NMEA 0183 | `.txt` | `pynmea2` | GGA, RMC, VTG, ZDA sentences; timestamped or raw |
| CSV | `.csv` | pandas | Auto-detect columns; EMSO, R2R, generic |
| Sea-Bird CNV | `.cnv` | pandas | Processed CTD with header metadata |
| Sea-Bird HEX | `.hex` + `.hdr` + `.XMLCON` | `seabirdscientific` + `gsw` | Raw CTD — frequency → T/C/P/S/depth calibration |
| tar.gz | `.tar.gz`, `.tgz` | `tarfile` | Extracts and processes contained files |

### Hex File Parsing

The hex parser (`ingest/hex_parser.py`) requires companion files in the same directory:
- `.hex` — raw scan data (hex-encoded frequency counts)
- `.hdr` — cast metadata (lat/lon, station, time, bytes_per_scan)
- `.XMLCON` — instrument configuration and calibration coefficients

`find_hex_group(hex_path)` locates companions via case-insensitive stem matching. `parse_hex_file()` uses `seabirdscientific` for frequency→engineering-unit conversion and `gsw` for derived quantities (depth, salinity).

## Code Style

- **Python**: 3.11+ with `from __future__ import annotations` in all files
- **Async**: `asyncio` throughout — all pipeline functions are `async def`
- **Logging**: `logging.getLogger("sensorstream")` — never bare `print()`
- **Type hints**: Use throughout; `TYPE_CHECKING` blocks for heavy imports (pandas, geopandas)
- **Optional deps**: Guard with try/except and module-level flags (e.g. `HAS_SBS` for seabirdscientific)
- **Docstrings**: Module-level docstrings on every `.py` file; NumPy-style for public functions
- **Line length**: ~100 characters (no formal linter configured yet)

## Build and Test

```bash
# Create venv and install
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Run tests (always use .venv/bin/python directly — pyenv can interfere)
.venv/bin/python -m pytest test/ -v --tb=short

# Run standalone pipeline
.venv/bin/python standalone.py --input-dir ./test_data --output-dir ./output --campaign-id test

# Run CTD stream simulator
.venv/bin/python standalone.py --simulate-ctd ./test_data/ctd/hex/11901.hex --output-dir ./output
```

### Test Data

Test data lives in Azure Blob Storage, not in the repo:

- **Account**: `ne1osvmdevtest`
- **Container**: `sensorstream-test`
- **Prefix**: `raw/` (mirrors the `test_data/` directory structure)

The `test/conftest.py` session fixtures automatically:
1. Download from blob to `.test_data_cache/` on first run
2. Use size-based freshness checks on subsequent runs
3. Fall back to local `test_data/` if no Azure connection

**Connection string**: Set `AZURE_CONNECTION_STRING` in `.env` or environment. The conftest also checks `../oceanstream-cli/.env`.

Key fixtures (all session-scoped):
- `test_data_dir` — root of synced test data
- `hex_dir`, `hex_file` — Sea-Bird cast 11901 hex data
- `ctd_csv_file`, `ctd_cnv_file` — EMSO CSV and Sea-Bird CNV
- `gnss_nmea_file` — R2R GNSS NMEA track

### Test Files

| File | Count | Scope |
|------|-------|-------|
| `test_config.py` | 10 | EdgeConfig creation, twin updates, literal validation |
| `test_stream_parser.py` | 17 | NMEA/CSV parsing, format detection |
| `test_pipeline.py` | 4 | Pipeline with inline data (no external files) |
| `test_file_watcher.py` | 4 | FileWatcher events |
| `test_simulators.py` | 3 | File/stream simulators |
| `test_hex_parser.py` | 25 | Hex parsing, calibration, pipeline integration |
| `test_ctd_stream.py` | 9 | CTD stream simulator, hex format detection |
| `test_throttle.py` | 16 | Telemetry downsampling, config fields |
| `test_azure_e2e.py` | — | Standalone E2E script (`python test/test_azure_e2e.py`) |

## Docker

Two Dockerfiles for IoT Edge deployment:

- `Dockerfile.Linux.amd64` — x86_64 (Ubuntu-based dev machines, cloud VMs)
- `Dockerfile.Linux.arm64` — ARM64 (Jetson Orin edge devices)

Both install from `requirements.txt` and copy the full project. Entry point: `python -u main.py`.

Build via Streambase CLI:
```bash
streambase module build -e <env> -m iotedge-sensorstream -o Linux -a amd64 -t latest
```

## IoT Edge Integration

### Input Messages

Receives `sensorfileadded` messages from the `filenotifier` module:
```json
{"event": "fileadd", "path": "/data/raw/ctd/cast.cnv", "size": 17500}
```

### Output Messages

Sends D2C telemetry to IoT Hub (downsampled per `telemetry_downsample_seconds`):
- Individual sensor records (if `telemetry_send_records` is true)
- Periodic summaries (if `telemetry_send_summaries` is true)

### Twin Desired Properties

All `EdgeConfig` fields can be set via twin desired properties. The module reports back accepted values to twin reported properties.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AZURE_CONNECTION_STRING` | — | Azure Storage connection string (for tests and E2E) |
| `STORAGE_BACKEND` | `azure-blob-edge` | `local` for standalone |
| `OUTPUT_BASE_PATH` | `/app/processed` | Output directory |
| `PROCESSED_CONTAINER_NAME` | `sensordata` | Blob container for processed output |
| `WATCH_DIR` | `/data/sensor` | File watch directory |
| `STREAM_HOST` | `0.0.0.0` | Stream listener bind address |
| `STREAM_PORT` | `9100` | Stream listener port |
| `CAMPAIGN_ID` | — | Campaign identifier for output partitioning |
| `PLATFORM_ID` | — | Platform/vessel identifier |
| `LOG_LEVEL` | `INFO` | Logging level |

## Conventions

- **Storage abstraction**: All file I/O goes through `StorageBackend` (ABC in `azure_handler/storage.py`). Implementations: `LocalStorage`, `AzureBlobEdgeStorage`. Never write files directly.
- **Processing results**: `process_file()` and `process_stream_batch()` return `dict` with keys: `status` (`ok`/`error`/`skipped`), `record_count`, `file_type`, `processing_time_ms`.
- **GeoParquet output**: One `.parquet` + one `.metadata.json` per input file, written to `{output_base_path}/{campaign_id}/`.
- **Telemetry throttle**: `TelemetryThrottle` in `exports/throttle.py` rate-limits D2C messages. Window is twin-controllable via `telemetry_downsample_seconds`.
- **No setup.py/pyproject.toml**: This is a deployable module, not a pip-installable library. Dependencies are in `requirements.txt`.
