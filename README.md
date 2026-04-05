# iotedge-sensorstream

Azure IoT Edge module for ingesting and processing oceanographic sensor data (CTD, GNSS) from USV and ship-based platforms. Outputs GeoParquet files with per-record metadata JSON. Runs on edge devices (Jetson Orin, x86 Ubuntu) and locally in standalone mode.

## Data Flow

```
Sensor data (TCP/UDP stream, file drop, IoT Edge message)
  → ingest/ (parse NMEA, CSV, .hex, .cnv)
  → process/pipeline.py (DataFrame → enrich → validate)
  → exports/ (GeoParquet + metadata JSON → storage, telemetry → IoT Hub)
```

## Supported Formats

| Format | Extension | Parser | Notes |
|--------|-----------|--------|-------|
| NMEA 0183 | `.txt` | pynmea2 | GGA, RMC, VTG, ZDA; timestamped or raw |
| CSV | `.csv` | pandas | Auto-detect columns; EMSO, R2R, generic |
| Sea-Bird CNV | `.cnv` | pandas | Processed CTD with header metadata |
| Sea-Bird HEX | `.hex` + `.hdr` + `.XMLCON` | seabirdscientific + gsw | Raw CTD frequency → T/C/P/S/depth |
| tar.gz | `.tar.gz`, `.tgz` | tarfile | Extracts and processes contained files |

## Quick Start

### Standalone (local processing)

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements.txt

# Process files from a directory
.venv/bin/python standalone.py \
    --input-dir ./test_data \
    --output-dir ./output \
    --campaign-id my_cruise

# Watch a directory for new files
.venv/bin/python standalone.py \
    --watch /tmp/sensorstream-watch \
    --output-dir ./output

# Simulate a CTD hex stream over TCP
.venv/bin/python standalone.py \
    --simulate-ctd ./test_data/ctd/hex/11901.hex \
    --output-dir ./output
```

### IoT Edge

Deploy via the [Streambase CLI](https://github.com/OceanStreamIO/streambase):

```bash
streambase module build -e <env> -m iotedge-sensorstream -o Linux -a amd64 -t latest
streambase device apply -e <env> -d <device-id>
```

Configuration is driven by IoT Hub module twin desired properties. All `EdgeConfig` fields can be set via the twin and are reported back as reported properties.

## Architecture

| Module | Purpose |
|--------|---------|
| `azure_handler/` | IoT Hub client, message sending, twin sync, storage abstraction |
| `ingest/` | TCP/UDP stream listener, file watcher, IoT Edge triggers, hex/NMEA/CSV parsing |
| `process/` | Processing pipeline: file → DataFrame → GeoParquet + metadata JSON |
| `exports/` | D2C telemetry, metadata JSON generation, telemetry throttle/downsampling |
| `simulate/` | Built-in simulators: file dropper, NMEA stream replayer, CTD hex stream |

### Entry Points

| File | Purpose |
|------|---------|
| `main.py` | IoT Edge module — runs under aziot-edge runtime |
| `standalone.py` | CLI — local processing, file watching, stream listening, simulators |
| `simulate/__main__.py` | `python -m simulate` — run simulators directly |

## Configuration

All config flows through the `EdgeConfig` dataclass in `config.py`:

| Field | Default | Description |
|-------|---------|-------------|
| `input_mode` | `both` | `stream`, `file`, or `both` |
| `stream_format` | `auto` | `nmea`, `csv`, `hex`, or `auto` |
| `stream_port` | `9100` | TCP/UDP listen port |
| `watch_dir` | `/data/sensor` | Directory to watch for new files |
| `batch_interval_seconds` | `60` | Stream batch flush interval |
| `telemetry_downsample_seconds` | `30` | Min interval between D2C messages |
| `storage_backend` | `azure-blob-edge` | `azure-blob-edge` or `local` |
| `output_base_path` | `/app/processed` | Output root directory |

See `config.py` for the full list. Standalone mode uses env vars and CLI args; IoT Edge mode uses twin desired properties.

## Testing

```bash
# Run all tests
.venv/bin/python -m pytest test/ -v --tb=short

# Run Azure E2E test (standalone script)
.venv/bin/python test/test_azure_e2e.py
```

Test data is stored in Azure Blob Storage (`sensorstream-test` container) and downloaded automatically on first run. Set `AZURE_CONNECTION_STRING` in `.env` or environment. Tests fall back to local `test_data/` if no connection is available.

**90 tests** across 8 files: config, stream parsing, pipeline, file watcher, simulators, hex parsing, CTD stream, and telemetry throttling.

## IoT Edge Integration

**Input**: Receives `sensorfileadded` messages from the `filenotifier` module:
```json
{"event": "fileadd", "path": "/data/raw/ctd/cast.cnv", "size": 17500}
```

**Output**: D2C telemetry to IoT Hub (rate-limited by `telemetry_downsample_seconds`), GeoParquet + metadata JSON to blob storage.

**Twin**: All config fields are readable/writable via module twin. Changes are applied live without restart.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `AZURE_CONNECTION_STRING` | — | Storage connection string (tests, E2E) |
| `STORAGE_BACKEND` | `azure-blob-edge` | `local` for standalone |
| `OUTPUT_BASE_PATH` | `/app/processed` | Output directory |
| `WATCH_DIR` | `/data/sensor` | File watch directory |
| `STREAM_PORT` | `9100` | Stream listener port |
| `CAMPAIGN_ID` | — | Campaign identifier |
| `LOG_LEVEL` | `INFO` | Logging level |

## Docker

Two Dockerfiles for IoT Edge deployment:

- `Dockerfile.Linux.amd64` — x86_64
- `Dockerfile.Linux.arm64` — ARM64 (Jetson Orin)

## License

See [LICENSE](LICENSE).
