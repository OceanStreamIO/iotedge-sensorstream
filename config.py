"""Edge processing configuration for the sensorstream module.

Populated from IoT Hub module twin desired properties and environment
variables.  Provides a single typed dataclass for all processing
parameters.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field, fields
from typing import Any, Dict, Literal, Optional

logger = logging.getLogger("sensorstream")


def _parse_bool(val: Any, default: bool = True) -> bool:
    """Parse a value that might be bool, str, int, or None into a Python bool."""
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        return val.lower() not in ("false", "0", "no", "off", "")
    return default


def _unwrap(val: Any) -> Any:
    """Unwrap ``{"value": X}`` IoT Hub twin wrapper, if present."""
    if isinstance(val, dict) and "value" in val:
        return val["value"]
    return val


def sanitize_container_name(name: str) -> str:
    """Sanitize a string into a valid Azure Blob Storage container name.

    Azure rules: 3-63 chars, lowercase alphanumeric and hyphens only,
    no leading/trailing/consecutive hyphens, must start with letter or number.
    """
    if not name:
        return "default"
    s = name.lower()
    s = re.sub(r"[^a-z0-9-]", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip("-")
    s = s[:63]
    s = s.strip("-")
    if not s or not s[0].isalnum():
        s = "default"
    if len(s) < 3:
        s = s.ljust(3, "0")
    return s


def _parse_dict(val: Any) -> Optional[Dict[str, Any]]:
    """Parse a value that may be a dict, JSON string, or None."""
    if val is None or val == "":
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            parsed = json.loads(val)
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            logger.warning("Invalid JSON for dict field: %r", val)
    return None


# Literal field restrictions
_LITERAL_FIELDS: dict[str, frozenset[str]] = {
    "input_mode": frozenset({"stream", "file", "both"}),
    "stream_protocol": frozenset({"tcp", "udp", "auto"}),
    "stream_format": frozenset({"nmea", "csv", "hex", "auto"}),
    "stream_connect_mode": frozenset({"server", "client"}),
    "storage_backend": frozenset({"azure-blob-edge", "local"}),
}

_TWIN_KEY_MAP: dict[str, str] = {"Log_Level": "log_level"}

_BOOL_FIELDS: set[str] = {
    "watch_polling",
    "telemetry_send_records",
    "telemetry_send_summaries",
}

_INT_FIELDS: set[str] = {
    "stream_port",
    "batch_interval_seconds",
    "batch_max_records",
    "telemetry_interval_seconds",
    "telemetry_downsample_seconds",
    "watch_poll_interval",
}

_FLOAT_FIELDS: set[str] = set()

_DICT_FIELDS: set[str] = set()


@dataclass
class EdgeConfig:
    """Unified configuration for the sensorstream edge module."""

    # --- Input mode ---
    input_mode: Literal["stream", "file", "both"] = "both"

    # --- Network stream settings ---
    stream_protocol: Literal["tcp", "udp", "auto"] = "auto"
    stream_host: str = "0.0.0.0"
    stream_port: int = 9100
    stream_format: Literal["nmea", "csv", "hex", "auto"] = "auto"
    stream_connect_mode: Literal["server", "client"] = "server"

    # --- File watcher settings ---
    watch_dir: str = "/data/sensor"
    watch_patterns: str = "*.csv,*.txt,*.hex,*.cnv,*.raw,*.tar.gz"
    watch_polling: bool = False
    watch_poll_interval: int = 2

    # --- Batching ---
    batch_interval_seconds: int = 60
    batch_max_records: int = 1000

    # --- Metadata ---
    survey_id: str = ""
    campaign_id: str = ""
    platform_id: str = ""
    platform_name: str = ""
    provider: str = "auto"

    # --- Telemetry ---
    telemetry_interval_seconds: int = 300
    telemetry_send_records: bool = True
    telemetry_send_summaries: bool = True
    telemetry_downsample_seconds: int = 30

    # --- Storage ---
    storage_backend: Literal["azure-blob-edge", "local"] = "azure-blob-edge"
    output_base_path: str = "/app/processed"
    processed_container: str = "sensordata"

    # --- Logging ---
    log_level: str = "INFO"

    # --- Internal ---
    _config_version: int = field(default=0, repr=False)

    @property
    def campaign_container(self) -> str:
        """Azure Blob container name derived from survey_id or campaign_id.

        All data for a campaign lands in one container with subfolders
        for each data type.  Falls back to ``"default"`` when both are empty.
        """
        return sanitize_container_name(self.survey_id or self.campaign_id or "default")

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_twin_and_env(cls, twin_desired: Dict[str, Any]) -> "EdgeConfig":
        """Create config from IoT Hub module twin + environment variables."""

        def _get(key: str, default: Any = "") -> Any:
            val = twin_desired.get(key, default)
            return _unwrap(val)

        def _safe_int(val, default, name):
            try:
                return int(val)
            except (ValueError, TypeError):
                logger.warning("Invalid %s in twin: %r — using default %s", name, val, default)
                return default

        return cls(
            input_mode=_get("input_mode", "both"),
            stream_protocol=_get("stream_protocol", "auto"),
            stream_host=os.getenv("STREAM_HOST", _get("stream_host", "0.0.0.0")),
            stream_port=_safe_int(
                os.getenv("STREAM_PORT", _get("stream_port", 9100)), 9100, "stream_port"
            ),
            stream_format=_get("stream_format", "auto"),
            stream_connect_mode=_get("stream_connect_mode", "server"),
            watch_dir=os.getenv("WATCH_DIR", _get("watch_dir", "/data/sensor")),
            watch_patterns=_get("watch_patterns", "*.csv,*.txt,*.hex,*.cnv,*.raw,*.tar.gz"),
            watch_polling=_parse_bool(_get("watch_polling", False), default=False),
            watch_poll_interval=_safe_int(
                _get("watch_poll_interval", 2), 2, "watch_poll_interval"
            ),
            batch_interval_seconds=_safe_int(
                _get("batch_interval_seconds", 60), 60, "batch_interval_seconds"
            ),
            batch_max_records=_safe_int(
                _get("batch_max_records", 1000), 1000, "batch_max_records"
            ),
            campaign_id=os.getenv("CAMPAIGN_ID", _get("campaign_id", "")),
            survey_id=os.getenv("SURVEY_ID", _get("survey_id", "")),
            platform_id=os.getenv("PLATFORM_ID", _get("platform_id", "")),
            platform_name=os.getenv("PLATFORM_NAME", _get("platform_name", "")),
            provider=_get("provider", "auto"),
            telemetry_interval_seconds=_safe_int(
                _get("telemetry_interval_seconds", 300), 300, "telemetry_interval_seconds"
            ),
            telemetry_send_records=_parse_bool(_get("telemetry_send_records", True)),
            telemetry_send_summaries=_parse_bool(_get("telemetry_send_summaries", True)),
            telemetry_downsample_seconds=_safe_int(
                _get("telemetry_downsample_seconds", 30), 30, "telemetry_downsample_seconds"
            ),
            storage_backend=os.getenv("STORAGE_BACKEND", _get("storage_backend", "azure-blob-edge")),
            output_base_path=os.getenv("OUTPUT_BASE_PATH", "/app/processed"),
            processed_container=os.getenv("PROCESSED_CONTAINER_NAME", "sensordata"),
            log_level=_get("Log_Level", "INFO"),
        )

    def update_from_twin(self, patch: Dict[str, Any]) -> None:
        """Apply a twin desired-properties patch in place."""
        known_fields = {f.name for f in fields(self)}

        for key, raw_val in patch.items():
            val = _unwrap(raw_val)
            field_name = _TWIN_KEY_MAP.get(key, key)
            if field_name not in known_fields:
                continue

            if field_name in _BOOL_FIELDS:
                setattr(self, field_name, _parse_bool(val))
            elif field_name in _INT_FIELDS:
                try:
                    setattr(self, field_name, int(val))
                except (ValueError, TypeError):
                    pass
            elif field_name in _FLOAT_FIELDS:
                try:
                    setattr(self, field_name, float(val))
                except (ValueError, TypeError):
                    pass
            elif field_name in _DICT_FIELDS:
                parsed = _parse_dict(val)
                if parsed is not None:
                    setattr(self, field_name, parsed)
            elif field_name in _LITERAL_FIELDS:
                str_val = str(val) if not isinstance(val, str) else val
                if str_val in _LITERAL_FIELDS[field_name]:
                    setattr(self, field_name, str_val)
                else:
                    logger.warning(
                        "Invalid value %r for %s — allowed: %s",
                        str_val, field_name, _LITERAL_FIELDS[field_name],
                    )
            else:
                setattr(self, field_name, str(val) if not isinstance(val, str) else val)

        self._config_version += 1
        logger.info(
            "Config updated from twin patch (v%d): %s",
            self._config_version,
            [k for k in patch if not k.startswith("$")],
        )

    @classmethod
    def from_standalone(cls, **kwargs: Any) -> "EdgeConfig":
        """Create config for standalone (non-IoT-Edge) operation."""
        defaults = {
            "storage_backend": os.getenv("STORAGE_BACKEND", "local"),
            "output_base_path": os.getenv("OUTPUT_BASE_PATH", "./output"),
            "input_mode": "file",
            "campaign_id": os.getenv("CAMPAIGN_ID", ""),
            "survey_id": os.getenv("SURVEY_ID", ""),
            "platform_id": os.getenv("PLATFORM_ID", ""),
            "platform_name": os.getenv("PLATFORM_NAME", ""),
            "provider": os.getenv("PROVIDER", "auto"),
            "watch_dir": os.getenv("WATCH_DIR", "/tmp/sensorstream-watch"),
            "stream_host": os.getenv("STREAM_HOST", "0.0.0.0"),
            "stream_port": int(os.getenv("STREAM_PORT", "9100")),
            "batch_interval_seconds": int(os.getenv("BATCH_INTERVAL_SECONDS", "60")),
            "batch_max_records": int(os.getenv("BATCH_MAX_RECORDS", "1000")),
            "log_level": os.getenv("LOG_LEVEL", "INFO"),
        }
        defaults.update(kwargs)
        return cls(**defaults)
