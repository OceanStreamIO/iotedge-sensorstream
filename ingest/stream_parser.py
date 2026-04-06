"""Parse NMEA and CSV lines from network streams or files.

.. deprecated::
    This module is a thin re-export layer. All parsing is now handled
    by ``ingest.adapter`` which delegates to the ``oceanstream`` library.
    Import from ``ingest.adapter`` directly in new code.
"""

from __future__ import annotations

from ingest.adapter import (  # noqa: F401
    detect_format,
    parse_csv_line,
    parse_nmea_line,
    records_to_dataframe,
)

# Re-export OUTPUT_COLUMNS for backwards compatibility
OUTPUT_COLUMNS = [
    "time",
    "latitude",
    "longitude",
    "gps_quality",
    "num_satellites",
    "horizontal_dilution",
    "gps_antenna_height",
    "speed_over_ground",
    "course_over_ground",
    "depth",
    "TEMP",
    "PSAL",
    "CNDC",
]
