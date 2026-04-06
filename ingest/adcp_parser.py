"""Parse RDI (Teledyne) ADCP binary files.

.. deprecated::
    This module is a thin re-export layer. All ADCP parsing is now
    handled by the ``oceanstream`` library via ``ingest.adapter``.
    Import from ``ingest.adapter`` or ``oceanstream.adcp`` directly
    in new code.
"""

from __future__ import annotations

import logging

from ingest.adapter import HAS_ADCP as HAS_DOLFYN  # noqa: F401
from ingest.adapter import parse_adcp_file  # noqa: F401

logger = logging.getLogger("sensorstream")

# Re-export oceanstream ADCP functions for backwards compatibility
if HAS_DOLFYN:
    from oceanstream.adcp.rdi_reader import read_rdi  # noqa: F401
    from oceanstream.adcp.transforms import (  # noqa: F401
        adcp_to_dataframe,
        beam_to_earth,
        ensemble_average,
    )
