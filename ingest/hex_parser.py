"""Parse raw SeaBird CTD .hex files using seabirdscientific.

A ``.hex`` file contains concatenated hexadecimal scans produced by an
SBE 911plus (or similar) CTD.  Each scan encodes frequency counts for
temperature/conductivity/pressure, auxiliary voltage channels, optional
NMEA position, and optional system time.

To decode the hex data we need the companion ``.XMLCON`` (or ``.xmlcon``)
file that describes the sensor configuration and calibration coefficients,
and optionally the ``.hdr`` file for cast metadata (lat/lon, station name,
start time).

This module provides:

- ``parse_hdr_file()`` — extract metadata from a .hdr file
- ``parse_xmlcon_file()`` — extract sensor config + calibration from XMLCON
- ``parse_hex_file()`` — decode a .hex file to a :class:`pandas.DataFrame`
  with calibrated T/C/P/S/depth columns and raw frequency columns.
- ``find_hex_group()`` — find associated .hdr / .xmlcon for a .hex file
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pandas as pd

logger = logging.getLogger("sensorstream")

# ---------------------------------------------------------------------------
# Optional heavy deps — fail gracefully when not installed
# ---------------------------------------------------------------------------

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore[assignment]

try:
    import seabirdscientific.conversion as sbs_conv
    import seabirdscientific.instrument_data as sbs_id
    from seabirdscientific.cal_coefficients import (
        ConductivityCoefficients,
        Oxygen43Coefficients,
        PressureDigiquartzCoefficients,
        TemperatureFrequencyCoefficients,
    )

    HAS_SBS = True
except ImportError:
    sbs_conv = None  # type: ignore[assignment]
    sbs_id = None  # type: ignore[assignment]
    HAS_SBS = False

try:
    import gsw as _gsw
except ImportError:
    _gsw = None

# Scan rate for SBE 911plus (Hz)
_SCAN_RATE = 24.0


# ---- HDR parsing ----------------------------------------------------------


def parse_hdr_file(hdr_path: Path) -> dict[str, Any]:
    """Extract metadata from a SeaBird ``.hdr`` file.

    Returns
    -------
    dict
        Keys may include: ``latitude``, ``longitude``, ``start_time``,
        ``station``, ``bytes_per_scan``, ``voltage_words``.
    """
    metadata: dict[str, Any] = {}
    with open(hdr_path, encoding="utf-8", errors="replace") as f:
        content = f.read()

    lat_m = re.search(r"\*\s*NMEA Latitude\s*=\s*(\d+)\s+(\d+\.?\d*)\s*([NS])", content)
    if lat_m:
        lat = float(lat_m.group(1)) + float(lat_m.group(2)) / 60
        if lat_m.group(3) == "S":
            lat = -lat
        metadata["latitude"] = lat

    lon_m = re.search(r"\*\s*NMEA Longitude\s*=\s*(\d+)\s+(\d+\.?\d*)\s*([EW])", content)
    if lon_m:
        lon = float(lon_m.group(1)) + float(lon_m.group(2)) / 60
        if lon_m.group(3) == "W":
            lon = -lon
        metadata["longitude"] = lon

    time_m = re.search(
        r"\*\s*NMEA UTC \(Time\)\s*=\s*(\w+ \d+ \d{4}\s+\d{2}:\d{2}:\d{2})", content
    )
    if time_m:
        try:
            metadata["start_time"] = pd.Timestamp(time_m.group(1).strip(), tz="UTC")
        except Exception:
            pass

    station_m = re.search(r"\*\*\s*Station:\s*(.+)", content)
    if station_m:
        metadata["station"] = station_m.group(1).strip()

    bytes_m = re.search(r"\*\s*Number of Bytes Per Scan\s*=\s*(\d+)", content)
    if bytes_m:
        metadata["bytes_per_scan"] = int(bytes_m.group(1))

    voltage_m = re.search(r"\*\s*Number of Voltage Words\s*=\s*(\d+)", content)
    if voltage_m:
        metadata["voltage_words"] = int(voltage_m.group(1))

    return metadata


# ---- XMLCON parsing -------------------------------------------------------


def parse_xmlcon_file(xmlcon_path: Path) -> dict[str, Any]:
    """Parse a SeaBird ``.XMLCON`` file for sensor configuration.

    Returns
    -------
    dict
        Keys: ``sensors`` (list of sensor dicts with ``type``, ``index``,
        ``coefficients``), ``frequency_channels_suppressed``,
        ``voltage_words_suppressed``, ``nmea_position_added``,
        ``scan_time_added``.
    """
    config: dict[str, Any] = {
        "sensors": [],
        "frequency_channels_suppressed": 0,
        "voltage_words_suppressed": 0,
        "nmea_position_added": False,
        "scan_time_added": False,
    }

    try:
        tree = ET.parse(xmlcon_path)
        root = tree.getroot()

        instrument = root.find(".//Instrument")
        if instrument is not None:
            config["frequency_channels_suppressed"] = int(
                instrument.findtext("FrequencyChannelsSuppressed", "0")
            )
            config["voltage_words_suppressed"] = int(
                instrument.findtext("VoltageWordsSuppressed", "0")
            )
            config["nmea_position_added"] = instrument.findtext("NmeaPositionDataAdded", "0") == "1"
            config["scan_time_added"] = instrument.findtext("ScanTimeAdded", "0") == "1"

        sensor_array = root.find(".//SensorArray")
        if sensor_array is not None:
            for sensor_elem in sensor_array.findall("Sensor"):
                info: dict[str, Any] = {
                    "index": int(sensor_elem.get("index", -1)),
                    "sensor_id": sensor_elem.get("SensorID", ""),
                }
                for child in sensor_elem:
                    if child.tag.endswith("Sensor") or child.tag in ("NotInUse", "UserPolynomialSensor"):
                        info["type"] = child.tag
                        info["serial_number"] = child.findtext("SerialNumber", "")
                        info["calibration_date"] = child.findtext("CalibrationDate", "")
                        coefs: dict[str, Any] = {}
                        for el in child.iter():
                            if el.tag in (child.tag, "Coefficients", "CalibrationCoefficients"):
                                continue
                            if el.text and el.text.strip():
                                try:
                                    coefs[el.tag] = float(el.text)
                                except ValueError:
                                    coefs[el.tag] = el.text.strip()
                        # For sensors with multiple <Coefficients equation="N">
                        # blocks, prefer equation="1" (G/H/I/J formulation).
                        coef_blocks = child.findall("Coefficients")
                        if not coef_blocks:
                            coef_blocks = child.findall("CalibrationCoefficients")
                        if len(coef_blocks) > 1:
                            for cb in coef_blocks:
                                if cb.get("equation") == "1":
                                    for el in cb:
                                        if el.text and el.text.strip():
                                            try:
                                                coefs[el.tag] = float(el.text)
                                            except ValueError:
                                                coefs[el.tag] = el.text.strip()
                                    break
                        info["coefficients"] = coefs
                        break
                config["sensors"].append(info)

    except ET.ParseError as e:
        logger.warning("Could not parse XMLCON file %s: %s", xmlcon_path, e)

    return config


# ---- helpers for calibration coefficient builders -------------------------


def _find_sensor(sensors: list[dict[str, Any]], sensor_type: str, *, start: int = 0):
    """Return the first sensor dict matching *sensor_type* from *start*."""
    for s in sensors[start:]:
        if s.get("type") == sensor_type:
            return s
    return None


def _build_temp_coefs(sensor: dict[str, Any]):
    c = sensor.get("coefficients", {})
    try:
        return TemperatureFrequencyCoefficients(
            g=float(c["G"]), h=float(c["H"]), i=float(c["I"]), j=float(c["J"]), f0=float(c["F0"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _build_cond_coefs(sensor: dict[str, Any]):
    c = sensor.get("coefficients", {})
    try:
        return ConductivityCoefficients(
            g=float(c["G"]), h=float(c["H"]), i=float(c["I"]), j=float(c["J"]),
            cpcor=float(c["CPcor"]), ctcor=float(c["CTcor"]),
            wbotc=float(c.get("WBOTC", 0.0)),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _build_press_coefs(sensor: dict[str, Any]):
    c = sensor.get("coefficients", {})
    try:
        return PressureDigiquartzCoefficients(
            c1=float(c["C1"]), c2=float(c["C2"]), c3=float(c["C3"]),
            d1=float(c["D1"]), d2=float(c["D2"]),
            t1=float(c["T1"]), t2=float(c["T2"]), t3=float(c["T3"]),
            t4=float(c["T4"]), t5=float(c.get("T5", 0.0)),
            AD590M=float(c.get("AD590M", 0.01278)),
            AD590B=float(c.get("AD590B", -9.256)),
        )
    except (KeyError, TypeError, ValueError):
        return None


# ---- calibrated conversion ------------------------------------------------


def _calibrate(
    raw_data: pd.DataFrame,
    sensors: list[dict[str, Any]],
    latitude: float,
) -> dict[str, Any]:
    """Apply seabirdscientific calibration equations to raw scan data.

    Returns a dict of ``column_name → ndarray`` for calibrated channels.
    """
    if sbs_conv is None:
        raise ImportError("seabirdscientific is required for calibration")

    out: dict[str, Any] = {}

    # Primary temperature
    ts = _find_sensor(sensors, "TemperatureSensor")
    tc = _build_temp_coefs(ts) if ts else None
    if tc and "temperature" in raw_data.columns:
        out["temperature"] = sbs_conv.convert_temperature_frequency(raw_data["temperature"].values, tc)

    # Pressure
    ps = _find_sensor(sensors, "PressureSensor")
    pc = _build_press_coefs(ps) if ps else None
    if (
        pc
        and "digiquartz pressure" in raw_data.columns
        and "temperature compensation" in raw_data.columns
    ):
        out["pressure"] = sbs_conv.convert_pressure_digiquartz(
            raw_data["digiquartz pressure"].values,
            raw_data["temperature compensation"].values,
            pc,
            "dbar",
            1.0 / _SCAN_RATE,
        )

    # Primary conductivity (needs calibrated T and P)
    cs = _find_sensor(sensors, "ConductivitySensor")
    cc = _build_cond_coefs(cs) if cs else None
    if cc and "conductivity" in raw_data.columns and "temperature" in out and "pressure" in out:
        out["conductivity"] = (
            sbs_conv.convert_conductivity(
                raw_data["conductivity"].values,
                out["temperature"],
                out["pressure"],
                cc,
            )
            / 10.0  # mS/cm → S/m
        )

    # Salinity via GSW
    if _gsw is not None and all(k in out for k in ("conductivity", "temperature", "pressure")):
        out["salinity"] = _gsw.SP_from_C(out["conductivity"] * 10.0, out["temperature"], out["pressure"])

    # Depth from pressure
    if sbs_conv is not None and "pressure" in out:
        out["depth"] = sbs_conv.depth_from_pressure(out["pressure"], latitude)

    return out


# ---- public API -----------------------------------------------------------


def find_hex_group(hex_path: Path) -> dict[str, Path | None]:
    """Find companion .hdr and .XMLCON files for a .hex file.

    Searches the same directory for files with the same stem.

    Returns
    -------
    dict
        Keys: ``hex``, ``hdr``, ``xmlcon`` — each ``Path | None``.
    """
    parent = hex_path.parent
    stem = hex_path.stem

    hdr = None
    for candidate in parent.glob(f"{stem}.*"):
        if candidate.suffix.lower() == ".hdr":
            hdr = candidate
            break

    xmlcon = None
    for candidate in parent.glob(f"{stem}.*"):
        if candidate.suffix.lower() == ".xmlcon":
            xmlcon = candidate
            break

    return {"hex": hex_path, "hdr": hdr, "xmlcon": xmlcon}


def parse_hex_file(hex_path: Path) -> pd.DataFrame:
    """Read a SeaBird ``.hex`` file and return a DataFrame.

    Automatically looks for matching ``.hdr`` and ``.XMLCON`` files in
    the same directory to obtain metadata and calibration coefficients.

    If ``seabirdscientific`` is installed **and** an ``.XMLCON`` file is
    found, calibrated columns (``temperature``, ``conductivity``,
    ``pressure``, ``salinity``, ``depth``) are added.  Raw frequency
    columns are always included.

    Returns
    -------
    pd.DataFrame
        Columns vary by configuration.  Typical calibrated output::

            time, latitude, longitude, station, depth, temperature,
            conductivity, pressure, salinity, temperature_freq,
            conductivity_freq, pressure_freq, scan
    """
    if not HAS_SBS:
        raise ImportError(
            "seabirdscientific is required for .hex file parsing. "
            "Install with: pip install seabirdscientific"
        )
    if np is None:
        raise ImportError("numpy is required for .hex file parsing")

    group = find_hex_group(hex_path)

    # -- metadata from .hdr -------------------------------------------------
    hdr_meta: dict[str, Any] = {}
    if group["hdr"] is not None:
        hdr_meta = parse_hdr_file(group["hdr"])

    # -- sensor config from .XMLCON -----------------------------------------
    xmlcon_cfg: dict[str, Any] = {}
    if group["xmlcon"] is not None:
        xmlcon_cfg = parse_xmlcon_file(group["xmlcon"])

    # -- build enabled-sensors list for sbs_id.read_hex_file ----------------
    enabled_sensors = [
        sbs_id.Sensors.Temperature,
        sbs_id.Sensors.Conductivity,
        sbs_id.Sensors.Pressure,
    ]

    if xmlcon_cfg.get("sensors"):
        types = [s.get("type", "") for s in xmlcon_cfg["sensors"]]
        if "TemperatureSensor" in types[3:]:
            enabled_sensors.append(sbs_id.Sensors.SecondaryTemperature)
        if "ConductivitySensor" in types[4:]:
            enabled_sensors.append(sbs_id.Sensors.SecondaryConductivity)

    voltage_count = 8 - xmlcon_cfg.get("voltage_words_suppressed", 0)
    for i in range(min(voltage_count, 8)):
        attr = f"ExtVolt{i}"
        if hasattr(sbs_id.Sensors, attr):
            enabled_sensors.append(getattr(sbs_id.Sensors, attr))

    if xmlcon_cfg.get("nmea_position_added", False):
        enabled_sensors.append(sbs_id.Sensors.nmeaLocation)
    if xmlcon_cfg.get("scan_time_added", False):
        enabled_sensors.append(sbs_id.Sensors.SystemTime)

    # -- read the hex file --------------------------------------------------
    raw = sbs_id.read_hex_file(
        filepath=hex_path,
        instrument_type=sbs_id.InstrumentType.SBE911Plus,
        enabled_sensors=enabled_sensors,
        frequency_channels_suppressed=xmlcon_cfg.get("frequency_channels_suppressed", 0),
        voltage_words_suppressed=xmlcon_cfg.get("voltage_words_suppressed", 0),
    )

    logger.info("Read %d scans from %s", len(raw), hex_path.name)

    # -- build output DataFrame ---------------------------------------------
    out: dict[str, Any] = {}

    # Metadata columns
    station = hdr_meta.get("station")
    if station:
        out["station"] = station

    # Position: header-level (constant) or per-scan NMEA
    if "latitude" in hdr_meta:
        out["latitude"] = hdr_meta["latitude"]
    elif "NMEA Latitude" in raw.columns:
        out["latitude"] = raw["NMEA Latitude"]

    if "longitude" in hdr_meta:
        out["longitude"] = hdr_meta["longitude"]
    elif "NMEA Longitude" in raw.columns:
        out["longitude"] = raw["NMEA Longitude"]

    # Time
    if "system time" in raw.columns:
        out["time"] = raw["system time"]
    elif "start_time" in hdr_meta:
        out["time"] = hdr_meta["start_time"]

    # Calibrated conversion
    if xmlcon_cfg.get("sensors") and HAS_SBS:
        lat = hdr_meta.get("latitude", 0.0)
        try:
            cal = _calibrate(raw, xmlcon_cfg["sensors"], lat)
            out.update(cal)
            logger.info("Calibrated %d columns: %s", len(cal), ", ".join(cal.keys()))
        except Exception as e:
            logger.warning("Calibrated conversion failed, using raw only: %s", e)

    # Raw frequency / voltage columns
    raw_col_map = {
        "temperature": "temperature_freq",
        "conductivity": "conductivity_freq",
        "digiquartz pressure": "pressure_freq",
        "temperature compensation": "temperature_comp",
        "secondary temperature": "temperature2_freq",
        "secondary conductivity": "conductivity2_freq",
    }
    for i in range(8):
        raw_col_map[f"volt {i}"] = f"volt{i}"

    for src, dst in raw_col_map.items():
        if src in raw.columns:
            out[dst] = raw[src]

    df = pd.DataFrame(out)
    df["scan"] = range(1, len(df) + 1)

    return df
