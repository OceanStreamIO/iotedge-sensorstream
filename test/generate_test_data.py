#!/usr/bin/env python3
"""Generate synthetic sensor test data for the sensorstream module.

Creates:
- A multi-day EMSO-style CTD CSV with realistic variations
- A synthetic GNSS NMEA file simulating a ship track
- A small mixed CTD .cnv file
"""

from __future__ import annotations

import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pynmea2


def generate_emso_ctd_csv(output_path: Path, days: int = 7, interval_minutes: int = 30) -> int:
    """Generate a multi-day CTD CSV in EMSO format.

    Simulates a moored CTD at OBSEA (41.18°N, 1.75°E) with realistic
    diurnal temperature cycles and tidal salinity variations.
    """
    base_time = datetime(2023, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_lat, base_lon = 41.18212, 1.75257
    base_depth = 20.0
    base_temp = 19.5  # September Mediterranean
    base_psal = 37.74
    base_cndc = 4.85

    rows = []
    t = base_time
    end = base_time + timedelta(days=days)

    while t < end:
        hours = (t - base_time).total_seconds() / 3600.0
        # Diurnal temperature cycle (~2°C amplitude) + noise
        temp = base_temp + 1.0 * math.sin(2 * math.pi * hours / 24.0) + random.gauss(0, 0.05)
        # Tidal salinity (~0.02 PSU amplitude, 12.42h period)
        psal = base_psal + 0.01 * math.sin(2 * math.pi * hours / 12.42) + random.gauss(0, 0.005)
        cndc = base_cndc + 0.005 * (temp - base_temp) + random.gauss(0, 0.002)

        rows.append({
            "time": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latitude": base_lat,
            "longitude": base_lon,
            "depth": base_depth,
            "TEMP": round(temp, 4),
            "PSAL": round(psal, 4),
            "CNDC": round(cndc, 4),
        })
        t += timedelta(minutes=interval_minutes)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["time", "latitude", "longitude", "depth", "TEMP", "PSAL", "CNDC"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} CTD records → {output_path}")
    return len(rows)


def generate_gnss_nmea(output_path: Path, duration_hours: float = 4, rate_hz: float = 1.0) -> int:
    """Generate a GNSS NMEA file simulating a ship track.

    Ship starts near San Diego (32.7°N, 117.2°W) and follows a
    gentle curved path northwestward at ~5 knots.
    """
    base_time = datetime(2024, 3, 15, 8, 0, 0, tzinfo=timezone.utc)
    lat = 32.706527  # Decimal degrees
    lon = -117.236067
    speed_knots = 5.0
    heading = 315.0  # NW

    lines = []
    t = base_time
    end = base_time + timedelta(hours=duration_hours)
    dt = 1.0 / rate_hz

    num_sats = 10
    hdop = 0.8

    while t < end:
        # Move ship
        speed_ms = speed_knots * 0.514444
        dlat = speed_ms * math.cos(math.radians(heading)) * dt / 111320.0
        dlon = speed_ms * math.sin(math.radians(heading)) * dt / (111320.0 * math.cos(math.radians(lat)))
        lat += dlat
        lon += dlon
        # Gentle heading drift
        heading += random.gauss(0, 0.1)
        heading = heading % 360

        # Convert to NMEA format (DDMM.MMMM)
        lat_deg = int(abs(lat))
        lat_min = (abs(lat) - lat_deg) * 60
        lat_dir = "N" if lat >= 0 else "S"
        lon_deg = int(abs(lon))
        lon_min = (abs(lon) - lon_deg) * 60
        lon_dir = "E" if lon >= 0 else "W"

        lat_nmea = f"{lat_deg:02d}{lat_min:08.5f}"
        lon_nmea = f"{lon_deg:03d}{lon_min:08.5f}"

        time_str = t.strftime("%H%M%S.%f")[:12]
        altitude = 10.0 + random.gauss(0, 0.2)
        geoidal_sep = -34.3

        # GGA sentence
        gga = pynmea2.GGA("GP", "GGA", (
            time_str, lat_nmea, lat_dir, lon_nmea, lon_dir,
            "1", str(num_sats), f"{hdop:.1f}",
            f"{altitude:.1f}", "M", f"{geoidal_sep:.1f}", "M", "", "",
        ))
        ts_iso = t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        lines.append(f"{ts_iso} {gga}\n")

        # RMC sentence every other second
        if int(t.timestamp()) % 2 == 0:
            date_str = t.strftime("%d%m%y")
            mag_var = 11.0
            rmc = pynmea2.RMC("GP", "RMC", (
                time_str, "A", lat_nmea, lat_dir, lon_nmea, lon_dir,
                f"{speed_knots:.1f}", f"{heading:.1f}", date_str,
                f"{mag_var:.1f}", "E",
            ))
            lines.append(f"{ts_iso} {rmc}\n")

        t += timedelta(seconds=dt)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.writelines(lines)

    print(f"Generated {len(lines)} NMEA lines ({duration_hours}h track) → {output_path}")
    return len(lines)


def generate_ctd_cnv(output_path: Path, n_depths: int = 50) -> int:
    """Generate a synthetic Sea-Bird CTD .cnv file."""
    lat, lon = 30.0, -160.6360
    header = f"""* Sea-Bird SBE 9 Data File:
* FileName = synthetic_ctd.hex
* Software Version Seasave V 7.26.7.121
* Temperature SN = 9999
* Conductivity SN = 9999
* NMEA Latitude = {int(abs(lat)):02d} {(abs(lat) - int(abs(lat))) * 60:05.2f} {'N' if lat >= 0 else 'S'}
* NMEA Longitude = {int(abs(lon)):03d} {(abs(lon) - int(abs(lon))) * 60:05.2f} {'W' if lon < 0 else 'E'}
* NMEA UTC (Time) = Mar 15 2024  14:30:00
** Ship: R/V Synthetic
** Station: 99001
** Operator: TEST
# nquan = 6
# nvalues = {n_depths}
# units = specified
# name 0 = latitude: Latitude [deg]
# name 1 = longitude: Longitude [deg]
# name 2 = depSM: Depth [salt water, m]
# name 3 = t090C: Temperature [ITS-90, deg C]
# name 4 = sal00: Salinity, Practical [PSU]
# name 5 = c0S/m: Conductivity [S/m]
*END*
"""
    data_lines = []
    for i in range(n_depths):
        depth = i * 2.0 + 1.0
        # Typical ocean temperature profile (thermocline around 50-100m)
        temp = 25.0 - 15.0 * (1 / (1 + math.exp(-(depth - 60) / 20))) + random.gauss(0, 0.05)
        sal = 35.0 + 0.5 * (depth / 100) + random.gauss(0, 0.01)
        cond = 4.5 + 0.01 * temp + random.gauss(0, 0.005)
        data_lines.append(f" {lat:11.5f} {lon:12.5f} {depth:8.2f} {temp:8.4f} {sal:8.4f} {cond:8.4f}\n")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(header)
        f.writelines(data_lines)

    print(f"Generated {n_depths}-depth CTD profile → {output_path}")
    return n_depths


def main():
    output_dir = Path(__file__).parent / "test_data"

    generate_emso_ctd_csv(output_dir / "ctd" / "EMSO_OBSEA_CTD_7day.csv", days=7)
    generate_gnss_nmea(output_dir / "gnss" / "synthetic_ship_track_4h.txt", duration_hours=4)
    generate_ctd_cnv(output_dir / "ctd" / "synthetic_station_99001.cnv", n_depths=50)

    # Copy the original EMSO for comparison
    import shutil
    src = Path(__file__).parent.parent / "sd-data-ingest" / "raw_data" / "emso" / "EMSO_OBSEA_CTD_30min.csv"
    if src.exists():
        shutil.copy2(src, output_dir / "ctd" / "EMSO_OBSEA_CTD_30min.csv")
        print(f"Copied original EMSO CSV → {output_dir / 'ctd' / 'EMSO_OBSEA_CTD_30min.csv'}")

    # Copy processed R2R CNV for testing
    cnv_src = Path(__file__).parent.parent / "sd-data-ingest" / "raw_data" / "r2r" / "RR2205_ctd_validation" / "processed" / "11901_1db.cnv"
    if cnv_src.exists():
        shutil.copy2(cnv_src, output_dir / "ctd" / "RR2205_11901_1db.cnv")
        print(f"Copied R2R CNV → {output_dir / 'ctd' / 'RR2205_11901_1db.cnv'}")

    # Take a 1000-line slice of the GNSS file for a smaller test
    gnss_src = Path(__file__).parent.parent / "sd-data-ingest" / "raw_data" / "r2r" / "RR2401_gnss_gp170_aft-2024-02-17.txt"
    if gnss_src.exists():
        with open(gnss_src) as f:
            lines = [next(f) for _ in range(2000)]
        slice_path = output_dir / "gnss" / "RR2401_gnss_2000lines.txt"
        with open(slice_path, "w") as f:
            f.writelines(lines)
        print(f"Sliced 2000 lines of R2R GNSS → {slice_path}")

    print(f"\nAll test data in: {output_dir}")


if __name__ == "__main__":
    main()
