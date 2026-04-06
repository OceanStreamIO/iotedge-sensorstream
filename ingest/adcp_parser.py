"""Parse RDI (Teledyne) ADCP binary files and produce earth-coordinate velocities.

Uses ``dolfyn`` to read ``.raw`` binary files, transforms from beam to
earth coordinates, applies QC masks, and optionally time-averages into
ensembles.  Returns a flat ``pandas.DataFrame`` suitable for GeoParquet
output (one row per ensemble × depth cell).

Companion files
----------------
Only the ``.raw`` file is required.  Optional UHDAS metadata files
(``*_sensor.toml``, ``cruise_info.txt``) are not used at parse time.

Dependencies
------------
- ``dolfyn`` — core RDI binary reader
- ``numpy``, ``scipy`` — coordinate transforms
- ``xarray``, ``pandas`` — data structures

A compatibility shim patches numpy 2.x / scipy 1.14+ issues in dolfyn.
"""
from __future__ import annotations

import logging
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional

import numpy as np

if TYPE_CHECKING:
    import pandas as pd
    import xarray as xr

logger = logging.getLogger("sensorstream")

# ---------------------------------------------------------------------------
# Optional dependency guard
# ---------------------------------------------------------------------------

HAS_DOLFYN = False
try:
    # Apply compat patches before dolfyn touches numpy/scipy
    if not hasattr(np, "NaN"):
        np.NaN = np.nan  # type: ignore[attr-defined]
    if not hasattr(np, "RankWarning"):
        np.RankWarning = np.exceptions.RankWarning  # type: ignore[attr-defined]

    import scipy.integrate
    if not hasattr(scipy.integrate, "cumtrapz"):
        scipy.integrate.cumtrapz = scipy.integrate.cumulative_trapezoid  # type: ignore[attr-defined]

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, message="pkg_resources")
        warnings.filterwarnings("ignore", category=FutureWarning)
        from dolfyn.io.rdi import read_rdi as _dolfyn_read_rdi

    HAS_DOLFYN = True
except ImportError:
    _dolfyn_read_rdi = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Reading
# ---------------------------------------------------------------------------


def read_rdi(path: Path) -> "xr.Dataset":
    """Read an RDI ADCP binary ``.raw`` file.

    Parameters
    ----------
    path : Path
        Path to a ``.raw`` binary file.

    Returns
    -------
    xr.Dataset
        Raw dataset with velocity in beam coordinates.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    ValueError
        If the file extension is not ``.raw``.
    RuntimeError
        If dolfyn is not installed.
    """
    if not HAS_DOLFYN:
        raise RuntimeError(
            "dolfyn is not installed — cannot parse ADCP .raw files. "
            "Install with: pip install dolfyn"
        )

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"RDI file not found: {path}")
    if path.suffix.lower() != ".raw":
        raise ValueError(f"Expected .raw file, got: {path.suffix}")

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        ds = _dolfyn_read_rdi(str(path))  # type: ignore[misc]

    return ds


# ---------------------------------------------------------------------------
# Beam → Earth transform
# ---------------------------------------------------------------------------


def beam_to_earth(
    ds: "xr.Dataset",
    transducer_depth: float = 7.0,
    corr_threshold: int = 64,
    pg_threshold: float = 0.5,
) -> "xr.Dataset":
    """Convert beam-coordinate ADCP data to earth-coordinate velocities.

    Parameters
    ----------
    ds : xr.Dataset
        Raw dataset from :func:`read_rdi` with ``coord_sys='beam'``.
    transducer_depth : float
        Transducer depth below surface in metres.
    corr_threshold : int
        Minimum beam-averaged correlation (0–255).
    pg_threshold : float
        Fraction of beams that must pass the correlation check (0–1).

    Returns
    -------
    xr.Dataset
        Dataset with ``u``, ``v``, ``w`` earth-coordinate velocities.
    """
    import xarray as xr

    coord_sys = ds.attrs.get("coord_sys", "")
    if coord_sys != "beam":
        raise ValueError(
            f"Expected coord_sys='beam', got '{coord_sys}'. "
            "Data may already be in earth coordinates."
        )

    vel = ds["vel"].values  # (4, range, time)
    b2i = ds["beam2inst_orientmat"].values  # (4, 4)
    orientmat = ds["orientmat"].values  # (3, 3, time)

    # beam → instrument
    vel_inst = np.einsum("ij,jrt->irt", b2i, vel)
    # instrument → earth
    vel_earth = np.einsum("eit,irt->ert", orientmat, vel_inst[:3, :, :])

    u = vel_earth[0].astype(np.float32)
    v = vel_earth[1].astype(np.float32)
    w = vel_earth[2].astype(np.float32)

    # QC — mask low-correlation bins
    corr = ds["corr"].values  # (beam, range, time)
    good_beams = (corr >= corr_threshold).sum(axis=0)
    n_beams = corr.shape[0]
    bad_mask = (good_beams / n_beams) < pg_threshold
    u[bad_mask] = np.nan
    v[bad_mask] = np.nan
    w[bad_mask] = np.nan

    # Mask extreme error velocities
    err_vel = np.abs(vel_inst[3])
    err_mask = err_vel > 0.3
    u[err_mask] = np.nan
    v[err_mask] = np.nan
    w[err_mask] = np.nan

    depth = ds["range"].values + transducer_depth

    out = xr.Dataset(
        {
            "u": (["range", "time"], u, {"units": "m s-1", "long_name": "Eastward velocity"}),
            "v": (["range", "time"], v, {"units": "m s-1", "long_name": "Northward velocity"}),
            "w": (["range", "time"], w, {"units": "m s-1", "long_name": "Upward velocity"}),
            "amp": (["beam", "range", "time"], ds["amp"].values),
            "corr": (["beam", "range", "time"], ds["corr"].values),
            "heading": (["time"], ds["heading"].values, {"units": "degrees"}),
            "pitch": (["time"], ds["pitch"].values, {"units": "degrees"}),
            "roll": (["time"], ds["roll"].values, {"units": "degrees"}),
            "temp": (["time"], ds["temp"].values, {"units": "degree_C"}),
            "pressure": (["time"], ds["pressure"].values, {"units": "dbar"}),
        },
        coords={
            "time": ds["time"].values,
            "range": ds["range"].values,
            "depth": (["range"], depth, {"units": "meter"}),
            "beam": ds["beam"].values,
        },
        attrs={
            "inst_make": ds.attrs.get("inst_make", ""),
            "inst_model": ds.attrs.get("inst_model", ""),
            "freq": ds.attrs.get("freq", 0),
            "n_beams": ds.attrs.get("n_beams", 0),
            "coord_sys": "earth",
            "transducer_depth": transducer_depth,
            "corr_threshold": corr_threshold,
        },
    )
    return out


# ---------------------------------------------------------------------------
# Ensemble averaging
# ---------------------------------------------------------------------------


def ensemble_average(
    ds: "xr.Dataset",
    interval_seconds: float = 120.0,
) -> "xr.Dataset":
    """Time-average earth-coordinate velocities into ensembles.

    Parameters
    ----------
    ds : xr.Dataset
        Earth-coordinate dataset from :func:`beam_to_earth`.
    interval_seconds : float
        Averaging interval in seconds (default 120).

    Returns
    -------
    xr.Dataset
        Averaged dataset with reduced time dimension.
    """
    import pandas as pd
    import xarray as xr

    interval = pd.Timedelta(seconds=interval_seconds)
    bin_edges = pd.date_range(
        start=ds["time"].values[0],
        end=ds["time"].values[-1] + interval,
        freq=interval,
    )
    time_bins = pd.cut(pd.DatetimeIndex(ds["time"].values), bins=bin_edges)

    results: Dict[str, list] = {
        "u": [], "v": [], "w": [],
        "amp_mean": [], "heading": [], "temp": [],
        "num_pings": [],
    }
    time_centers = []

    for bin_interval in time_bins.categories:
        mask = (
            (ds["time"].values >= bin_interval.left.to_datetime64())
            & (ds["time"].values < bin_interval.right.to_datetime64())
        )
        n_pings = int(mask.sum())
        if n_pings == 0:
            continue

        time_centers.append(bin_interval.mid.to_datetime64())
        results["num_pings"].append(n_pings)

        sub = ds.isel(time=mask)
        results["u"].append(sub["u"].mean(dim="time").values)
        results["v"].append(sub["v"].mean(dim="time").values)
        results["w"].append(sub["w"].mean(dim="time").values)
        results["amp_mean"].append(
            sub["amp"].mean(dim=["beam", "time"]).values.astype(np.float32)
        )
        results["heading"].append(float(sub["heading"].mean().values))
        results["temp"].append(float(sub["temp"].mean().values))

    n_ens = len(time_centers)
    n_range = ds.sizes["range"]
    depth = ds["depth"].values

    out = xr.Dataset(
        {
            "u": (["time", "depth_cell"], np.array(results["u"]).reshape(n_ens, n_range),
                  {"units": "m s-1", "long_name": "Zonal velocity"}),
            "v": (["time", "depth_cell"], np.array(results["v"]).reshape(n_ens, n_range),
                  {"units": "m s-1", "long_name": "Meridional velocity"}),
            "w": (["time", "depth_cell"], np.array(results["w"]).reshape(n_ens, n_range),
                  {"units": "m s-1", "long_name": "Vertical velocity"}),
            "amp": (["time", "depth_cell"], np.array(results["amp_mean"]).reshape(n_ens, n_range),
                    {"long_name": "Mean signal amplitude"}),
            "heading": (["time"], np.array(results["heading"]),
                        {"units": "degrees", "long_name": "Mean heading"}),
            "tr_temp": (["time"], np.array(results["temp"]),
                        {"units": "degree_C", "long_name": "Transducer temperature"}),
            "num_pings": (["time"], np.array(results["num_pings"]),
                          {"long_name": "Pings per ensemble"}),
        },
        coords={
            "time": np.array(time_centers),
            "depth_cell": np.arange(n_range),
            "depth": (["depth_cell"], depth, {"units": "meter"}),
        },
        attrs={
            **ds.attrs,
            "ensemble_interval_seconds": interval_seconds,
        },
    )
    return out


# ---------------------------------------------------------------------------
# Flatten to DataFrame for GeoParquet output
# ---------------------------------------------------------------------------


def adcp_to_dataframe(avg_ds: "xr.Dataset") -> "pd.DataFrame":
    """Flatten an ensemble-averaged ADCP dataset to a tabular DataFrame.

    One row per (time, depth_cell) combination.  Suitable for writing as
    GeoParquet in the sensorstream pipeline.

    Parameters
    ----------
    avg_ds : xr.Dataset
        Ensemble-averaged dataset from :func:`ensemble_average`.

    Returns
    -------
    pd.DataFrame
        Flat DataFrame with columns: ``time``, ``depth``, ``u``, ``v``,
        ``w``, ``amp``, ``heading``, ``temperature``, ``num_pings``.
    """
    import pandas as pd

    rows = []
    times = avg_ds["time"].values
    depths = avg_ds["depth"].values

    for t_idx in range(avg_ds.sizes["time"]):
        t_val = pd.Timestamp(times[t_idx], tz="UTC")
        heading = float(avg_ds["heading"].values[t_idx])
        temp = float(avg_ds["tr_temp"].values[t_idx])
        n_pings = int(avg_ds["num_pings"].values[t_idx])

        for d_idx in range(avg_ds.sizes["depth_cell"]):
            rows.append({
                "time": t_val,
                "depth": float(depths[d_idx]),
                "u": float(avg_ds["u"].values[t_idx, d_idx]),
                "v": float(avg_ds["v"].values[t_idx, d_idx]),
                "w": float(avg_ds["w"].values[t_idx, d_idx]),
                "amp": float(avg_ds["amp"].values[t_idx, d_idx]),
                "heading": heading,
                "temperature": temp,
                "num_pings": n_pings,
            })

    df = pd.DataFrame(rows)
    logger.info(
        "ADCP flattened: %d ensembles × %d depth cells = %d rows",
        avg_ds.sizes["time"], avg_ds.sizes["depth_cell"], len(df),
    )
    return df


# ---------------------------------------------------------------------------
# High-level parse function (matches hex_parser.parse_hex_file pattern)
# ---------------------------------------------------------------------------


def parse_adcp_file(
    raw_path: Path,
    transducer_depth: float = 7.0,
    ensemble_interval: float = 120.0,
    corr_threshold: int = 64,
) -> "pd.DataFrame":
    """Parse an RDI ADCP ``.raw`` file into a flat DataFrame.

    Full pipeline: read → beam→earth → ensemble average → flatten.

    Parameters
    ----------
    raw_path : Path
        Path to the ``.raw`` binary file.
    transducer_depth : float
        Transducer depth below surface in metres.
    ensemble_interval : float
        Averaging interval in seconds.
    corr_threshold : int
        Minimum beam-averaged correlation (0–255).

    Returns
    -------
    pd.DataFrame
        One row per (ensemble, depth cell).
    """
    raw_ds = read_rdi(raw_path)

    n_pings = raw_ds.sizes["time"]
    n_bins = raw_ds.sizes["range"]
    freq = raw_ds.attrs.get("freq", "?")
    logger.info(
        "ADCP: %s — %d pings, %d bins, %s kHz, coord_sys=%s",
        raw_path.name, n_pings, n_bins, freq,
        raw_ds.attrs.get("coord_sys", "?"),
    )

    earth_ds = beam_to_earth(
        raw_ds,
        transducer_depth=transducer_depth,
        corr_threshold=corr_threshold,
    )

    avg_ds = ensemble_average(earth_ds, interval_seconds=ensemble_interval)

    logger.info(
        "ADCP: %d ensembles at %ds interval",
        avg_ds.sizes["time"], int(ensemble_interval),
    )

    return adcp_to_dataframe(avg_ds)
