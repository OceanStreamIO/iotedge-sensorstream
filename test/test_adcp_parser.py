"""Tests for ingest/adcp_parser.py — RDI ADCP binary file parsing."""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

# Ensure the project root is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingest.adcp_parser import HAS_DOLFYN

# Note: adcp_raw_file and adcp_reference_file fixtures come from conftest.py
# (downloaded from Azure blob on first run, cached locally)


# -------------------------------------------------------------------
# Tests for read_rdi()
# -------------------------------------------------------------------


class TestReadRdi:
    @pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
    def test_returns_dataset(self, adcp_raw_file):
        import xarray as xr

        from ingest.adcp_parser import read_rdi

        ds = read_rdi(adcp_raw_file)
        assert isinstance(ds, xr.Dataset)

    @pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
    def test_has_required_variables(self, adcp_raw_file):
        from ingest.adcp_parser import read_rdi

        ds = read_rdi(adcp_raw_file)
        for var in ("vel", "amp", "corr", "heading", "temp", "beam2inst_orientmat", "orientmat"):
            assert var in ds.data_vars, f"Missing variable: {var}"

    @pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
    def test_correct_instrument_attrs(self, adcp_raw_file):
        from ingest.adcp_parser import read_rdi

        ds = read_rdi(adcp_raw_file)
        assert ds.attrs["inst_make"] == "TRDI"
        assert ds.attrs["inst_model"] == "Workhorse"
        assert ds.attrs["freq"] == 300
        assert ds.attrs["coord_sys"] == "beam"
        assert ds.attrs["n_beams"] == 4

    @pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
    def test_dimensions(self, adcp_raw_file):
        from ingest.adcp_parser import read_rdi

        ds = read_rdi(adcp_raw_file)
        assert ds.sizes["time"] > 1000  # ~7342 pings
        assert ds.sizes["range"] == 70
        assert ds.sizes["beam"] == 4

    def test_file_not_found(self):
        from ingest.adcp_parser import read_rdi

        with pytest.raises((FileNotFoundError, RuntimeError)):
            read_rdi(Path("/nonexistent/file.raw"))

    def test_wrong_extension(self, tmp_path):
        from ingest.adcp_parser import read_rdi

        bad = tmp_path / "test.csv"
        bad.touch()
        with pytest.raises((ValueError, RuntimeError)):
            read_rdi(bad)


# -------------------------------------------------------------------
# Tests for beam_to_earth()
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
class TestBeamToEarth:
    @pytest.fixture(scope="class")
    def raw_ds(self, adcp_raw_file):
        from ingest.adcp_parser import read_rdi

        return read_rdi(adcp_raw_file)

    def test_produces_earth_coords(self, raw_ds):
        from ingest.adcp_parser import beam_to_earth

        earth = beam_to_earth(raw_ds)
        assert earth.attrs["coord_sys"] == "earth"
        assert "u" in earth.data_vars
        assert "v" in earth.data_vars
        assert "w" in earth.data_vars

    def test_depth_offset(self, raw_ds):
        from ingest.adcp_parser import beam_to_earth

        earth = beam_to_earth(raw_ds, transducer_depth=7.0)
        expected = raw_ds["range"].values + 7.0
        np.testing.assert_allclose(earth["depth"].values, expected, atol=0.01)

    def test_qc_masks_bad_data(self, raw_ds):
        from ingest.adcp_parser import beam_to_earth

        earth = beam_to_earth(raw_ds, corr_threshold=64)
        # Some data should be masked (bad pings, deep bins)
        assert np.isnan(earth["u"].values).any()
        # Most shallow bins should have valid data
        shallow = earth["u"].values[:10, :]
        valid_frac = (~np.isnan(shallow)).sum() / shallow.size
        assert valid_frac > 0.5, f"Only {valid_frac:.1%} valid in shallow bins"

    def test_rejects_already_transformed(self, raw_ds):
        from ingest.adcp_parser import beam_to_earth

        earth = beam_to_earth(raw_ds)
        with pytest.raises(ValueError, match="coord_sys"):
            beam_to_earth(earth)

    def test_preserves_ancillary(self, raw_ds):
        from ingest.adcp_parser import beam_to_earth

        earth = beam_to_earth(raw_ds)
        assert "heading" in earth.data_vars
        assert "temp" in earth.data_vars
        assert "pressure" in earth.data_vars


# -------------------------------------------------------------------
# Tests for ensemble_average()
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
class TestEnsembleAverage:
    @pytest.fixture(scope="class")
    def earth_ds(self, adcp_raw_file):
        from ingest.adcp_parser import beam_to_earth, read_rdi

        raw = read_rdi(adcp_raw_file)
        return beam_to_earth(raw, transducer_depth=7.0)

    def test_reduces_time(self, earth_ds):
        from ingest.adcp_parser import ensemble_average

        avg = ensemble_average(earth_ds, interval_seconds=120.0)
        # ~1.6 hours at 120s → ~48 ensembles
        assert 30 < avg.sizes["time"] < 60
        assert "num_pings" in avg.data_vars
        assert avg["num_pings"].values.min() > 0

    def test_has_velocity_components(self, earth_ds):
        from ingest.adcp_parser import ensemble_average

        avg = ensemble_average(earth_ds, interval_seconds=120.0)
        assert "u" in avg.data_vars
        assert "v" in avg.data_vars
        assert "w" in avg.data_vars
        assert avg["u"].dims == ("time", "depth_cell")

    def test_has_depth_coordinate(self, earth_ds):
        from ingest.adcp_parser import ensemble_average

        avg = ensemble_average(earth_ds, interval_seconds=120.0)
        assert "depth" in avg  # depth is a data variable (2D) in oceanstream
        # Depth should start at transducer_depth + first range bin
        assert avg["depth"].values[0, 0] > 7.0

    def test_ping_count_reasonable(self, earth_ds):
        from ingest.adcp_parser import ensemble_average

        avg = ensemble_average(earth_ds, interval_seconds=120.0)
        total_pings = int(avg["num_pings"].values.sum())
        # Should account for most of the original pings
        original_pings = earth_ds.sizes["time"]
        assert total_pings > original_pings * 0.9


# -------------------------------------------------------------------
# Tests for adcp_to_dataframe()
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
class TestAdcpToDataframe:
    @pytest.fixture(scope="class")
    def avg_ds(self, adcp_raw_file):
        from ingest.adcp_parser import beam_to_earth, ensemble_average, read_rdi

        raw = read_rdi(adcp_raw_file)
        earth = beam_to_earth(raw, transducer_depth=7.0)
        return ensemble_average(earth, interval_seconds=120.0)

    def test_returns_dataframe(self, avg_ds):
        import pandas as pd

        from ingest.adcp_parser import adcp_to_dataframe

        df = adcp_to_dataframe(avg_ds)
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_row_count(self, avg_ds):
        from ingest.adcp_parser import adcp_to_dataframe

        df = adcp_to_dataframe(avg_ds)
        expected = avg_ds.sizes["time"] * avg_ds.sizes["depth_cell"]
        assert len(df) == expected

    def test_has_required_columns(self, avg_ds):
        from ingest.adcp_parser import adcp_to_dataframe

        df = adcp_to_dataframe(avg_ds)
        for col in ("time", "depth", "u", "v", "w", "amp", "heading", "temperature", "num_pings"):
            assert col in df.columns, f"Missing column: {col}"

    def test_depth_values_positive(self, avg_ds):
        from ingest.adcp_parser import adcp_to_dataframe

        df = adcp_to_dataframe(avg_ds)
        assert (df["depth"] > 0).all()

    def test_time_is_utc(self, avg_ds):
        from ingest.adcp_parser import adcp_to_dataframe

        df = adcp_to_dataframe(avg_ds)
        assert df["time"].dt.tz is not None  # Should be timezone-aware


# -------------------------------------------------------------------
# Tests for parse_adcp_file() — full pipeline
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
class TestParseAdcpFile:
    @pytest.fixture(scope="class")
    def adcp_df(self, adcp_raw_file):
        from ingest.adcp_parser import parse_adcp_file

        return parse_adcp_file(adcp_raw_file)

    def test_returns_nonempty_dataframe(self, adcp_df):
        import pandas as pd

        assert isinstance(adcp_df, pd.DataFrame)
        assert len(adcp_df) > 0

    def test_velocity_range_sane(self, adcp_df):
        # Velocities include ship motion (not navigation-corrected),
        # so they can be several m/s. Just verify they're finite
        # and not completely unreasonable (< 30 m/s).
        for col in ("u", "v"):
            valid = adcp_df[col].dropna()
            assert len(valid) > 0, f"No valid {col} velocities"
            assert valid.abs().max() < 30.0, f"{col} max velocity unreasonably high"

    def test_temperature_sane(self, adcp_df):
        temp = adcp_df["temperature"].dropna()
        assert 0 < temp.mean() < 35, f"Mean temp {temp.mean()} outside expected range"

    def test_depth_range(self, adcp_df):
        # 70 bins × ~8m cell size → up to ~600m
        assert adcp_df["depth"].max() > 100
        assert adcp_df["depth"].min() > 0


# -------------------------------------------------------------------
# Tests for reference comparison
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
class TestReferenceComparison:
    """Compare our processing against the UHDAS reference NetCDF."""

    def test_temperature_matches_reference(self, adcp_raw_file, adcp_reference_file):
        import xarray as xr

        from ingest.adcp_parser import beam_to_earth, ensemble_average, read_rdi

        raw = read_rdi(adcp_raw_file)
        earth = beam_to_earth(raw, transducer_depth=7.0)
        avg = ensemble_average(earth, interval_seconds=120.0)

        ref = xr.open_dataset(adcp_reference_file)

        # Compare transducer temperature
        our_temp = avg["tr_temp"].values.mean()
        if "tr_temp" in ref:
            ref_temp = float(ref["tr_temp"].values.mean())
        elif "temperature" in ref:
            ref_temp = float(ref["temperature"].values.mean())
        else:
            pytest.skip("No temperature variable in reference file")

        assert abs(our_temp - ref_temp) < 0.5, (
            f"Temperature mismatch: ours={our_temp:.2f}, ref={ref_temp:.2f}"
        )

    def test_ensemble_count_close(self, adcp_raw_file, adcp_reference_file):
        import xarray as xr

        from ingest.adcp_parser import beam_to_earth, ensemble_average, read_rdi

        raw = read_rdi(adcp_raw_file)
        earth = beam_to_earth(raw, transducer_depth=7.0)
        avg = ensemble_average(earth, interval_seconds=120.0)

        ref = xr.open_dataset(adcp_reference_file)

        our_count = avg.sizes["time"]
        ref_n_bins = ref.sizes.get("depth_cell", ref.sizes.get("depth", 0))

        # Reference file may span multiple raw files, so just verify
        # our depth bin count matches
        our_n_bins = avg.sizes["depth_cell"]
        if ref_n_bins > 0:
            assert our_n_bins == ref_n_bins, (
                f"Depth bin mismatch: ours={our_n_bins}, ref={ref_n_bins}"
            )

        # Verify we got a reasonable ensemble count for ~1.6h of data at 120s
        assert 30 < our_count < 60, f"Unexpected ensemble count: {our_count}"


# -------------------------------------------------------------------
# Tests for pipeline integration
# -------------------------------------------------------------------


@pytest.mark.skipif(not HAS_DOLFYN, reason="dolfyn not installed")
class TestPipelineAdcpIntegration:
    def test_detect_file_type_adcp(self):
        from process.pipeline import _detect_file_type

        assert _detect_file_type(Path("cast.raw")) == "adcp"

    @pytest.mark.asyncio
    async def test_process_adcp_file(self, tmp_path, adcp_raw_file):
        from azure_handler.storage import LocalStorage
        from config import EdgeConfig

        config = EdgeConfig.from_standalone(
            output_base_path=str(tmp_path),
        )
        storage = LocalStorage(str(tmp_path))

        from process.pipeline import process_file

        result = await process_file(str(adcp_raw_file), config, storage)

        assert result["status"] == "ok"
        assert result["record_count"] > 1000  # 49 ensembles × 70 bins = 3430
        assert result["file_type"] == "adcp"

        # Verify parquet was written
        parquet_files = list(tmp_path.rglob("*.parquet"))
        assert len(parquet_files) == 1

        # Verify metadata was written
        meta_files = list(tmp_path.rglob("*.metadata.json"))
        assert len(meta_files) == 1
