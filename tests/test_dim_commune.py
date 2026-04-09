"""Tests for src/transform/dim_commune.py."""

import pandas as pd
import pytest

from src.transform._exceptions import DataQualityError
from src.transform.dim_commune import build_dim_commune
from tests.sampling_builders import write_parquet_frame

_COG_MAP = {
    "COM": "commune_insee",
    "LIBELLE": "commune_name",
    "DEP": "dep_code",
    "REG": "reg_code",
    "TYPECOM": "typecom",
}
_SEATS_MAP = {
    "CODE_COMMUNE": "commune_insee",
    "LIB_COMMUNE": "commune_name",
    "CODE_DPT": "dep_code",
    "LIB_DPT": "dep_name",
    "POPULATION": "population",
    "NBRE_SAP_COM": "seats_municipal",
    "NBRE_SAP_EPCI": "seats_epci",
}


@pytest.fixture
def bronze_commune_parquets(tmp_path):
    """Write minimal COG + seats bronze Parquets to a temp bronze directory.

    The seats fixture deliberately includes LIB_COMMUNE and CODE_DPT. Those
    columns also exist in COG after rename, so this fixture protects the
    suffix-collision regressions that previously nulled commune attributes.
    """
    cog_df = pd.DataFrame(
        {
            "COM": ["75056", "69123"],
            "LIBELLE": ["Paris", "Lyon"],
            "DEP": ["75", "69"],
            "REG": ["11", "84"],
            "TYPECOM": ["COM", "COM"],
            "_source_url": ["http://example.com", "http://example.com"],
            "_ingested_at": ["2026-01-01", "2026-01-01"],
            "_source_hash": ["aaa", "aaa"],
        }
    )
    seats_df = pd.DataFrame(
        {
            "CODE_COMMUNE": ["75056", "69123"],
            "LIB_COMMUNE": ["Paris", "Lyon"],
            "CODE_DPT": ["75", "69"],
            "LIB_DPT": ["Paris dept", "Rhône"],
            "POPULATION": ["2161000", "522000"],
            "NBRE_SAP_COM": ["163", "73"],
            "NBRE_SAP_EPCI": ["0", "44"],
            "_source_url": ["http://example.com", "http://example.com"],
            "_ingested_at": ["2026-01-01", "2026-01-01"],
            "_source_hash": ["bbb", "bbb"],
        }
    )

    bronze_dir = tmp_path / "bronze"
    write_parquet_frame(cog_df, bronze_dir / "geography" / "cog_communes.parquet")
    write_parquet_frame(seats_df, bronze_dir / "seats" / "seats_population.parquet")
    return bronze_dir


def test_build_dim_commune_commune_name_not_none_after_merge(
    bronze_commune_parquets, tmp_path
):
    """Regression: commune_name must survive overlapping seats columns."""
    result_df = build_dim_commune(
        bronze_dir=bronze_commune_parquets,
        silver_dir=tmp_path / "silver",
        duckdb_path=tmp_path / "warehouse.duckdb",
        cog_column_map=_COG_MAP,
        seats_column_map=_SEATS_MAP,
    )

    assert result_df["commune_name"].notna().all(), (
        "commune_name must not be None after merge; COG is authoritative and "
        "must not be shadowed by seats suffix columns."
    )


def test_build_dim_commune_dep_code_not_none_after_merge(
    bronze_commune_parquets, tmp_path
):
    """Regression: dep_code must survive the same suffix-collision path."""
    result_df = build_dim_commune(
        bronze_dir=bronze_commune_parquets,
        silver_dir=tmp_path / "silver",
        duckdb_path=tmp_path / "warehouse.duckdb",
        cog_column_map=_COG_MAP,
        seats_column_map=_SEATS_MAP,
    )

    assert result_df["dep_code"].notna().all(), (
        "dep_code must not be None after merge; overlapping seats columns "
        "must not erase the COG join key."
    )


def test_build_dim_commune_no_suffix_columns_in_output(
    bronze_commune_parquets, tmp_path
):
    """Regression: merge helpers must not leak _x/_y columns downstream."""
    result_df = build_dim_commune(
        bronze_dir=bronze_commune_parquets,
        silver_dir=tmp_path / "silver",
        duckdb_path=tmp_path / "warehouse.duckdb",
        cog_column_map=_COG_MAP,
        seats_column_map=_SEATS_MAP,
    )

    suffix_columns = [
        column_name
        for column_name in result_df.columns
        if column_name.endswith("_x") or column_name.endswith("_y")
    ]
    assert not suffix_columns, f"Unexpected suffix columns leaked: {suffix_columns}"


def test_build_dim_commune_raises_on_missing_seats_join_key(
    bronze_commune_parquets, tmp_path
):
    """Regression: empty seats mapping must fail instead of degrading silently."""
    with pytest.raises(DataQualityError, match="seats join failed"):
        build_dim_commune(
            bronze_dir=bronze_commune_parquets,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
            cog_column_map=_COG_MAP,
            seats_column_map={},
        )
