"""Regression tests for src/ingest/ modules.

Every test corresponds to a confirmed bug found during pipeline execution.
The test name encodes the bug prevented.

Hermetic design:
- No network calls — raw CSV files are written to pytest's tmp_path fixture.
- No writes to real data directories.
"""

import pandas as pd
import pytest

from src.ingest.geography import load_cog_to_bronze

# ── load_cog_to_bronze ─────────────────────────────────────────────────────────


def test_load_cog_to_bronze_parses_comma_separated_csv(tmp_path):
    """Regression: INSEE COG CSV is comma-separated, not semicolons.

    Bug: geography.py used sep=';' causing the entire header to be parsed
    as a single column name. This made COG_COLUMN_MAP renames a no-op and
    the subsequent COG+seats merge failed with KeyError: 'commune_insee'.

    Fix: sep=',' in pd.read_csv. This test ensures the bug cannot regress.
    """
    # Minimal CSV that mirrors the actual INSEE COG header structure.
    # Field names COM, LIBELLE, DEP, REG, TYPECOM are the real source columns
    # confirmed from the live file downloaded on 2026-03-23.
    raw_csv = tmp_path / "insee" / "test_cog.csv"
    raw_csv.parent.mkdir(parents=True, exist_ok=True)
    raw_csv.write_text(
        "TYPECOM,COM,REG,DEP,CTCD,ARR,TNCC,NCC,NCCENR,LIBELLE,CAN,COMPARENT\n"
        "COM,01001,84,01,,1,5,ABERGEMENT CLEMENCIAT,Abergement-Clémenciat,Abergement-Clémenciat,,\n"
        "COM,75056,11,75,,1,1,PARIS,Paris,Paris,,\n",
        encoding="utf-8",
    )

    bronze_dir = tmp_path / "bronze"
    bronze_path, row_count = load_cog_to_bronze(
        raw_csv_path=raw_csv, bronze_dir=bronze_dir
    )

    result_df = pd.read_parquet(bronze_path)

    # Pre-fix: sep=';' produced exactly 1 column (the entire header as a string).
    # Post-fix: sep=',' produces 12 source columns + 3 provenance columns = 15.
    assert (
        len(result_df.columns) > 1
    ), "COG CSV was parsed as a single column — sep=',' not applied correctly"

    # The canonical source columns must all be present so COG_COLUMN_MAP renames work.
    for expected_col in ("TYPECOM", "COM", "LIBELLE", "DEP", "REG"):
        assert expected_col in result_df.columns, (
            f"Expected column '{expected_col}' missing — "
            "CSV separator may be wrong (semi-colon vs comma)"
        )

    assert row_count == 2
    assert len(result_df) == 2


def test_load_cog_to_bronze_preserves_leading_zeros(tmp_path):
    """COG codes with leading zeros (e.g. DEP '01') must remain strings, not integers.

    Without dtype=str, pandas infers DEP='01' as integer 1. A JOIN on
    commune_insee or dep_code would then silently fail to match.
    """
    raw_csv = tmp_path / "insee" / "test_cog_zeros.csv"
    raw_csv.parent.mkdir(parents=True, exist_ok=True)
    raw_csv.write_text(
        "TYPECOM,COM,REG,DEP,CTCD,ARR,TNCC,NCC,NCCENR,LIBELLE,CAN,COMPARENT\n"
        "COM,01001,84,01,,1,5,ABERGEMENT CLEMENCIAT,Abergement-Clémenciat,Abergement-Clémenciat,,\n",
        encoding="utf-8",
    )

    bronze_dir = tmp_path / "bronze"
    bronze_path, _ = load_cog_to_bronze(raw_csv_path=raw_csv, bronze_dir=bronze_dir)

    result_df = pd.read_parquet(bronze_path)
    assert (
        result_df["DEP"].iloc[0] == "01"
    ), "DEP '01' was coerced to integer — dtype=str not applied"
    assert (
        result_df["COM"].iloc[0] == "01001"
    ), "COM '01001' was coerced to integer — dtype=str not applied"


def test_load_cog_to_bronze_raises_on_missing_file(tmp_path):
    """FileNotFoundError is raised immediately when the raw CSV does not exist.

    Fail-fast contract (§6 pipeline contract #1): missing inputs must raise,
    not silently produce an empty output.
    """
    missing_path = tmp_path / "nonexistent.csv"
    with pytest.raises(FileNotFoundError, match="Raw COG CSV not found"):
        load_cog_to_bronze(raw_csv_path=missing_path, bronze_dir=tmp_path / "bronze")
