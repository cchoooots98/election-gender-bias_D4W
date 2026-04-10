"""Tests for the municipal-results Bronze ingest module."""

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.ingest.results import _load_results_to_bronze, _read_results_csv


def _write_text(path: Path, content: str, encoding: str) -> None:
    """Write a text fixture file with the requested encoding."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding=encoding)


def test_read_results_csv_preserves_string_codes(tmp_path):
    """Happy path: commune and list codes must stay as strings."""
    raw_csv_path = tmp_path / "results.csv"
    _write_text(
        raw_csv_path,
        ('"Code commune";"Numéro de panneau 1";"Voix 1"\n' '"01001";"1";"345"\n'),
        encoding="utf-8",
    )

    result_df = _read_results_csv(raw_csv_path)

    assert result_df.loc[0, "Code commune"] == "01001"
    assert result_df.loc[0, "Numéro de panneau 1"] == "1"


def test_read_results_csv_falls_back_to_latin1(tmp_path):
    """Boundary: official CSVs with legacy accents should still load."""
    raw_csv_path = tmp_path / "results_latin1.csv"
    _write_text(
        raw_csv_path,
        (
            '"Code commune";"Libellé commune";"Numéro de panneau 1";"Voix 1"\n'
            '"01004";"Ambérieu-en-Bugey";"1";"1661"\n'
        ),
        encoding="latin-1",
    )

    result_df = _read_results_csv(raw_csv_path)

    assert result_df.loc[0, "Libellé commune"] == "Ambérieu-en-Bugey"


def test_read_results_csv_raises_on_empty_file(tmp_path):
    """Error: an empty CSV must fail fast rather than writing empty Bronze."""
    raw_csv_path = tmp_path / "empty_results.csv"
    _write_text(raw_csv_path, "", encoding="utf-8")

    with pytest.raises(ValueError, match="Municipal results CSV is empty"):
        _read_results_csv(raw_csv_path)


def test_load_results_to_bronze_writes_provenance_columns(tmp_path):
    """Happy path: Bronze output must carry the required provenance fields."""
    raw_csv_path = tmp_path / "results.csv"
    _write_text(
        raw_csv_path,
        ('"Code commune";"Numéro de panneau 1";"Voix 1"\n' '"01001";"1";"345"\n'),
        encoding="utf-8",
    )

    bronze_path, row_count = _load_results_to_bronze(
        raw_csv_path=raw_csv_path,
        bronze_dir=tmp_path / "bronze",
        source_cfg={"url": "https://example.com/results.csv"},
        bronze_filename="results_tour1.parquet",
    )

    bronze_df = pq.read_table(bronze_path).to_pandas()

    assert row_count == 1
    assert bronze_df["_source_url"].iloc[0] == "https://example.com/results.csv"
    assert bronze_df["_source_hash"].iloc[0]
    assert bronze_df["_ingested_at"].iloc[0]
