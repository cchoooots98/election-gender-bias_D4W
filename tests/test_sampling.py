"""Tests for the Gold sampling slice and its audit artifacts."""

import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transform._exceptions import SamplingError
from src.transform.sampling import build_sample


def _write_parquet(dataframe: pd.DataFrame, path) -> None:
    """Write a DataFrame to Parquet, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dataframe), path, compression="snappy")


def _build_candidate_and_commune_frames(
    include_extra_large_female: bool = False,
    insufficient_large_female: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create a deterministic dim_candidate_leader + dim_commune test fixture."""
    quota_by_bucket = {"large": 2, "medium": 4, "small": 6}
    population_by_bucket = {"large": 150_000, "medium": 50_000, "small": 10_000}

    leader_rows: list[dict[str, object]] = []
    commune_rows: list[dict[str, object]] = []
    commune_counter = 1
    leader_counter = 1

    for bucket, per_gender in quota_by_bucket.items():
        for gender in ("F", "M"):
            effective_count = per_gender
            if insufficient_large_female and bucket == "large" and gender == "F":
                effective_count = per_gender - 1

            for _candidate_index in range(effective_count):
                commune_insee = f"{commune_counter:05d}"
                commune_counter += 1
                leader_rows.append(
                    {
                        "leader_id": f"leader-{leader_counter:03d}",
                        "full_name": f"Candidate {leader_counter}",
                        "gender": gender,
                        "commune_insee": commune_insee,
                        "reg_code": f"R{(leader_counter % 5) + 1}",
                        "city_size_bucket": bucket,
                        "same_name_candidate_count": 1,
                        "list_nuance": "DVG",
                        "nuance_group": "gauche",
                        "is_incumbent": False,
                        "incumbent_match_score": None,
                        "incumbent_match_auditable": False,
                        "advanced_to_tour2": None,
                    }
                )
                commune_rows.append(
                    {
                        "commune_insee": commune_insee,
                        "commune_name": f"Commune {leader_counter}",
                        "dep_code": f"D{(leader_counter % 9) + 1}",
                        "population": population_by_bucket[bucket],
                    }
                )
                leader_counter += 1

    if include_extra_large_female:
        commune_insee = f"{commune_counter:05d}"
        leader_rows.append(
            {
                "leader_id": "leader-extra-large-f",
                "full_name": "Highly Ambiguous Name",
                "gender": "F",
                "commune_insee": commune_insee,
                "reg_code": "R9",
                "city_size_bucket": "large",
                "same_name_candidate_count": 5,
                "list_nuance": "DVG",
                "nuance_group": "gauche",
                "is_incumbent": False,
                "incumbent_match_score": None,
                "incumbent_match_auditable": False,
                "advanced_to_tour2": None,
            }
        )
        commune_rows.append(
            {
                "commune_insee": commune_insee,
                "commune_name": "Commune Extra",
                "dep_code": "D9",
                "population": population_by_bucket["large"],
            }
        )

    return pd.DataFrame(leader_rows), pd.DataFrame(commune_rows)


def _write_sampling_inputs(
    tmp_path,
    leader_df: pd.DataFrame,
    commune_df: pd.DataFrame | None,
) -> tuple:
    """Write sampling inputs to temp silver/gold/warehouse locations."""
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    duckdb_path = tmp_path / "warehouse.duckdb"

    _write_parquet(leader_df, silver_dir / "dim_candidate_leader.parquet")
    if commune_df is not None:
        _write_parquet(commune_df, silver_dir / "dim_commune.parquet")

    return silver_dir, gold_dir, duckdb_path


def test_sample_size_equals_24_and_no_duplicate_leader_id(tmp_path):
    """Happy path: the final sample must contain exactly 24 unique leaders."""
    leader_df, commune_df = _build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    sample_df = build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    assert len(sample_df) == 24
    assert sample_df["leader_id"].is_unique


def test_gender_balance_and_stratum_quotas_are_correct(tmp_path):
    """Happy path: quotas must be met within each city-size stratum."""
    leader_df, commune_df = _build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    sample_df = build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    assert (sample_df["gender"] == "F").sum() == 12
    assert (sample_df["gender"] == "M").sum() == 12

    quota_series = sample_df.groupby(["city_size_bucket", "gender"]).size().sort_index()
    assert quota_series[("large", "F")] == 2
    assert quota_series[("large", "M")] == 2
    assert quota_series[("medium", "F")] == 4
    assert quota_series[("medium", "M")] == 4
    assert quota_series[("small", "F")] == 6
    assert quota_series[("small", "M")] == 6


def test_raises_when_insufficient_female_pool(tmp_path):
    """Error path: a stratum with too few women must raise SamplingError."""
    leader_df, commune_df = _build_candidate_and_commune_frames(
        insufficient_large_female=True
    )
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    with pytest.raises(SamplingError, match="Insufficient female candidates"):
        build_sample(
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
        )


def test_manifest_json_written_and_contains_candidates(tmp_path):
    """Happy path: the sampling run must write a manifest with candidate detail."""
    leader_df, commune_df = _build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    manifest_path = gold_dir / "sample_manifest.json"
    with open(manifest_path, encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    assert manifest_path.exists()
    assert "candidates" in manifest
    assert len(manifest["candidates"]) == 24


def test_manifest_commune_audit_fields_not_empty(tmp_path):
    """Regression: commune audit fields must be populated via dim_commune join."""
    leader_df, commune_df = _build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    with open(gold_dir / "sample_manifest.json", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    for candidate in manifest["candidates"]:
        assert candidate["commune_name"]
        assert candidate["dep_code"]
        assert candidate["population"] is not None


def test_prefers_lower_same_name_candidate_count_within_pool(tmp_path):
    """Regression: ambiguous names should lose ties when the pool is larger."""
    leader_df, commune_df = _build_candidate_and_commune_frames(
        include_extra_large_female=True
    )
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    sample_df = build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    assert "leader-extra-large-f" not in sample_df["leader_id"].tolist()


def test_pipeline_run_id_is_preserved_in_manifest(tmp_path):
    """Happy path: orchestration can pass one shared run_id into the manifest."""
    leader_df, commune_df = _build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
        pipeline_run_id="run-123",
    )

    with open(gold_dir / "sample_manifest.json", encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    assert manifest["run_id"] == "run-123"


def test_build_sample_fails_fast_when_dim_commune_is_missing(tmp_path):
    """Contract: manifest audit fields make dim_commune a required input."""
    leader_df, _ = _build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = _write_sampling_inputs(
        tmp_path,
        leader_df,
        commune_df=None,
    )

    with pytest.raises(FileNotFoundError, match="dim_commune silver file not found"):
        build_sample(
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
        )
