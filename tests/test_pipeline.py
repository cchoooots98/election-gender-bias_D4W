"""Tests for the runnable sampling pipeline orchestration."""

import json

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.orchestration import sampling_pipeline


def _write_parquet(dataframe: pd.DataFrame, path) -> None:
    """Write a DataFrame to Parquet for pipeline test doubles."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dataframe), path, compression="snappy")


def _read_latest_meta_run(duckdb_path):
    """Fetch the latest meta_run row as a tuple."""
    conn = duckdb.connect(str(duckdb_path))
    try:
        return conn.execute(
            """
            SELECT run_id, status, rows_ingested, error_count, artifact_paths
            FROM meta.meta_run
            ORDER BY end_ts DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()


def _stub_ingest(relative_path: str, call_order: list[str], step_name: str):
    """Create an ingest stub that writes a 1-row bronze parquet."""

    def _runner(raw_dir, bronze_dir):
        call_order.append(step_name)
        output_path = bronze_dir / relative_path
        _write_parquet(pd.DataFrame({"value": [step_name]}), output_path)
        return output_path

    return _runner


def _stub_build_dim_commune(call_order: list[str]):
    """Create a dim_commune stub that writes one silver row."""

    def _runner(
        bronze_dir, silver_dir, duckdb_path, cog_column_map=None, seats_column_map=None
    ):
        del bronze_dir, duckdb_path, cog_column_map, seats_column_map
        call_order.append("build_dim_commune")
        dataframe = pd.DataFrame(
            {
                "commune_insee": ["00001"],
                "commune_name": ["Paris"],
                "dep_code": ["75"],
                "reg_code": ["11"],
                "population": [100_000],
                "city_size_bucket": ["large"],
            }
        )
        _write_parquet(dataframe, silver_dir / "dim_commune.parquet")
        return dataframe

    return _runner


def _stub_build_dim_candidate(call_order: list[str]):
    """Create a dim_candidate_leader stub that writes one silver row."""

    def _runner(
        bronze_dir,
        silver_dir,
        duckdb_path,
        candidates_column_map=None,
        rne_column_map=None,
        include_tour2_flag=True,
    ):
        del bronze_dir, duckdb_path, candidates_column_map, rne_column_map
        del include_tour2_flag
        call_order.append("build_dim_candidate_leader")
        dataframe = pd.DataFrame(
            {
                "leader_id": ["leader-001"],
                "full_name": ["Candidate 1"],
                "gender": ["F"],
                "commune_insee": ["00001"],
                "reg_code": ["11"],
                "city_size_bucket": ["large"],
                "same_name_candidate_count": [1],
                "list_nuance": ["DVG"],
                "nuance_group": ["gauche"],
                "is_incumbent": [False],
                "incumbent_match_score": [None],
                "incumbent_match_auditable": [False],
                "advanced_to_tour2": [None],
            }
        )
        _write_parquet(dataframe, silver_dir / "dim_candidate_leader.parquet")
        return dataframe

    return _runner


def _stub_build_sample(call_order: list[str]):
    """Create a Gold sample stub that writes parquet and manifest artifacts."""

    def _runner(
        silver_dir,
        gold_dir,
        duckdb_path,
        random_seed=42,
        pipeline_run_id=None,
    ):
        del silver_dir, duckdb_path, random_seed
        call_order.append("build_sample")
        dataframe = pd.DataFrame(
            {
                "leader_id": ["leader-001"],
                "full_name": ["Candidate 1"],
                "gender": ["F"],
                "commune_insee": ["00001"],
                "reg_code": ["11"],
                "city_size_bucket": ["large"],
                "same_name_candidate_count": [1],
                "list_nuance": ["DVG"],
                "nuance_group": ["gauche"],
                "is_incumbent": [False],
                "incumbent_match_score": [None],
                "incumbent_match_auditable": [False],
                "advanced_to_tour2": [None],
            }
        )
        _write_parquet(dataframe, gold_dir / "sample_leaders.parquet")
        gold_dir.mkdir(parents=True, exist_ok=True)
        with open(
            gold_dir / "sample_manifest.json", "w", encoding="utf-8"
        ) as file_handle:
            json.dump({"run_id": pipeline_run_id, "candidates": []}, file_handle)
        return dataframe

    return _runner


def test_sampling_pipeline_happy_path_runs_steps_in_order(tmp_path, monkeypatch):
    """Happy path: runner executes required steps in order and records success."""
    call_order: list[str] = []
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_geography",
        _stub_ingest("geography/cog_communes.parquet", call_order, "ingest_geography"),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_seats_population",
        _stub_ingest(
            "seats/seats_population.parquet", call_order, "ingest_seats_population"
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_candidates",
        _stub_ingest(
            "candidates/candidates_tour1.parquet",
            call_order,
            "ingest_candidates",
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_incumbents",
        _stub_ingest("rne/rne_incumbents.parquet", call_order, "ingest_incumbents"),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_candidates_tour2",
        _stub_ingest(
            "candidates/candidates_tour2.parquet",
            call_order,
            "ingest_candidates_tour2",
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_dim_commune",
        _stub_build_dim_commune(call_order),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_dim_candidate_leader",
        _stub_build_dim_candidate(call_order),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_sample",
        _stub_build_sample(call_order),
    )

    result = sampling_pipeline.run_sampling_pipeline(
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        duckdb_path=duckdb_path,
    )

    assert call_order == [
        "ingest_geography",
        "ingest_seats_population",
        "ingest_candidates",
        "ingest_incumbents",
        "ingest_candidates_tour2",
        "build_dim_commune",
        "build_dim_candidate_leader",
        "build_sample",
    ]
    assert result.status == "success"

    meta_run = _read_latest_meta_run(duckdb_path)
    assert meta_run is not None
    assert meta_run[1] == "success"
    artifact_paths = json.loads(meta_run[4])
    assert str(tmp_path / "gold" / "sample_leaders.parquet") in artifact_paths
    assert str(tmp_path / "gold" / "sample_manifest.json") in artifact_paths


def test_sampling_pipeline_optional_tour2_failure_is_partial(tmp_path, monkeypatch):
    """Boundary: Tour 2 failure must not stop the runnable sampling slice."""
    call_order: list[str] = []
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_geography",
        _stub_ingest("geography/cog_communes.parquet", call_order, "ingest_geography"),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_seats_population",
        _stub_ingest(
            "seats/seats_population.parquet", call_order, "ingest_seats_population"
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_candidates",
        _stub_ingest(
            "candidates/candidates_tour1.parquet",
            call_order,
            "ingest_candidates",
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_incumbents",
        _stub_ingest("rne/rne_incumbents.parquet", call_order, "ingest_incumbents"),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_candidates_tour2",
        lambda raw_dir, bronze_dir: (_ for _ in ()).throw(
            ValueError("tour2 unavailable")
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_dim_commune",
        _stub_build_dim_commune(call_order),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_dim_candidate_leader",
        _stub_build_dim_candidate(call_order),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_sample",
        _stub_build_sample(call_order),
    )

    result = sampling_pipeline.run_sampling_pipeline(
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        duckdb_path=duckdb_path,
    )

    assert result.status == "partial"
    meta_run = _read_latest_meta_run(duckdb_path)
    assert meta_run is not None
    assert meta_run[1] == "partial"
    assert meta_run[3] == 1


def test_sampling_pipeline_required_failure_marks_meta_run_failed(
    tmp_path, monkeypatch
):
    """Error path: required-step failures must be logged before re-raising."""
    call_order: list[str] = []
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_geography",
        _stub_ingest("geography/cog_communes.parquet", call_order, "ingest_geography"),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_seats_population",
        _stub_ingest(
            "seats/seats_population.parquet", call_order, "ingest_seats_population"
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_candidates",
        _stub_ingest(
            "candidates/candidates_tour1.parquet",
            call_order,
            "ingest_candidates",
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_incumbents",
        _stub_ingest("rne/rne_incumbents.parquet", call_order, "ingest_incumbents"),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "ingest_candidates_tour2",
        _stub_ingest(
            "candidates/candidates_tour2.parquet",
            call_order,
            "ingest_candidates_tour2",
        ),
    )
    monkeypatch.setattr(
        sampling_pipeline,
        "build_dim_commune",
        _stub_build_dim_commune(call_order),
    )

    def _raise_failure(
        bronze_dir,
        silver_dir,
        duckdb_path,
        candidates_column_map=None,
        rne_column_map=None,
        include_tour2_flag=True,
    ):
        del bronze_dir, silver_dir, duckdb_path
        del candidates_column_map, rne_column_map, include_tour2_flag
        call_order.append("build_dim_candidate_leader")
        raise ValueError("required step failed")

    monkeypatch.setattr(
        sampling_pipeline,
        "build_dim_candidate_leader",
        _raise_failure,
    )

    with pytest.raises(ValueError, match="required step failed"):
        sampling_pipeline.run_sampling_pipeline(
            raw_dir=tmp_path / "raw",
            bronze_dir=tmp_path / "bronze",
            silver_dir=tmp_path / "silver",
            gold_dir=tmp_path / "gold",
            duckdb_path=duckdb_path,
        )

    meta_run = _read_latest_meta_run(duckdb_path)
    assert meta_run is not None
    assert meta_run[1] == "failed"
    assert meta_run[3] == 1
