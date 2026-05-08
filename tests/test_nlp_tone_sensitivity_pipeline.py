"""Tests for tone sensitivity orchestration."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.orchestration.nlp_tone_sensitivity_pipeline import (
    run_nlp_tone_sensitivity_pipeline,
)


def _write_valid_inputs(tmp_path):
    """Write minimal valid sensitivity input artifacts."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    nlp_summary_path = input_dir / "fact_mention_nlp_summary.parquet"
    sample_leaders_path = input_dir / "sample_leaders.parquet"

    pd.DataFrame(
        [
            _summary_row(
                mention_id="mention-001",
                leader_id="leader-f",
                label="favorable",
                probability=0.82,
            ),
            _summary_row(
                mention_id="mention-002",
                leader_id="leader-f",
                label="unclassified",
                probability=0.58,
            ),
            _summary_row(
                mention_id="mention-003",
                leader_id="leader-m",
                label="neutral",
                probability=0.62,
            ),
        ]
    ).to_parquet(nlp_summary_path)
    pd.DataFrame(
        [
            {"leader_id": "leader-f", "gender": "F"},
            {"leader_id": "leader-m", "gender": "M"},
        ]
    ).to_parquet(sample_leaders_path)
    return nlp_summary_path, sample_leaders_path


def _summary_row(
    *,
    mention_id: str,
    leader_id: str,
    label: str,
    probability: float,
) -> dict[str, object]:
    """Return one valid sensitivity source row."""
    return {
        "mention_id": mention_id,
        "leader_id": leader_id,
        "target_tone_label": label,
        "target_tone_probability": probability,
        "nlp_enrichment_status": "scored",
        "nlp_model_bundle_version": "bundle-001",
    }


def test_run_nlp_tone_sensitivity_pipeline_writes_artifacts_and_logs_success(
    tmp_path,
    read_pipeline_meta_run,
):
    """Integration: sensitivity runs are materialized and observable."""
    duckdb = pytest.importorskip("duckdb")
    nlp_summary_path, sample_leaders_path = _write_valid_inputs(tmp_path)
    report_path = tmp_path / "gold" / "nlp_tone_sensitivity_report.json"
    parquet_path = tmp_path / "gold" / "nlp_tone_threshold_sensitivity.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    result = run_nlp_tone_sensitivity_pipeline(
        nlp_summary_path=nlp_summary_path,
        sample_leaders_path=sample_leaders_path,
        report_path=report_path,
        parquet_path=parquet_path,
        duckdb_path=duckdb_path,
        thresholds=[0.40, 0.60],
    )

    assert result.status == "success"
    assert result.rows_ingested == 6
    assert result.error_count == 0
    assert report_path.exists()
    assert parquet_path.exists()
    with report_path.open(encoding="utf-8") as file_handle:
        report = json.load(file_handle)
    assert report["analysis_scope"]["metric_type"] == "coverage sensitivity"

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM gold.nlp_tone_threshold_sensitivity"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == 6
    assert read_pipeline_meta_run(duckdb_path, "nlp_tone_sensitivity_pipeline") == (
        "success",
        6,
        0,
    )


def test_run_nlp_tone_sensitivity_pipeline_is_idempotent(tmp_path):
    """Regression: repeated runs replace the DuckDB QA table."""
    duckdb = pytest.importorskip("duckdb")
    nlp_summary_path, sample_leaders_path = _write_valid_inputs(tmp_path)
    report_path = tmp_path / "gold" / "nlp_tone_sensitivity_report.json"
    parquet_path = tmp_path / "gold" / "nlp_tone_threshold_sensitivity.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    run_nlp_tone_sensitivity_pipeline(
        nlp_summary_path=nlp_summary_path,
        sample_leaders_path=sample_leaders_path,
        report_path=report_path,
        parquet_path=parquet_path,
        duckdb_path=duckdb_path,
        thresholds=[0.40],
    )
    run_nlp_tone_sensitivity_pipeline(
        nlp_summary_path=nlp_summary_path,
        sample_leaders_path=sample_leaders_path,
        report_path=report_path,
        parquet_path=parquet_path,
        duckdb_path=duckdb_path,
        thresholds=[0.40],
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM gold.nlp_tone_threshold_sensitivity"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == 3


def test_run_nlp_tone_sensitivity_pipeline_logs_failed_meta_run(
    tmp_path,
    read_pipeline_meta_run,
):
    """Regression: source failures still leave a failed meta_run row."""
    _nlp_summary_path, sample_leaders_path = _write_valid_inputs(tmp_path)
    missing_summary_path = tmp_path / "missing_fact_mention_nlp_summary.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_tone_sensitivity_pipeline(
            nlp_summary_path=missing_summary_path,
            sample_leaders_path=sample_leaders_path,
            report_path=tmp_path / "gold" / "nlp_tone_sensitivity_report.json",
            parquet_path=tmp_path / "gold" / "nlp_tone_threshold_sensitivity.parquet",
            duckdb_path=duckdb_path,
            thresholds=[0.40],
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_tone_sensitivity_pipeline") == (
        "failed",
        0,
        1,
    )
