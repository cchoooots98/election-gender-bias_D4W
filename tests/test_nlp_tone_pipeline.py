"""Tests for the Phase 3 NLP tone orchestration entrypoint."""

from __future__ import annotations

import pandas as pd
import pytest
from conftest import ConfigurableToneRunner

from src.nlp.nli import TonePrediction
from src.orchestration.nlp_tone_pipeline import run_nlp_tone_pipeline
from src.transform._exceptions import DataQualityError


def _write_valid_inputs(tmp_path, model_bundle_version: str):
    """Write minimal valid Phase 3 input artifacts."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    nlp_input_path = input_dir / "fact_mention_nlp_input.parquet"
    nlp_summary_path = input_dir / "fact_mention_nlp_summary.parquet"
    sample_leaders_path = input_dir / "sample_leaders.parquet"

    pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "input_text": "Alice Martin presente son programme municipal local.",
                "input_hash": "hash-001",
                "eligible_for_inference": True,
                "skip_reason": None,
            }
        ]
    ).to_parquet(nlp_input_path)
    pd.DataFrame([_summary_row(model_bundle_version)]).to_parquet(nlp_summary_path)
    pd.DataFrame([{"leader_id": "leader-001", "full_name": "Alice Martin"}]).to_parquet(
        sample_leaders_path
    )
    return nlp_input_path, nlp_summary_path, sample_leaders_path


def _summary_row(model_bundle_version: str) -> dict[str, object]:
    """Return one valid Phase 2 summary row."""
    return {
        "mention_id": "mention-001",
        "leader_id": "leader-001",
        "canonical_article_id": "article-001",
        "input_hash": "hash-001",
        "generic_sentiment_label": "4 stars",
        "generic_sentiment_score": 0.25,
        "target_tone_label": "unclassified",
        "target_tone_probability": None,
        "primary_frame_label": "unclassified",
        "primary_frame_probability": None,
        "was_truncated_to_max_length": False,
        "nlp_enrichment_status": "scored",
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": pd.Timestamp("2026-04-02T10:00:00Z"),
        "error_type": None,
    }


def _tone_runner() -> ConfigurableToneRunner:
    """Return a deterministic tone runner for orchestration tests."""
    return ConfigurableToneRunner(
        {
            "mention-001": TonePrediction(
                probabilities_by_label={
                    "favorable": 0.82,
                    "unfavorable": 0.08,
                    "neutral": 0.10,
                }
            )
        }
    )


def test_run_nlp_tone_pipeline_materializes_artifact_and_logs_success(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Integration: successful Phase 3 runs are observable in meta_run."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, sample_leaders_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    tone_runner = _tone_runner()

    result = run_nlp_tone_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        sample_leaders_path=sample_leaders_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        tone_runner=tone_runner,
        model_bundle_config=model_bundle_config,
    )

    output_dataframe = pd.read_parquet(silver_dir / "fact_mention_nlp_summary.parquet")
    assert result.status == "success"
    assert result.rows_ingested == 1
    assert result.error_count == 0
    assert output_dataframe.loc[0, "generic_sentiment_label"] == "4 stars"
    assert output_dataframe.loc[0, "target_tone_label"] == "favorable"
    assert output_dataframe.loc[0, "target_tone_probability"] == pytest.approx(0.82)
    assert read_pipeline_meta_run(duckdb_path, "nlp_tone_pipeline") == (
        "success",
        1,
        0,
    )
    assert tone_runner.calls == [["mention-001"]]


def test_run_nlp_tone_pipeline_is_idempotent(
    tmp_path,
    model_bundle_config_factory,
):
    """Regression: repeated orchestration replaces the DuckDB summary table."""
    duckdb = pytest.importorskip("duckdb")
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, sample_leaders_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    run_nlp_tone_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        sample_leaders_path=sample_leaders_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        tone_runner=_tone_runner(),
        model_bundle_config=model_bundle_config,
    )
    run_nlp_tone_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        sample_leaders_path=sample_leaders_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        tone_runner=_tone_runner(),
        model_bundle_config=model_bundle_config,
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_summary"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == 1


def test_run_nlp_tone_pipeline_logs_failed_meta_run(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Regression: required-step failures still leave an audit row."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, _nlp_summary_path, sample_leaders_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    missing_summary_path = tmp_path / "missing_fact_mention_nlp_summary.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_tone_pipeline(
            nlp_input_path=nlp_input_path,
            nlp_summary_path=missing_summary_path,
            sample_leaders_path=sample_leaders_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
            tone_runner=_tone_runner(),
            model_bundle_config=model_bundle_config,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_tone_pipeline") == (
        "failed",
        0,
        1,
    )


def test_run_nlp_tone_pipeline_fails_on_bundle_mismatch(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Regression: stale summary rows fail before Phase 3 writes output."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, sample_leaders_path = _write_valid_inputs(
        tmp_path,
        "stale-bundle",
    )
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(DataQualityError, match="bundle version"):
        run_nlp_tone_pipeline(
            nlp_input_path=nlp_input_path,
            nlp_summary_path=nlp_summary_path,
            sample_leaders_path=sample_leaders_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
            tone_runner=_tone_runner(),
            model_bundle_config=model_bundle_config,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_tone_pipeline") == (
        "failed",
        0,
        1,
    )


def test_run_nlp_tone_pipeline_fails_on_missing_sample_leader(
    tmp_path,
    model_bundle_config_factory,
):
    """Error path: candidate-aware tone requires sampled leader names."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, sample_leaders_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    pd.DataFrame(
        [{"leader_id": "leader-999", "full_name": "Other Candidate"}]
    ).to_parquet(sample_leaders_path)

    with pytest.raises(DataQualityError, match="missing from sample_leaders"):
        run_nlp_tone_pipeline(
            nlp_input_path=nlp_input_path,
            nlp_summary_path=nlp_summary_path,
            sample_leaders_path=sample_leaders_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
            tone_runner=_tone_runner(),
            model_bundle_config=model_bundle_config,
        )
