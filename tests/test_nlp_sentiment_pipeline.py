"""Tests for the Phase 2 NLP sentiment orchestration entrypoint."""

from __future__ import annotations

import pandas as pd
import pytest

from src.orchestration.nlp_sentiment_pipeline import run_nlp_sentiment_pipeline


def _write_valid_nlp_input(tmp_path):
    """Write minimal valid NLP input rows for orchestration tests."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    nlp_input_path = input_dir / "fact_mention_nlp_input.parquet"
    pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "input_text": "Le programme municipal parle de logement.",
                "input_hash": "hash-001",
                "eligible_for_inference": True,
                "skip_reason": None,
            }
        ]
    ).to_parquet(nlp_input_path)
    return nlp_input_path


def _sentiment_predictions_by_text(sentiment_prediction_factory):
    """Return deterministic orchestration sentiment predictions by input text."""
    return {
        "Le programme municipal parle de logement.": sentiment_prediction_factory(
            label="4 stars",
            probabilities={
                "1 star": 0.05,
                "2 stars": 0.10,
                "3 stars": 0.25,
                "4 stars": 0.40,
                "5 stars": 0.20,
            },
        )
    }


def test_run_nlp_sentiment_pipeline_materializes_artifact_and_logs_success(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Integration: successful Phase 2 runs are observable in meta_run."""
    nlp_input_path = _write_valid_nlp_input(tmp_path)
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    sentiment_runner = sentiment_runner_factory(
        _sentiment_predictions_by_text(sentiment_prediction_factory)
    )

    result = run_nlp_sentiment_pipeline(
        nlp_input_path=nlp_input_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        sentiment_runner=sentiment_runner,
        model_bundle_config=model_bundle_config_factory(),
    )

    assert result.status == "success"
    assert result.rows_ingested == 1
    assert result.error_count == 0
    assert (silver_dir / "fact_mention_nlp_summary.parquet").exists()
    assert read_pipeline_meta_run(duckdb_path, "nlp_sentiment_pipeline") == (
        "success",
        1,
        0,
    )
    assert sentiment_runner.calls == [["Le programme municipal parle de logement."]]


def test_run_nlp_sentiment_pipeline_is_idempotent(
    tmp_path,
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Regression: repeated orchestration replaces the DuckDB summary table."""
    nlp_input_path = _write_valid_nlp_input(tmp_path)
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    duckdb = pytest.importorskip("duckdb")

    run_nlp_sentiment_pipeline(
        nlp_input_path=nlp_input_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        sentiment_runner=sentiment_runner_factory(
            _sentiment_predictions_by_text(sentiment_prediction_factory)
        ),
        model_bundle_config=model_bundle_config_factory(),
    )
    run_nlp_sentiment_pipeline(
        nlp_input_path=nlp_input_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        sentiment_runner=sentiment_runner_factory(
            _sentiment_predictions_by_text(sentiment_prediction_factory)
        ),
        model_bundle_config=model_bundle_config_factory(),
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_summary"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == 1


def test_run_nlp_sentiment_pipeline_logs_failed_meta_run(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Regression: required-step failures still leave an audit row."""
    missing_nlp_input_path = tmp_path / "missing_fact_mention_nlp_input.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_sentiment_pipeline(
            nlp_input_path=missing_nlp_input_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
            sentiment_runner=sentiment_runner_factory(
                _sentiment_predictions_by_text(sentiment_prediction_factory)
            ),
            model_bundle_config=model_bundle_config_factory(),
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_sentiment_pipeline") == (
        "failed",
        0,
        1,
    )
