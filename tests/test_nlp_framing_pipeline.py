"""Tests for the Phase 4 NLP framing orchestration entrypoint."""

from __future__ import annotations

import pandas as pd
import pytest
from conftest import ConfigurableFrameRunner

from src.nlp.nli import SCORABLE_FRAME_LABELS, FramePrediction
from src.orchestration.nlp_framing_pipeline import run_nlp_framing_pipeline
from src.transform._exceptions import DataQualityError


def _write_valid_inputs(tmp_path, model_bundle_version: str):
    """Write minimal valid Phase 4 input artifacts."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    nlp_input_path = input_dir / "fact_mention_nlp_input.parquet"
    nlp_summary_path = input_dir / "fact_mention_nlp_summary.parquet"

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
    return nlp_input_path, nlp_summary_path


def _summary_row(model_bundle_version: str) -> dict[str, object]:
    """Return one valid NLP summary row."""
    return {
        "mention_id": "mention-001",
        "leader_id": "leader-001",
        "canonical_article_id": "article-001",
        "input_hash": "hash-001",
        "generic_sentiment_label": "4 stars",
        "generic_sentiment_score": 0.25,
        "target_tone_label": "favorable",
        "target_tone_probability": 0.82,
        "primary_frame_label": "unclassified",
        "primary_frame_probability": None,
        "was_truncated_to_max_length": False,
        "nlp_enrichment_status": "scored",
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": pd.Timestamp("2026-04-02T10:00:00Z"),
        "error_type": None,
    }


def _frame_runner() -> ConfigurableFrameRunner:
    """Return a deterministic frame runner for orchestration tests."""
    return ConfigurableFrameRunner(
        {
            "mention-001": FramePrediction(
                probabilities_by_label={
                    "politique": 0.82,
                    "vie_privee": 0.08,
                    "apparence": 0.05,
                    "scandale": 0.04,
                    "personnalite": 0.12,
                    "securite": 0.10,
                }
            )
        }
    )


def test_run_nlp_framing_pipeline_materializes_artifacts_and_logs_success(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Integration: successful Phase 4 runs are observable in meta_run."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    frame_runner = _frame_runner()

    result = run_nlp_framing_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        frame_runner=frame_runner,
        model_bundle_config=model_bundle_config,
    )

    summary_dataframe = pd.read_parquet(silver_dir / "fact_mention_nlp_summary.parquet")
    frame_score_dataframe = pd.read_parquet(
        silver_dir / "fact_mention_frame_score.parquet"
    )
    assert result.status == "success"
    assert result.rows_ingested == len(SCORABLE_FRAME_LABELS)
    assert result.error_count == 0
    assert summary_dataframe.loc[0, "primary_frame_label"] == "politique"
    assert summary_dataframe.loc[0, "primary_frame_probability"] == pytest.approx(0.82)
    assert len(frame_score_dataframe) == len(SCORABLE_FRAME_LABELS)
    assert read_pipeline_meta_run(duckdb_path, "nlp_framing_pipeline") == (
        "success",
        len(SCORABLE_FRAME_LABELS),
        0,
    )
    assert frame_runner.calls == [["mention-001"]]


def test_run_nlp_framing_pipeline_is_idempotent(
    tmp_path,
    model_bundle_config_factory,
):
    """Regression: repeated orchestration replaces both DuckDB tables."""
    duckdb = pytest.importorskip("duckdb")
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    run_nlp_framing_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        frame_runner=_frame_runner(),
        model_bundle_config=model_bundle_config,
    )
    run_nlp_framing_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        frame_runner=_frame_runner(),
        model_bundle_config=model_bundle_config,
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        summary_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_summary"
        ).fetchone()[0]
        frame_score_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_frame_score"
        ).fetchone()[0]
    finally:
        conn.close()
    assert summary_count == 1
    assert frame_score_count == len(SCORABLE_FRAME_LABELS)


def test_run_nlp_framing_pipeline_logs_failed_meta_run(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Regression: required-step failures still leave an audit row."""
    model_bundle_config = model_bundle_config_factory()
    _nlp_input_path, nlp_summary_path = _write_valid_inputs(
        tmp_path,
        model_bundle_config.bundle_version,
    )
    missing_input_path = tmp_path / "missing_fact_mention_nlp_input.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_framing_pipeline(
            nlp_input_path=missing_input_path,
            nlp_summary_path=nlp_summary_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
            frame_runner=_frame_runner(),
            model_bundle_config=model_bundle_config,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_framing_pipeline") == (
        "failed",
        0,
        1,
    )


def test_run_nlp_framing_pipeline_fails_on_bundle_mismatch(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Regression: stale summary rows fail before Phase 4 writes output."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path = _write_valid_inputs(
        tmp_path,
        "stale-bundle",
    )
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(DataQualityError, match="bundle version"):
        run_nlp_framing_pipeline(
            nlp_input_path=nlp_input_path,
            nlp_summary_path=nlp_summary_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
            frame_runner=_frame_runner(),
            model_bundle_config=model_bundle_config,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_framing_pipeline") == (
        "failed",
        0,
        1,
    )
