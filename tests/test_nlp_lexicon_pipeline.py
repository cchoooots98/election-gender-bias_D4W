"""Tests for the Phase 1 NLP lexicon orchestration entrypoint."""

from __future__ import annotations

import pandas as pd
import pytest

from src.orchestration.nlp_lexicon_pipeline import run_nlp_lexicon_pipeline


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
                "input_text": "Le programme municipal parle de securite.",
                "context_word_count": 6,
                "eligible_for_lexicon": True,
            }
        ]
    ).to_parquet(nlp_input_path)
    return nlp_input_path


def _write_gold_inputs(tmp_path):
    """Write minimal Gold inputs required by trait metric materialization."""
    gold_dir = tmp_path / "gold_inputs"
    gold_dir.mkdir()
    sample_leaders_path = gold_dir / "sample_leaders.parquet"
    exposure_metrics_path = gold_dir / "mart_exposure_metrics.parquet"
    pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "full_name": "Candidate One",
                "gender": "F",
                "commune_name": "Commune One",
            }
        ]
    ).to_parquet(sample_leaders_path)
    pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "gender": "F",
                "article_count": 1,
            }
        ]
    ).to_parquet(exposure_metrics_path)
    return sample_leaders_path, exposure_metrics_path


def test_run_nlp_lexicon_pipeline_materializes_artifact_and_logs_success(
    tmp_path,
    read_pipeline_meta_run,
):
    """Integration: successful Phase 1 runs are observable in meta_run."""
    nlp_input_path = _write_valid_nlp_input(tmp_path)
    sample_leaders_path, exposure_metrics_path = _write_gold_inputs(tmp_path)
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    duckdb_path = tmp_path / "warehouse.duckdb"

    result = run_nlp_lexicon_pipeline(
        nlp_input_path=nlp_input_path,
        sample_leaders_path=sample_leaders_path,
        exposure_metrics_path=exposure_metrics_path,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    assert result.status == "success"
    assert result.rows_ingested > 2
    assert result.error_count == 0
    assert (silver_dir / "fact_stereotype_word_counts.parquet").exists()
    assert (silver_dir / "fact_trait_word_counts.parquet").exists()
    assert (gold_dir / "mart_trait_metrics.parquet").exists()
    assert read_pipeline_meta_run(duckdb_path, "nlp_lexicon_pipeline") == (
        "success",
        result.rows_ingested,
        0,
    )


def test_run_nlp_lexicon_pipeline_logs_failed_meta_run(
    tmp_path,
    read_pipeline_meta_run,
):
    """Regression: required-step failures still leave an audit row."""
    missing_nlp_input_path = tmp_path / "missing_fact_mention_nlp_input.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_lexicon_pipeline(
            nlp_input_path=missing_nlp_input_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_lexicon_pipeline") == (
        "failed",
        0,
        1,
    )
