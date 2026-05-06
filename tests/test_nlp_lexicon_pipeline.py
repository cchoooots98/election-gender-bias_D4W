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
                "input_text": "Le programme municipal parle de securite.",
                "eligible_for_lexicon": True,
            }
        ]
    ).to_parquet(nlp_input_path)
    return nlp_input_path


def test_run_nlp_lexicon_pipeline_materializes_artifact_and_logs_success(
    tmp_path,
    read_pipeline_meta_run,
):
    """Integration: successful Phase 1 runs are observable in meta_run."""
    nlp_input_path = _write_valid_nlp_input(tmp_path)
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    result = run_nlp_lexicon_pipeline(
        nlp_input_path=nlp_input_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )

    assert result.status == "success"
    assert result.rows_ingested == 2
    assert result.error_count == 0
    assert (silver_dir / "fact_stereotype_word_counts.parquet").exists()
    assert read_pipeline_meta_run(duckdb_path, "nlp_lexicon_pipeline") == (
        "success",
        2,
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
