"""Tests for the Phase 0 NLP input orchestration entrypoint."""

from __future__ import annotations

import pandas as pd
import pytest

from src.orchestration.nlp_input_pipeline import run_nlp_input_pipeline


def _write_valid_inputs(tmp_path):
    """Write minimal valid Silver inputs for orchestration tests."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    fact_mention_path = input_dir / "fact_mention.parquet"
    fact_article_path = input_dir / "fact_article.parquet"
    pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "canonical_article_id": "article-001",
                "leader_id": "leader-001",
                "context_sentences": (
                    "Alice Martin presente son programme local. "
                    "Elle defend le logement public devant les habitants."
                ),
            }
        ]
    ).to_parquet(fact_mention_path)
    pd.DataFrame(
        [{"canonical_article_id": "article-001", "language": "fr"}]
    ).to_parquet(fact_article_path)
    return fact_mention_path, fact_article_path


def test_run_nlp_input_pipeline_materializes_artifact_and_logs_success(
    tmp_path,
    read_pipeline_meta_run,
):
    """Integration: successful Phase 0 runs are observable in meta_run."""
    fact_mention_path, fact_article_path = _write_valid_inputs(tmp_path)
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    result = run_nlp_input_pipeline(
        fact_mention_path=fact_mention_path,
        fact_article_path=fact_article_path,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )

    assert result.status == "success"
    assert result.rows_ingested == 1
    assert result.error_count == 0
    assert (silver_dir / "fact_mention_nlp_input.parquet").exists()
    assert read_pipeline_meta_run(duckdb_path, "nlp_input_pipeline") == (
        "success",
        1,
        0,
    )


def test_run_nlp_input_pipeline_logs_failed_meta_run(
    tmp_path,
    read_pipeline_meta_run,
):
    """Regression: required-step failures still leave an audit row."""
    fact_mention_path, _fact_article_path = _write_valid_inputs(tmp_path)
    missing_fact_article_path = tmp_path / "missing_fact_article.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_input_pipeline(
            fact_mention_path=fact_mention_path,
            fact_article_path=missing_fact_article_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_input_pipeline") == (
        "failed",
        0,
        1,
    )
