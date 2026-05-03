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


def _read_nlp_input_meta_run(duckdb_path):
    """Return the meta_run row for the NLP input pipeline."""
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect(str(duckdb_path))
    try:
        return conn.execute(
            """
            SELECT status, rows_ingested, error_count
            FROM meta.meta_run
            WHERE flow_name = 'nlp_input_pipeline'
            ORDER BY end_ts DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()


def test_run_nlp_input_pipeline_materializes_artifact_and_logs_success(tmp_path):
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
    assert _read_nlp_input_meta_run(duckdb_path) == ("success", 1, 0)


def test_run_nlp_input_pipeline_logs_failed_meta_run(tmp_path):
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

    assert _read_nlp_input_meta_run(duckdb_path) == ("failed", 0, 1)


def test_run_nlp_input_pipeline_preserves_original_error_when_meta_run_fails(
    monkeypatch,
    tmp_path,
):
    """Regression: meta logging failures must not hide the root cause."""
    fact_mention_path, _fact_article_path = _write_valid_inputs(tmp_path)
    missing_fact_article_path = tmp_path / "missing_fact_article.parquet"

    def _raise_meta_error(*args, **kwargs):
        raise RuntimeError("meta_run unavailable")

    monkeypatch.setattr(
        "src.orchestration.nlp_input_pipeline.log_pipeline_run",
        _raise_meta_error,
    )

    with pytest.raises(FileNotFoundError):
        run_nlp_input_pipeline(
            fact_mention_path=fact_mention_path,
            fact_article_path=missing_fact_article_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )


def test_run_nlp_input_pipeline_raises_when_meta_run_fails_after_success(
    monkeypatch,
    tmp_path,
):
    """Regression: successful materialization must still surface meta failures."""
    fact_mention_path, fact_article_path = _write_valid_inputs(tmp_path)

    def _raise_meta_error(*args, **kwargs):
        raise RuntimeError("meta_run unavailable")

    monkeypatch.setattr(
        "src.orchestration.nlp_input_pipeline.log_pipeline_run",
        _raise_meta_error,
    )

    with pytest.raises(RuntimeError, match="meta_run unavailable"):
        run_nlp_input_pipeline(
            fact_mention_path=fact_mention_path,
            fact_article_path=fact_article_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )
