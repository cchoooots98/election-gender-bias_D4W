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


def _read_nlp_lexicon_meta_run(duckdb_path):
    """Return the meta_run row for the NLP lexicon pipeline."""
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect(str(duckdb_path))
    try:
        return conn.execute(
            """
            SELECT status, rows_ingested, error_count
            FROM meta.meta_run
            WHERE flow_name = 'nlp_lexicon_pipeline'
            ORDER BY end_ts DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()


def test_run_nlp_lexicon_pipeline_materializes_artifact_and_logs_success(tmp_path):
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
    assert _read_nlp_lexicon_meta_run(duckdb_path) == ("success", 2, 0)


def test_run_nlp_lexicon_pipeline_logs_failed_meta_run(tmp_path):
    """Regression: required-step failures still leave an audit row."""
    missing_nlp_input_path = tmp_path / "missing_fact_mention_nlp_input.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_lexicon_pipeline(
            nlp_input_path=missing_nlp_input_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=duckdb_path,
        )

    assert _read_nlp_lexicon_meta_run(duckdb_path) == ("failed", 0, 1)


def test_run_nlp_lexicon_pipeline_preserves_original_error_when_meta_run_fails(
    monkeypatch,
    tmp_path,
):
    """Regression: meta logging failures must not hide the root cause."""
    missing_nlp_input_path = tmp_path / "missing_fact_mention_nlp_input.parquet"

    def _raise_meta_error(*args, **kwargs):
        raise RuntimeError("meta_run unavailable")

    monkeypatch.setattr(
        "src.orchestration.nlp_lexicon_pipeline.log_pipeline_run",
        _raise_meta_error,
    )

    with pytest.raises(FileNotFoundError):
        run_nlp_lexicon_pipeline(
            nlp_input_path=missing_nlp_input_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )


def test_run_nlp_lexicon_pipeline_raises_when_meta_run_fails_after_success(
    monkeypatch,
    tmp_path,
):
    """Regression: successful materialization must still surface meta failures."""
    nlp_input_path = _write_valid_nlp_input(tmp_path)

    def _raise_meta_error(*args, **kwargs):
        raise RuntimeError("meta_run unavailable")

    monkeypatch.setattr(
        "src.orchestration.nlp_lexicon_pipeline.log_pipeline_run",
        _raise_meta_error,
    )

    with pytest.raises(RuntimeError, match="meta_run unavailable"):
        run_nlp_lexicon_pipeline(
            nlp_input_path=nlp_input_path,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )
