"""Tests for the Phase 5 NLP QA orchestration entrypoint."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.nlp.input_contracts import FACT_MENTION_NLP_INPUT_COLUMNS, compute_input_hash
from src.nlp.lexicon import FACT_STEREOTYPE_WORD_COUNTS_COLUMNS
from src.nlp.nli import FACT_MENTION_FRAME_SCORE_COLUMNS, SCORABLE_FRAME_LABELS
from src.nlp.sentiment import FACT_MENTION_NLP_SUMMARY_COLUMNS
from src.orchestration.nlp_qa_pipeline import run_nlp_qa_pipeline

_CONTEXT_TEXT = (
    "Alice Martin presente son programme municipal avec des mesures de "
    "transport, logement et securite locale pour les habitants."
)
_INPUT_HASH = compute_input_hash(_CONTEXT_TEXT)


def _write_valid_inputs(tmp_path, model_bundle_version: str):
    """Write minimal Phase 5 input artifacts."""
    input_dir = tmp_path / "inputs"
    input_dir.mkdir()
    nlp_input_path = input_dir / "fact_mention_nlp_input.parquet"
    nlp_summary_path = input_dir / "fact_mention_nlp_summary.parquet"
    frame_score_path = input_dir / "fact_mention_frame_score.parquet"
    stereotype_path = input_dir / "fact_stereotype_word_counts.parquet"

    pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "canonical_article_id": "article-001",
                "leader_id": "leader-001",
                "article_language": "fr",
                "input_text": _CONTEXT_TEXT,
                "input_hash": _INPUT_HASH,
                "context_word_count": 18,
                "eligible_for_lexicon": True,
                "eligible_for_inference": True,
                "skip_reason": None,
                "prepared_at": pd.Timestamp("2026-05-12T10:00:00Z"),
                "input_contract_version": "mention_context_v2",
            },
            {
                "mention_id": "mention-002",
                "canonical_article_id": "article-002",
                "leader_id": "leader-002",
                "article_language": "fr",
                "input_text": None,
                "input_hash": None,
                "context_word_count": 0,
                "eligible_for_lexicon": False,
                "eligible_for_inference": False,
                "skip_reason": "empty_context",
                "prepared_at": pd.Timestamp("2026-05-12T10:00:00Z"),
                "input_contract_version": "mention_context_v2",
            },
        ],
        columns=FACT_MENTION_NLP_INPUT_COLUMNS,
    ).to_parquet(nlp_input_path)
    pd.DataFrame(
        [
            _summary_row(
                mention_id="mention-001",
                input_hash=_INPUT_HASH,
                model_bundle_version=model_bundle_version,
            ),
            _summary_row(
                mention_id="mention-002",
                input_hash=None,
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
        ],
        columns=FACT_MENTION_NLP_SUMMARY_COLUMNS,
    ).to_parquet(nlp_summary_path)
    pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "frame_label": frame_label,
                "frame_probability": 0.72 if frame_label == "politique" else 0.10,
                "is_primary_frame": frame_label == "politique",
                "passes_threshold": frame_label == "politique",
                "nli_hypothesis": f"Le texte discute {frame_label}.",
                "nlp_model_bundle_version": model_bundle_version,
            }
            for frame_label in SCORABLE_FRAME_LABELS
        ],
        columns=FACT_MENTION_FRAME_SCORE_COLUMNS,
    ).to_parquet(frame_score_path)
    pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "lexicon_category": "politique",
                "term": "programme",
                "count": 1,
                "count_per_1k_tokens": 55.5,
                "lexicon_version": "stereotype_terms_v1",
            }
        ],
        columns=FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    ).to_parquet(stereotype_path)
    return nlp_input_path, nlp_summary_path, frame_score_path, stereotype_path


def _summary_row(
    *,
    mention_id: str,
    input_hash: str | None,
    model_bundle_version: str,
    status: str = "scored",
) -> dict[str, object]:
    """Return one valid NLP summary row."""
    scored = status == "scored"
    return {
        "mention_id": mention_id,
        "leader_id": mention_id.replace("mention", "leader"),
        "canonical_article_id": mention_id.replace("mention", "article"),
        "input_hash": input_hash,
        "generic_sentiment_label": "4 stars" if scored else None,
        "generic_sentiment_score": 0.25 if scored else None,
        "target_tone_label": "favorable" if scored else "unclassified",
        "target_tone_probability": 0.72 if scored else None,
        "primary_frame_label": "politique" if scored else "unclassified",
        "primary_frame_probability": 0.72 if scored else None,
        "was_truncated_to_max_length": False,
        "nlp_enrichment_status": status,
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": pd.Timestamp("2026-05-12T11:00:00Z") if scored else None,
        "error_type": None,
    }


def test_run_nlp_qa_pipeline_writes_report_and_logs_success(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Integration: successful Phase 5 QA runs are observable in meta_run."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, frame_score_path, stereotype_path = (
        _write_valid_inputs(tmp_path, model_bundle_config.bundle_version)
    )
    report_path = tmp_path / "gold" / "nlp_qa_report.json"
    duckdb_path = tmp_path / "warehouse.duckdb"

    result = run_nlp_qa_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        frame_score_path=frame_score_path,
        stereotype_word_counts_path=stereotype_path,
        report_path=report_path,
        duckdb_path=duckdb_path,
    )

    with report_path.open(encoding="utf-8") as file_handle:
        report = json.load(file_handle)
    assert result.status == "success"
    assert result.rows_ingested == 1
    assert result.error_count == 0
    assert report["report_name"] == "nlp_qa_report"
    assert read_pipeline_meta_run(duckdb_path, "nlp_qa_pipeline") == (
        "success",
        1,
        0,
    )


def test_run_nlp_qa_pipeline_logs_failed_meta_run(
    tmp_path,
    model_bundle_config_factory,
    read_pipeline_meta_run,
):
    """Regression: source failures still leave a failed meta_run row."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, _frame_score_path, stereotype_path = (
        _write_valid_inputs(tmp_path, model_bundle_config.bundle_version)
    )
    missing_frame_score_path = tmp_path / "missing_fact_mention_frame_score.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with pytest.raises(FileNotFoundError):
        run_nlp_qa_pipeline(
            nlp_input_path=nlp_input_path,
            nlp_summary_path=nlp_summary_path,
            frame_score_path=missing_frame_score_path,
            stereotype_word_counts_path=stereotype_path,
            report_path=tmp_path / "gold" / "nlp_qa_report.json",
            duckdb_path=duckdb_path,
        )

    assert read_pipeline_meta_run(duckdb_path, "nlp_qa_pipeline") == (
        "failed",
        0,
        1,
    )


def test_run_nlp_qa_pipeline_overwrites_report(tmp_path, model_bundle_config_factory):
    """Regression: repeated runs replace the JSON report artifact."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_path, nlp_summary_path, frame_score_path, stereotype_path = (
        _write_valid_inputs(tmp_path, model_bundle_config.bundle_version)
    )
    report_path = tmp_path / "gold" / "nlp_qa_report.json"
    duckdb_path = tmp_path / "warehouse.duckdb"

    run_nlp_qa_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        frame_score_path=frame_score_path,
        stereotype_word_counts_path=stereotype_path,
        report_path=report_path,
        duckdb_path=duckdb_path,
        thresholds=[0.50],
    )
    run_nlp_qa_pipeline(
        nlp_input_path=nlp_input_path,
        nlp_summary_path=nlp_summary_path,
        frame_score_path=frame_score_path,
        stereotype_word_counts_path=stereotype_path,
        report_path=report_path,
        duckdb_path=duckdb_path,
        thresholds=[0.50, 0.80],
    )

    with report_path.open(encoding="utf-8") as file_handle:
        report = json.load(file_handle)
    assert report["threshold_sensitivity"]["thresholds"] == [0.50, 0.80]
