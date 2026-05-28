"""Tests for the Phase 5 unified NLP QA report."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd
import pytest

from src.nlp.input_contracts import FACT_MENTION_NLP_INPUT_COLUMNS, compute_input_hash
from src.nlp.lexicon import FACT_STEREOTYPE_WORD_COUNTS_COLUMNS
from src.nlp.nli import FACT_MENTION_FRAME_SCORE_COLUMNS, SCORABLE_FRAME_LABELS
from src.nlp.qa import (
    NLP_QA_REPORT_SCHEMA_VERSION,
    build_nlp_qa_report,
    materialize_nlp_qa_report,
)
from src.nlp.sentiment import FACT_MENTION_NLP_SUMMARY_COLUMNS
from src.transform._exceptions import DataQualityError

_CONTEXT_TEXT_BY_MENTION = {
    "mention-001": (
        "Alice Martin presente son programme municipal avec des mesures de "
        "transport, logement et securite locale pour les habitants."
    ),
    "mention-002": (
        "Bernard Durand repond aux questions sur sa campagne locale et son "
        "style de leadership municipal."
    ),
    "mention-003": "Court contexte municipal local.",
}


def _nlp_input_dataframe(rows: list[dict[str, object]] | None = None) -> pd.DataFrame:
    """Return valid Phase 0 input rows for QA tests."""
    return pd.DataFrame(
        rows
        or [
            _input_row(
                mention_id="mention-001",
                context_word_count=18,
                eligible_for_lexicon=True,
                eligible_for_inference=True,
                skip_reason=None,
            ),
            _input_row(
                mention_id="mention-002",
                context_word_count=16,
                eligible_for_lexicon=True,
                eligible_for_inference=True,
                skip_reason=None,
            ),
            _input_row(
                mention_id="mention-003",
                context_word_count=7,
                eligible_for_lexicon=True,
                eligible_for_inference=False,
                skip_reason="too_short_for_inference",
            ),
            _input_row(
                mention_id="mention-004",
                context_word_count=0,
                eligible_for_lexicon=False,
                eligible_for_inference=False,
                skip_reason="empty_context",
            ),
        ],
        columns=FACT_MENTION_NLP_INPUT_COLUMNS,
    )


def _input_row(
    *,
    mention_id: str,
    context_word_count: int,
    eligible_for_lexicon: bool,
    eligible_for_inference: bool,
    skip_reason: str | None,
) -> dict[str, object]:
    """Return one valid NLP input row."""
    input_text = _CONTEXT_TEXT_BY_MENTION.get(mention_id)
    return {
        "mention_id": mention_id,
        "canonical_article_id": mention_id.replace("mention", "article"),
        "leader_id": mention_id.replace("mention", "leader"),
        "article_language": "fr",
        "input_text": input_text,
        "input_hash": compute_input_hash(input_text),
        "context_word_count": context_word_count,
        "eligible_for_lexicon": eligible_for_lexicon,
        "eligible_for_inference": eligible_for_inference,
        "skip_reason": skip_reason,
        "prepared_at": pd.Timestamp("2026-05-12T10:00:00Z"),
        "input_contract_version": "mention_context_v2",
    }


def _nlp_summary_dataframe(model_bundle_version: str) -> pd.DataFrame:
    """Return valid Phase 2/3/4 NLP summary rows."""
    return pd.DataFrame(
        [
            _summary_row(
                mention_id="mention-001",
                input_hash=compute_input_hash(_CONTEXT_TEXT_BY_MENTION["mention-001"]),
                model_bundle_version=model_bundle_version,
                target_tone_label="favorable",
                target_tone_probability=0.82,
                primary_frame_label="politique",
                primary_frame_probability=0.71,
            ),
            _summary_row(
                mention_id="mention-002",
                input_hash=compute_input_hash(_CONTEXT_TEXT_BY_MENTION["mention-002"]),
                model_bundle_version=model_bundle_version,
                target_tone_label="unclassified",
                target_tone_probability=0.55,
                primary_frame_label="unclassified",
                primary_frame_probability=None,
            ),
            _summary_row(
                mention_id="mention-003",
                input_hash=compute_input_hash(_CONTEXT_TEXT_BY_MENTION["mention-003"]),
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
            _summary_row(
                mention_id="mention-004",
                input_hash=None,
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
        ],
        columns=FACT_MENTION_NLP_SUMMARY_COLUMNS,
    )


def _lexicon_only_summary_dataframe(model_bundle_version: str) -> pd.DataFrame:
    """Return valid summary rows for a run where Transformer scoring was skipped."""
    return pd.DataFrame(
        [
            _summary_row(
                mention_id="mention-001",
                input_hash=compute_input_hash(_CONTEXT_TEXT_BY_MENTION["mention-001"]),
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
            _summary_row(
                mention_id="mention-002",
                input_hash=compute_input_hash(_CONTEXT_TEXT_BY_MENTION["mention-002"]),
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
            _summary_row(
                mention_id="mention-003",
                input_hash=compute_input_hash(_CONTEXT_TEXT_BY_MENTION["mention-003"]),
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
            _summary_row(
                mention_id="mention-004",
                input_hash=None,
                model_bundle_version=model_bundle_version,
                status="skipped",
            ),
        ],
        columns=FACT_MENTION_NLP_SUMMARY_COLUMNS,
    )


def _summary_row(
    *,
    mention_id: str,
    input_hash: str | None,
    model_bundle_version: str,
    status: str = "scored",
    target_tone_label: str = "unclassified",
    target_tone_probability: float | None = None,
    primary_frame_label: str = "unclassified",
    primary_frame_probability: float | None = None,
    error_type: str | None = None,
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
        "target_tone_label": target_tone_label,
        "target_tone_probability": target_tone_probability,
        "primary_frame_label": primary_frame_label,
        "primary_frame_probability": primary_frame_probability,
        "was_truncated_to_max_length": False,
        "nlp_enrichment_status": status,
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": pd.Timestamp("2026-05-12T11:00:00Z") if scored else None,
        "error_type": error_type,
    }


def _frame_score_dataframe(
    model_bundle_version: str,
    *,
    all_unclassified: bool = False,
) -> pd.DataFrame:
    """Return valid Phase 4 frame-score rows."""
    rows: list[dict[str, object]] = []
    for mention_id in ("mention-001", "mention-002"):
        for frame_label in SCORABLE_FRAME_LABELS:
            probability = _frame_probability(
                mention_id,
                frame_label,
                all_unclassified=all_unclassified,
            )
            is_primary = (
                mention_id == "mention-001"
                and frame_label == "politique"
                and not all_unclassified
            )
            rows.append(
                {
                    "mention_id": mention_id,
                    "frame_label": frame_label,
                    "frame_probability": probability,
                    "is_primary_frame": is_primary,
                    "passes_threshold": probability >= 0.60,
                    "nli_hypothesis": f"Le texte discute {frame_label}.",
                    "nlp_model_bundle_version": model_bundle_version,
                }
            )
    return pd.DataFrame(rows, columns=FACT_MENTION_FRAME_SCORE_COLUMNS)


def _frame_probability(
    mention_id: str,
    frame_label: str,
    *,
    all_unclassified: bool,
) -> float:
    """Return deterministic frame probabilities for QA fixtures."""
    if all_unclassified:
        return 0.42
    if mention_id == "mention-001" and frame_label == "politique":
        return 0.71
    if mention_id == "mention-002" and frame_label == "personnalite":
        return 0.55
    return 0.12


def _stereotype_word_counts_dataframe() -> pd.DataFrame:
    """Return valid Phase 1 stereotype word-count rows."""
    return pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "lexicon_category": "politique",
                "term": "programme",
                "count": 1,
                "count_per_1k_tokens": 55.5,
                "lexicon_version": "stereotype_terms_v1",
            },
            {
                "mention_id": "mention-002",
                "lexicon_category": "apparence",
                "term": "age",
                "count": 2,
                "count_per_1k_tokens": 125.0,
                "lexicon_version": "stereotype_terms_v1",
            },
        ],
        columns=FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    )


def test_build_nlp_qa_report_summarizes_phase_outputs(model_bundle_config_factory):
    """Happy path: Phase 5 reports input, output, and threshold coverage."""
    model_bundle_config = model_bundle_config_factory()

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(model_bundle_config.bundle_version),
        _frame_score_dataframe(model_bundle_config.bundle_version),
        _stereotype_word_counts_dataframe(),
        generated_at=datetime(2026, 5, 12, 12, 0, tzinfo=UTC),
        model_bundle_config=model_bundle_config,
    )

    assert report["report_schema_version"] == NLP_QA_REPORT_SCHEMA_VERSION
    assert report["input_coverage"]["total_mentions"] == 4
    assert report["input_coverage"]["eligible_for_inference_mentions"] == 2
    assert report["input_coverage"]["skipped_mentions_by_reason"] == {
        "empty_context": 1,
        "too_short_for_lexicon": 0,
        "too_short_for_inference": 1,
        "language_not_french": 0,
    }
    assert report["output_coverage"]["sentiment"]["scored_mentions"] == 2
    assert report["output_coverage"]["tone"]["classified_mentions"] == 1
    assert report["output_coverage"]["framing"]["mentions_with_primary_frame"] == 1
    assert report["output_coverage"]["stereotype_lexicon"]["stereotype_rows"] == 2
    assert report["threshold_sensitivity"]["tone"][0] == {
        "threshold": 0.4,
        "scoreable_mentions": 2,
        "classified_mentions_at_threshold": 2,
        "low_confidence_mentions_at_threshold": 0,
        "classified_share_of_scoreable": 1.0,
    }
    assert "frame_hypotheses" in report["hypothesis_examples"]
    assert report["blessed_bundle_comparison"]["status"] == "not_configured"
    assert report["backup_model_agreement"]["status"] == "not_available"


def test_build_nlp_qa_report_allows_empty_lexicon_and_low_confidence_frames(
    model_bundle_config_factory,
):
    """Boundary: sparse lexicons and unclassified frames still produce QA."""
    model_bundle_config = model_bundle_config_factory()
    summary_dataframe = _nlp_summary_dataframe(model_bundle_config.bundle_version)
    summary_dataframe.loc[:, "primary_frame_label"] = "unclassified"
    summary_dataframe["primary_frame_probability"] = pd.Series(
        [None] * len(summary_dataframe),
        dtype="object",
    )

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        summary_dataframe,
        _frame_score_dataframe(
            model_bundle_config.bundle_version,
            all_unclassified=True,
        ),
        pd.DataFrame(columns=FACT_STEREOTYPE_WORD_COUNTS_COLUMNS),
        model_bundle_config=model_bundle_config,
    )

    assert report["output_coverage"]["framing"]["mentions_with_primary_frame"] == 0
    assert report["output_coverage"]["stereotype_lexicon"]["stereotype_rows"] == 0


def test_build_nlp_qa_report_warns_on_zero_unfavorable_low_tone_coverage(
    model_bundle_config_factory,
):
    """Regression: structurally zero unfavorable tone should be a QA warning."""
    model_bundle_config = model_bundle_config_factory()
    summary_dataframe = _nlp_summary_dataframe(model_bundle_config.bundle_version)
    summary_dataframe.loc[
        summary_dataframe["nlp_enrichment_status"].eq("scored"),
        "target_tone_label",
    ] = "unclassified"

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        summary_dataframe,
        _frame_score_dataframe(model_bundle_config.bundle_version),
        _stereotype_word_counts_dataframe(),
        model_bundle_config=model_bundle_config,
    )

    assert any(
        "under-calibrated NLI for unfavorable polarity" in warning
        for warning in report["warnings"]
    )


def test_build_nlp_qa_report_computes_backup_agreement_when_available(
    model_bundle_config_factory,
):
    """Regression: precomputed backup summaries are compared when provided."""
    primary_model_bundle_config = model_bundle_config_factory()
    backup_model_bundle_config = model_bundle_config_factory(
        nli_model_revision="d" * 40,
    )
    backup_summary_dataframe = _nlp_summary_dataframe(
        backup_model_bundle_config.bundle_version,
    )
    backup_summary_dataframe.loc[
        backup_summary_dataframe["mention_id"].eq("mention-001"),
        "primary_frame_label",
    ] = "personnalite"
    backup_summary_dataframe.loc[
        backup_summary_dataframe["mention_id"].eq("mention-001"),
        "primary_frame_probability",
    ] = 0.72

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(primary_model_bundle_config.bundle_version),
        _frame_score_dataframe(primary_model_bundle_config.bundle_version),
        _stereotype_word_counts_dataframe(),
        backup_summary_dataframe=backup_summary_dataframe,
        model_bundle_config=primary_model_bundle_config,
    )

    assert report["backup_model_agreement"] == {
        "status": "available",
        "primary_model_bundle_version": primary_model_bundle_config.bundle_version,
        "backup_model_bundle_version": backup_model_bundle_config.bundle_version,
        "backup_summary_joined_mentions": 4,
        "backup_scored_mentions": 2,
        "tone_compared_mentions": 1,
        "tone_agreement_rate": 1.0,
        "tone_cohens_kappa": 1.0,
        "frame_compared_mentions": 1,
        "frame_agreement_rate": 0.0,
        "frame_cohens_kappa": 0.0,
    }
    assert any(
        "Backup model frame agreement is below 0.80" in warning
        for warning in report["warnings"]
    )
    assert not any(
        "Backup model tone agreement is below 0.80" in warning
        for warning in report["warnings"]
    )


def test_build_nlp_qa_report_handles_lexicon_only_run(
    model_bundle_config_factory,
):
    """Regression: lexicon-only runs keep Transformer counters at zero."""
    model_bundle_config = model_bundle_config_factory()

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        _lexicon_only_summary_dataframe(model_bundle_config.bundle_version),
        pd.DataFrame(columns=FACT_MENTION_FRAME_SCORE_COLUMNS),
        _stereotype_word_counts_dataframe(),
        model_bundle_config=model_bundle_config,
    )

    assert report["output_coverage"]["summary_status_counts"] == {
        "scored": 0,
        "skipped": 4,
        "failed": 0,
    }
    assert report["output_coverage"]["sentiment"]["scored_mentions"] == 0
    assert report["output_coverage"]["tone"]["scoreable_mentions"] == 0
    assert report["output_coverage"]["tone"]["classified_mentions"] == 0
    assert report["output_coverage"]["framing"]["frame_score_rows"] == 0
    assert report["output_coverage"]["framing"]["frame_scored_mentions"] == 0
    assert report["output_coverage"]["stereotype_lexicon"]["stereotype_rows"] == 2


def test_build_nlp_qa_report_rejects_duplicate_input_mentions(
    model_bundle_config_factory,
):
    """Error path: duplicate Phase 0 mention IDs fail before reporting."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_dataframe = _nlp_input_dataframe()
    nlp_input_dataframe.loc[1, "mention_id"] = "mention-001"

    with pytest.raises(DataQualityError, match="duplicate"):
        build_nlp_qa_report(
            nlp_input_dataframe,
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            _frame_score_dataframe(model_bundle_config.bundle_version),
            _stereotype_word_counts_dataframe(),
            model_bundle_config=model_bundle_config,
        )


def test_build_nlp_qa_report_rejects_orphan_summary_rows(
    model_bundle_config_factory,
):
    """Error path: every summary row must match the current NLP input table."""
    model_bundle_config = model_bundle_config_factory()
    nlp_summary_dataframe = _nlp_summary_dataframe(model_bundle_config.bundle_version)
    nlp_summary_dataframe.loc[0, "mention_id"] = "mention-999"

    with pytest.raises(DataQualityError, match="without matching NLP input"):
        build_nlp_qa_report(
            _nlp_input_dataframe(),
            nlp_summary_dataframe,
            _frame_score_dataframe(model_bundle_config.bundle_version),
            _stereotype_word_counts_dataframe(),
            model_bundle_config=model_bundle_config,
        )


def test_build_nlp_qa_report_rejects_invalid_frame_probabilities(
    model_bundle_config_factory,
):
    """Error path: frame probabilities must remain valid probabilities."""
    model_bundle_config = model_bundle_config_factory()
    frame_score_dataframe = _frame_score_dataframe(model_bundle_config.bundle_version)
    frame_score_dataframe.loc[0, "frame_probability"] = 1.2

    with pytest.raises(DataQualityError, match="between 0 and 1"):
        build_nlp_qa_report(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            frame_score_dataframe,
            _stereotype_word_counts_dataframe(),
            model_bundle_config=model_bundle_config,
        )


def test_build_nlp_qa_report_rejects_mixed_bundle_versions(
    model_bundle_config_factory,
):
    """Error path: primary NLP outputs must use one model bundle version."""
    model_bundle_config = model_bundle_config_factory()
    nlp_summary_dataframe = _nlp_summary_dataframe(model_bundle_config.bundle_version)
    nlp_summary_dataframe.loc[1, "nlp_model_bundle_version"] = "other-bundle"

    with pytest.raises(DataQualityError, match="single model bundle"):
        build_nlp_qa_report(
            _nlp_input_dataframe(),
            nlp_summary_dataframe,
            _frame_score_dataframe(model_bundle_config.bundle_version),
            _stereotype_word_counts_dataframe(),
            model_bundle_config=model_bundle_config,
        )


def test_build_nlp_qa_report_threshold_grid_keeps_report_schema(
    model_bundle_config_factory,
):
    """Regression: threshold changes do not change the report contract."""
    model_bundle_config = model_bundle_config_factory()

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(model_bundle_config.bundle_version),
        _frame_score_dataframe(model_bundle_config.bundle_version),
        _stereotype_word_counts_dataframe(),
        thresholds=[0.50, 0.80],
        model_bundle_config=model_bundle_config,
    )

    assert list(report) == [
        "report_name",
        "report_schema_version",
        "generated_at",
        "model_bundle",
        "source_tables",
        "input_coverage",
        "output_coverage",
        "failure_summary",
        "threshold_sensitivity",
        "hypothesis_examples",
        "backup_model_agreement",
        "blessed_bundle_comparison",
        "warnings",
    ]
    assert report["threshold_sensitivity"]["thresholds"] == [0.50, 0.80]
    assert len(report["threshold_sensitivity"]["tone"]) == 2
    assert len(report["threshold_sensitivity"]["framing"]) == 2


def test_build_nlp_qa_report_reports_omitted_backup_as_unavailable(
    model_bundle_config_factory,
):
    """Regression: Phase 5 Core QA does not imply backup-model inference."""
    model_bundle_config = model_bundle_config_factory()

    report = build_nlp_qa_report(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(model_bundle_config.bundle_version),
        _frame_score_dataframe(model_bundle_config.bundle_version),
        _stereotype_word_counts_dataframe(),
        model_bundle_config=model_bundle_config,
    )

    assert report["backup_model_agreement"]["status"] == "not_available"


def test_materialize_nlp_qa_report_writes_json(tmp_path, model_bundle_config_factory):
    """Integration: the QA report is persisted as JSON."""
    model_bundle_config = model_bundle_config_factory()
    report_path = tmp_path / "nlp_qa_report.json"

    report = materialize_nlp_qa_report(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(model_bundle_config.bundle_version),
        _frame_score_dataframe(model_bundle_config.bundle_version),
        _stereotype_word_counts_dataframe(),
        report_path=report_path,
        model_bundle_config=model_bundle_config,
    )

    with report_path.open(encoding="utf-8") as file_handle:
        persisted_report = json.load(file_handle)
    assert persisted_report["report_name"] == "nlp_qa_report"
    assert persisted_report["report_schema_version"] == report["report_schema_version"]
