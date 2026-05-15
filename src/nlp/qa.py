"""Phase 5 NLP QA report for governed model-output review.

This module reads the implemented Phase 0-4 NLP artifacts and produces one
JSON report for model governance. It deliberately does not run Transformer
inference; Phase 5 is an audit layer over existing Silver outputs.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR
from src.nlp.input_contracts import (
    FACT_MENTION_NLP_INPUT_COLUMNS,
    validate_fact_mention_nlp_input,
)
from src.nlp.lexicon import (
    FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    validate_fact_stereotype_word_counts,
)
from src.nlp.model_bundle import ModelBundleConfig, build_model_bundle_config
from src.nlp.nli import (
    FACT_MENTION_FRAME_SCORE_COLUMNS,
    validate_fact_mention_frame_score,
)
from src.nlp.sentiment import (
    FACT_MENTION_NLP_SUMMARY_COLUMNS,
    validate_fact_mention_nlp_summary,
)
from src.storage.tables import write_json_report
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

NLP_QA_REPORT_SCHEMA_VERSION = "nlp_qa_report_v1"
DEFAULT_NLP_QA_THRESHOLDS: tuple[float, ...] = (0.40, 0.50, 0.60, 0.70, 0.80)
_LOW_BACKUP_AGREEMENT_WARNING_THRESHOLD = 0.80

_CONTROLLED_SKIP_REASON_ORDER: tuple[str, ...] = (
    "empty_context",
    "too_short_for_lexicon",
    "too_short_for_inference",
    "language_not_french",
)
_CONTROLLED_STATUS_ORDER: tuple[str, ...] = ("scored", "skipped", "failed")
_TOP_LEVEL_REPORT_KEYS: tuple[str, ...] = (
    "report_name",
    "report_schema_version",
    "generated_at",
    "model_bundle",
    "source_tables",
    "input_coverage",
    "output_coverage",
    "failure_summary",
    "threshold_sensitivity",
    "backup_model_agreement",
    "warnings",
)


def validate_nlp_qa_sources(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    stereotype_word_counts_dataframe: pd.DataFrame,
    backup_summary_dataframe: pd.DataFrame | None = None,
) -> None:
    """Validate all source artifacts required by the Phase 5 QA report.

    Args:
        nlp_input_dataframe: Phase 0 ``silver.fact_mention_nlp_input`` rows.
        nlp_summary_dataframe: Phase 2/3/4
            ``silver.fact_mention_nlp_summary`` rows.
        frame_score_dataframe: Phase 4 ``silver.fact_mention_frame_score`` rows.
        stereotype_word_counts_dataframe: Phase 1
            ``silver.fact_stereotype_word_counts`` rows.
        backup_summary_dataframe: Optional precomputed backup-model summary
            rows. Phase 5 does not run backup inference.

    Raises:
        DataQualityError: If any source table violates its contract, contains
            orphan rows, or mixes primary model-bundle versions.
    """
    validate_fact_mention_nlp_input(nlp_input_dataframe)
    validate_fact_mention_nlp_summary(nlp_summary_dataframe, nlp_input_dataframe)
    validate_fact_mention_frame_score(frame_score_dataframe, nlp_input_dataframe)
    validate_fact_stereotype_word_counts(stereotype_word_counts_dataframe)
    _validate_stereotype_rows_match_input(
        stereotype_word_counts_dataframe,
        nlp_input_dataframe,
    )

    primary_bundle_version = _get_single_non_blank_value(
        nlp_summary_dataframe,
        "nlp_model_bundle_version",
        dataframe_name="fact_mention_nlp_summary",
    )
    if not frame_score_dataframe.empty:
        frame_bundle_version = _get_single_non_blank_value(
            frame_score_dataframe,
            "nlp_model_bundle_version",
            dataframe_name="fact_mention_frame_score",
        )
        if frame_bundle_version != primary_bundle_version:
            raise DataQualityError(
                "fact_mention_frame_score model bundle version does not match "
                "fact_mention_nlp_summary"
            )

    if backup_summary_dataframe is not None:
        validate_fact_mention_nlp_summary(
            backup_summary_dataframe,
            nlp_input_dataframe,
        )
        _get_single_non_blank_value(
            backup_summary_dataframe,
            "nlp_model_bundle_version",
            dataframe_name="backup_fact_mention_nlp_summary",
        )


def build_nlp_qa_report(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    stereotype_word_counts_dataframe: pd.DataFrame,
    *,
    backup_summary_dataframe: pd.DataFrame | None = None,
    thresholds: Sequence[float] = DEFAULT_NLP_QA_THRESHOLDS,
    generated_at: datetime | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
) -> dict[str, object]:
    """Build the Phase 5 NLP QA report payload.

    Args:
        nlp_input_dataframe: Phase 0 NLP input rows.
        nlp_summary_dataframe: Phase 2/3/4 NLP summary rows.
        frame_score_dataframe: Phase 4 frame-score rows.
        stereotype_word_counts_dataframe: Phase 1 lexicon-count rows.
        backup_summary_dataframe: Optional precomputed backup-model summary.
        thresholds: Probability thresholds used for tone and frame coverage
            sensitivity.
        generated_at: UTC timestamp for the report. Defaults to now.
        model_bundle_config: Optional current model-bundle config override for
            tests or controlled local runs.

    Returns:
        JSON-serializable report dictionary matching
        ``NLP_QA_REPORT_SCHEMA_VERSION``.

    Raises:
        DataQualityError: If source artifacts fail QA validation.
        ValueError: If thresholds are invalid.
    """
    normalized_thresholds = _validate_thresholds(thresholds)
    effective_generated_at = generated_at or datetime.now(UTC)
    effective_model_bundle_config = model_bundle_config or build_model_bundle_config()

    validate_nlp_qa_sources(
        nlp_input_dataframe,
        nlp_summary_dataframe,
        frame_score_dataframe,
        stereotype_word_counts_dataframe,
        backup_summary_dataframe,
    )

    observed_bundle_version = _get_single_non_blank_value(
        nlp_summary_dataframe,
        "nlp_model_bundle_version",
        dataframe_name="fact_mention_nlp_summary",
    )
    backup_model_agreement = _build_backup_model_agreement(
        nlp_summary_dataframe,
        backup_summary_dataframe,
    )
    warnings = _build_warnings(
        nlp_input_dataframe=nlp_input_dataframe,
        stereotype_word_counts_dataframe=stereotype_word_counts_dataframe,
        frame_score_dataframe=frame_score_dataframe,
        observed_bundle_version=observed_bundle_version,
        model_bundle_config=effective_model_bundle_config,
        backup_model_agreement=backup_model_agreement,
    )

    report = {
        "report_name": "nlp_qa_report",
        "report_schema_version": NLP_QA_REPORT_SCHEMA_VERSION,
        "generated_at": effective_generated_at.isoformat(),
        "model_bundle": _build_model_bundle_section(
            observed_bundle_version,
            effective_model_bundle_config,
        ),
        "source_tables": _build_source_table_section(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            frame_score_dataframe,
            stereotype_word_counts_dataframe,
            backup_summary_dataframe,
        ),
        "input_coverage": _build_input_coverage(nlp_input_dataframe),
        "output_coverage": _build_output_coverage(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            frame_score_dataframe,
            stereotype_word_counts_dataframe,
        ),
        "failure_summary": _build_failure_summary(
            nlp_input_dataframe,
            nlp_summary_dataframe,
        ),
        "threshold_sensitivity": _build_threshold_sensitivity(
            nlp_summary_dataframe,
            frame_score_dataframe,
            thresholds=normalized_thresholds,
        ),
        "backup_model_agreement": backup_model_agreement,
        "warnings": warnings,
    }
    return {key: report[key] for key in _TOP_LEVEL_REPORT_KEYS}


def materialize_nlp_qa_report(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    stereotype_word_counts_dataframe: pd.DataFrame,
    *,
    backup_summary_dataframe: pd.DataFrame | None = None,
    report_path: Path = GOLD_DIR / "nlp_qa_report.json",
    thresholds: Sequence[float] = DEFAULT_NLP_QA_THRESHOLDS,
    generated_at: datetime | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
) -> dict[str, object]:
    """Build and persist the Phase 5 NLP QA report JSON artifact.

    Args:
        nlp_input_dataframe: Phase 0 NLP input rows.
        nlp_summary_dataframe: Phase 2/3/4 NLP summary rows.
        frame_score_dataframe: Phase 4 frame-score rows.
        stereotype_word_counts_dataframe: Phase 1 lexicon-count rows.
        backup_summary_dataframe: Optional precomputed backup-model summary.
        report_path: JSON output path.
        thresholds: Probability thresholds for sensitivity summaries.
        generated_at: UTC report timestamp. Defaults to now.
        model_bundle_config: Optional current model-bundle config override.

    Returns:
        The report payload written to ``report_path``.
    """
    report = build_nlp_qa_report(
        nlp_input_dataframe,
        nlp_summary_dataframe,
        frame_score_dataframe,
        stereotype_word_counts_dataframe,
        backup_summary_dataframe=backup_summary_dataframe,
        thresholds=thresholds,
        generated_at=generated_at,
        model_bundle_config=model_bundle_config,
    )
    write_json_report(report, report_path)
    logger.info("Materialized NLP QA report report_path=%s", report_path)
    return report


def _build_model_bundle_section(
    observed_bundle_version: str,
    model_bundle_config: ModelBundleConfig,
) -> dict[str, object]:
    """Return model provenance and current configuration metadata."""
    current_metadata = model_bundle_config.to_metadata()
    return {
        "observed_nlp_model_bundle_version": observed_bundle_version,
        "current_config_nlp_model_bundle_version": model_bundle_config.bundle_version,
        "matches_current_config": observed_bundle_version
        == model_bundle_config.bundle_version,
        "current_config": current_metadata,
    }


def _build_source_table_section(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    stereotype_word_counts_dataframe: pd.DataFrame,
    backup_summary_dataframe: pd.DataFrame | None,
) -> dict[str, object]:
    """Return source row-count metadata for the QA report."""
    source_tables: dict[str, object] = {
        "silver.fact_mention_nlp_input": {
            "rows": int(len(nlp_input_dataframe)),
            "columns": list(FACT_MENTION_NLP_INPUT_COLUMNS),
        },
        "silver.fact_mention_nlp_summary": {
            "rows": int(len(nlp_summary_dataframe)),
            "columns": list(FACT_MENTION_NLP_SUMMARY_COLUMNS),
        },
        "silver.fact_mention_frame_score": {
            "rows": int(len(frame_score_dataframe)),
            "columns": list(FACT_MENTION_FRAME_SCORE_COLUMNS),
        },
        "silver.fact_stereotype_word_counts": {
            "rows": int(len(stereotype_word_counts_dataframe)),
            "columns": list(FACT_STEREOTYPE_WORD_COUNTS_COLUMNS),
        },
    }
    if backup_summary_dataframe is not None:
        source_tables["backup.fact_mention_nlp_summary"] = {
            "rows": int(len(backup_summary_dataframe)),
            "columns": list(FACT_MENTION_NLP_SUMMARY_COLUMNS),
        }
    return source_tables


def _build_input_coverage(nlp_input_dataframe: pd.DataFrame) -> dict[str, object]:
    """Return Phase 0 input eligibility counters."""
    total_mentions = int(len(nlp_input_dataframe))
    lexicon_eligible_mentions = int(
        nlp_input_dataframe["eligible_for_lexicon"].astype(bool).sum()
    )
    inference_eligible_mentions = int(
        nlp_input_dataframe["eligible_for_inference"].astype(bool).sum()
    )
    return {
        "total_mentions": total_mentions,
        "eligible_for_lexicon_mentions": lexicon_eligible_mentions,
        "eligible_for_inference_mentions": inference_eligible_mentions,
        "lexicon_eligible_share": _safe_ratio(
            lexicon_eligible_mentions,
            total_mentions,
        ),
        "inference_eligible_share": _safe_ratio(
            inference_eligible_mentions,
            total_mentions,
        ),
        "skipped_mentions": int(total_mentions - inference_eligible_mentions),
        "skipped_mentions_by_reason": _value_counts(
            nlp_input_dataframe.loc[
                ~nlp_input_dataframe["eligible_for_inference"].astype(bool),
                "skip_reason",
            ],
            _CONTROLLED_SKIP_REASON_ORDER,
        ),
    }


def _build_output_coverage(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    stereotype_word_counts_dataframe: pd.DataFrame,
) -> dict[str, object]:
    """Return output coverage counters across Phase 1-4 artifacts."""
    total_mentions = int(len(nlp_input_dataframe))
    scored_summary = nlp_summary_dataframe["nlp_enrichment_status"].eq("scored")
    sentiment_scored_mentions = int(
        (
            scored_summary
            & nlp_summary_dataframe["generic_sentiment_label"].notna()
            & nlp_summary_dataframe["generic_sentiment_score"].notna()
        ).sum()
    )
    tone_scoreable_mentions = int(
        (
            scored_summary & nlp_summary_dataframe["target_tone_probability"].notna()
        ).sum()
    )
    tone_classified_mentions = int(
        (
            scored_summary
            & nlp_summary_dataframe["target_tone_probability"].notna()
            & nlp_summary_dataframe["target_tone_label"].ne("unclassified")
        ).sum()
    )
    primary_frame_mentions = int(
        (
            scored_summary
            & nlp_summary_dataframe["primary_frame_label"].ne("unclassified")
        ).sum()
    )
    frame_scored_mentions = int(frame_score_dataframe["mention_id"].nunique())
    frame_mentions_passing_threshold = int(
        frame_score_dataframe.loc[
            frame_score_dataframe["passes_threshold"].astype(bool),
            "mention_id",
        ].nunique()
    )
    lexicon_eligible_mentions = int(
        nlp_input_dataframe["eligible_for_lexicon"].astype(bool).sum()
    )
    stereotype_mentions = int(stereotype_word_counts_dataframe["mention_id"].nunique())
    total_stereotype_count = (
        int(stereotype_word_counts_dataframe["count"].sum())
        if not stereotype_word_counts_dataframe.empty
        else 0
    )
    return {
        "summary_status_counts": _value_counts(
            nlp_summary_dataframe["nlp_enrichment_status"],
            _CONTROLLED_STATUS_ORDER,
        ),
        "sentiment": {
            "scored_mentions": sentiment_scored_mentions,
            "coverage_share_of_total": _safe_ratio(
                sentiment_scored_mentions,
                total_mentions,
            ),
        },
        "tone": {
            "scoreable_mentions": tone_scoreable_mentions,
            "classified_mentions": tone_classified_mentions,
            "scoreable_share_of_total": _safe_ratio(
                tone_scoreable_mentions,
                total_mentions,
            ),
            "classified_share_of_scoreable": _safe_ratio(
                tone_classified_mentions,
                tone_scoreable_mentions,
            ),
        },
        "framing": {
            "frame_score_rows": int(len(frame_score_dataframe)),
            "frame_scored_mentions": frame_scored_mentions,
            "mentions_with_any_frame_passing_threshold": frame_mentions_passing_threshold,
            "mentions_with_primary_frame": primary_frame_mentions,
            "frame_scored_share_of_total": _safe_ratio(
                frame_scored_mentions,
                total_mentions,
            ),
            "primary_frame_share_of_frame_scored": _safe_ratio(
                primary_frame_mentions,
                frame_scored_mentions,
            ),
        },
        "stereotype_lexicon": {
            "stereotype_rows": int(len(stereotype_word_counts_dataframe)),
            "mentions_with_stereotype_terms": stereotype_mentions,
            "total_stereotype_term_count": total_stereotype_count,
            "mention_share_of_lexicon_eligible": _safe_ratio(
                stereotype_mentions,
                lexicon_eligible_mentions,
            ),
        },
    }


def _build_failure_summary(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
) -> dict[str, object]:
    """Return skipped and failed row summaries."""
    failed_rows = nlp_summary_dataframe.loc[
        nlp_summary_dataframe["nlp_enrichment_status"].eq("failed")
    ]
    return {
        "skipped_mentions_by_reason": _value_counts(
            nlp_input_dataframe.loc[
                ~nlp_input_dataframe["eligible_for_inference"].astype(bool),
                "skip_reason",
            ],
            _CONTROLLED_SKIP_REASON_ORDER,
        ),
        "failed_mentions": int(len(failed_rows)),
        "failed_mentions_by_error_type": _value_counts(
            failed_rows["error_type"],
            tuple(sorted(str(value) for value in failed_rows["error_type"].dropna())),
        ),
    }


def _build_threshold_sensitivity(
    nlp_summary_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    *,
    thresholds: tuple[float, ...],
) -> dict[str, object]:
    """Return tone and frame coverage sensitivity summaries."""
    return {
        "thresholds": list(thresholds),
        "tone": _build_tone_threshold_sensitivity(
            nlp_summary_dataframe,
            thresholds,
        ),
        "framing": _build_frame_threshold_sensitivity(
            frame_score_dataframe,
            thresholds,
        ),
    }


def _build_tone_threshold_sensitivity(
    nlp_summary_dataframe: pd.DataFrame,
    thresholds: tuple[float, ...],
) -> list[dict[str, object]]:
    """Return scoreable tone coverage across thresholds."""
    scoreable_rows = nlp_summary_dataframe.loc[
        nlp_summary_dataframe["target_tone_probability"].notna()
    ].copy()
    scoreable_count = int(len(scoreable_rows))
    sensitivity_rows: list[dict[str, object]] = []
    for threshold in thresholds:
        classified_count = int(
            scoreable_rows["target_tone_probability"].ge(threshold).sum()
        )
        sensitivity_rows.append(
            {
                "threshold": float(threshold),
                "scoreable_mentions": scoreable_count,
                "classified_mentions_at_threshold": classified_count,
                "low_confidence_mentions_at_threshold": int(
                    scoreable_count - classified_count
                ),
                "classified_share_of_scoreable": _safe_ratio(
                    classified_count,
                    scoreable_count,
                ),
            }
        )
    return sensitivity_rows


def _build_frame_threshold_sensitivity(
    frame_score_dataframe: pd.DataFrame,
    thresholds: tuple[float, ...],
) -> list[dict[str, object]]:
    """Return frame coverage based on each mention's maximum frame score."""
    if frame_score_dataframe.empty:
        max_frame_probability = pd.Series(dtype="float64")
    else:
        max_frame_probability = frame_score_dataframe.groupby("mention_id")[
            "frame_probability"
        ].max()

    scoreable_count = int(len(max_frame_probability))
    sensitivity_rows: list[dict[str, object]] = []
    for threshold in thresholds:
        classified_count = int(max_frame_probability.ge(threshold).sum())
        sensitivity_rows.append(
            {
                "threshold": float(threshold),
                "scoreable_mentions": scoreable_count,
                "classified_mentions_at_threshold": classified_count,
                "low_confidence_mentions_at_threshold": int(
                    scoreable_count - classified_count
                ),
                "classified_share_of_scoreable": _safe_ratio(
                    classified_count,
                    scoreable_count,
                ),
            }
        )
    return sensitivity_rows


def _build_backup_model_agreement(
    nlp_summary_dataframe: pd.DataFrame,
    backup_summary_dataframe: pd.DataFrame | None,
) -> dict[str, object]:
    """Return optional precomputed backup-model agreement metrics."""
    if backup_summary_dataframe is None:
        return {
            "status": "not_available",
            "reason": (
                "No precomputed backup summary was provided; Phase 5 Core QA "
                "does not run backup model inference."
            ),
        }

    comparison_dataframe = nlp_summary_dataframe.merge(
        backup_summary_dataframe[
            [
                "mention_id",
                "target_tone_label",
                "primary_frame_label",
                "nlp_enrichment_status",
                "nlp_model_bundle_version",
            ]
        ],
        on="mention_id",
        how="inner",
        suffixes=("_primary", "_backup"),
        validate="one_to_one",
    )
    tone_comparison = comparison_dataframe.loc[
        comparison_dataframe["nlp_enrichment_status_primary"].eq("scored")
        & comparison_dataframe["nlp_enrichment_status_backup"].eq("scored")
        & comparison_dataframe["target_tone_label_primary"].ne("unclassified")
        & comparison_dataframe["target_tone_label_backup"].ne("unclassified")
    ]
    frame_comparison = comparison_dataframe.loc[
        comparison_dataframe["nlp_enrichment_status_primary"].eq("scored")
        & comparison_dataframe["nlp_enrichment_status_backup"].eq("scored")
        & comparison_dataframe["primary_frame_label_primary"].ne("unclassified")
        & comparison_dataframe["primary_frame_label_backup"].ne("unclassified")
    ]
    tone_matches = int(
        (
            tone_comparison["target_tone_label_primary"]
            == tone_comparison["target_tone_label_backup"]
        ).sum()
    )
    frame_matches = int(
        (
            frame_comparison["primary_frame_label_primary"]
            == frame_comparison["primary_frame_label_backup"]
        ).sum()
    )
    return {
        "status": "available",
        "primary_model_bundle_version": _get_single_non_blank_value(
            nlp_summary_dataframe,
            "nlp_model_bundle_version",
            dataframe_name="fact_mention_nlp_summary",
        ),
        "backup_model_bundle_version": _get_single_non_blank_value(
            backup_summary_dataframe,
            "nlp_model_bundle_version",
            dataframe_name="backup_fact_mention_nlp_summary",
        ),
        "common_mentions": int(len(comparison_dataframe)),
        "tone_compared_mentions": int(len(tone_comparison)),
        "tone_agreement_rate": _safe_ratio(tone_matches, int(len(tone_comparison))),
        "frame_compared_mentions": int(len(frame_comparison)),
        "frame_agreement_rate": _safe_ratio(frame_matches, int(len(frame_comparison))),
    }


def _build_warnings(
    *,
    nlp_input_dataframe: pd.DataFrame,
    stereotype_word_counts_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
    observed_bundle_version: str,
    model_bundle_config: ModelBundleConfig,
    backup_model_agreement: dict[str, object],
) -> list[str]:
    """Return governance caveats and non-fatal completeness warnings."""
    warnings = [
        (
            "Generic sentiment is a baseline diagnostic and must not be "
            "interpreted as candidate-aware political tone."
        ),
        (
            "Phase 5 QA reports descriptive model-governance signals only; it "
            "does not establish causal gender-bias claims."
        ),
    ]
    if observed_bundle_version != model_bundle_config.bundle_version:
        warnings.append(
            "Observed NLP output bundle differs from the current local model "
            "configuration; compare results only with explicit provenance."
        )
    if (
        int(nlp_input_dataframe["eligible_for_lexicon"].astype(bool).sum()) > 0
        and stereotype_word_counts_dataframe.empty
    ):
        warnings.append(
            "No stereotype lexicon rows were emitted despite lexicon-eligible "
            "mentions; this may be valid for a sparse seed lexicon."
        )
    if (
        int(nlp_input_dataframe["eligible_for_inference"].astype(bool).sum()) > 0
        and frame_score_dataframe.empty
    ):
        warnings.append(
            "No frame-score rows are present despite inference-eligible "
            "mentions; Phase 4 framing may not have run."
        )
    if backup_model_agreement.get("status") == "not_available":
        warnings.append(
            "Backup model agreement is unavailable because no precomputed "
            "backup summary was provided."
        )
    if backup_model_agreement.get("status") == "available":
        tone_agreement_rate = backup_model_agreement.get("tone_agreement_rate")
        if (
            isinstance(tone_agreement_rate, int | float)
            and tone_agreement_rate < _LOW_BACKUP_AGREEMENT_WARNING_THRESHOLD
        ):
            warnings.append(
                "Backup model tone agreement is below 0.80; treat "
                "target-aware tone outputs as requiring manual review before "
                "analytical promotion."
            )
        frame_agreement_rate = backup_model_agreement.get("frame_agreement_rate")
        if (
            isinstance(frame_agreement_rate, int | float)
            and frame_agreement_rate < _LOW_BACKUP_AGREEMENT_WARNING_THRESHOLD
        ):
            warnings.append(
                "Backup model frame agreement is below 0.80; treat framing "
                "outputs as requiring manual review before analytical "
                "promotion."
            )
    return warnings


def _validate_stereotype_rows_match_input(
    stereotype_word_counts_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame,
) -> None:
    """Raise when stereotype-count rows reference unknown mentions."""
    input_mentions = set(nlp_input_dataframe["mention_id"].astype(str))
    stereotype_mentions = set(
        stereotype_word_counts_dataframe["mention_id"].astype(str)
    )
    orphan_mentions = stereotype_mentions - input_mentions
    if orphan_mentions:
        examples = sorted(orphan_mentions)[:5]
        raise DataQualityError(
            "fact_stereotype_word_counts has rows without matching NLP input: "
            f"{examples}"
        )


def _get_single_non_blank_value(
    dataframe: pd.DataFrame,
    column_name: str,
    *,
    dataframe_name: str,
) -> str:
    """Return the single non-blank value for one lineage column."""
    values = sorted(
        {
            str(value).strip()
            for value in dataframe[column_name].dropna()
            if str(value).strip()
        }
    )
    if not values:
        raise DataQualityError(f"{dataframe_name} has no {column_name} values")
    if len(values) > 1:
        raise DataQualityError(
            f"{dataframe_name} must contain a single model bundle version: {values}"
        )
    return values[0]


def _value_counts(values: pd.Series, ordered_values: tuple[str, ...]) -> dict[str, int]:
    """Return stable string-key counts for controlled or observed values."""
    normalized_values = values.dropna().astype(str).str.strip()
    count_map = normalized_values.value_counts().to_dict()
    output = {value: int(count_map.get(value, 0)) for value in ordered_values}
    for value in sorted(set(count_map) - set(output)):
        output[value] = int(count_map[value])
    return output


def _validate_thresholds(thresholds: Sequence[float]) -> tuple[float, ...]:
    """Normalize and validate probability thresholds."""
    if not thresholds:
        raise ValueError("thresholds must not be empty")
    normalized_thresholds: list[float] = []
    for raw_threshold in thresholds:
        try:
            threshold = float(raw_threshold)
        except (TypeError, ValueError) as exc:
            raise ValueError("thresholds must be numeric") from exc
        if not math.isfinite(threshold):
            raise ValueError("thresholds must be finite")
        if not 0 <= threshold <= 1:
            raise ValueError("thresholds must be between 0 and 1")
        normalized_thresholds.append(threshold)
    unique_thresholds = tuple(sorted(set(normalized_thresholds)))
    if len(unique_thresholds) != len(normalized_thresholds):
        raise ValueError("thresholds must be unique")
    return unique_thresholds


def _safe_ratio(numerator: int, denominator: int) -> float | None:
    """Return a ratio, preserving undefined zero-denominator cases as null."""
    if denominator == 0:
        return None
    return float(numerator / denominator)
