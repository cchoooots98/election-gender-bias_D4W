"""Threshold sensitivity analysis for Phase 3 target-aware tone.

This module audits how the candidate-aware tone coverage changes when the
classification probability threshold is varied. It intentionally reports
coverage sensitivity rather than alternate tone-label distributions because
the Silver summary table stores only the persisted label and the top
probability, not the full raw NLI probability vector.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.settings import GOLD_DIR, WAREHOUSE_PATH
from src.nlp._validation import (
    require_columns,
    validate_unique_key,
)
from src.nlp.model_bundle import build_model_bundle_config
from src.nlp.nli import CONTROLLED_TARGET_TONE_LABELS
from src.nlp.normalization import is_null_or_blank
from src.storage.tables import (
    write_duckdb_table,
    write_json_report,
    write_parquet_table,
)
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

DEFAULT_TONE_SENSITIVITY_THRESHOLDS: tuple[float, ...] = (
    0.40,
    0.45,
    0.50,
    0.55,
    0.60,
    0.65,
    0.70,
    0.75,
    0.80,
)

TONE_SENSITIVITY_TABLE_COLUMNS: tuple[str, ...] = (
    "generated_at",
    "nlp_model_bundle_version",
    "threshold",
    "segment_type",
    "segment_value",
    "total_mentions",
    "scoreable_mentions",
    "not_scoreable_mentions",
    "classified_mentions_at_threshold",
    "low_confidence_mentions_at_threshold",
    "classified_share_of_scoreable",
)

_REQUIRED_NLP_SUMMARY_COLUMNS = frozenset(
    {
        "mention_id",
        "leader_id",
        "target_tone_label",
        "target_tone_probability",
        "nlp_enrichment_status",
        "nlp_model_bundle_version",
    }
)
_REQUIRED_SAMPLE_LEADER_COLUMNS = frozenset({"leader_id", "gender"})
_CONTROLLED_GENDERS = frozenset({"F", "M"})
_PROBABILITY_BIN_ORDER = {
    "not_scoreable": 0,
    "<0.40": 1,
    "0.40-0.50": 2,
    "0.50-0.60": 3,
    "0.60-0.70": 4,
    "0.70-0.80": 5,
    ">=0.80": 6,
}


@dataclass(frozen=True)
class ToneSensitivityAnalysis:
    """Artifacts produced by one tone threshold sensitivity analysis.

    Args:
        sensitivity_table: Long-form threshold coverage table.
        report: JSON-serializable QA report payload.
    """

    sensitivity_table: pd.DataFrame
    report: dict[str, object]


def build_tone_sensitivity_analysis(
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    *,
    thresholds: Sequence[float] = DEFAULT_TONE_SENSITIVITY_THRESHOLDS,
    generated_at: datetime | None = None,
    configured_tone_threshold: float | None = None,
) -> ToneSensitivityAnalysis:
    """Build the Phase 3 tone threshold sensitivity analysis.

    Args:
        nlp_summary_dataframe: Existing ``silver.fact_mention_nlp_summary`` rows.
        sample_leaders_dataframe: ``gold.sample_leaders`` rows containing
            leader gender metadata.
        thresholds: Probability thresholds to audit.
        generated_at: UTC timestamp attached to the output artifacts. Defaults
            to the current UTC timestamp.
        configured_tone_threshold: Current production tone threshold, if known.

    Returns:
        A ``ToneSensitivityAnalysis`` with a long-form table and JSON report.

    Raises:
        DataQualityError: If source rows violate the report input contract.
        ValueError: If the threshold grid is empty, duplicated, non-finite, or
            outside ``[0, 1]``.
    """
    normalized_thresholds = _validate_thresholds(thresholds)
    effective_generated_at = generated_at or datetime.now(UTC)
    analysis_dataframe = _build_analysis_dataframe(
        nlp_summary_dataframe,
        sample_leaders_dataframe,
    )
    bundle_version = _get_single_bundle_version(analysis_dataframe)
    sensitivity_table = _build_sensitivity_table(
        analysis_dataframe=analysis_dataframe,
        thresholds=normalized_thresholds,
        generated_at=effective_generated_at,
        bundle_version=bundle_version,
    )
    report = _build_report_payload(
        analysis_dataframe=analysis_dataframe,
        sensitivity_table=sensitivity_table,
        thresholds=normalized_thresholds,
        generated_at=effective_generated_at,
        bundle_version=bundle_version,
        configured_tone_threshold=configured_tone_threshold,
    )
    return ToneSensitivityAnalysis(
        sensitivity_table=sensitivity_table,
        report=report,
    )


def materialize_tone_sensitivity_analysis(
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    *,
    thresholds: Sequence[float] = DEFAULT_TONE_SENSITIVITY_THRESHOLDS,
    report_path: Path = GOLD_DIR / "nlp_tone_sensitivity_report.json",
    parquet_path: Path = GOLD_DIR / "nlp_tone_threshold_sensitivity.parquet",
    duckdb_path: Path = WAREHOUSE_PATH,
    configured_tone_threshold: float | None = None,
) -> ToneSensitivityAnalysis:
    """Build and persist the Phase 3 tone sensitivity QA artifacts.

    Args:
        nlp_summary_dataframe: Existing ``silver.fact_mention_nlp_summary`` rows.
        sample_leaders_dataframe: ``gold.sample_leaders`` rows.
        thresholds: Probability thresholds to audit.
        report_path: Output path for the JSON QA report.
        parquet_path: Output path for the long-form Parquet table.
        duckdb_path: DuckDB warehouse path for the queryable QA table.
        configured_tone_threshold: Current production tone threshold. Defaults
            to the value resolved from project settings.

    Returns:
        The materialized analysis artifacts.

    Raises:
        DataQualityError: If source rows violate the report input contract.
        ValueError: If the threshold grid is invalid.
        RuntimeError: If DuckDB is unavailable while persisting the table.
    """
    effective_configured_threshold = configured_tone_threshold
    if effective_configured_threshold is None:
        effective_configured_threshold = build_model_bundle_config().tone_threshold

    analysis = build_tone_sensitivity_analysis(
        nlp_summary_dataframe,
        sample_leaders_dataframe,
        thresholds=thresholds,
        configured_tone_threshold=effective_configured_threshold,
    )
    write_json_report(analysis.report, report_path)
    write_parquet_table(analysis.sensitivity_table, parquet_path)
    write_duckdb_table(
        dataframe=analysis.sensitivity_table,
        schema_name="gold",
        table_name="nlp_tone_threshold_sensitivity",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized tone sensitivity analysis report_path=%s parquet_path=%s "
        "duckdb_path=%s rows=%d",
        report_path,
        parquet_path,
        duckdb_path,
        len(analysis.sensitivity_table),
    )
    return analysis


def _build_analysis_dataframe(
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Validate and join summary rows to gender metadata."""
    _validate_source_columns(nlp_summary_dataframe, sample_leaders_dataframe)

    summary_dataframe = nlp_summary_dataframe.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "target_tone_label",
            "target_tone_probability",
            "nlp_enrichment_status",
            "nlp_model_bundle_version",
        ],
    ].copy()
    sample_dataframe = sample_leaders_dataframe.loc[:, ["leader_id", "gender"]].copy()

    for column_name in ("mention_id", "leader_id", "nlp_model_bundle_version"):
        blank_mask = summary_dataframe[column_name].map(is_null_or_blank)
        if blank_mask.any():
            raise DataQualityError(
                f"fact_mention_nlp_summary has blank {column_name} values"
            )
        summary_dataframe[column_name] = summary_dataframe[column_name].map(
            lambda value: str(value).strip()
        )

    for column_name in ("leader_id", "gender"):
        blank_mask = sample_dataframe[column_name].map(is_null_or_blank)
        if blank_mask.any():
            raise DataQualityError(f"sample_leaders has blank {column_name} values")
        sample_dataframe[column_name] = sample_dataframe[column_name].map(
            lambda value: str(value).strip()
        )

    validate_unique_key(
        dataframe=summary_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention_nlp_summary",
    )
    validate_unique_key(
        dataframe=sample_dataframe,
        key_columns=("leader_id",),
        dataframe_name="sample_leaders",
    )
    summary_dataframe = _normalize_tone_labels(summary_dataframe)
    _validate_tone_labels(summary_dataframe)
    _validate_gender_values(sample_dataframe)
    _coerce_and_validate_probability(summary_dataframe)
    _validate_probability_status_contract(summary_dataframe)

    analysis_dataframe = summary_dataframe.merge(
        sample_dataframe,
        on="leader_id",
        how="left",
        validate="many_to_one",
    )
    if analysis_dataframe["gender"].map(is_null_or_blank).any():
        missing_leader_ids = (
            analysis_dataframe.loc[
                analysis_dataframe["gender"].map(is_null_or_blank),
                "leader_id",
            ]
            .drop_duplicates()
            .head(5)
            .tolist()
        )
        raise DataQualityError(
            "fact_mention_nlp_summary has leaders missing from sample_leaders: "
            f"{missing_leader_ids}"
        )

    analysis_dataframe["is_scoreable_for_tone"] = analysis_dataframe[
        "target_tone_probability"
    ].notna()
    return analysis_dataframe


def _build_sensitivity_table(
    *,
    analysis_dataframe: pd.DataFrame,
    thresholds: tuple[float, ...],
    generated_at: datetime,
    bundle_version: str,
) -> pd.DataFrame:
    """Return long-form threshold coverage rows."""
    segment_rows: list[dict[str, object]] = []
    for threshold in thresholds:
        segment_rows.append(
            _build_segment_row(
                segment_dataframe=analysis_dataframe,
                threshold=threshold,
                segment_type="overall",
                segment_value="all",
                generated_at=generated_at,
                bundle_version=bundle_version,
            )
        )
        for gender in sorted(analysis_dataframe["gender"].drop_duplicates()):
            gender_dataframe = analysis_dataframe.loc[
                analysis_dataframe["gender"].eq(gender)
            ]
            segment_rows.append(
                _build_segment_row(
                    segment_dataframe=gender_dataframe,
                    threshold=threshold,
                    segment_type="gender",
                    segment_value=str(gender),
                    generated_at=generated_at,
                    bundle_version=bundle_version,
                )
            )
    return pd.DataFrame(segment_rows, columns=list(TONE_SENSITIVITY_TABLE_COLUMNS))


def _build_segment_row(
    *,
    segment_dataframe: pd.DataFrame,
    threshold: float,
    segment_type: str,
    segment_value: str,
    generated_at: datetime,
    bundle_version: str,
) -> dict[str, object]:
    """Build one threshold coverage row for an overall or gender segment."""
    scoreable_mask = segment_dataframe["is_scoreable_for_tone"].astype(bool)
    scoreable_dataframe = segment_dataframe.loc[scoreable_mask]
    classified_mentions = int(
        scoreable_dataframe["target_tone_probability"].ge(threshold).sum()
    )
    scoreable_mentions = int(scoreable_mask.sum())
    low_confidence_mentions = scoreable_mentions - classified_mentions
    classified_share = (
        classified_mentions / scoreable_mentions if scoreable_mentions else None
    )
    return {
        "generated_at": pd.Timestamp(generated_at),
        "nlp_model_bundle_version": bundle_version,
        "threshold": float(threshold),
        "segment_type": segment_type,
        "segment_value": segment_value,
        "total_mentions": int(len(segment_dataframe)),
        "scoreable_mentions": scoreable_mentions,
        "not_scoreable_mentions": int(len(segment_dataframe) - scoreable_mentions),
        "classified_mentions_at_threshold": classified_mentions,
        "low_confidence_mentions_at_threshold": low_confidence_mentions,
        "classified_share_of_scoreable": classified_share,
    }


def _build_report_payload(
    *,
    analysis_dataframe: pd.DataFrame,
    sensitivity_table: pd.DataFrame,
    thresholds: tuple[float, ...],
    generated_at: datetime,
    bundle_version: str,
    configured_tone_threshold: float | None,
) -> dict[str, object]:
    """Build a JSON-serializable QA report."""
    report: dict[str, object] = {
        "report_name": "nlp_tone_threshold_sensitivity",
        "generated_at": generated_at.isoformat(),
        "nlp_model_bundle_version": bundle_version,
        "configured_tone_threshold": configured_tone_threshold,
        "thresholds": list(thresholds),
        "analysis_scope": {
            "source_table": "silver.fact_mention_nlp_summary",
            "comparison_unit": "mention",
            "metric_type": "coverage sensitivity",
            "limitation": (
                "The Silver summary stores the persisted tone label and top "
                "probability only. Low-confidence raw top labels and full NLI "
                "probability vectors are not persisted, so this report does "
                "not reconstruct alternate label distributions."
            ),
        },
        "current_summary": _build_current_summary(analysis_dataframe),
        "threshold_sensitivity": _json_records(sensitivity_table),
        "gender_gap": _build_gender_gap_records(sensitivity_table),
        "observed_current_label_distribution": _build_label_distribution_records(
            analysis_dataframe
        ),
        "probability_bins_by_gender": _build_probability_bin_records(
            analysis_dataframe
        ),
    }
    return report


def _build_current_summary(analysis_dataframe: pd.DataFrame) -> dict[str, object]:
    """Return current persisted label coverage metrics."""
    scoreable_mentions = int(analysis_dataframe["is_scoreable_for_tone"].sum())
    persisted_classified_mentions = int(
        analysis_dataframe["target_tone_label"].ne("unclassified").sum()
    )
    return {
        "total_mentions": int(len(analysis_dataframe)),
        "scoreable_mentions": scoreable_mentions,
        "not_scoreable_mentions": int(len(analysis_dataframe) - scoreable_mentions),
        "persisted_classified_mentions": persisted_classified_mentions,
        "persisted_unclassified_mentions": int(
            analysis_dataframe["target_tone_label"].eq("unclassified").sum()
        ),
        "persisted_classified_share_of_scoreable": _safe_ratio(
            persisted_classified_mentions,
            scoreable_mentions,
        ),
    }


def _build_gender_gap_records(
    sensitivity_table: pd.DataFrame,
) -> list[dict[str, object]]:
    """Return female-minus-male coverage gap rows for every threshold."""
    gender_table = sensitivity_table.loc[sensitivity_table["segment_type"].eq("gender")]
    gap_rows: list[dict[str, object]] = []
    for threshold in sorted(gender_table["threshold"].drop_duplicates()):
        threshold_table = gender_table.loc[gender_table["threshold"].eq(threshold)]
        share_by_gender = {
            str(row["segment_value"]): _json_scalar(
                row["classified_share_of_scoreable"]
            )
            for row in threshold_table.to_dict("records")
        }
        female_share = share_by_gender.get("F")
        male_share = share_by_gender.get("M")
        gap = None
        if female_share is not None and male_share is not None:
            gap = float(female_share) - float(male_share)
        gap_rows.append(
            {
                "threshold": float(threshold),
                "female_classified_share_of_scoreable": female_share,
                "male_classified_share_of_scoreable": male_share,
                "female_minus_male_classified_share": gap,
            }
        )
    return gap_rows


def _build_label_distribution_records(
    analysis_dataframe: pd.DataFrame,
) -> list[dict[str, object]]:
    """Return observed persisted label counts by overall and gender segments."""
    records: list[dict[str, object]] = []
    records.extend(
        _segment_label_distribution(
            analysis_dataframe,
            segment_type="overall",
            segment_value="all",
        )
    )
    for gender in sorted(analysis_dataframe["gender"].drop_duplicates()):
        records.extend(
            _segment_label_distribution(
                analysis_dataframe.loc[analysis_dataframe["gender"].eq(gender)],
                segment_type="gender",
                segment_value=str(gender),
            )
        )
    return records


def _segment_label_distribution(
    segment_dataframe: pd.DataFrame,
    *,
    segment_type: str,
    segment_value: str,
) -> list[dict[str, object]]:
    """Return label counts for one segment."""
    total_mentions = len(segment_dataframe)
    rows: list[dict[str, object]] = []
    for label in CONTROLLED_TARGET_TONE_LABELS:
        mention_count = int(segment_dataframe["target_tone_label"].eq(label).sum())
        rows.append(
            {
                "segment_type": segment_type,
                "segment_value": segment_value,
                "target_tone_label": label,
                "mentions": mention_count,
                "share_of_segment_mentions": _safe_ratio(
                    mention_count,
                    total_mentions,
                ),
            }
        )
    return rows


def _build_probability_bin_records(
    analysis_dataframe: pd.DataFrame,
) -> list[dict[str, object]]:
    """Return top-probability bin counts for overall and gender segments."""
    binned_dataframe = analysis_dataframe.copy()
    binned_dataframe["probability_bin"] = binned_dataframe[
        "target_tone_probability"
    ].map(_probability_bin_label)

    records: list[dict[str, object]] = []
    records.extend(
        _segment_probability_bins(
            binned_dataframe,
            segment_type="overall",
            segment_value="all",
        )
    )
    for gender in sorted(binned_dataframe["gender"].drop_duplicates()):
        records.extend(
            _segment_probability_bins(
                binned_dataframe.loc[binned_dataframe["gender"].eq(gender)],
                segment_type="gender",
                segment_value=str(gender),
            )
        )
    return sorted(
        records,
        key=lambda row: (
            str(row["segment_type"]),
            str(row["segment_value"]),
            _PROBABILITY_BIN_ORDER[str(row["probability_bin"])],
        ),
    )


def _segment_probability_bins(
    segment_dataframe: pd.DataFrame,
    *,
    segment_type: str,
    segment_value: str,
) -> list[dict[str, object]]:
    """Return probability bin counts for one segment."""
    total_mentions = len(segment_dataframe)
    rows: list[dict[str, object]] = []
    for probability_bin in _PROBABILITY_BIN_ORDER:
        mention_count = int(
            segment_dataframe["probability_bin"].eq(probability_bin).sum()
        )
        rows.append(
            {
                "segment_type": segment_type,
                "segment_value": segment_value,
                "probability_bin": probability_bin,
                "mentions": mention_count,
                "share_of_segment_mentions": _safe_ratio(
                    mention_count,
                    total_mentions,
                ),
            }
        )
    return rows


def _probability_bin_label(probability: object) -> str:
    """Return the fixed audit bin for a top tone probability."""
    if pd.isna(probability):
        return "not_scoreable"
    probability_float = float(probability)
    if probability_float < 0.40:
        return "<0.40"
    if probability_float < 0.50:
        return "0.40-0.50"
    if probability_float < 0.60:
        return "0.50-0.60"
    if probability_float < 0.70:
        return "0.60-0.70"
    if probability_float < 0.80:
        return "0.70-0.80"
    return ">=0.80"


def _validate_source_columns(
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
) -> None:
    """Validate required source columns."""
    require_columns(
        dataframe=nlp_summary_dataframe,
        required_columns=_REQUIRED_NLP_SUMMARY_COLUMNS,
        dataframe_name="fact_mention_nlp_summary",
    )
    require_columns(
        dataframe=sample_leaders_dataframe,
        required_columns=_REQUIRED_SAMPLE_LEADER_COLUMNS,
        dataframe_name="sample_leaders",
    )


def _normalize_tone_labels(summary_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with non-blank tone labels stripped."""
    normalized_dataframe = summary_dataframe.copy()
    normalized_dataframe["target_tone_label"] = normalized_dataframe[
        "target_tone_label"
    ].map(lambda value: value if is_null_or_blank(value) else str(value).strip())
    return normalized_dataframe


def _validate_tone_labels(summary_dataframe: pd.DataFrame) -> None:
    """Validate persisted tone labels against the controlled vocabulary."""
    blank_mask = summary_dataframe["target_tone_label"].map(is_null_or_blank)
    if blank_mask.any():
        raise DataQualityError("fact_mention_nlp_summary has blank tone labels")
    labels = summary_dataframe["target_tone_label"].map(lambda value: str(value))
    unsupported_labels = sorted(set(labels) - set(CONTROLLED_TARGET_TONE_LABELS))
    if unsupported_labels:
        raise DataQualityError(
            "fact_mention_nlp_summary unsupported tone labels: " f"{unsupported_labels}"
        )


def _validate_gender_values(sample_dataframe: pd.DataFrame) -> None:
    """Validate sampled leader genders used for segment comparison."""
    unsupported_genders = sorted(set(sample_dataframe["gender"]) - _CONTROLLED_GENDERS)
    if unsupported_genders:
        raise DataQualityError(
            f"sample_leaders unsupported gender values: {unsupported_genders}"
        )


def _coerce_and_validate_probability(summary_dataframe: pd.DataFrame) -> None:
    """Convert tone probabilities to numeric values and validate bounds."""
    non_null_probability = summary_dataframe["target_tone_probability"].notna()
    numeric_probability = pd.to_numeric(
        summary_dataframe["target_tone_probability"],
        errors="coerce",
    )
    if numeric_probability.loc[non_null_probability].isna().any():
        raise DataQualityError(
            "fact_mention_nlp_summary target_tone_probability must be numeric"
        )
    if (
        numeric_probability.loc[non_null_probability].lt(0).any()
        or numeric_probability.loc[non_null_probability].gt(1).any()
    ):
        raise DataQualityError(
            "fact_mention_nlp_summary target_tone_probability must be between 0 and 1"
        )
    summary_dataframe["target_tone_probability"] = numeric_probability


def _validate_probability_status_contract(summary_dataframe: pd.DataFrame) -> None:
    """Ensure probability presence is compatible with enrichment status."""
    probability_on_non_scored = summary_dataframe.loc[
        summary_dataframe["nlp_enrichment_status"].ne("scored"),
        "target_tone_probability",
    ].notna()
    if probability_on_non_scored.any():
        raise DataQualityError(
            "fact_mention_nlp_summary non-scored rows cannot have tone probability"
        )


def _get_single_bundle_version(analysis_dataframe: pd.DataFrame) -> str:
    """Return the single bundle version represented by the report source."""
    bundle_versions = (
        analysis_dataframe["nlp_model_bundle_version"]
        .astype(str)
        .str.strip()
        .drop_duplicates()
        .tolist()
    )
    if len(bundle_versions) != 1:
        raise DataQualityError(
            "tone sensitivity analysis requires exactly one model bundle version; "
            f"found={bundle_versions[:5]}"
        )
    return bundle_versions[0]


def _validate_thresholds(thresholds: Sequence[float]) -> tuple[float, ...]:
    """Normalize and validate the threshold grid."""
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
    """Return a ratio or ``None`` when the denominator is zero."""
    if denominator == 0:
        return None
    return numerator / denominator


def _json_records(dataframe: pd.DataFrame) -> list[dict[str, object]]:
    """Convert a DataFrame to JSON-safe records."""
    return [
        {key: _json_scalar(value) for key, value in row.items()}
        for row in dataframe.to_dict("records")
    ]


def _json_scalar(value: Any) -> object:
    """Convert pandas, NumPy, and NaN scalar values to JSON-safe values."""
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value
