"""Data quality gates for the news corpus analytical layer."""

from __future__ import annotations

import pandas as pd

from src.config.settings import DQ_MAX_NULL_RATE
from src.transform._exceptions import DataQualityError


def _require_columns(
    dataframe: pd.DataFrame,
    *,
    dataframe_name: str,
    required_columns: set[str],
) -> None:
    """Fail fast when a production DataFrame violates its table contract."""
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise DataQualityError(
            f"{dataframe_name} missing required columns: " + ", ".join(missing_columns)
        )


def run_news_corpus_quality_checks(
    *,
    sample_leaders_df: pd.DataFrame,
    fact_article_source_df: pd.DataFrame,
    fact_article_source_rejected_df: pd.DataFrame,
    fact_article_df: pd.DataFrame,
    fact_mention_df: pd.DataFrame,
    mart_exposure_metrics_df: pd.DataFrame,
    mart_regression_results_df: pd.DataFrame,
    web_enrichment_report: dict[str, int] | None = None,
) -> dict[str, object]:
    """Validate the main corpus contracts before a run is successful.

    Args:
        sample_leaders_df: Frozen sampled cohort.
        fact_article_source_df: Accepted article-source rows.
        fact_article_source_rejected_df: Rejected article-source rows.
        fact_article_df: Canonical article rows.
        fact_mention_df: Candidate mention rows.
        mart_exposure_metrics_df: dbt-built leader-level exposure mart.
        mart_regression_results_df: Python-built regression diagnostics.
        web_enrichment_report: Optional web-cache enrichment counters.

    Returns:
        Dictionary of data quality counters written to the QA JSON artifact.

    Raises:
        DataQualityError: If a critical contract is violated.
    """
    if fact_article_source_df.empty:
        raise DataQualityError("fact_article_source is empty after normalization")

    critical_source_columns = [
        "article_source_id",
        "title_normalized",
        "published_at_normalized",
        "outlet_name_normalized",
        "language",
    ]
    null_violations = {
        column_name: int(fact_article_source_df[column_name].isna().sum())
        for column_name in critical_source_columns
        if fact_article_source_df[column_name].isna().sum() > 0
    }
    if null_violations:
        raise DataQualityError(
            "fact_article_source contains nulls in critical columns: "
            + ", ".join(
                f"{column_name}={null_count}"
                for column_name, null_count in sorted(null_violations.items())
            )
        )

    if "has_full_text" in fact_article_source_df.columns:
        full_text_source_df = fact_article_source_df[
            fact_article_source_df["has_full_text"].astype("boolean").fillna(False)
        ]
        if full_text_source_df["body_text_hash"].isna().sum() > 0:
            raise DataQualityError(
                "fact_article_source contains full-text rows with null body_text_hash"
            )
    elif fact_article_source_df["body_text_hash"].isna().sum() > 0:
        raise DataQualityError("fact_article_source contains null body_text_hash")

    non_french_rows = int((fact_article_source_df["language"] != "fr").sum())
    if non_french_rows > 0:
        raise DataQualityError(
            f"fact_article_source contains {non_french_rows} non-French rows"
        )

    if fact_article_df["canonical_article_id"].duplicated().any():
        raise DataQualityError(
            "fact_article canonical_article_id values are not unique"
        )

    article_id_set = set(fact_article_df["canonical_article_id"].astype(str))
    mention_article_set = set(fact_mention_df["canonical_article_id"].astype(str))
    if not mention_article_set.issubset(article_id_set):
        raise DataQualityError(
            "fact_mention contains article IDs missing from fact_article"
        )

    leader_id_set = set(sample_leaders_df["leader_id"].astype(str))
    mention_leader_set = set(fact_mention_df["leader_id"].astype(str))
    if not mention_leader_set.issubset(leader_id_set):
        raise DataQualityError(
            "fact_mention contains leader IDs missing from sample_leaders"
        )

    if len(mart_exposure_metrics_df) != len(sample_leaders_df):
        raise DataQualityError(
            "mart_exposure_metrics does not preserve the full sampled cohort"
        )

    rejected_ratio = len(fact_article_source_rejected_df) / max(
        len(fact_article_source_df) + len(fact_article_source_rejected_df),
        1,
    )
    if rejected_ratio > DQ_MAX_NULL_RATE:
        raise DataQualityError(
            f"fact_article_source rejected ratio {rejected_ratio:.1%} exceeds "
            f"threshold {DQ_MAX_NULL_RATE:.1%}"
        )

    _require_columns(
        mart_regression_results_df,
        dataframe_name="mart_regression_results",
        required_columns={"status"},
    )
    regression_status = mart_regression_results_df["status"]
    regression_warning_statuses = sorted(
        {
            str(status)
            for status in regression_status.dropna().astype(str).tolist()
            if str(status).startswith("fitted_with_warning:")
        }
    )
    regression_failure_statuses = sorted(
        {
            str(status)
            for status in regression_status.dropna().astype(str).tolist()
            if not str(status).startswith("fitted")
        }
    )

    effective_web_enrichment_report = web_enrichment_report or {
        "web_scrape_queued_count": 0,
        "web_scrape_cache_hit_count": 0,
        "web_scrape_success_count": 0,
        "url_metadata_only_count": 0,
        "web_scrape_failure_count": 0,
    }
    warnings = _build_quality_warnings(effective_web_enrichment_report)

    report = {
        "accepted_article_source_count": int(len(fact_article_source_df)),
        "rejected_article_source_count": int(len(fact_article_source_rejected_df)),
        "canonical_article_count": int(len(fact_article_df)),
        "mention_count": int(len(fact_mention_df)),
        "coverage_row_count": int(len(mart_exposure_metrics_df)),
        "zero_coverage_leader_count": int(
            (mart_exposure_metrics_df["article_count"] == 0).sum()
        ),
        "rejected_ratio": rejected_ratio,
        "regression_warning_statuses": regression_warning_statuses,
        "regression_failure_statuses": regression_failure_statuses,
        "regression_warning_count": len(regression_warning_statuses),
        "regression_failure_count": len(regression_failure_statuses),
        "warnings": warnings,
        "warning_count": len(warnings),
    }
    report.update(effective_web_enrichment_report)
    return report


def _build_quality_warnings(web_enrichment_report: dict[str, int]) -> list[str]:
    """Return non-fatal data-quality warnings for the corpus QA report."""
    queued_count = int(web_enrichment_report.get("web_scrape_queued_count", 0) or 0)
    success_count = int(web_enrichment_report.get("web_scrape_success_count", 0) or 0)
    failure_count = int(web_enrichment_report.get("web_scrape_failure_count", 0) or 0)
    warnings: list[str] = []
    if queued_count > 0 and success_count == 0 and failure_count == 0:
        warnings.append(
            "Web enrichment ran in cache-only mode: queued URL rows were handled "
            "without new successful or failed network fetches."
        )
    return warnings
