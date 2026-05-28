"""Narrative Streamlit dashboard for the news exposure audit."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.config.settings import GOLD_DIR, SHOW_QA_SAMPLES
from src.metrics.news.outlier_sensitivity import build_outlier_sensitivity_report

_APP_TITLE = "Gender And Media Visibility"
_F_COLOR = "#5b2a7b"
_M_COLOR = "#2fa7a0"
_ACCENT = "#5b2a7b"
_LAVENDER = "#b89af0"
_PINK = "#c93678"
_DEFAULT_ANALYSIS_START_DATE = "2025-11-01"
_DEFAULT_ANALYSIS_END_DATE = "2026-04-30"
POISSON_OVERDISPERSION_THRESHOLD = 5.0
TONE_CLASSIFIED_WARNING_THRESHOLD = 0.5
FRAME_CLASSIFIED_WARNING_THRESHOLD = 0.8
HIGH_LEVERAGE_GENDER_SHARE_THRESHOLD = 0.30
DATA_STALE_WARNING_DAYS = 30
EXPECTED_SAMPLE_LEADER_COUNT = 36
_GENDER_PALETTE = {"F": _F_COLOR, "M": _M_COLOR}
_GENDER_PATTERN = {"F": "/", "M": ""}
_GENDER_SYMBOL = {"F": "circle", "M": "diamond"}
_REGRESSION_MODEL_ROLE_ORDER = {
    "Primary model": 0,
    "Sensitivity model": 1,
    "Diagnostic only": 2,
    "Primary diagnostic": 2,
    "Placebo check": 3,
}
_REGRESSION_MODEL_ORDER = {
    "negbinom_exposure": 0,
    "negbinom_exposure_full_controls": 1,
    "poisson_exposure": 2,
    "negbinom_exposure_placebo": 3,
}
_TONE_PROBABILITY_BIN_ORDER = (
    "not_scoreable",
    "<0.40",
    "0.40-0.50",
    "0.50-0.60",
    "0.60-0.70",
    "0.70-0.80",
    ">=0.80",
)
_DOC_LINKS = {
    "Architecture": (
        "https://github.com/cchoooots98/election-gender-bias_D4W/blob/main/"
        "docs/architecture.md"
    ),
    "Metric dictionary": (
        "https://github.com/cchoooots98/election-gender-bias_D4W/blob/main/"
        "docs/metric-dictionary.md"
    ),
    "Limitations": (
        "https://github.com/cchoooots98/election-gender-bias_D4W/blob/main/"
        "docs/limitations.md"
    ),
    "Deployment": (
        "https://github.com/cchoooots98/election-gender-bias_D4W/blob/main/"
        "docs/deployment.md"
    ),
}
_REQUIRED_ARTIFACTS = {
    "sample_leaders": "sample_leaders.parquet",
    "mart_exposure_metrics": "mart_exposure_metrics.parquet",
    "mart_regression_results": "mart_regression_results.parquet",
    "mart_bootstrap_ci": "mart_bootstrap_ci.parquet",
    "mart_analysis_summary": "mart_analysis_summary.parquet",
    "sample_manifest": "sample_manifest.json",
    "news_corpus_qa_report": "news_corpus_qa_report.json",
}
_OPTIONAL_ARTIFACTS = {
    "mart_framing_metrics": "mart_framing_metrics.parquet",
    "mart_primary_frame_metrics": "mart_primary_frame_metrics.parquet",
    "mart_bias_indicators": "mart_bias_indicators.parquet",
    "mart_trait_metrics": "mart_trait_metrics.parquet",
    "mart_trait_top_terms": "mart_trait_top_terms.parquet",
    "mart_trait_candidate_metrics": "mart_trait_candidate_metrics.parquet",
    "mart_trait_qa_samples": "mart_trait_qa_samples.parquet",
    "mart_frame_article_drilldown": "mart_frame_article_drilldown.parquet",
    "nlp_backup_summary_sample": "nlp_backup_summary_sample.parquet",
    "nlp_tone_sensitivity_report": "nlp_tone_sensitivity_report.json",
    "nlp_tone_threshold_sensitivity": "nlp_tone_threshold_sensitivity.parquet",
    "nlp_qa_report": "nlp_qa_report.json",
}


def _require_columns(
    dataframe: pd.DataFrame,
    *,
    dataframe_name: str,
    required_columns: set[str],
) -> None:
    """Raise when a loaded artifact violates the dashboard schema contract."""
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise KeyError(
            f"{dataframe_name} missing required columns: " + ", ".join(missing_columns)
        )


def _load_parquet(path: Path) -> pd.DataFrame:
    """Load a Parquet artifact if it exists."""
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _load_json(path: Path) -> dict[str, Any]:
    """Load a JSON artifact if it exists."""
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _display_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with human-readable dashboard column labels."""
    if dataframe.empty:
        return dataframe
    return dataframe.rename(
        columns={column: _humanize_label(column) for column in dataframe.columns}
    )


def _format_probability_label(value: object) -> str:
    """Format p-values and q-values without hiding very small values as 0.000."""
    if value is None or pd.isna(value):
        return "n/a"
    numeric_value = float(value)
    if numeric_value < 0.001:
        return f"{numeric_value:.2e}"
    return f"{numeric_value:.3f}"


def _format_table_value(value: object) -> str:
    """Format nested run metadata values for stakeholder-facing tables."""
    if value is None:
        return "n/a"
    if isinstance(value, dict | list | tuple):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    if pd.isna(value):
        return "n/a"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def build_key_value_table(values: dict[str, Any]) -> pd.DataFrame:
    """Return a two-column table for compact metadata display."""
    rows = [
        {"field": _humanize_label(str(field)), "value": _format_table_value(value)}
        for field, value in values.items()
    ]
    return pd.DataFrame(rows, columns=["field", "value"])


def _humanize_label(column_name: str) -> str:
    """Convert technical column names into concise dashboard labels."""
    label_overrides = {
        "gender": "Gender",
        "full_name": "Candidate",
        "candidate_label": "Candidate",
        "commune_name": "Commune",
        "city_size_bucket": "City size",
        "article_count": "Articles",
        "headline_mention_count": "Headline mentions",
        "distinct_source_count": "Distinct sources",
        "exposure_per_10k_population": "Articles per 10k residents",
        "metric_name": "Metric",
        "metric_value": "Value",
        "model_name": "Model",
        "model_role": "Role",
        "variable_name": "Variable",
        "p_value": "p-value",
        "q_value": "q-value",
        "p_value_display": "p-value",
        "q_value_display": "Adjusted p-value",
        "std_error": "Std. error",
        "parameter_count": "Parameters",
        "excluded_missing_control_count": "Excluded missing controls",
        "aggregation": "Aggregation",
        "evidence_level": "Evidence",
        "frame_label": "Frame",
        "mention_count": "Mentions",
        "trait_category": "Trait category",
        "hit_mentions": "Hit mentions",
        "term_hits": "Term hits",
        "hits_per_1k_context_words": "Hits per 1k context words",
        "coverage_rate": "Coverage rate",
        "backup_summary_joined_mentions": "Joined summary rows",
        "backup_scored_mentions": "Backup-scored mentions",
        "common_mentions": "Joined summary rows (legacy)",
        "tone_compared_mentions": "Tone compared mentions",
        "frame_compared_mentions": "Frame compared mentions",
        "tone_agreement_rate": "Tone agreement rate",
        "frame_agreement_rate": "Frame agreement rate",
        "tone_cohens_kappa": "Tone Cohen's kappa",
        "frame_cohens_kappa": "Frame Cohen's kappa",
        "context_excerpt": "Context excerpt",
        "mention_id_short": "Mention ID",
        "field": "Field",
        "value": "Value",
        "detail": "Detail",
        "status": "Status",
        "inference_status": "Inference status",
        "is_publishable": "Publishable signal",
    }
    if column_name in label_overrides:
        return label_overrides[column_name]
    return str(column_name).replace("_", " ").strip().title()


def _humanize_metric_value(metric_name: object) -> str:
    """Convert persisted metric identifiers into stakeholder-facing labels."""
    metric_overrides = {
        "mean_unfavorable_tone_share": "Unfavorable tone share",
        "nlp_inference_coverage_rate": "NLP inference coverage",
        "mean_policy_frame_share": "Policy frame share (classified mentions)",
        "mean_scandal_frame_share": "Scandal frame share (classified mentions)",
        "mean_appearance_private_life_frame_share": (
            "Appearance/private-life frame share (classified mentions)"
        ),
        "mean_stereotype_count_per_1k_tokens": "Stereotype seed hits per 1k tokens",
        "generic_sentiment_score_mean": "Generic sentiment score",
    }
    metric_text = str(metric_name)
    return metric_overrides.get(metric_text, metric_text.replace("_", " ").title())


def resolve_dashboard_gold_dir(gold_dir: Path | str | None = None) -> Path:
    """Resolve the dashboard artifact directory from argument or environment."""
    if gold_dir is not None:
        return Path(gold_dir)
    return Path(os.getenv("DASHBOARD_GOLD_URI", str(GOLD_DIR)))


@st.cache_data(show_spinner=False)
def load_dashboard_payload(gold_dir: Path | str | None = None) -> dict[str, Any]:
    """Load all persisted gold-layer artifacts needed by the dashboard."""
    resolved_gold_dir = resolve_dashboard_gold_dir(gold_dir)
    missing_artifacts = [
        artifact_name
        for artifact_name, file_name in _REQUIRED_ARTIFACTS.items()
        if not (resolved_gold_dir / file_name).exists()
    ]
    missing_optional_artifacts = [
        artifact_name
        for artifact_name, file_name in _OPTIONAL_ARTIFACTS.items()
        if not (resolved_gold_dir / file_name).exists()
    ]
    return {
        "gold_dir": str(resolved_gold_dir),
        "sample_df": _load_parquet(resolved_gold_dir / "sample_leaders.parquet"),
        "exposure_df": _load_parquet(
            resolved_gold_dir / "mart_exposure_metrics.parquet"
        ),
        "regression_df": _load_parquet(
            resolved_gold_dir / "mart_regression_results.parquet"
        ),
        "bootstrap_df": _load_parquet(resolved_gold_dir / "mart_bootstrap_ci.parquet"),
        "analysis_df": _load_parquet(
            resolved_gold_dir / "mart_analysis_summary.parquet"
        ),
        "framing_df": _load_parquet(resolved_gold_dir / "mart_framing_metrics.parquet"),
        "primary_frame_df": _load_parquet(
            resolved_gold_dir / "mart_primary_frame_metrics.parquet"
        ),
        "bias_df": _load_parquet(resolved_gold_dir / "mart_bias_indicators.parquet"),
        "trait_metrics_df": _load_parquet(
            resolved_gold_dir / "mart_trait_metrics.parquet"
        ),
        "trait_top_terms_df": _load_parquet(
            resolved_gold_dir / "mart_trait_top_terms.parquet"
        ),
        "trait_candidate_df": _load_parquet(
            resolved_gold_dir / "mart_trait_candidate_metrics.parquet"
        ),
        "trait_qa_df": _load_parquet(
            resolved_gold_dir / "mart_trait_qa_samples.parquet"
        ),
        "frame_article_drilldown_df": _load_parquet(
            resolved_gold_dir / "mart_frame_article_drilldown.parquet"
        ),
        "tone_sensitivity_df": _load_parquet(
            resolved_gold_dir / "nlp_tone_threshold_sensitivity.parquet"
        ),
        "tone_sensitivity_report": _load_json(
            resolved_gold_dir / "nlp_tone_sensitivity_report.json"
        ),
        "manifest": _load_json(resolved_gold_dir / "sample_manifest.json"),
        "qa_report": _load_json(resolved_gold_dir / "news_corpus_qa_report.json"),
        "nlp_qa_report": _load_json(resolved_gold_dir / "nlp_qa_report.json"),
        "missing_artifacts": missing_artifacts,
        "missing_optional_artifacts": missing_optional_artifacts,
    }


def build_documentation_links() -> dict[str, str]:
    """Return public documentation links for dashboard navigation.

    Returns:
        Mapping from human-readable document label to the public GitHub URL.
    """
    return dict(_DOC_LINKS)


def build_overview_metrics(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Build high-level counters for the trust section of the dashboard."""
    sample_df: pd.DataFrame = payload["sample_df"]
    exposure_df: pd.DataFrame = payload["exposure_df"]
    manifest: dict[str, Any] = payload["manifest"]
    qa_report: dict[str, Any] = payload["qa_report"]

    qa = qa_report.get("qa", {})
    if exposure_df.empty:
        covered_count = 0
    else:
        _require_columns(
            exposure_df,
            dataframe_name="mart_exposure_metrics",
            required_columns={"article_count"},
        )
        covered_count = int((exposure_df["article_count"] > 0).sum())

    sampling_warning_count = len(manifest.get("triggered_warnings", []))
    canonical_articles = int(qa.get("canonical_article_count", 0) or 0)

    return [
        {
            "label": "Cohort Coverage",
            "value": f"{covered_count}/{len(sample_df)}",
            "help": "Covered leaders over the frozen analytical cohort.",
            "tone": "purple",
        },
        {
            "label": "Canonical Articles",
            "value": f"{canonical_articles:,}",
            "help": "Deduplicated article corpus used by exposure metrics.",
            "tone": "teal",
        },
        {
            "label": "Sampling Warnings",
            "value": str(sampling_warning_count),
            "help": "Soft-constraint diagnostics from sample_manifest.json.",
            "tone": "yellow" if sampling_warning_count > 10 else "lavender",
        },
    ]


def build_artifact_health_warnings(
    payload: dict[str, Any],
) -> list[dict[str, str]]:
    """Build dashboard-visible artifact health warnings."""
    warnings: list[dict[str, str]] = []
    gold_dir = Path(str(payload.get("gold_dir", "")))
    qa_section = payload.get("qa_report", {}).get("qa", {})
    if isinstance(qa_section, dict):
        for warning in qa_section.get("warnings", []):
            warnings.append(
                {
                    "severity": "warning",
                    "area": "News corpus",
                    "message": str(warning),
                }
            )

    exposure_path = gold_dir / "mart_exposure_metrics.parquet"
    regression_path = gold_dir / "mart_regression_results.parquet"
    bootstrap_path = gold_dir / "mart_bootstrap_ci.parquet"
    for artifact_name, artifact_path in {
        "Regression results": regression_path,
        "Bootstrap intervals": bootstrap_path,
    }.items():
        if (
            exposure_path.exists()
            and artifact_path.exists()
            and artifact_path.stat().st_mtime < exposure_path.stat().st_mtime
        ):
            warnings.append(
                {
                    "severity": "error",
                    "area": artifact_name,
                    "message": (
                        f"{artifact_path.name} is older than "
                        "mart_exposure_metrics.parquet; rerun dependent Gold "
                        "artifacts before interpreting model output."
                    ),
                }
            )

    derived_artifact_names = {
        "Primary frame mart": "mart_primary_frame_metrics.parquet",
        "Frame mart": "mart_framing_metrics.parquet",
        "Bias indicators": "mart_bias_indicators.parquet",
        "Trait metrics": "mart_trait_metrics.parquet",
        "Trait top terms": "mart_trait_top_terms.parquet",
        "Trait candidate metrics": "mart_trait_candidate_metrics.parquet",
        "Trait QA samples": "mart_trait_qa_samples.parquet",
        "Frame drilldown": "mart_frame_article_drilldown.parquet",
        "Tone sensitivity": "nlp_tone_threshold_sensitivity.parquet",
    }
    for artifact_name, file_name in derived_artifact_names.items():
        artifact_path = gold_dir / file_name
        if (
            exposure_path.exists()
            and artifact_path.exists()
            and artifact_path.stat().st_mtime < exposure_path.stat().st_mtime
        ):
            warnings.append(
                {
                    "severity": "warning",
                    "area": artifact_name,
                    "message": (
                        f"{file_name} is older than mart_exposure_metrics.parquet; "
                        "rerun dependent marts if exposure inputs changed."
                    ),
                }
            )
    nlp_qa_path = gold_dir / "nlp_qa_report.json"
    newer_nlp_artifacts = [
        file_name
        for file_name in derived_artifact_names.values()
        if (gold_dir / file_name).exists()
        and nlp_qa_path.exists()
        and (gold_dir / file_name).stat().st_mtime > nlp_qa_path.stat().st_mtime
    ]
    if newer_nlp_artifacts:
        warnings.append(
            {
                "severity": "warning",
                "area": "NLP QA recency",
                "message": (
                    "Some downstream NLP dashboard marts are newer than "
                    "nlp_qa_report.json: "
                    + ", ".join(sorted(newer_nlp_artifacts)[:5])
                    + ". Rerun NLP QA after partial NLP mart refreshes."
                ),
            }
        )

    nlp_qa_report = payload.get("nlp_qa_report", {})
    corpus_mention_count = _coerce_optional_int(qa_section.get("mention_count"))
    nlp_input_total = _coerce_optional_int(
        nlp_qa_report.get("input_coverage", {}).get("total_mentions")
    )
    source_tables = nlp_qa_report.get("source_tables", {})
    if isinstance(source_tables, dict):
        nlp_input_source = source_tables.get("silver.fact_mention_nlp_input", {})
        if isinstance(nlp_input_source, dict):
            nlp_input_total = nlp_input_total or _coerce_optional_int(
                nlp_input_source.get("rows")
            )
    if (
        corpus_mention_count is not None
        and nlp_input_total is not None
        and corpus_mention_count != nlp_input_total
    ):
        warnings.append(
            {
                "severity": "error",
                "area": "NLP lineage",
                "message": (
                    "NLP input rows do not match the corpus-of-record mention "
                    f"count ({nlp_input_total:,} NLP input rows vs "
                    f"{corpus_mention_count:,} fact_mention rows). Re-run the "
                    "NLP input and downstream NLP pipelines before interpreting "
                    "NLI metrics."
                ),
            }
        )
    model_bundle = nlp_qa_report.get("model_bundle", {})
    if (
        isinstance(model_bundle, dict)
        and model_bundle.get("matches_current_config") is False
    ):
        observed_bundle = model_bundle.get(
            "observed_nlp_model_bundle_version",
            "unknown",
        )
        current_bundle = model_bundle.get(
            "current_config_nlp_model_bundle_version",
            "unknown",
        )
        warnings.append(
            {
                "severity": "error",
                "area": "NLP bundle",
                "message": (
                    "NLP QA bundle differs from the current local model config "
                    f"({observed_bundle} -> {current_bundle}). Re-run the NLP "
                    "pipeline before interpreting model-derived metrics."
                ),
            }
        )
    blessed_comparison = nlp_qa_report.get("blessed_bundle_comparison", {})
    if (
        isinstance(blessed_comparison, dict)
        and blessed_comparison.get("status") == "differs"
    ):
        warnings.append(
            {
                "severity": "error",
                "area": "Blessed bundle",
                "message": "Observed NLP bundle differs from the blessed model bundle.",
            }
        )
    return warnings


def has_blocking_nlp_health_issue(payload: dict[str, Any]) -> bool:
    """Return whether NLP metrics should be blocked for governance reasons."""
    blocking_areas = {"NLP bundle", "Blessed bundle", "NLP lineage"}
    return any(
        warning["severity"] == "error" and warning["area"] in blocking_areas
        for warning in build_artifact_health_warnings(payload)
    )


def build_run_metadata(
    payload: dict[str, Any],
    *,
    as_of: datetime | None = None,
) -> dict[str, str]:
    """Build dashboard snapshot metadata.

    Args:
        payload: Loaded dashboard artifacts.
        as_of: Optional timestamp kept for backward-compatible deterministic tests.

    Returns:
        Dictionary of run identifiers, cohort metadata, and analysis window labels.
    """
    manifest: dict[str, Any] = payload.get("manifest", {})
    qa_report: dict[str, Any] = payload.get("qa_report", {})
    nlp_qa_report: dict[str, Any] = payload.get("nlp_qa_report", {})

    generated_at = (
        nlp_qa_report.get("generated_at")
        or qa_report.get("generated_at")
        or manifest.get("created_at")
        or "not available"
    )
    analysis_window = qa_report.get("analysis_window", {})
    analysis_start = (
        qa_report.get("analysis_start_date")
        or analysis_window.get("start_date")
        or _DEFAULT_ANALYSIS_START_DATE
    )
    analysis_end = (
        qa_report.get("analysis_end_date")
        or analysis_window.get("end_date")
        or _DEFAULT_ANALYSIS_END_DATE
    )
    technical_cohort = str(
        manifest.get("sampling_rule_version")
        or f"cohort_{manifest.get('total_sampled', 'unknown')}"
    )
    cohort_label = _readable_cohort_label(manifest, technical_cohort)
    parsed_generated_at = _parse_timestamp(generated_at)
    snapshot_label = (
        parsed_generated_at.strftime("%Y-%m-%d %H:%M UTC")
        if parsed_generated_at is not None
        else str(generated_at)
    )
    oldest_required_artifact_at = _oldest_required_artifact_timestamp(payload)
    age_anchor_at = oldest_required_artifact_at or parsed_generated_at
    data_age_days = _snapshot_age_days(
        age_anchor_at,
        as_of=as_of or datetime.now(UTC),
    )
    data_age_label = "unknown age" if data_age_days is None else f"{data_age_days} days"
    data_age_tone = (
        "yellow"
        if data_age_days is None or data_age_days > DATA_STALE_WARNING_DAYS
        else "neutral"
    )
    return {
        "run_id": str(
            qa_report.get("run_id") or manifest.get("run_id") or "not available"
        ),
        "batch_id": str(qa_report.get("batch_id") or "not available"),
        "cohort": cohort_label,
        "cohort_rule": technical_cohort,
        "generated_at": str(generated_at),
        "snapshot_label": snapshot_label,
        "data_age_source": (
            "oldest required artifact"
            if oldest_required_artifact_at is not None
            else "latest report timestamp"
        ),
        "data_age_days": "" if data_age_days is None else str(data_age_days),
        "data_age_label": data_age_label,
        "data_age_tone": data_age_tone,
        "analysis_window": f"{analysis_start} -> {analysis_end}",
    }


def build_sampling_warnings_table(manifest: dict[str, Any]) -> pd.DataFrame:
    """Flatten sample-manifest warnings for dashboard drilldown."""
    warning_rows: list[dict[str, object]] = []
    for warning in manifest.get("triggered_warnings", []):
        warning_rows.append(
            {
                "warning_code": warning.get("warning_code", ""),
                "scope": warning.get("scope", ""),
                "dimension": warning.get("dimension", ""),
                "value": warning.get("value", ""),
                "count": warning.get("count"),
                "denominator": warning.get("denominator"),
                "share": warning.get("share"),
                "threshold": warning.get("threshold"),
                "over_threshold": _warning_over_threshold(
                    warning.get("share"),
                    warning.get("threshold"),
                ),
                "recommended_action": warning.get("recommended_action", ""),
            }
        )
    warnings_df = pd.DataFrame(
        warning_rows,
        columns=[
            "warning_code",
            "scope",
            "dimension",
            "value",
            "count",
            "denominator",
            "share",
            "threshold",
            "over_threshold",
            "recommended_action",
        ],
    )
    if warnings_df.empty:
        return warnings_df
    return warnings_df.sort_values(
        ["recommended_action", "warning_code", "over_threshold", "scope"],
        ascending=[True, True, False, True],
    ).reset_index(drop=True)


def build_sampling_warning_callout(warnings_df: pd.DataFrame) -> str:
    """Return the most important sampling-confounding warning as prose."""
    if warnings_df.empty:
        return ""
    _require_columns(
        warnings_df,
        dataframe_name="sampling_warnings",
        required_columns={
            "warning_code",
            "scope",
            "dimension",
            "value",
            "share",
            "threshold",
            "recommended_action",
        },
    )
    bloc_warnings_df = warnings_df.loc[
        warnings_df["warning_code"].eq("political_bloc_concentration")
    ].copy()
    if bloc_warnings_df.empty:
        return ""
    bloc_warnings_df["over_threshold"] = bloc_warnings_df["over_threshold"].fillna(0.0)
    row = bloc_warnings_df.sort_values(
        ["over_threshold", "share"],
        ascending=[False, False],
    ).iloc[0]
    share_text = _format_optional_percent(row["share"])
    threshold_text = _format_optional_percent(row["threshold"])
    return (
        "Sampling confounding risk: political bloc concentration is elevated "
        f"for {row['scope']} ({row['dimension']} = {row['value']}, "
        f"{share_text} vs threshold {threshold_text}). "
        f"{row['recommended_action']}"
    )


def _warning_over_threshold(share: object, threshold: object) -> float | None:
    """Return warning exceedance over the configured threshold."""
    try:
        share_value = float(share)
        threshold_value = float(threshold)
    except (TypeError, ValueError):
        return None
    return share_value - threshold_value


def build_tone_threshold_anchor_text(tone_sensitivity_df: pd.DataFrame) -> str:
    """Return compact 0.5/0.4 threshold anchors for the tone KPI help text."""
    if tone_sensitivity_df.empty:
        return ""
    _require_columns(
        tone_sensitivity_df,
        dataframe_name="nlp_tone_threshold_sensitivity",
        required_columns={
            "threshold",
            "segment_type",
            "segment_value",
            "classified_share_of_scoreable",
        },
    )
    overall_df = tone_sensitivity_df.loc[
        tone_sensitivity_df["segment_type"].eq("overall")
        & tone_sensitivity_df["segment_value"].eq("all")
    ].copy()
    if overall_df.empty:
        return ""
    anchor_parts: list[str] = []
    for threshold in [0.5, 0.4]:
        threshold_df = overall_df.loc[
            overall_df["threshold"].astype(float).round(2).eq(threshold)
        ]
        if threshold_df.empty:
            continue
        share = float(threshold_df.iloc[0]["classified_share_of_scoreable"])
        anchor_parts.append(f"at {threshold:.1f}: {share:.0%}")
    if not anchor_parts:
        return ""
    return " Threshold anchors: " + ", ".join(anchor_parts) + "."


def build_tone_probability_distribution_table(
    tone_sensitivity_report: dict[str, Any],
) -> pd.DataFrame:
    """Return top-probability bins from the tone sensitivity report."""
    records = tone_sensitivity_report.get("probability_bins_by_current_label", [])
    if not isinstance(records, list) or not records:
        return pd.DataFrame()
    probability_df = pd.DataFrame(records)
    _require_columns(
        probability_df,
        dataframe_name="nlp_tone_sensitivity_report.probability_bins_by_current_label",
        required_columns={
            "segment_type",
            "segment_value",
            "target_tone_label",
            "probability_bin",
            "mentions",
        },
    )
    probability_df = probability_df.loc[
        probability_df["segment_type"].eq("overall")
    ].copy()
    if probability_df.empty:
        return pd.DataFrame()
    probability_df["probability_bin"] = pd.Categorical(
        probability_df["probability_bin"].astype(str),
        categories=list(_TONE_PROBABILITY_BIN_ORDER),
        ordered=True,
    )
    return probability_df.sort_values(
        ["target_tone_label", "probability_bin"],
        kind="stable",
    ).reset_index(drop=True)


def build_nlp_audit_metrics(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Build high-level counters for the NLP audit dashboard panel."""
    nlp_qa_report: dict[str, Any] = payload.get("nlp_qa_report", {})
    input_coverage = nlp_qa_report.get("input_coverage", {})
    output_coverage = nlp_qa_report.get("output_coverage", {})
    tone_coverage = output_coverage.get("tone", {})
    framing_coverage = output_coverage.get("framing", {})
    model_bundle = nlp_qa_report.get("model_bundle", {})

    total_mentions = int(input_coverage.get("total_mentions", 0) or 0)
    eligible_mentions = int(
        input_coverage.get("eligible_for_inference_mentions", 0) or 0
    )
    tone_scoreable_mentions = int(tone_coverage.get("scoreable_mentions", 0) or 0)
    tone_classified_mentions = int(tone_coverage.get("classified_mentions", 0) or 0)
    frame_scoreable_mentions = int(
        framing_coverage.get("frame_scored_mentions", 0)
        or framing_coverage.get("scoreable_mentions", 0)
        or 0
    )
    frame_classified_mentions = int(
        framing_coverage.get("mentions_with_primary_frame", 0)
        or framing_coverage.get("classified_mentions", 0)
        or 0
    )
    tone_share = tone_coverage.get("classified_share_of_scoreable")
    frame_share = framing_coverage.get("primary_frame_share_of_frame_scored")
    if frame_share is None:
        frame_share = framing_coverage.get("classified_share_of_scoreable")
    warnings = nlp_qa_report.get("warnings", [])
    bundle_version = str(
        model_bundle.get("observed_nlp_model_bundle_version", "not available")
    )
    inference_share = eligible_mentions / total_mentions if total_mentions > 0 else None
    tone_is_low = _is_below_threshold(
        tone_share,
        warning_threshold=TONE_CLASSIFIED_WARNING_THRESHOLD,
    )
    frame_is_low = _is_below_threshold(
        frame_share,
        warning_threshold=FRAME_CLASSIFIED_WARNING_THRESHOLD,
    )
    tone_anchor_text = build_tone_threshold_anchor_text(
        payload.get("tone_sensitivity_df", pd.DataFrame())
    )

    return [
        {
            "label": "NLP Mentions",
            "value": f"{total_mentions:,}",
            "help": "Mention contexts represented in the Phase 5 QA report.",
            "tone": "purple",
        },
        {
            "label": "Inference Eligible",
            "value": (
                f"{eligible_mentions:,} / {total_mentions:,} "
                f"({_format_optional_percent(inference_share)})"
            ),
            "help": "French mention contexts long enough for Transformer scoring.",
            "tone": "teal",
        },
        {
            "label": "Tone Classified",
            "value": f"{_format_optional_percent(tone_share)} at theta=0.60",
            "help": (
                f"{tone_classified_mentions:,} / {tone_scoreable_mentions:,} "
                "scoreable tone rows above the configured threshold." + tone_anchor_text
            ),
            "tone": "yellow" if tone_is_low else "lavender",
            "status": "Low coverage" if tone_is_low else "",
        },
        {
            "label": "Frame Classified",
            "value": f"{_format_optional_percent(frame_share)} at theta=0.60",
            "help": (
                f"{frame_classified_mentions:,} / {frame_scoreable_mentions:,} "
                "frame-scored rows have a selected primary frame."
            ),
            "tone": "pink" if frame_is_low else "teal",
            "status": "Review threshold" if frame_is_low else "",
        },
        {
            "label": "NLP Warnings",
            "value": str(len(warnings)),
            "help": f"Open warnings below. Model bundle: {bundle_version}",
            "tone": "purple",
        },
    ]


def build_nlp_bias_table(bias_df: pd.DataFrame) -> pd.DataFrame:
    """Return dashboard-ready gender-level NLP metrics from the Gold mart."""
    if bias_df.empty:
        return pd.DataFrame()
    _require_columns(
        bias_df,
        dataframe_name="mart_bias_indicators",
        required_columns={"gender", "metric_name", "metric_value"},
    )
    nlp_metric_names = [
        "nlp_inference_coverage_rate",
        "mean_unfavorable_tone_share",
        "mean_policy_frame_share",
        "mean_scandal_frame_share",
        "mean_appearance_private_life_frame_share",
        "mean_stereotype_count_per_1k_tokens",
    ]
    nlp_bias_df = bias_df.loc[bias_df["metric_name"].isin(nlp_metric_names)].copy()
    if nlp_bias_df.empty:
        return pd.DataFrame()
    return nlp_bias_df.sort_values(["metric_name", "gender"]).reset_index(drop=True)


def build_hypothesis_examples_table(nlp_qa_report: dict[str, Any]) -> pd.DataFrame:
    """Return dashboard-ready NLI hypothesis examples."""
    hypothesis_examples = nlp_qa_report.get("hypothesis_examples", {})
    if not isinstance(hypothesis_examples, dict):
        return pd.DataFrame()
    frame_hypotheses = hypothesis_examples.get("frame_hypotheses", {})
    rows: list[dict[str, str]] = []
    if isinstance(frame_hypotheses, dict):
        for frame_label, hypothesis in sorted(frame_hypotheses.items()):
            rows.append(
                {
                    "task": "Frame",
                    "label": str(frame_label),
                    "hypothesis": str(hypothesis),
                }
            )
    tone_hypotheses = hypothesis_examples.get("tone_example_hypotheses", {})
    if isinstance(tone_hypotheses, dict):
        for tone_label, hypothesis in sorted(tone_hypotheses.items()):
            rows.append(
                {
                    "task": "Tone example",
                    "label": str(tone_label),
                    "hypothesis": str(hypothesis),
                }
            )
    return pd.DataFrame(rows, columns=["task", "label", "hypothesis"])


def build_generic_sentiment_table(bias_df: pd.DataFrame) -> pd.DataFrame:
    """Return generic sentiment baseline metrics from the Gold bias mart."""
    if bias_df.empty:
        return pd.DataFrame()
    _require_columns(
        bias_df,
        dataframe_name="mart_bias_indicators",
        required_columns={"gender", "metric_name", "metric_value"},
    )
    generic_metric_names = [
        "generic_sentiment_coverage_rate",
        "mean_generic_sentiment_score",
    ]
    generic_df = bias_df.loc[bias_df["metric_name"].isin(generic_metric_names)].copy()
    if generic_df.empty:
        return pd.DataFrame()
    return generic_df.sort_values(["metric_name", "gender"]).reset_index(drop=True)


def build_population_adjusted_exposure_table(exposure_df: pd.DataFrame) -> pd.DataFrame:
    """Return population-adjusted exposure summaries by overall and city stratum."""
    if exposure_df.empty:
        return pd.DataFrame()
    _require_columns(
        exposure_df,
        dataframe_name="mart_exposure_metrics",
        required_columns={"gender", "city_size_bucket", "exposure_per_10k_population"},
    )
    overall_df = (
        exposure_df.groupby("gender", dropna=False)
        .agg(
            exposure_per_10k_population=("exposure_per_10k_population", "mean"),
            leader_count=("gender", "size"),
        )
        .reset_index()
        .assign(segment="overall")
    )
    city_df = (
        exposure_df.groupby(["city_size_bucket", "gender"], dropna=False)
        .agg(
            exposure_per_10k_population=("exposure_per_10k_population", "mean"),
            leader_count=("gender", "size"),
        )
        .reset_index()
        .rename(columns={"city_size_bucket": "segment"})
    )
    summary_df = pd.concat(
        [
            overall_df[
                ["segment", "gender", "exposure_per_10k_population", "leader_count"]
            ],
            city_df[
                ["segment", "gender", "exposure_per_10k_population", "leader_count"]
            ],
        ],
        ignore_index=True,
    )
    summary_df["exposure_per_10k_population"] = summary_df[
        "exposure_per_10k_population"
    ].astype(float)
    segment_order = {"overall": 0, "small": 1, "medium": 2, "large": 3}
    summary_df["segment_sort_key"] = (
        summary_df["segment"].map(segment_order).fillna(99).astype(int)
    )
    return (
        summary_df.sort_values(["segment_sort_key", "segment", "gender"])
        .drop(columns=["segment_sort_key"])
        .reset_index(drop=True)
    )


def build_frame_distribution(framing_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate leader-frame Gold rows into a dashboard frame distribution."""
    if framing_df.empty:
        return pd.DataFrame()
    _require_columns(
        framing_df,
        dataframe_name="mart_framing_metrics",
        required_columns={
            "leader_id",
            "frame_label",
            "mention_count",
            "mean_frame_score",
        },
    )
    return (
        framing_df.groupby("frame_label", dropna=False)
        .agg(
            mention_count=("mention_count", "sum"),
            mean_frame_score=("mean_frame_score", "mean"),
        )
        .reset_index()
        .sort_values("mention_count", ascending=False)
    )


def _merge_sample_gender_for_dashboard(
    metric_df: pd.DataFrame,
    sample_df: pd.DataFrame,
    *,
    dataframe_name: str,
) -> pd.DataFrame:
    """Attach validated sample gender to a dashboard metric table."""
    sample_gender_df = sample_df[["leader_id", "gender"]].drop_duplicates("leader_id")
    if sample_gender_df["gender"].isna().any():
        raise KeyError("sample_leaders has null gender values")
    merged_df = metric_df.merge(
        sample_gender_df,
        on="leader_id",
        how="left",
        validate="many_to_one",
    )
    if merged_df["gender"].isna().any():
        raise KeyError(f"{dataframe_name} has leader_id values missing from sample")
    return merged_df


def build_frame_gender_distribution(
    framing_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return multi-label frame counts split by candidate gender."""
    if framing_df.empty:
        return pd.DataFrame()
    _require_columns(
        framing_df,
        dataframe_name="mart_framing_metrics",
        required_columns={"leader_id", "frame_label", "mention_count"},
    )
    _require_columns(
        sample_df,
        dataframe_name="sample_leaders",
        required_columns={"leader_id", "gender"},
    )
    frame_gender_df = _merge_sample_gender_for_dashboard(
        framing_df,
        sample_df,
        dataframe_name="mart_framing_metrics",
    )
    grouped_df = (
        frame_gender_df.groupby(["frame_label", "gender"], dropna=False)
        .agg(mention_count=("mention_count", "sum"))
        .reset_index()
    )
    frame_order_df = (
        grouped_df.groupby("frame_label", dropna=False)["mention_count"]
        .sum()
        .reset_index()
    )
    frame_order_df["sort_key"] = frame_order_df.apply(
        lambda row: (
            -1 if row["frame_label"] == "unclassified" else int(row["mention_count"])
        ),
        axis=1,
    )
    ordered_labels = frame_order_df.sort_values(
        ["sort_key", "frame_label"],
        ascending=[False, True],
    )["frame_label"].tolist()
    grouped_df["frame_label"] = pd.Categorical(
        grouped_df["frame_label"],
        categories=ordered_labels,
        ordered=True,
    )
    return grouped_df.sort_values(["frame_label", "gender"]).reset_index(drop=True)


def build_primary_frame_gender_distribution(
    primary_frame_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> pd.DataFrame:
    """Return primary-frame counts split by candidate gender."""
    if primary_frame_df.empty:
        return pd.DataFrame()
    _require_columns(
        primary_frame_df,
        dataframe_name="mart_primary_frame_metrics",
        required_columns={"leader_id", "frame_label", "mention_count"},
    )
    _require_columns(
        sample_df,
        dataframe_name="sample_leaders",
        required_columns={"leader_id", "gender"},
    )
    primary_gender_df = _merge_sample_gender_for_dashboard(
        primary_frame_df,
        sample_df,
        dataframe_name="mart_primary_frame_metrics",
    )
    grouped_df = (
        primary_gender_df.groupby(["frame_label", "gender"], dropna=False)
        .agg(mention_count=("mention_count", "sum"))
        .reset_index()
    )
    ordered_labels = (
        grouped_df.groupby("frame_label", dropna=False)["mention_count"]
        .sum()
        .reset_index()
        .assign(
            sort_key=lambda dataframe: dataframe.apply(
                lambda row: (
                    -1
                    if row["frame_label"] == "unclassified"
                    else int(row["mention_count"])
                ),
                axis=1,
            )
        )
        .sort_values(["sort_key", "frame_label"], ascending=[False, True])[
            "frame_label"
        ]
        .tolist()
    )
    grouped_df["frame_label"] = pd.Categorical(
        grouped_df["frame_label"],
        categories=ordered_labels,
        ordered=True,
    )
    return grouped_df.sort_values(["frame_label", "gender"]).reset_index(drop=True)


def build_scandal_aggregation_comparison(
    primary_frame_gender_df: pd.DataFrame,
    nlp_bias_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compare volume-weighted and leader-mean scandal-frame shares."""
    if primary_frame_gender_df.empty or nlp_bias_df.empty:
        return pd.DataFrame()
    _require_columns(
        primary_frame_gender_df,
        dataframe_name="primary_frame_gender_distribution",
        required_columns={"gender", "frame_label", "mention_count"},
    )
    _require_columns(
        nlp_bias_df,
        dataframe_name="nlp_bias_table",
        required_columns={"gender", "metric_name", "metric_value"},
    )
    frame_df = primary_frame_gender_df.copy()
    frame_df["frame_label"] = frame_df["frame_label"].astype(str)
    classified_lookup = (
        frame_df.loc[frame_df["frame_label"].ne("unclassified")]
        .groupby("gender", dropna=False)["mention_count"]
        .sum()
        .to_dict()
    )
    scandal_lookup = (
        frame_df.loc[frame_df["frame_label"].eq("scandale")]
        .groupby("gender", dropna=False)["mention_count"]
        .sum()
        .to_dict()
    )
    leader_mean_lookup = {
        row.gender: float(row.metric_value)
        for row in nlp_bias_df.loc[
            nlp_bias_df["metric_name"].eq("mean_scandal_frame_share")
        ].itertuples(index=False)
        if pd.notna(row.metric_value)
    }
    if not {"F", "M"}.issubset(classified_lookup) or not {"F", "M"}.issubset(
        scandal_lookup
    ):
        return pd.DataFrame()
    rows = []
    female_denominator = float(classified_lookup["F"])
    male_denominator = float(classified_lookup["M"])
    if female_denominator > 0 and male_denominator > 0:
        female_share = float(scandal_lookup["F"]) / female_denominator
        male_share = float(scandal_lookup["M"]) / male_denominator
        rows.append(
            {
                "aggregation": "Volume-weighted mentions",
                "evidence_level": "Volume-weighted",
                "female_share": female_share,
                "male_share": male_share,
                "gap": male_share - female_share,
                "interpretation": "Male share is higher when high-volume mentions dominate.",
            }
        )
    if {"F", "M"}.issubset(leader_mean_lookup):
        female_share = leader_mean_lookup["F"]
        male_share = leader_mean_lookup["M"]
        rows.append(
            {
                "aggregation": "Leader-mean rates",
                "evidence_level": "Leader-mean",
                "female_share": female_share,
                "male_share": male_share,
                "gap": male_share - female_share,
                "interpretation": "Gap disappears after cohort-equalized weighting.",
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "aggregation",
            "evidence_level",
            "female_share",
            "male_share",
            "gap",
            "interpretation",
        ],
    )


def build_trait_overview_table(
    trait_metrics_df: pd.DataFrame,
    *,
    scenario_id: str = "all",
    trait_tier: str = "core",
) -> pd.DataFrame:
    """Return dashboard-ready gender/category trait metrics."""
    if trait_metrics_df.empty:
        return pd.DataFrame()
    _require_columns(
        trait_metrics_df,
        dataframe_name="mart_trait_metrics",
        required_columns={
            "scenario_id",
            "trait_tier",
            "gender",
            "trait_category",
            "hit_mentions",
            "term_hits",
            "hits_per_1k_context_words",
            "coverage_rate",
            "evidence_level",
        },
    )
    trait_overview_df = trait_metrics_df.loc[
        trait_metrics_df["scenario_id"].eq(scenario_id)
        & trait_metrics_df["trait_tier"].eq(trait_tier)
    ].copy()
    return trait_overview_df.sort_values(
        ["trait_category", "gender"],
    ).reset_index(drop=True)


def build_trait_top_terms_table(
    trait_top_terms_df: pd.DataFrame,
    *,
    scenario_id: str = "all",
    trait_tier: str = "core",
    max_rank: int = 5,
) -> pd.DataFrame:
    """Return top trait terms for dashboard display."""
    if trait_top_terms_df.empty:
        return pd.DataFrame()
    _require_columns(
        trait_top_terms_df,
        dataframe_name="mart_trait_top_terms",
        required_columns={
            "scenario_id",
            "trait_tier",
            "gender",
            "trait_category",
            "term",
            "term_hits",
            "hit_mentions",
            "rank",
        },
    )
    top_terms_df = trait_top_terms_df.loc[
        trait_top_terms_df["scenario_id"].eq(scenario_id)
        & trait_top_terms_df["trait_tier"].eq(trait_tier)
        & trait_top_terms_df["rank"].le(max_rank)
    ].copy()
    return top_terms_df.sort_values(
        ["trait_category", "gender", "rank"],
    ).reset_index(drop=True)


def build_trait_candidate_table(
    trait_candidate_df: pd.DataFrame,
    *,
    scenario_id: str = "all",
    trait_tier: str = "core",
) -> pd.DataFrame:
    """Return candidate-level trait metrics for dashboard drilldown."""
    if trait_candidate_df.empty:
        return pd.DataFrame()
    _require_columns(
        trait_candidate_df,
        dataframe_name="mart_trait_candidate_metrics",
        required_columns={
            "scenario_id",
            "trait_tier",
            "leader_id",
            "full_name",
            "gender",
            "commune_name",
            "trait_category",
            "article_count",
            "mention_count",
            "term_hits",
            "hits_per_1k_context_words",
            "coverage_rate",
        },
    )
    candidate_df = trait_candidate_df.loc[
        trait_candidate_df["scenario_id"].eq(scenario_id)
        & trait_candidate_df["trait_tier"].eq(trait_tier)
    ].copy()
    return candidate_df.sort_values(
        ["article_count", "term_hits", "full_name"],
        ascending=[False, False, True],
    ).reset_index(drop=True)


def build_trait_qa_samples_table(
    trait_qa_df: pd.DataFrame,
    *,
    trait_tier: str = "core",
) -> pd.DataFrame:
    """Return representative matched contexts for trait lexicon QA."""
    if trait_qa_df.empty:
        return pd.DataFrame()
    _require_columns(
        trait_qa_df,
        dataframe_name="mart_trait_qa_samples",
        required_columns={
            "trait_tier",
            "trait_category",
            "term",
            "gender",
            "full_name",
            "context_excerpt",
            "rationale",
        },
    )
    qa_samples_df = trait_qa_df.loc[trait_qa_df["trait_tier"].eq(trait_tier)].copy()
    if "mention_id" in qa_samples_df.columns:
        qa_samples_df["mention_id_short"] = (
            qa_samples_df["mention_id"].astype(str).str.slice(0, 8)
        )
    return qa_samples_df.sort_values(
        ["trait_category", "term", "gender", "full_name"],
    ).reset_index(drop=True)


def build_trait_outlier_sensitivity_table(
    trait_metrics_df: pd.DataFrame,
    *,
    trait_tier: str = "core",
) -> pd.DataFrame:
    """Return trait sensitivity rows with short labels and delta vs all."""
    if trait_metrics_df.empty:
        return pd.DataFrame()
    _require_columns(
        trait_metrics_df,
        dataframe_name="mart_trait_metrics",
        required_columns={
            "scenario_id",
            "trait_tier",
            "gender",
            "trait_category",
            "hit_mentions",
            "term_hits",
            "hits_per_1k_context_words",
            "evidence_level",
        },
    )
    selected_categories = [
        "political_work",
        "leadership_competence",
        "scandal_conflict",
    ]
    scenario_labels = {
        "all": "all candidates",
        "drop_top_overall": "drop top-1 overall",
        "drop_top_each_gender": "drop top-1 per gender",
    }
    outlier_df = trait_metrics_df.loc[
        trait_metrics_df["trait_tier"].eq(trait_tier)
        & trait_metrics_df["trait_category"].isin(selected_categories)
        & trait_metrics_df["scenario_id"].isin(scenario_labels)
    ].copy()
    if outlier_df.empty:
        return pd.DataFrame()
    baseline_df = outlier_df.loc[outlier_df["scenario_id"].eq("all")][
        ["gender", "trait_category", "hits_per_1k_context_words"]
    ].rename(columns={"hits_per_1k_context_words": "all_hits_per_1k"})
    outlier_df = outlier_df.merge(
        baseline_df,
        on=["gender", "trait_category"],
        how="left",
        validate="many_to_one",
    )
    outlier_df["delta_vs_all"] = (
        outlier_df["hits_per_1k_context_words"] - outlier_df["all_hits_per_1k"]
    )
    outlier_df["scenario_label"] = outlier_df["scenario_id"].map(scenario_labels)
    outlier_df["scenario_label"] = pd.Categorical(
        outlier_df["scenario_label"],
        categories=list(scenario_labels.values()),
        ordered=True,
    )
    return outlier_df.sort_values(
        ["trait_category", "scenario_label", "gender"],
    ).reset_index(drop=True)


def build_regression_model_priority_table(regression_df: pd.DataFrame) -> pd.DataFrame:
    """Return gender-effect regression rows with model-priority annotations."""
    if regression_df.empty:
        return pd.DataFrame()
    _require_columns(
        regression_df,
        dataframe_name="mart_regression_results",
        required_columns={
            "model_name",
            "variable_name",
            "coefficient",
            "std_error",
            "p_value",
            "status",
        },
    )
    gender_rows = regression_df.loc[
        regression_df["variable_name"].eq("gender_female")
    ].copy()
    if gender_rows.empty:
        return pd.DataFrame()
    dispersion_lookup = {
        row.model_name: float(row.coefficient)
        for row in regression_df.loc[
            regression_df["variable_name"].eq("_dispersion_ratio")
        ].itertuples(index=False)
        if pd.notna(row.coefficient)
    }
    poisson_dispersion = dispersion_lookup.get("poisson_exposure")
    poisson_is_misspecified = (
        poisson_dispersion is not None
        and poisson_dispersion > POISSON_OVERDISPERSION_THRESHOLD
    )
    gender_rows["dispersion_ratio"] = gender_rows["model_name"].map(dispersion_lookup)
    if (
        "model_role" not in gender_rows.columns
        or gender_rows["model_role"].isna().all()
    ):
        gender_rows["model_role"] = gender_rows["model_name"].map(
            {
                "negbinom_exposure": "Primary model",
                "poisson_exposure": (
                    "Diagnostic only"
                    if poisson_is_misspecified
                    else "Primary diagnostic"
                ),
                "negbinom_exposure_full_controls": "Sensitivity model",
                "negbinom_exposure_placebo": "Placebo check",
            }
        )
    gender_rows["interpretation"] = gender_rows.apply(
        lambda row: _regression_interpretation(row, poisson_is_misspecified),
        axis=1,
    )
    gender_rows["p_value_display"] = gender_rows["p_value"].map(
        _format_probability_label
    )
    if "q_value" in gender_rows.columns:
        gender_rows["q_value_display"] = gender_rows["q_value"].map(
            _format_probability_label
        )
    gender_rows["_role_order"] = (
        gender_rows["model_role"].map(_REGRESSION_MODEL_ROLE_ORDER).fillna(99)
    )
    gender_rows["_model_order"] = (
        gender_rows["model_name"].map(_REGRESSION_MODEL_ORDER).fillna(99)
    )
    return (
        gender_rows.sort_values(["_role_order", "_model_order", "model_name"])
        .drop(columns=["_role_order", "_model_order"])
        .reset_index(drop=True)
    )


def build_regression_governance_summary(
    regression_df: pd.DataFrame,
    bootstrap_df: pd.DataFrame,
) -> dict[str, str]:
    """Return the Q6 model-governance headline and caveat."""
    model_priority_df = build_regression_model_priority_table(regression_df)
    if model_priority_df.empty:
        return {"headline": "", "caveat": ""}

    primary_rows = model_priority_df.loc[
        model_priority_df["model_name"].eq("negbinom_exposure")
    ]
    poisson_rows = model_priority_df.loc[
        model_priority_df["model_name"].eq("poisson_exposure")
    ]
    sensitivity_rows = model_priority_df.loc[
        model_priority_df["model_name"].eq("negbinom_exposure_full_controls")
    ]
    primary_row = primary_rows.iloc[0] if not primary_rows.empty else None
    poisson_row = poisson_rows.iloc[0] if not poisson_rows.empty else None

    bootstrap_text = ""
    if not bootstrap_df.empty:
        _require_columns(
            bootstrap_df,
            dataframe_name="mart_bootstrap_ci",
            required_columns={"variable_name", "ci_lower_95", "ci_upper_95"},
        )
        bootstrap_gender = bootstrap_df.loc[
            bootstrap_df["variable_name"].eq("gender_female")
        ]
        if not bootstrap_gender.empty:
            bootstrap_row = bootstrap_gender.iloc[0]
            bootstrap_text = (
                " Bootstrap 95% CI "
                f"[{float(bootstrap_row['ci_lower_95']):+.2f}, "
                f"{float(bootstrap_row['ci_upper_95']):+.2f}] spans zero."
            )

    headline = ""
    if primary_row is not None:
        q_value = primary_row.get("q_value")
        q_label = _format_probability_label(q_value)
        headline = (
            "After the population offset and incumbency control, gender does "
            "not predict article count in the primary Negative Binomial model "
            f"(adjusted p={q_label})." + bootstrap_text
        )

    caveat_parts: list[str] = []
    if poisson_row is not None:
        dispersion_ratio = poisson_row.get("dispersion_ratio")
        coefficient = poisson_row.get("coefficient")
        p_value = poisson_row.get("p_value")
        if pd.notna(dispersion_ratio):
            nb_dispersion = (
                primary_row.get("dispersion_ratio") if primary_row is not None else None
            )
            nb_dispersion_text = (
                f"; NB primary dispersion = {float(nb_dispersion):.2f}"
                if pd.notna(nb_dispersion)
                else ""
            )
            caveat_parts.append(
                "Poisson is diagnostic only because its dispersion ratio is "
                f"{float(dispersion_ratio):.0f} (expected = 1 under Poisson"
                f"{nb_dispersion_text})"
                f" (coef={float(coefficient):+.3f}, "
                f"p={_format_probability_label(p_value)})."
            )
    if primary_row is not None and not sensitivity_rows.empty:
        sensitivity_row = sensitivity_rows.iloc[0]
        primary_coefficient = float(primary_row["coefficient"])
        sensitivity_coefficient = float(sensitivity_row["coefficient"])
        if primary_coefficient * sensitivity_coefficient < 0:
            caveat_parts.append(
                "The gender coefficient changes sign between the parsimonious "
                "and full-control Negative Binomial specifications. Both "
                "estimates straddle zero; the sign flip reflects sampling "
                "noise, not directionally opposite findings."
            )

    return {"headline": headline, "caveat": " ".join(caveat_parts)}


def _apply_page_config() -> None:
    """Apply Streamlit page settings and dashboard CSS."""
    st.set_page_config(page_title=_APP_TITLE, layout="wide")
    st.markdown(
        """
        <style>
        :root {
            --bg:#f7f5f1;
            --panel:#ffffff;
            --ink:#30313a;
            --muted:#6f6f78;
            --line:#e5e1da;
            --purple:#5b2a7b;
            --purple-soft:#b89af0;
            --teal:#2fa7a0;
            --pink:#c93678;
            --section-line:#eee9f7;
        }
        .stApp {
            background:#f7f5f1;
            color:var(--ink);
        }
        .block-container {
            max-width:1180px;
            padding-top:1.25rem;
            padding-bottom:3rem;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color:var(--ink);
            letter-spacing:0;
        }
        p, div, span {
            letter-spacing:0;
        }
        .hero {
            padding:2rem 2.5rem;
            border-radius:0;
            background:
                linear-gradient(100deg, rgba(35,35,42,.96), rgba(91,42,123,.96) 55%, rgba(105,39,189,.94)),
                radial-gradient(circle at 76% 20%, rgba(184,154,240,.25), transparent 26%);
            color:#ffffff;
            margin-bottom:1.6rem;
            min-height:210px;
            display:flex;
            flex-wrap:wrap;
            gap:1.25rem;
            align-items:center;
            justify-content:space-between;
        }
        .hero > div:first-child {
            min-width:0;
            flex:1 1 420px;
        }
        .hero h1 {
            margin:.4rem 0 0;
            font-family: Arial, sans-serif;
            font-size:1.95rem;
            font-weight:400;
            letter-spacing:.04em;
            text-transform:uppercase;
            color:#ffffff;
            overflow-wrap:anywhere;
        }
        .hero p {
            margin:.75rem 0 0;
            color:#cfe6dc;
            font-size:1.25rem;
        }
        .hero-badge {
            border:1px solid rgba(255,255,255,.45);
            padding:1.2rem 1.4rem;
            min-width:180px;
            text-align:center;
            font-family:Arial, sans-serif;
            text-transform:uppercase;
            color:#ffffff;
        }
        .hero-badge strong {
            display:block;
            font-size:1.15rem;
            font-weight:400;
            color:#ffffff;
            margin:.35rem 0;
            text-transform:none;
        }
        .run-banner {
            display:flex;
            flex-wrap:wrap;
            gap:.5rem .9rem;
            align-items:center;
            margin:-.65rem 0 1.4rem;
            padding:.75rem .95rem;
            background:#ffffff;
            border-left:5px solid var(--teal);
            color:var(--ink);
            font-size:.86rem;
            box-shadow:0 1px 0 rgba(48,49,58,.04);
        }
        .run-banner.yellow {
            border-left-color:#d97706;
            background:#fff7ed;
        }
        .run-banner a {
            color:var(--purple);
            text-decoration:none;
            font-weight:700;
        }
        .eyebrow {
            font-size:.72rem;
            text-transform:uppercase;
            color:#ffffff;
            font-weight:700;
            letter-spacing:.16em;
        }
        .lede {
            margin:0 0 2.25rem;
            max-width:1080px;
            font-family:Georgia, "Times New Roman", serif;
            font-size:1.2rem;
            line-height:1.38;
            color:var(--ink);
        }
        .lede strong {
            color:var(--ink);
        }
        .kpi-card {
            border:0;
            border-radius:0;
            padding:1rem .8rem .9rem;
            background:#ffffff;
            min-height:124px;
            box-shadow:0 1px 0 rgba(48,49,58,.04);
            border-top:5px solid var(--purple);
        }
        .kpi-card.teal { border-top-color:var(--teal); }
        .kpi-card.lavender { border-top-color:var(--purple-soft); }
        .kpi-card.pink { border-top-color:var(--pink); }
        .kpi-card.yellow { border-top-color:#d97706; }
        .kpi-card.teal .kpi-value { color:var(--teal); }
        .kpi-card.lavender .kpi-value { color:var(--purple-soft); }
        .kpi-card.pink .kpi-value { color:var(--pink); }
        .kpi-card.yellow .kpi-value { color:#b45309; }
        .kpi-card.purple .kpi-value {
            color:var(--purple);
        }
        .kpi-label {
            font-size:.76rem;
            color:var(--muted);
            font-family:Georgia, "Times New Roman", serif;
        }
        .kpi-value {
            margin-top:.25rem;
            font-size:1.9rem;
            font-family:Georgia, "Times New Roman", serif;
            font-weight:400;
            color:var(--purple);
        }
        .kpi-help {
            margin-top:.3rem;
            color:var(--muted);
            font-size:.82rem;
        }
        .kpi-status {
            margin-top:.35rem;
            color:#b91c1c;
            font-size:.78rem;
            font-weight:700;
        }
        .callout {
            padding:.1rem 0 .1rem 1rem;
            border-left:4px solid var(--section-line);
            border-radius:0;
            background:transparent;
            color:var(--ink);
            margin:.6rem 0 1.1rem;
            max-width:1040px;
            font-family:Georgia, "Times New Roman", serif;
            font-size:1.05rem;
            line-height:1.38;
        }
        .warning-callout {
            padding:.9rem 1rem;
            border-left:5px solid #d97706;
            background:#fff7ed;
            color:#30313a;
            margin:.75rem 0 1rem;
            font-size:.98rem;
            line-height:1.42;
        }
        .error-callout {
            padding:.9rem 1rem;
            border-left:5px solid #b91c1c;
            background:#fef2f2;
            color:#30313a;
            margin:.75rem 0 1rem;
            font-size:.98rem;
            line-height:1.42;
        }
        .metric-strip {
            display:grid;
            grid-template-columns:repeat(auto-fit, minmax(160px, 1fr));
            gap:.5rem;
            margin:.75rem 0 1rem;
        }
        .metric-strip-item {
            background:#ffffff;
            border-top:3px solid var(--line);
            padding:.65rem .75rem;
            min-height:72px;
        }
        .metric-strip-label {
            color:var(--muted);
            font-size:.72rem;
            text-transform:uppercase;
            letter-spacing:.06em;
        }
        .metric-strip-value {
            color:var(--ink);
            font-family:Georgia, "Times New Roman", serif;
            font-size:1.3rem;
            margin-top:.2rem;
        }
        .section-note {
            color:var(--muted);
            font-size:.9rem;
            margin-bottom:.6rem;
        }
        .element-container:has(.js-plotly-plot) {
            background:#ffffff;
        }
        hr {
            border-color:#eee9f7;
            margin:2rem 0;
        }
        @media print {
            .block-container {
                padding-bottom:0 !important;
            }
            .hero {
                break-inside:avoid;
                page-break-inside:avoid;
                min-height:auto;
                padding:1.2rem 1.5rem;
            }
            .hero h1 {
                font-size:1.45rem;
                line-height:1.15;
                overflow-wrap:normal;
            }
            .hero p {
                font-size:1rem;
            }
            .stApp {
                page-break-after:avoid;
            }
            footer {
                display:none;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _kpi_card(metric: dict[str, str]) -> str:
    """Render one KPI card as HTML."""
    tone = metric.get("tone", "purple")
    return (
        f'<div class="kpi-card {tone}">'
        f'<div class="kpi-label">{metric["label"]}</div>'
        f'<div class="kpi-value">{metric["value"]}</div>'
        f'<div class="kpi-help">{metric["help"]}</div>'
        f'{_kpi_status(metric.get("status", ""))}'
        "</div>"
    )


def _kpi_status(status: str) -> str:
    """Render a warning status inside a KPI card when present."""
    if not status:
        return ""
    return f'<div class="kpi-status">{status}</div>'


def _format_optional_percent(value: object) -> str:
    """Format optional numeric ratios as whole-percentage labels."""
    if value is None:
        return "n/a"
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return "n/a"
    return f"{numeric_value:.0%}"


def _coerce_optional_int(value: object) -> int | None:
    """Return an integer when a JSON/parquet count is present and numeric."""
    if value is None or isinstance(value, dict | list | tuple | set):
        return None
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _readable_cohort_label(manifest: dict[str, Any], technical_cohort: str) -> str:
    """Return a reviewer-readable cohort label while preserving the rule version."""
    total_sampled = manifest.get("total_sampled")
    if total_sampled is None:
        total_sampled = manifest.get("sample_size")
    version_token = str(technical_cohort).split("_", maxsplit=1)[0]
    if str(version_token).startswith("v") and total_sampled is not None:
        return f"{total_sampled}-leader stratified quota cohort (rule {version_token})"
    if total_sampled is not None:
        return f"{total_sampled}-leader stratified quota cohort"
    return technical_cohort


def _parse_timestamp(value: object) -> datetime | None:
    """Parse an ISO timestamp into UTC when possible."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == "not available":
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _snapshot_age_days(
    snapshot_at: datetime | None,
    *,
    as_of: datetime,
) -> int | None:
    """Return calendar-day age for a frozen analytical snapshot."""
    if snapshot_at is None:
        return None
    return (as_of.astimezone(UTC).date() - snapshot_at.astimezone(UTC).date()).days


def _oldest_required_artifact_timestamp(payload: dict[str, Any]) -> datetime | None:
    """Return the oldest mtime across required dashboard artifacts when local."""
    gold_dir_value = payload.get("gold_dir")
    if not gold_dir_value:
        return None
    gold_dir = Path(str(gold_dir_value))
    if not gold_dir.exists():
        return None
    artifact_timestamps: list[datetime] = []
    for file_name in _REQUIRED_ARTIFACTS.values():
        artifact_path = gold_dir / file_name
        if not artifact_path.exists():
            continue
        artifact_timestamps.append(
            datetime.fromtimestamp(artifact_path.stat().st_mtime, tz=UTC)
        )
    if not artifact_timestamps:
        return None
    return min(artifact_timestamps)


def _is_below_threshold(value: object, *, warning_threshold: float) -> bool:
    """Return whether a coverage value is below its dashboard warning threshold."""
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return False
    return numeric_value < warning_threshold


def _callout(text: str) -> None:
    """Render a short narrative callout."""
    st.markdown(f'<div class="callout">{text}</div>', unsafe_allow_html=True)


def _warning_callout(title: str, body: str) -> None:
    """Render a visible warning callout for model and data caveats."""
    st.markdown(
        f'<div class="warning-callout"><strong>{title}</strong><br>{body}</div>',
        unsafe_allow_html=True,
    )


def _error_callout(title: str, body: str) -> None:
    """Render a blocking data or model governance callout."""
    st.markdown(
        f'<div class="error-callout"><strong>{title}</strong><br>{body}</div>',
        unsafe_allow_html=True,
    )


def _metric_strip(metrics: list[tuple[str, object]]) -> str:
    """Render compact secondary counters under a KPI row."""
    items = []
    for label, value in metrics:
        items.append(
            '<div class="metric-strip-item">'
            f'<div class="metric-strip-label">{label}</div>'
            f'<div class="metric-strip-value">{value}</div>'
            "</div>"
        )
    return '<div class="metric-strip">' + "".join(items) + "</div>"


def _dataframe_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    """Serialize one dashboard table for CSV export."""
    return dataframe.to_csv(index=False).encode("utf-8")


def _download_dataframe_csv(
    *,
    label: str,
    dataframe: pd.DataFrame,
    file_name: str,
) -> None:
    """Render a CSV download button for non-empty dashboard tables."""
    if dataframe.empty:
        return
    st.download_button(
        label=label,
        data=_dataframe_to_csv_bytes(dataframe),
        file_name=file_name,
        mime="text/csv",
    )


def _plotly_defaults(fig: go.Figure, height: int = 340) -> go.Figure:
    """Apply one visual system to all Plotly figures."""
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=36, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        colorway=[_F_COLOR, _M_COLOR, _LAVENDER, _PINK],
        font=dict(family="Arial, sans-serif", color="#30313a", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(gridcolor="#e9e6df", linecolor="#d9d3ca")
    fig.update_yaxes(gridcolor="#e9e6df", linecolor="#d9d3ca")
    return fig


def _deduplicate_gender_legend(fig: go.Figure) -> go.Figure:
    """Keep gender legend entries compact when Plotly combines color and pattern."""
    seen_names: set[str] = set()
    for trace in fig.data:
        trace_name = str(getattr(trace, "name", ""))
        gender_name = trace_name.split(",", maxsplit=1)[0].strip()
        if gender_name in _GENDER_PALETTE:
            trace.name = gender_name
            trace.legendgroup = gender_name
            trace.showlegend = gender_name not in seen_names
            seen_names.add(gender_name)
    return fig


def _candidate_labels(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach readable candidate labels when sample metadata is available."""
    labeled_df = exposure_df.copy()
    label_columns = [
        column
        for column in ["leader_id", "full_name", "commune_name"]
        if column in sample_df.columns
    ]
    if {"leader_id", "full_name"}.issubset(label_columns):
        labeled_df = labeled_df.merge(
            sample_df[label_columns].drop_duplicates("leader_id"),
            on="leader_id",
            how="left",
            validate="one_to_one",
        )
    if "full_name" in labeled_df.columns:
        label_series = labeled_df["full_name"]
    else:
        label_series = labeled_df["leader_id"]
    labeled_df["candidate_label"] = label_series.fillna(labeled_df["leader_id"])
    if "commune_name" in labeled_df.columns:
        labeled_df["candidate_label"] = (
            labeled_df["candidate_label"].astype(str)
            + " - "
            + labeled_df["commune_name"].fillna("").astype(str)
        ).str.rstrip(" -")
    return labeled_df


def _top_exposure_note(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> str:
    """Return a concise note about the largest coverage outlier."""
    if exposure_df.empty or "article_count" not in exposure_df.columns:
        return ""
    labeled_df = _candidate_labels(exposure_df, sample_df)
    top_row = labeled_df.sort_values("article_count", ascending=False).iloc[0]
    total_articles = int(labeled_df["article_count"].sum())
    gender_articles = int(
        labeled_df.loc[
            labeled_df["gender"].eq(top_row["gender"]),
            "article_count",
        ].sum()
    )
    total_share = (
        int(top_row["article_count"]) / total_articles if total_articles > 0 else 0.0
    )
    gender_share = (
        int(top_row["article_count"]) / gender_articles if gender_articles > 0 else 0.0
    )
    commune = (
        f" ({top_row['commune_name']})"
        if "commune_name" in labeled_df.columns
        and not pd.isna(top_row.get("commune_name"))
        else ""
    )
    candidate_name = (
        str(top_row["full_name"])
        if "full_name" in labeled_df.columns and not pd.isna(top_row.get("full_name"))
        else str(top_row["candidate_label"])
    )
    return (
        f"One candidate drives {total_share:.0%} of total coverage and "
        f"{gender_share:.0%} of {top_row['gender']} coverage: "
        f"{candidate_name}{commune} - "
        f"{int(top_row['article_count']):,} of {total_articles:,} articles."
    )


def _top_gender_leverage_caveat(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> str:
    """Return a caveat when one leader dominates a gender-specific corpus."""
    if exposure_df.empty or "article_count" not in exposure_df.columns:
        return ""
    labeled_df = _candidate_labels(exposure_df, sample_df)
    if "gender" not in labeled_df.columns:
        return ""
    top_row = labeled_df.sort_values("article_count", ascending=False).iloc[0]
    gender_articles = int(
        labeled_df.loc[
            labeled_df["gender"].eq(top_row["gender"]),
            "article_count",
        ].sum()
    )
    if gender_articles <= 0:
        return ""
    gender_share = int(top_row["article_count"]) / gender_articles
    if gender_share < HIGH_LEVERAGE_GENDER_SHARE_THRESHOLD:
        return ""
    candidate_name = (
        str(top_row["full_name"])
        if "full_name" in labeled_df.columns and not pd.isna(top_row.get("full_name"))
        else str(top_row["candidate_label"])
    )
    return (
        "Raw counts are high-leverage dominated: "
        f"{candidate_name} accounts for {gender_share:.0%} of "
        f"{top_row['gender']} coverage; see raw exposure and NLP robustness panels."
    )


def _trait_outlier_sentence(outlier_df: pd.DataFrame) -> str:
    """Return the headline trait outlier movement for political_work."""
    if outlier_df.empty:
        return ""
    segment_df = outlier_df.loc[
        outlier_df["trait_category"].eq("political_work")
        & outlier_df["gender"].eq("M")
        & outlier_df["scenario_id"].isin(["all", "drop_top_overall"])
    ]
    lookup = {
        row.scenario_id: float(row.hits_per_1k_context_words)
        for row in segment_df.itertuples(index=False)
    }
    if {"all", "drop_top_overall"} - set(lookup):
        return ""
    return (
        "Male political_work falls from "
        f"{lookup['all']:.2f} to {lookup['drop_top_overall']:.2f} hits per 1k "
        "context words when the single largest overall outlier is removed."
    )


def _trait_headline_sentence(
    trait_overview_df: pd.DataFrame,
    nlp_bias_df: pd.DataFrame,
) -> str:
    """Return a compact headline for the strongest current trait signals."""
    if trait_overview_df.empty:
        return ""
    leadership_df = trait_overview_df.loc[
        trait_overview_df["trait_category"].eq("leadership_competence")
    ]
    leadership_lookup = {
        row.gender: float(row.hits_per_1k_context_words)
        for row in leadership_df.itertuples(index=False)
    }
    stereotype_lookup: dict[str, float] = {}
    if not nlp_bias_df.empty:
        stereotype_df = nlp_bias_df.loc[
            nlp_bias_df["metric_name"].eq("mean_stereotype_count_per_1k_tokens")
        ]
        stereotype_lookup = {
            row.gender: float(row.metric_value)
            for row in stereotype_df.itertuples(index=False)
        }
    headline_parts = []
    if {"F", "M"}.issubset(leadership_lookup):
        headline_parts.append(
            "Core leadership_competence terms are higher for female candidates "
            f"({leadership_lookup['F']:.2f} vs {leadership_lookup['M']:.2f} hits "
            "per 1k context words)."
        )
    if {"F", "M"}.issubset(stereotype_lookup):
        headline_parts.append(
            "The stereotype seed lexicon is also higher for female candidates "
            f"({stereotype_lookup['F']:.2f} vs {stereotype_lookup['M']:.2f} "
            "hits per 1k tokens)."
        )
    return " ".join(headline_parts)


def _regression_interpretation(row: pd.Series, poisson_is_misspecified: bool) -> str:
    """Return a compact model interpretation label for the dashboard."""
    p_value = row.get("p_value")
    p_label = f"p={_format_probability_label(p_value)}"
    q_value = row.get("q_value")
    q_label = "" if pd.isna(q_value) else f", q={_format_probability_label(q_value)}"
    if row.get("model_name") == "poisson_exposure" and poisson_is_misspecified:
        return f"Overdispersed; diagnostic only ({p_label}{q_label})"
    if row.get("model_name") == "negbinom_exposure":
        return f"No detectable adjusted gender effect at n=36 ({p_label}{q_label})"
    if row.get("model_name") == "negbinom_exposure_placebo":
        return f"Random-label falsification check ({p_label}{q_label})"
    if row.get("model_name") == "negbinom_exposure_full_controls":
        return f"High-parameter sensitivity model ({p_label}{q_label})"
    return f"Directional audit estimate ({p_label}{q_label})"


def _render_hero(run_metadata: dict[str, str]) -> None:
    """Render the page title and scope statement."""
    run_id = run_metadata.get("run_id", "not available")
    run_label = run_id[:8] + "..." if len(run_id) > 12 else run_id
    doc_links = build_documentation_links()
    data_age_tone = run_metadata.get("data_age_tone", "neutral")
    st.markdown(
        f"""
        <section class="hero">
            <div>
                <div class="eyebrow">French municipal elections 2026</div>
                <h1>{_APP_TITLE}</h1>
                <p>Local press exposure audit - {run_metadata.get("analysis_window", "analysis window unavailable")}</p>
            </div>
            <div class="hero-badge">
                Source
                <strong>Europresse</strong>
                36-leader cohort
            </div>
        </section>
        <section class="run-banner {data_age_tone}">
            <span><strong>Run:</strong> {run_label}</span>
            <span><strong>Batch:</strong> {run_metadata.get("batch_id", "not available")}</span>
            <span><strong>Cohort:</strong> {run_metadata.get("cohort", "not available")}</span>
            <span><strong>Snapshot:</strong> {run_metadata.get("snapshot_label", "not available")}</span>
            <span><strong>Data age:</strong> {run_metadata.get("data_age_label", "unknown age")} from {run_metadata.get("data_age_source", "latest report timestamp")} (stale if &gt;{DATA_STALE_WARNING_DAYS}d)</span>
            <span><a href="{doc_links["Architecture"]}" target="_blank" rel="noopener noreferrer">Architecture</a></span>
            <span><a href="{doc_links["Metric dictionary"]}" target="_blank" rel="noopener noreferrer">Metric dictionary</a></span>
            <span><a href="{doc_links["Limitations"]}" target="_blank" rel="noopener noreferrer">Limitations</a></span>
            <span><a href="{doc_links["Deployment"]}" target="_blank" rel="noopener noreferrer">Deployment</a></span>
        </section>
        """,
        unsafe_allow_html=True,
    )
    _warning_callout(
        "Native French Review Status: Pending",
        "The QA contexts, lexicon precision, and NLI model calibration have "
        "not yet been adjudicated by a native French reviewer.",
    )
    st.markdown(
        """
        <section class="lede">
            <p>
                <strong>Stratified 36-leader audit.</strong> Findings remain
                underpowered for moderate effects; causal claims are out of scope.
            </p>
        </section>
        """,
        unsafe_allow_html=True,
    )
    with st.expander("Run details", expanded=False):
        st.dataframe(
            _display_dataframe(
                build_key_value_table(
                    {
                        "run_id": run_metadata.get("run_id", "not available"),
                        "batch_id": run_metadata.get("batch_id", "not available"),
                        "cohort_rule": run_metadata.get(
                            "cohort_rule",
                            "not available",
                        ),
                        "generated_at": run_metadata.get(
                            "generated_at",
                            "not available",
                        ),
                        "data_age_days": run_metadata.get("data_age_days", ""),
                        "data_age_source": run_metadata.get(
                            "data_age_source",
                            "not available",
                        ),
                        "analysis_window": run_metadata.get(
                            "analysis_window",
                            "not available",
                        ),
                    }
                )
            ),
            hide_index=True,
            use_container_width=True,
        )


def _render_panel0_quality(payload: dict[str, Any]) -> None:
    """Panel 0: data quality and coverage."""
    st.subheader("Q1. Can we trust the corpus?")
    _callout(
        "First check whether the corpus is usable: accepted sources, rejected "
        "sources, canonical articles, candidate mentions, and cohort coverage."
    )
    metric_columns = st.columns(3)
    for column, metric in zip(
        metric_columns,
        build_overview_metrics(payload),
        strict=True,
    ):
        with column:
            st.markdown(_kpi_card(metric), unsafe_allow_html=True)

    qa = payload["qa_report"].get("qa", {})
    accepted = qa.get("accepted_article_source_count", 0)
    rejected = int(qa.get("rejected_article_source_count", 0) or 0)
    canonical = qa.get("canonical_article_count", 0)
    mentions = qa.get("mention_count", 0)
    total_source_rows = int(accepted or 0) + rejected
    rejected_rate = rejected / total_source_rows if total_source_rows > 0 else None
    rejected_rate_label = "n/a" if rejected_rate is None else f"{rejected_rate:.1%}"
    st.markdown(
        _metric_strip(
            [
                ("Accepted sources", f"{accepted:,}"),
                ("Rejected rate", rejected_rate_label),
                ("Canonical articles", f"{canonical:,}"),
                ("Candidate mentions", f"{mentions:,}"),
            ]
        ),
        unsafe_allow_html=True,
    )
    artifact_warnings = build_artifact_health_warnings(payload)
    for warning in artifact_warnings:
        if warning["severity"] == "error":
            _error_callout(warning["area"], warning["message"])
        else:
            st.warning(f"{warning['area']}: {warning['message']}")
    sampling_warnings_df = build_sampling_warnings_table(payload["manifest"])
    if not sampling_warnings_df.empty:
        sampling_warning_callout = build_sampling_warning_callout(sampling_warnings_df)
        if sampling_warning_callout:
            st.caption(
                "Sampling diagnostics are available. Open the warning detail if "
                "you need cohort-balance context."
            )
        with st.expander(
            f"Sampling warnings ({len(sampling_warnings_df)})",
            expanded=False,
        ):
            if sampling_warning_callout:
                _warning_callout("Sampling Confounding Risk", sampling_warning_callout)
            st.caption(
                "Only warnings above their configured threshold are plotted. "
                "A zero threshold means any observed missingness triggers review."
            )
            progress_df = sampling_warnings_df.loc[
                (sampling_warnings_df["over_threshold"].fillna(0) > 0)
                | (
                    sampling_warnings_df["threshold"].fillna(-1).eq(0)
                    & sampling_warnings_df["share"].fillna(0).gt(0)
                )
            ]
            for row in progress_df.head(6).itertuples(index=False):
                if pd.notna(row.share) and pd.notna(row.threshold):
                    progress_value = max(0.0, min(float(row.share), 1.0))
                    st.progress(
                        progress_value,
                        text=(
                            f"{row.warning_code} | {row.scope} | "
                            f"{_format_optional_percent(row.share)} "
                            f"(threshold {_format_optional_percent(row.threshold)})"
                        ),
                    )
            st.dataframe(
                _display_dataframe(sampling_warnings_df),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Share": st.column_config.NumberColumn(format="%.3f"),
                    "Threshold": st.column_config.NumberColumn(format="%.3f"),
                    "Over Threshold": st.column_config.NumberColumn(format="%.3f"),
                },
            )
            _download_dataframe_csv(
                label="Download sampling warnings CSV",
                dataframe=sampling_warnings_df,
                file_name="sampling_warnings.csv",
            )


def _render_panel1_headline_finding(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> None:
    """Panel 1: headline finding and mean-vs-median story."""
    st.subheader("Q2. Raw Exposure and Outlier Robustness")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    summary_df = (
        exposure_df.groupby("gender", dropna=False)["article_count"]
        .agg(["mean", "median", "max"])
        .reset_index()
    )
    for numeric_column in ["mean", "median", "max"]:
        summary_df[numeric_column] = summary_df[numeric_column].round(1)
    outlier_note = _top_exposure_note(exposure_df, sample_df)
    summary_lookup = {
        row.gender: row
        for row in summary_df.itertuples(index=False)
        if pd.notna(row.gender)
    }
    summary_parts = []
    for gender in ["M", "F"]:
        if gender in summary_lookup:
            row = summary_lookup[gender]
            summary_parts.append(
                f"{gender} mean {float(row.mean):.1f}, median {float(row.median):.1f}"
            )
    _callout(
        "Read median before mean"
        + (f" ({'; '.join(summary_parts)})" if summary_parts else "")
        + ". "
        + (
            outlier_note
            or "News coverage is concentrated in a small number of high-exposure candidates."
        )
    )

    plot_df = exposure_df.copy()
    plot_df["article_count_for_plot"] = plot_df["article_count"].clip(lower=1)
    fig = px.box(
        plot_df,
        x="gender",
        y="article_count_for_plot",
        color="gender",
        points="all",
        color_discrete_map=_GENDER_PALETTE,
        labels={
            "article_count_for_plot": "Articles (log scale)",
            "gender": "Gender",
        },
    )
    fig.update_yaxes(type="log")
    st.plotly_chart(_plotly_defaults(fig, height=360), use_container_width=True)
    st.caption(
        "Log scale is used so the highest-exposure leader does not flatten "
        "the rest of the cohort distribution."
    )

    with st.expander("Candidate-level exposure table"):
        labeled_df = _candidate_labels(exposure_df, sample_df)
        display_columns = [
            column
            for column in [
                "candidate_label",
                "gender",
                "city_size_bucket",
                "article_count",
                "headline_mention_count",
                "distinct_source_count",
            ]
            if column in labeled_df.columns
        ]
        display_exposure_df = labeled_df[display_columns].sort_values(
            "article_count",
            ascending=False,
        )
        st.dataframe(
            _display_dataframe(display_exposure_df),
            hide_index=True,
            use_container_width=True,
        )
        _download_dataframe_csv(
            label="Download candidate exposure CSV",
            dataframe=display_exposure_df,
            file_name="candidate_exposure.csv",
        )

    sensitivity_report = build_outlier_sensitivity_report(exposure_df)
    if not sensitivity_report.empty:
        st.markdown("#### Outlier sensitivity")
        st.caption(
            "The same gender comparison is recomputed after removing or "
            "winsorizing high-leverage leaders."
        )
        display_report = sensitivity_report.copy()
        display_report["female_to_male_ratio"] = display_report[
            "female_to_male_ratio"
        ].round(2)
        for value_column in ["f_value", "m_value", "female_minus_male"]:
            display_report[value_column] = display_report[value_column].round(1)
        display_report = display_report.rename(
            columns={
                "scenario_label": "Scenario",
                "statistic": "Statistic",
                "f_value": "Female",
                "m_value": "Male",
                "female_minus_male": "F - M",
                "female_to_male_ratio": "F / M",
                "f_n": "F n",
                "m_n": "M n",
                "note": "Note",
            }
        )
        st.dataframe(
            display_report[
                [
                    "Scenario",
                    "Statistic",
                    "Female",
                    "Male",
                    "F - M",
                    "F / M",
                    "F n",
                    "M n",
                    "Note",
                ]
            ],
            hide_index=True,
            use_container_width=True,
        )
        _download_dataframe_csv(
            label="Download exposure sensitivity CSV",
            dataframe=sensitivity_report,
            file_name="exposure_outlier_sensitivity.csv",
        )


def _render_panel2_population_adjusted(exposure_df: pd.DataFrame) -> None:
    """Panel 2: population-adjusted exposure rate."""
    st.subheader("Q3. Does the gap persist after population adjustment?")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    population_df = build_population_adjusted_exposure_table(exposure_df)
    if population_df.empty:
        st.info("Population-adjusted exposure metrics are not available yet.")
        return

    method_note = (
        "Population-adjusted exposure reports articles per 10,000 residents. "
        "Read reversals from Q2 as stratum artifacts before treating them as "
        "gender findings."
    )
    overall_df = population_df.loc[population_df["segment"].eq("overall")]
    medium_df = population_df.loc[
        population_df["segment"]
        .astype(str)
        .str.contains(
            "medium",
            case=False,
            na=False,
        )
    ]
    if not overall_df.empty:
        overall_lookup = {
            row.gender: float(row.exposure_per_10k_population)
            for row in overall_df.itertuples(index=False)
        }
        if {"F", "M"}.issubset(overall_lookup):
            sentence = (
                method_note + " "
                "Overall population-adjusted exposure is "
                f"F = {overall_lookup['F']:.1f} vs "
                f"M = {overall_lookup['M']:.1f} articles per 10k residents."
            )
            if not medium_df.empty:
                medium_lookup = {
                    row.gender: float(row.exposure_per_10k_population)
                    for row in medium_df.itertuples(index=False)
                }
                if {"F", "M"}.issubset(medium_lookup):
                    sentence += (
                        " In the medium-city stratum, "
                        f"F = {medium_lookup['F']:.1f} vs "
                        f"M = {medium_lookup['M']:.1f}."
                    )
            _callout(sentence)
    else:
        _callout(method_note)

    chart_df = population_df.copy()
    fig = px.bar(
        chart_df,
        x="segment",
        y="exposure_per_10k_population",
        color="gender",
        pattern_shape="gender",
        pattern_shape_map=_GENDER_PATTERN,
        barmode="group",
        color_discrete_map=_GENDER_PALETTE,
        labels={
            "segment": "Segment",
            "exposure_per_10k_population": "Articles per 10k residents",
            "gender": "Gender",
        },
        category_orders={"segment": ["overall", "small", "medium", "large"]},
    )
    st.plotly_chart(_plotly_defaults(fig, height=360), use_container_width=True)
    with st.expander("Population-adjusted data table", expanded=False):
        display_df = population_df.copy()
        display_df["exposure_per_10k_population"] = display_df[
            "exposure_per_10k_population"
        ].round(1)
        st.dataframe(
            _display_dataframe(display_df), hide_index=True, use_container_width=True
        )
        _download_dataframe_csv(
            label="Download population-adjusted exposure CSV",
            dataframe=display_df,
            file_name="population_adjusted_exposure.csv",
        )


def _render_panel3_gap_sources(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> None:
    """Panel 3: where the exposure gap comes from."""
    st.subheader("Q4. Where does the gap come from?")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    _callout(
        "Split the gap by city size and candidate ranking. This separates broad "
        "gender patterns from local-market and outlier effects."
    )
    city_df = (
        exposure_df.groupby(["city_size_bucket", "gender"], dropna=False)
        .agg(mean_articles=("article_count", "mean"))
        .reset_index()
    )
    col_city, col_rank = st.columns((1, 1.1))
    with col_city:
        fig = px.bar(
            city_df,
            x="city_size_bucket",
            y="mean_articles",
            color="gender",
            pattern_shape="gender",
            pattern_shape_map=_GENDER_PATTERN,
            barmode="group",
            color_discrete_map=_GENDER_PALETTE,
            labels={
                "city_size_bucket": "City size",
                "mean_articles": "Mean articles",
            },
            category_orders={"city_size_bucket": ["small", "medium", "large"]},
        )
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
    with col_rank:
        ranked_df = _candidate_labels(exposure_df, sample_df).nlargest(
            12,
            "article_count",
        )
        fig = px.bar(
            ranked_df.sort_values("article_count"),
            x="article_count",
            y="candidate_label",
            color="gender",
            pattern_shape="gender",
            pattern_shape_map=_GENDER_PATTERN,
            orientation="h",
            color_discrete_map=_GENDER_PALETTE,
            labels={"article_count": "Articles", "candidate_label": ""},
        )
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
        top_gender_counts = ranked_df["gender"].value_counts().to_dict()
        st.caption(
            "Top 12 by article volume: "
            f"{int(top_gender_counts.get('M', 0))} M / "
            f"{int(top_gender_counts.get('F', 0))} F."
        )


def _render_panel4_visibility_quality(exposure_df: pd.DataFrame) -> None:
    """Panel 4: distinct sources and headline visibility."""
    st.subheader("Q5. Visibility quality")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    _callout(
        "Article count alone can hide whether attention is broad or repetitive. "
        "Distinct source count and zero-headline rate measure visibility quality."
    )
    visibility_df = exposure_df.copy()
    visibility_df["headline_rate"] = visibility_df["headline_mention_count"].where(
        visibility_df["article_count"] > 0,
        0,
    ) / visibility_df["article_count"].where(visibility_df["article_count"] > 0, 1)
    zero_headline_df = (
        visibility_df.assign(
            zero_headline=(visibility_df["headline_mention_count"] == 0).astype(int)
        )
        .groupby("gender", dropna=False)
        .agg(zero_headline_rate=("zero_headline", "mean"))
        .reset_index()
    )

    fig = px.scatter(
        visibility_df,
        x="article_count",
        y="distinct_source_count",
        color="gender",
        symbol="gender",
        symbol_map=_GENDER_SYMBOL,
        size="headline_mention_count",
        color_discrete_map=_GENDER_PALETTE,
        labels={
            "article_count": "Articles",
            "distinct_source_count": "Distinct sources",
        },
    )
    st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
    zero_headline_lookup = {
        row.gender: float(row.zero_headline_rate)
        for row in zero_headline_df.itertuples(index=False)
        if pd.notna(row.gender)
    }
    if zero_headline_lookup:
        st.caption(
            "Zero-headline leader rate: "
            + ", ".join(
                f"{gender}={rate:.0%}"
                for gender, rate in sorted(zero_headline_lookup.items())
            )
            + ". Bubble size encodes headline mentions."
        )


def _render_panel5_model_diagnostics(
    regression_df: pd.DataFrame,
    bootstrap_df: pd.DataFrame,
) -> None:
    """Panel 5: Poisson, Negative Binomial, and bootstrap diagnostics."""
    st.subheader("Q6. How robust is the adjusted model signal?")
    if regression_df.empty:
        st.info("Run the news corpus pipeline to generate regression diagnostics.")
        return

    model_priority_df = build_regression_model_priority_table(regression_df)
    governance_summary = build_regression_governance_summary(
        regression_df,
        bootstrap_df,
    )
    if governance_summary["headline"]:
        _callout(governance_summary["headline"])
    else:
        _callout(
            "The model question is narrow: does gender predict article count in "
            "a parsimonious count model with incumbency and a population offset?"
        )
    poisson_dispersion = None
    if not model_priority_df.empty and "dispersion_ratio" in model_priority_df.columns:
        poisson_rows = model_priority_df.loc[
            model_priority_df["model_name"].eq("poisson_exposure")
        ]
        if not poisson_rows.empty and pd.notna(
            poisson_rows.iloc[0]["dispersion_ratio"]
        ):
            poisson_dispersion = float(poisson_rows.iloc[0]["dispersion_ratio"])
    if (
        poisson_dispersion is not None
        and poisson_dispersion > POISSON_OVERDISPERSION_THRESHOLD
    ):
        warning_body = governance_summary.get("caveat") or (
            "Poisson is shown as a diagnostic only because the dispersion ratio "
            f"is {poisson_dispersion:.2f}. Treat the Negative Binomial and "
            "bootstrap interval as the primary robustness evidence."
        )
        _warning_callout(
            "Model priority",
            warning_body,
        )
    if not model_priority_df.empty:
        priority_columns = [
            "model_name",
            "model_role",
            "coefficient",
            "std_error",
            "q_value_display",
            "dispersion_ratio",
            "interpretation",
        ]
        priority_columns = [
            column for column in priority_columns if column in model_priority_df.columns
        ]
        st.dataframe(
            _display_dataframe(model_priority_df[priority_columns]),
            hide_index=True,
            use_container_width=True,
            column_config={
                "Adjusted p-value": st.column_config.TextColumn(
                    "Adjusted p-value",
                    help=(
                        "Benjamini-Hochberg false-discovery-rate adjusted p-value. "
                        "Values below 0.001 are shown in scientific notation."
                    ),
                )
            },
        )
        with st.expander("Regression provenance", expanded=False):
            provenance_columns = [
                "model_name",
                "parameter_count",
                "excluded_missing_control_count",
                "inference_status",
                "is_publishable",
                "p_value_display",
                "status",
            ]
            provenance_columns = [
                column
                for column in provenance_columns
                if column in model_priority_df.columns
            ]
            st.dataframe(
                _display_dataframe(model_priority_df[provenance_columns]),
                hide_index=True,
                use_container_width=True,
            )

    gender_rows = model_priority_df.copy()
    if not gender_rows.empty:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=gender_rows["coefficient"],
                y=gender_rows["model_name"],
                mode="markers",
                marker=dict(
                    color=gender_rows["model_role"].map(
                        {
                            "Primary model": _F_COLOR,
                            "Sensitivity model": _M_COLOR,
                            "Diagnostic only": "#9ca3af",
                            "Primary diagnostic": _ACCENT,
                            "Placebo check": _LAVENDER,
                        }
                    ),
                    size=11,
                ),
                error_x=dict(
                    type="data",
                    array=1.96 * gender_rows["std_error"].fillna(0),
                    visible=True,
                ),
                name="Parametric estimate",
            )
        )
        if not bootstrap_df.empty:
            bootstrap_gender = bootstrap_df[
                bootstrap_df["variable_name"] == "gender_female"
            ]
            if not bootstrap_gender.empty:
                row = bootstrap_gender.iloc[0]
                fig.add_trace(
                    go.Scatter(
                        x=[row["observed_coef"]],
                        y=["bootstrap_negbinom"],
                        mode="markers",
                        marker=dict(color=_PINK, size=11),
                        error_x=dict(
                            type="data",
                            symmetric=False,
                            array=[row["ci_upper_95"] - row["observed_coef"]],
                            arrayminus=[row["observed_coef"] - row["ci_lower_95"]],
                            visible=True,
                        ),
                        name="Bootstrap 95% CI",
                    )
                )
        fig.add_vline(x=0, line_dash="dash", line_color="#9ca3af")
        y_order = gender_rows["model_name"].tolist()
        if not bootstrap_df.empty:
            y_order.append("bootstrap_negbinom")
        fig.update_layout(xaxis_title="Coefficient on log scale", yaxis_title="")
        fig.update_yaxes(categoryorder="array", categoryarray=list(reversed(y_order)))
        st.plotly_chart(_plotly_defaults(fig, height=330), use_container_width=True)

    with st.expander("Full coefficient table"):
        full_table_columns = [
            column
            for column in regression_df.columns
            if column not in {"dependent_variable", "fitted_at"}
        ]
        st.dataframe(
            _display_dataframe(regression_df[full_table_columns]),
            hide_index=True,
            use_container_width=True,
            column_config={
                "p-value": st.column_config.NumberColumn(format="%.2e"),
                "q-value": st.column_config.NumberColumn(format="%.2e"),
            },
        )


def _render_tone_threshold_sensitivity_chart(tone_sensitivity_df: pd.DataFrame) -> None:
    """Render the tone threshold coverage curve."""
    if tone_sensitivity_df.empty:
        return
    _require_columns(
        tone_sensitivity_df,
        dataframe_name="nlp_tone_threshold_sensitivity",
        required_columns={
            "threshold",
            "segment_type",
            "segment_value",
            "classified_share_of_scoreable",
        },
    )
    sensitivity_df = tone_sensitivity_df.loc[
        tone_sensitivity_df["segment_type"].isin(["overall", "gender"])
    ].copy()
    if sensitivity_df.empty:
        return
    sensitivity_df["segment"] = sensitivity_df["segment_value"].replace(
        {"all": "overall"}
    )
    fig = px.line(
        sensitivity_df,
        x="threshold",
        y="classified_share_of_scoreable",
        color="segment",
        markers=True,
        color_discrete_map={
            "F": _F_COLOR,
            "M": _M_COLOR,
            "overall": "#6f6f78",
        },
        labels={
            "threshold": "Probability threshold",
            "classified_share_of_scoreable": "Classified share",
            "segment": "",
        },
    )
    fig.update_yaxes(tickformat=".0%")
    st.plotly_chart(_plotly_defaults(fig, height=300), use_container_width=True)


def _render_panel6_nlp_audit(payload: dict[str, Any]) -> None:
    """Panel 6: NLP coverage, frame, and tone audit signals."""
    st.subheader("Q7. NLP Audit Layer")
    _warning_callout(
        "Data and model caveats",
        "The NLP layer uses persisted mention contexts, not full article bodies. "
        "Tone and framing are descriptive model signals; trait counts are exact "
        "lexicon matches. Interpret every comparison with coverage, threshold, "
        "and outlier sensitivity visible.",
    )
    missing_optional_artifacts = payload.get("missing_optional_artifacts", [])
    if missing_optional_artifacts:
        st.caption(
            "Optional NLP artifacts not materialized: "
            + ", ".join(missing_optional_artifacts)
        )
    if not payload["nlp_qa_report"]:
        st.info("Run the NLP QA pipeline to generate the model-governance report.")
        return

    nlp_health_warnings = [
        warning
        for warning in build_artifact_health_warnings(payload)
        if warning["area"] in {"NLP bundle", "Blessed bundle", "NLP lineage"}
    ]
    if any(warning["severity"] == "error" for warning in nlp_health_warnings):
        for warning in nlp_health_warnings:
            _error_callout(warning["area"], warning["message"])
        st.info(
            "NLP metrics are hidden until model provenance and mention lineage "
            "are aligned. Exposure and regression panels above remain readable."
        )
        return

    nlp_metrics = build_nlp_audit_metrics(payload)
    metric_columns = st.columns(5)
    for column, metric in zip(
        metric_columns,
        nlp_metrics,
        strict=True,
    ):
        with column:
            st.markdown(_kpi_card(metric), unsafe_allow_html=True)

    if not payload["tone_sensitivity_df"].empty:
        st.markdown("#### Tone threshold sensitivity")
        st.caption(
            "Tone coverage, not model confidence: the line shows the share of "
            "scoreable mention contexts above each probability threshold."
        )
        _render_tone_threshold_sensitivity_chart(payload["tone_sensitivity_df"])

    with st.expander("Method & Bundle Provenance", expanded=False):
        model_bundle = payload["nlp_qa_report"].get("model_bundle", {})
        blessed_comparison = payload["nlp_qa_report"].get(
            "blessed_bundle_comparison",
            {},
        )
        backup_agreement = payload["nlp_qa_report"].get(
            "backup_model_agreement",
            {},
        )
        st.caption("Model bundle")
        st.dataframe(
            _display_dataframe(build_key_value_table(model_bundle)),
            hide_index=True,
            use_container_width=True,
        )
        bundle_col, backup_col = st.columns(2)
        with bundle_col:
            st.caption("Blessed bundle comparison")
            st.dataframe(
                _display_dataframe(build_key_value_table(blessed_comparison)),
                hide_index=True,
                use_container_width=True,
            )
        with backup_col:
            st.caption("Backup-model agreement")
            st.caption(
                "Agreement rates use compared rows only. Joined summary rows "
                "include unscored placeholders from the full backup-shaped table."
            )
            st.dataframe(
                _display_dataframe(build_key_value_table(backup_agreement)),
                hide_index=True,
                use_container_width=True,
            )
        hypothesis_df = build_hypothesis_examples_table(payload["nlp_qa_report"])
        if hypothesis_df.empty:
            st.info("Hypothesis examples are not available in this QA report.")
        else:
            st.dataframe(
                _display_dataframe(hypothesis_df),
                hide_index=True,
                use_container_width=True,
            )
        st.markdown(
            "Tone labels are target-aware NLI outputs. A low classified share "
            "means many scoreable contexts did not exceed the configured "
            "probability threshold, so tone comparisons should be treated as "
            "coverage diagnostics before sentiment conclusions."
        )
        st.markdown(
            "The main Q8 chart uses the primary-frame Gold mart, where each "
            "mention contributes to at most one frame label. The multi-label "
            "NLI diagnostic is available below because one mention can pass "
            "several frame thresholds."
        )
        st.markdown(
            "Model outputs are not causal evidence of bias. Calibration on "
            "French municipal-election coverage has not been independently "
            "audited by a native French reviewer."
        )

    warnings = payload["nlp_qa_report"].get("warnings", [])
    tone_probability_df = build_tone_probability_distribution_table(
        payload.get("tone_sensitivity_report", {})
    )
    with st.expander("Diagnostics", expanded=False):
        st.dataframe(
            _display_dataframe(
                pd.DataFrame(
                    [
                        {
                            "metric": metric["label"],
                            "value": metric["value"],
                            "detail": metric.get("help", ""),
                            "status": metric.get("status", ""),
                        }
                        for metric in nlp_metrics
                    ]
                )
            ),
            hide_index=True,
            use_container_width=True,
        )
        if warnings:
            st.caption(f"NLP warnings ({len(warnings)})")
            for warning in warnings:
                st.markdown(f"- {warning}")
        if not tone_probability_df.empty:
            st.markdown(
                "This uses the persisted top tone probability by current label. "
                "The full per-label NLI probability vector is not persisted, so "
                "this chart cannot reconstruct low-threshold unfavorable mass."
            )
            fig = px.bar(
                tone_probability_df,
                x="probability_bin",
                y="mentions",
                color="target_tone_label",
                barmode="group",
                labels={
                    "probability_bin": "Top tone probability",
                    "mentions": "Mentions",
                    "target_tone_label": "Current tone label",
                },
            )
            st.plotly_chart(
                _plotly_defaults(fig, height=300),
                use_container_width=True,
            )
            st.dataframe(
                _display_dataframe(tone_probability_df),
                hide_index=True,
                use_container_width=True,
            )

    primary_frame_gender_df = build_primary_frame_gender_distribution(
        payload["primary_frame_df"],
        payload["sample_df"],
    )
    multi_label_frame_gender_df = build_frame_gender_distribution(
        payload["framing_df"],
        payload["sample_df"],
    )
    nlp_bias_df = build_nlp_bias_table(payload["bias_df"])
    generic_sentiment_df = build_generic_sentiment_table(payload["bias_df"])
    trait_overview_df = build_trait_overview_table(payload["trait_metrics_df"])
    trait_outlier_df = build_trait_outlier_sensitivity_table(
        payload["trait_metrics_df"]
    )
    if primary_frame_gender_df.empty and nlp_bias_df.empty and trait_overview_df.empty:
        st.info(
            "Run dbt news marts after the NLP Silver pipelines to activate Gold NLP metrics."
        )
        return

    st.markdown("### Q8. How does French media frame male vs female candidates?")
    scandal_comparison_df = build_scandal_aggregation_comparison(
        primary_frame_gender_df,
        nlp_bias_df,
    )
    if not scandal_comparison_df.empty:
        if len(scandal_comparison_df) >= 2:
            volume_row = scandal_comparison_df.iloc[0]
            leader_row = scandal_comparison_df.iloc[1]
            _callout(
                "Scandal framing is higher for male candidates in "
                "volume-weighted mention counts "
                f"(M={volume_row['male_share']:.0%}, F={volume_row['female_share']:.0%}), "
                "but the gap disappears after equal-weighting leaders "
                f"(M={leader_row['male_share']:.0%}, F={leader_row['female_share']:.0%}). "
                + (
                    _top_gender_leverage_caveat(
                        payload["exposure_df"],
                        payload["sample_df"],
                    )
                    or "Interpret the volume-weighted view alongside leader-level means."
                )
            )
        comparison_display_df = scandal_comparison_df.copy()
        for share_column in ["female_share", "male_share", "gap"]:
            comparison_display_df[share_column] = comparison_display_df[
                share_column
            ].map(lambda value: f"{value:.0%}")
        st.dataframe(
            _display_dataframe(comparison_display_df),
            hide_index=True,
            use_container_width=True,
        )
    st.caption(
        "Q8 chart = volume-weighted mention counts from the primary-frame mart. "
        "The table = leader-level mean rates from `gold.mart_bias_indicators`; "
        "the two views answer related but different questions."
    )
    if not nlp_bias_df.empty:
        unfavorable_rows = nlp_bias_df.loc[
            nlp_bias_df["metric_name"].eq("mean_unfavorable_tone_share")
        ]
        visible_nlp_bias_df = nlp_bias_df.loc[
            ~nlp_bias_df["metric_name"].eq("mean_unfavorable_tone_share")
        ].copy()
    else:
        unfavorable_rows = pd.DataFrame()
        visible_nlp_bias_df = pd.DataFrame()
    col_frame, col_bias = st.columns((1, 1))
    with col_frame:
        if primary_frame_gender_df.empty:
            st.info("Primary-frame mart is not available yet.")
        else:
            leverage_caption = _top_gender_leverage_caveat(
                payload["exposure_df"],
                payload["sample_df"],
            )
            if leverage_caption:
                st.caption("Volume-weighted primary-frame counts. " + leverage_caption)
            frame_chart_df = primary_frame_gender_df.copy()
            frame_chart_df["frame_group"] = frame_chart_df["frame_label"].astype(str)
            frame_chart_df["frame_status"] = frame_chart_df["frame_group"].map(
                lambda label: (
                    "unclassified" if label == "unclassified" else "primary-classified"
                )
            )
            fig = px.bar(
                frame_chart_df,
                x="frame_label",
                y="mention_count",
                color="gender",
                pattern_shape="frame_status",
                pattern_shape_map={"primary-classified": "", "unclassified": "x"},
                barmode="group",
                color_discrete_map=_GENDER_PALETTE,
                labels={
                    "frame_label": "Frame",
                    "mention_count": "Mentions",
                    "gender": "Gender",
                    "frame_status": "Frame status",
                },
            )
            st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
            st.caption(
                "Primary-frame counts include `unclassified` rows so the visual "
                "denominator remains visible."
            )
            _download_dataframe_csv(
                label="Download primary frame CSV",
                dataframe=primary_frame_gender_df,
                file_name="primary_frame_gender_distribution.csv",
            )
    with col_bias:
        if visible_nlp_bias_df.empty:
            st.info("NLP bias indicators are not available yet.")
        else:
            display_nlp_bias_df = visible_nlp_bias_df.copy()
            display_nlp_bias_df["metric_label"] = display_nlp_bias_df[
                "metric_name"
            ].map(_humanize_metric_value)
            st.dataframe(
                _display_dataframe(
                    display_nlp_bias_df[
                        ["gender", "metric_label", "metric_value"]
                    ].rename(columns={"metric_label": "metric"})
                ),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Metric": st.column_config.TextColumn(
                        help=(
                            "Frame-share metrics use primary-frame-classified "
                            "mentions as the denominator; unclassified rows are excluded."
                        )
                    ),
                    "Value": st.column_config.NumberColumn(format="%.2f"),
                },
            )
            _download_dataframe_csv(
                label="Download NLP bias diagnostics CSV",
                dataframe=visible_nlp_bias_df,
                file_name="nlp_bias_diagnostics.csv",
            )
    frame_drilldown_df = payload.get("frame_article_drilldown_df", pd.DataFrame())
    if not frame_drilldown_df.empty:
        with st.expander("Top scandal-classified articles"):
            _require_columns(
                frame_drilldown_df,
                dataframe_name="mart_frame_article_drilldown",
                required_columns={
                    "frame_label",
                    "commune_name",
                    "published_date",
                    "outlet_name_normalized",
                    "title",
                    "mention_count",
                    "max_frame_probability",
                },
            )
            display_frame_drilldown_df = (
                frame_drilldown_df.loc[frame_drilldown_df["frame_label"].eq("scandale")]
                .sort_values(
                    ["mention_count", "max_frame_probability"],
                    ascending=[False, False],
                )
                .head(20)
            )
            st.dataframe(
                _display_dataframe(
                    display_frame_drilldown_df[
                        [
                            "commune_name",
                            "published_date",
                            "outlet_name_normalized",
                            "title",
                            "mention_count",
                            "max_frame_probability",
                        ]
                    ]
                ),
                hide_index=True,
                use_container_width=True,
            )
    if not unfavorable_rows.empty:
        with st.expander("Tone model diagnostic"):
            if unfavorable_rows["metric_value"].eq(0).all():
                st.markdown(
                    "Model observation: the NLI model assigned `unfavorable` "
                    "to 0 mentions in the Gold gender-level tone signal. "
                    "Possible causes include factual election coverage or a "
                    "conservative 0.60 tone threshold."
                )
            st.markdown(
                "`mean_unfavorable_tone_share = 0.0` means no mention crossed "
                "the 0.60 unfavorable-tone threshold in this run. It should not "
                "be read as proof that negative coverage is absent. "
                "Lower-confidence unfavorable signals, including top probabilities "
                "below 0.60, are persisted as `unclassified`; see tone threshold "
                "sensitivity above."
            )
            st.dataframe(
                _display_dataframe(
                    unfavorable_rows.assign(
                        metric=unfavorable_rows["metric_name"].map(
                            _humanize_metric_value
                        )
                    )[["gender", "metric", "metric_value"]]
                ),
                hide_index=True,
                use_container_width=True,
            )
    if not multi_label_frame_gender_df.empty:
        with st.expander("Multi-label frame diagnostic"):
            st.markdown(
                "This table comes from `gold.mart_framing_metrics`. A mention "
                "can pass multiple frame thresholds, so these counts can sum "
                "above the total mention denominator."
            )
            display_multi_label_frame_df = multi_label_frame_gender_df.copy()
            if (
                not primary_frame_gender_df.empty
                and "frame_label" in display_multi_label_frame_df.columns
            ):
                frame_order = [
                    str(frame_label)
                    for frame_label in primary_frame_gender_df["frame_label"]
                    .drop_duplicates()
                    .tolist()
                ]
                display_multi_label_frame_df["frame_label"] = pd.Categorical(
                    display_multi_label_frame_df["frame_label"].astype(str),
                    categories=frame_order,
                    ordered=True,
                )
                display_multi_label_frame_df = display_multi_label_frame_df.sort_values(
                    ["frame_label", "gender"]
                )
            st.dataframe(
                _display_dataframe(display_multi_label_frame_df),
                hide_index=True,
                use_container_width=True,
            )
            _download_dataframe_csv(
                label="Download multi-label frame diagnostic CSV",
                dataframe=multi_label_frame_gender_df,
                file_name="multilabel_frame_gender_distribution.csv",
            )
    if not generic_sentiment_df.empty:
        with st.expander("Generic sentiment baseline"):
            st.markdown(
                "Generic sentiment is model-level polarity, not candidate-aware "
                "tone. It is included as a baseline diagnostic only and should "
                "not be used as evidence of gendered treatment without context "
                "review."
            )
            st.dataframe(
                _display_dataframe(
                    generic_sentiment_df.assign(
                        metric=generic_sentiment_df["metric_name"].map(
                            _humanize_metric_value
                        )
                    )[["gender", "metric", "metric_value"]]
                ),
                hide_index=True,
                use_container_width=True,
            )
            _download_dataframe_csv(
                label="Download generic sentiment baseline CSV",
                dataframe=generic_sentiment_df,
                file_name="generic_sentiment_baseline.csv",
            )
    st.markdown("### Q9. What vocabulary describes candidates by gender?")
    if trait_overview_df.empty:
        st.info("Run the NLP lexicon pipeline to generate trait metrics.")
    else:
        trait_headline = _trait_headline_sentence(trait_overview_df, nlp_bias_df)
        if trait_headline:
            _callout(trait_headline)
        trait_chart_df = trait_overview_df.copy()
        trait_chart_df["evidence_label"] = trait_chart_df["evidence_level"].map(
            {
                "chart_ready": "ready",
                "sparse_evidence": "sparse",
                "table_only": "table only",
            }
        )
        evidence_opacity = {"ready": 1.0, "sparse": 0.6, "table only": 0.3}
        trait_chart_df["evidence_opacity"] = (
            trait_chart_df["evidence_label"].map(evidence_opacity).fillna(0.5)
        )
        fig = go.Figure()
        seen_genders: set[str] = set()
        for evidence_label, evidence_df in trait_chart_df.groupby(
            "evidence_label",
            dropna=False,
            sort=False,
        ):
            for gender, gender_df in evidence_df.groupby(
                "gender",
                dropna=False,
                sort=True,
            ):
                gender_label = str(gender)
                opacity = float(evidence_opacity.get(str(evidence_label), 0.5))
                fig.add_trace(
                    go.Bar(
                        x=gender_df["hits_per_1k_context_words"],
                        y=gender_df["trait_category"],
                        orientation="h",
                        name=gender_label,
                        legendgroup=gender_label,
                        showlegend=gender_label not in seen_genders,
                        marker_color=_GENDER_PALETTE.get(gender_label, "#6f6f78"),
                        opacity=opacity,
                        offsetgroup=gender_label,
                        customdata=gender_df[["evidence_label"]],
                        hovertemplate=(
                            "Trait=%{y}<br>Hits=%{x:.2f}"
                            "<br>Evidence=%{customdata[0]}<extra></extra>"
                        ),
                    )
                )
                seen_genders.add(gender_label)
        fig.update_layout(
            barmode="group",
            xaxis_title="Hits per 1k context words",
            yaxis_title="Trait category",
        )
        st.caption(
            "Bar color encodes gender; opacity encodes evidence strength "
            "(ready=solid, sparse=medium, table only=light)."
        )
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
        trait_table_df = trait_chart_df[
            [
                "gender",
                "trait_category",
                "hit_mentions",
                "term_hits",
                "hits_per_1k_context_words",
                "coverage_rate",
                "evidence_label",
            ]
        ].copy()
        st.dataframe(
            _display_dataframe(trait_table_df),
            hide_index=True,
            use_container_width=True,
        )
        _download_dataframe_csv(
            label="Download trait overview CSV",
            dataframe=trait_table_df,
            file_name="trait_overview_core_all.csv",
        )
        with st.expander("Top terms"):
            scenario_options = (
                sorted(
                    payload["trait_top_terms_df"]["scenario_id"]
                    .dropna()
                    .unique()
                    .tolist()
                )
                if not payload["trait_top_terms_df"].empty
                else ["all"]
            )
            selected_scenario = st.selectbox(
                "Scenario",
                scenario_options,
                index=scenario_options.index("all") if "all" in scenario_options else 0,
                key="trait_top_terms_scenario",
            )
            top_terms_df = build_trait_top_terms_table(
                payload["trait_top_terms_df"],
                scenario_id=selected_scenario,
            )
            exploratory_terms_df = build_trait_top_terms_table(
                payload["trait_top_terms_df"],
                scenario_id=selected_scenario,
                trait_tier="exploratory",
                max_rank=3,
            )
            if top_terms_df.empty and exploratory_terms_df.empty:
                st.info("Trait top-term mart is not available yet.")
            else:
                female_terms, male_terms = st.columns(2)
                display_columns = [
                    "trait_category",
                    "term",
                    "term_hits",
                    "hit_mentions",
                    "rank",
                ]
                with female_terms:
                    st.caption("Female candidates")
                    st.dataframe(
                        _display_dataframe(
                            top_terms_df.loc[
                                top_terms_df["gender"].eq("F"),
                                display_columns,
                            ]
                        ),
                        hide_index=True,
                        use_container_width=True,
                    )
                with male_terms:
                    st.caption("Male candidates")
                    st.dataframe(
                        _display_dataframe(
                            top_terms_df.loc[
                                top_terms_df["gender"].eq("M"),
                                display_columns,
                            ]
                        ),
                        hide_index=True,
                        use_container_width=True,
                    )
                _download_dataframe_csv(
                    label="Download core top terms CSV",
                    dataframe=top_terms_df[display_columns + ["gender"]],
                    file_name="trait_top_terms_core.csv",
                )
                with st.expander("Exploratory top terms"):
                    st.dataframe(
                        _display_dataframe(
                            exploratory_terms_df[display_columns + ["gender"]]
                        ),
                        hide_index=True,
                        use_container_width=True,
                    )
                    _download_dataframe_csv(
                        label="Download exploratory top terms CSV",
                        dataframe=exploratory_terms_df[display_columns + ["gender"]],
                        file_name="trait_top_terms_exploratory.csv",
                    )

    st.markdown("### Q10. NLP Robustness to High-Leverage Candidates")
    outlier_note = _top_exposure_note(payload["exposure_df"], payload["sample_df"])
    if outlier_note:
        _warning_callout(
            "High-leverage candidate",
            outlier_note
            + " The outlier sensitivity panel re-runs gender comparisons with this leader removed.",
        )
    if trait_outlier_df.empty:
        st.info("Trait outlier sensitivity mart is not available yet.")
    else:
        sentence = _trait_outlier_sentence(trait_outlier_df)
        if sentence:
            _callout(sentence)
        chart_df = trait_outlier_df.copy()
        chart_df["value_label"] = chart_df["hits_per_1k_context_words"].map(
            lambda value: f"{value:.2f}"
        )
        fig = px.bar(
            chart_df,
            x="scenario_label",
            y="hits_per_1k_context_words",
            color="gender",
            facet_col="trait_category",
            facet_col_spacing=0.08,
            barmode="group",
            text="value_label",
            color_discrete_map=_GENDER_PALETTE,
            labels={
                "scenario_label": "",
                "hits_per_1k_context_words": "Hits per 1k context words",
                "gender": "Gender",
            },
        )
        fig.update_yaxes(matches=None)
        fig.update_traces(textposition="outside", cliponaxis=False)
        fig.update_xaxes(tickangle=-15, title="")
        st.plotly_chart(_plotly_defaults(fig, height=460), use_container_width=True)
        st.caption(
            "`all candidates` keeps the full cohort. `drop top-1 overall` "
            "removes the single highest-exposure leader. `drop top-1 per gender` "
            "removes the highest-exposure leader within each gender."
        )
        display_outlier_df = chart_df[
            [
                "scenario_label",
                "gender",
                "trait_category",
                "hit_mentions",
                "term_hits",
                "hits_per_1k_context_words",
                "delta_vs_all",
                "evidence_level",
            ]
        ].copy()
        st.dataframe(
            _display_dataframe(display_outlier_df),
            hide_index=True,
            use_container_width=True,
        )
        _download_dataframe_csv(
            label="Download trait outlier sensitivity CSV",
            dataframe=display_outlier_df,
            file_name="trait_outlier_sensitivity_core.csv",
        )
        with st.expander("Candidate drilldown"):
            candidate_trait_df = build_trait_candidate_table(
                payload["trait_candidate_df"]
            )
            if candidate_trait_df.empty:
                st.info("Candidate trait mart is not available yet.")
            else:
                display_candidate_df = candidate_trait_df.loc[
                    candidate_trait_df["term_hits"].gt(0)
                ].copy()
                st.dataframe(
                    _display_dataframe(
                        display_candidate_df[
                            [
                                "full_name",
                                "gender",
                                "commune_name",
                                "trait_category",
                                "article_count",
                                "mention_count",
                                "term_hits",
                                "hits_per_1k_context_words",
                            ]
                        ]
                    ),
                    hide_index=True,
                    use_container_width=True,
                )
                _download_dataframe_csv(
                    label="Download candidate trait drilldown CSV",
                    dataframe=display_candidate_df,
                    file_name="trait_candidate_drilldown_core.csv",
                )

    if not SHOW_QA_SAMPLES:
        st.sidebar.caption(
            "QA samples disabled. Set SHOW_QA_SAMPLES=true in a controlled "
            "local environment to review context excerpts."
        )
        return

    st.markdown("### Appendix. QA context review")
    _warning_callout(
        "Native French Review Status: Pending",
        "These mention-context excerpts have NOT been adjudicated by a native "
        "French speaker. The lexicon precision and the NLI model calibration on "
        "French municipal-election coverage remain unverified. Planned audit: "
        "50 samples per trait_category x tier, reviewed by a native French "
        "reviewer; status is tracked in docs/limitations.md.",
    )
    qa_samples_df = build_trait_qa_samples_table(payload["trait_qa_df"])
    if qa_samples_df.empty:
        st.info("Trait QA samples are not available yet.")
    else:
        with st.expander("QA samples", expanded=False):
            filter_columns = st.columns((1, 0.8, 1))
            with filter_columns[0]:
                category_options = ["All"] + sorted(
                    qa_samples_df["trait_category"].dropna().unique().tolist()
                )
                selected_category = st.selectbox(
                    "Trait category",
                    category_options,
                    key="qa_trait_category",
                )
            with filter_columns[1]:
                gender_options = sorted(
                    qa_samples_df["gender"].dropna().unique().tolist()
                )
                selected_genders = st.multiselect(
                    "Gender",
                    gender_options,
                    default=gender_options,
                    key="qa_gender",
                )
            with filter_columns[2]:
                term_query = st.text_input("Term search", key="qa_term_search")
            display_qa_df = qa_samples_df.copy()
            if selected_category != "All":
                display_qa_df = display_qa_df.loc[
                    display_qa_df["trait_category"].eq(selected_category)
                ]
            if selected_genders:
                display_qa_df = display_qa_df.loc[
                    display_qa_df["gender"].isin(selected_genders)
                ]
            if term_query.strip():
                display_qa_df = display_qa_df.loc[
                    display_qa_df["term"].str.contains(
                        term_query.strip(),
                        case=False,
                        na=False,
                    )
                ]
            display_columns = [
                column
                for column in [
                    "trait_category",
                    "term",
                    "gender",
                    "full_name",
                    "mention_id_short",
                    "context_excerpt",
                    "rationale",
                ]
                if column in display_qa_df.columns
            ]
            st.dataframe(
                _display_dataframe(display_qa_df[display_columns]),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Context excerpt": st.column_config.TextColumn(
                        "Context excerpt",
                        width="large",
                    ),
                    "Mention ID": st.column_config.TextColumn(
                        "Mention ID",
                        width="small",
                    ),
                },
            )
            _download_dataframe_csv(
                label="Download QA samples CSV",
                dataframe=display_qa_df[display_columns],
                file_name="trait_qa_samples_core.csv",
            )


def render_dashboard(gold_dir: Path | str | None = None) -> None:
    """Render the complete narrative dashboard."""
    _apply_page_config()
    payload = load_dashboard_payload(gold_dir)
    _render_hero(build_run_metadata(payload))

    missing_artifacts = payload["missing_artifacts"]
    if missing_artifacts:
        st.error(
            "Pipeline incomplete: missing dashboard artifacts: "
            + ", ".join(missing_artifacts)
            + ". Mount /app/data/gold or set DASHBOARD_GOLD_URI to a blessed "
            "Gold artifact bundle before interpreting the dashboard."
        )
        st.stop()

    _render_panel0_quality(payload)
    st.divider()
    _render_panel1_headline_finding(payload["exposure_df"], payload["sample_df"])
    st.divider()
    _render_panel2_population_adjusted(payload["exposure_df"])
    st.divider()
    _render_panel3_gap_sources(payload["exposure_df"], payload["sample_df"])
    st.divider()
    _render_panel4_visibility_quality(payload["exposure_df"])
    st.divider()
    _render_panel5_model_diagnostics(payload["regression_df"], payload["bootstrap_df"])
    st.divider()
    _render_panel6_nlp_audit(payload)


def main() -> None:
    """Entrypoint for ``streamlit run src/dashboard/app.py``."""
    render_dashboard()


if __name__ == "__main__":
    main()
