"""Gold marts and data-quality gates for the enterprise news corpus."""

from __future__ import annotations

import logging
import warnings
from datetime import UTC, datetime

import numpy as np
import pandas as pd

try:
    import statsmodels.api as sm
except ImportError:  # pragma: no cover - depends on local environment
    sm = None

from src.config.settings import DQ_MAX_NULL_RATE
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

_FRAME_LABELS = (
    "politique",
    "vie_privee",
    "apparence",
    "scandale",
    "personnalite",
    "securite",
    "unclassified",
)
_EXPOSURE_COLUMNS = [
    "leader_id",
    "gender",
    "commune_insee",
    "city_size_bucket",
    "reg_code",
    "nuance_group",
    "is_incumbent",
    "won_final_round",
    "population",
    "article_count",
    "headline_mention_count",
    "distinct_source_count",
    "restricted_source_article_count",
    "supplemental_source_article_count",
    "exposure_per_10k_population",
]
_FRAMING_COLUMNS = [
    "leader_id",
    "frame_label",
    "mention_count",
    "mean_frame_score",
]
_BIAS_COLUMNS = ["gender", "metric_name", "metric_value"]
_REGRESSION_FEATURE_COLUMNS = [
    "leader_id",
    "gender",
    "gender_female",
    "commune_insee",
    "city_size_bucket",
    "reg_code",
    "nuance_group",
    "is_incumbent",
    "won_final_round",
    "population",
    "article_count",
    "headline_mention_count",
    "distinct_source_count",
    "restricted_source_article_count",
    "supplemental_source_article_count",
    "exposure_per_10k_population",
]
_REGRESSION_RESULT_COLUMNS = [
    "model_name",
    "dependent_variable",
    "variable_name",
    "coefficient",
    "std_error",
    "p_value",
    "status",
    "sample_size",
    "fitted_at",
]


def _build_regression_design_matrix(modeling_df: pd.DataFrame) -> pd.DataFrame:
    """Build the documented regression controls in one deterministic place."""
    exog_df = pd.DataFrame(
        {
            "const": 1.0,
            "gender_female": modeling_df["gender_female"].astype(float),
            "is_incumbent": modeling_df["is_incumbent"].astype(float),
            "won_final_round": modeling_df["won_final_round"].astype(float),
            "bucket_large": (modeling_df["city_size_bucket"] == "large").astype(float),
            "bucket_medium": (modeling_df["city_size_bucket"] == "medium").astype(
                float
            ),
            "restricted_source_article_count": modeling_df[
                "restricted_source_article_count"
            ].astype(float),
            "supplemental_source_article_count": modeling_df[
                "supplemental_source_article_count"
            ].astype(float),
        }
    )

    nuance_categories = sorted(
        {
            str(value)
            for value in modeling_df["nuance_group"].dropna().astype(str).tolist()
            if str(value).strip()
        }
    )
    if len(nuance_categories) > 1:
        nuance_dummies = pd.get_dummies(
            pd.Categorical(modeling_df["nuance_group"], categories=nuance_categories),
            prefix="nuance_group",
            drop_first=True,
            dtype=float,
        )
        exog_df = pd.concat([exog_df, nuance_dummies], axis=1)

    region_categories = sorted(
        {
            str(value)
            for value in modeling_df["reg_code"].dropna().astype(str).tolist()
            if str(value).strip()
        }
    )
    if len(region_categories) > 1:
        region_dummies = pd.get_dummies(
            pd.Categorical(modeling_df["reg_code"], categories=region_categories),
            prefix="reg_code",
            drop_first=True,
            dtype=float,
        )
        exog_df = pd.concat([exog_df, region_dummies], axis=1)

    return exog_df


def _build_unfitted_regression_rows(
    *,
    exog_df: pd.DataFrame,
    status: str,
    sample_size: int,
    fitted_at: str,
) -> pd.DataFrame:
    """Return auditable model rows even when coefficients cannot be estimated."""
    result_rows = []
    for variable_name in exog_df.columns:
        result_rows.append(
            {
                "model_name": "poisson_exposure",
                "dependent_variable": "article_count",
                "variable_name": variable_name,
                "coefficient": None,
                "std_error": None,
                "p_value": None,
                "status": status,
                "sample_size": sample_size,
                "fitted_at": fitted_at,
            }
        )
    return pd.DataFrame(result_rows, columns=_REGRESSION_RESULT_COLUMNS)


def build_mart_exposure_metrics(
    sample_leaders_df: pd.DataFrame,
    fact_article_df: pd.DataFrame,
    fact_mention_df: pd.DataFrame,
    dim_commune_df: pd.DataFrame,
) -> pd.DataFrame:
    """Aggregate canonical article coverage into the leader-level exposure mart."""
    if sample_leaders_df.empty:
        return pd.DataFrame(columns=_EXPOSURE_COLUMNS)

    sample_df = sample_leaders_df.copy()
    for optional_control_column in ("is_incumbent", "won_final_round"):
        if optional_control_column not in sample_df.columns:
            sample_df[optional_control_column] = pd.NA
    population_df = dim_commune_df[["commune_insee", "population"]].copy()
    sample_df = sample_df.merge(
        population_df,
        on="commune_insee",
        how="left",
        validate="many_to_one",
    )

    if fact_mention_df.empty or fact_article_df.empty:
        for metric_column in (
            "article_count",
            "headline_mention_count",
            "distinct_source_count",
            "restricted_source_article_count",
            "supplemental_source_article_count",
            "exposure_per_10k_population",
        ):
            sample_df[metric_column] = 0
        return sample_df[_EXPOSURE_COLUMNS].copy()

    mention_article_df = fact_mention_df.merge(
        fact_article_df[
            [
                "canonical_article_id",
                "outlet_name_normalized",
                "rights_class",
                "acquisition_methods",
            ]
        ],
        on="canonical_article_id",
        how="left",
        validate="many_to_one",
    )
    mention_article_df["supplemental_flag"] = (
        mention_article_df["acquisition_methods"]
        .fillna("")
        .str.contains("supplemental")
    )
    mention_article_df["restricted_flag"] = (
        mention_article_df["rights_class"].fillna("") == "restricted_local"
    )

    aggregated_df = (
        mention_article_df.groupby("leader_id", dropna=False)
        .agg(
            article_count=("canonical_article_id", "nunique"),
            headline_mention_count=("headline_mention_flag", "sum"),
            distinct_source_count=("outlet_name_normalized", "nunique"),
            restricted_source_article_count=("restricted_flag", "sum"),
            supplemental_source_article_count=("supplemental_flag", "sum"),
        )
        .reset_index()
    )

    mart_df = sample_df.merge(
        aggregated_df,
        on="leader_id",
        how="left",
        validate="one_to_one",
    )
    fill_zero_columns = [
        "article_count",
        "headline_mention_count",
        "distinct_source_count",
        "restricted_source_article_count",
        "supplemental_source_article_count",
    ]
    mart_df[fill_zero_columns] = mart_df[fill_zero_columns].fillna(0).astype(int)
    mart_df["exposure_per_10k_population"] = np.where(
        mart_df["population"].fillna(0) > 0,
        mart_df["article_count"] / (mart_df["population"] / 10_000),
        0.0,
    )
    return mart_df[_EXPOSURE_COLUMNS].copy()


def build_mart_framing_metrics(
    sample_leaders_df: pd.DataFrame,
    fact_mention_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build a stable leader-by-frame matrix even before NLP scores are present."""
    if sample_leaders_df.empty:
        return pd.DataFrame(columns=_FRAMING_COLUMNS)

    cross_join_df = (
        sample_leaders_df[["leader_id"]]
        .assign(_join_key=1)
        .merge(
            pd.DataFrame({"frame_label": list(_FRAME_LABELS), "_join_key": 1}),
            on="_join_key",
            how="inner",
        )
        .drop(columns="_join_key")
    )

    if fact_mention_df.empty:
        cross_join_df["mention_count"] = 0
        cross_join_df["mean_frame_score"] = 0.0
        return cross_join_df[_FRAMING_COLUMNS].copy()

    available_mentions = fact_mention_df.copy()
    available_mentions["frame_label"] = available_mentions["frame_label"].fillna(
        "unclassified"
    )
    aggregated_df = (
        available_mentions.groupby(["leader_id", "frame_label"], dropna=False)
        .agg(
            mention_count=("mention_id", "count"),
            mean_frame_score=("frame_score", "mean"),
        )
        .reset_index()
    )
    mart_df = cross_join_df.merge(
        aggregated_df,
        on=["leader_id", "frame_label"],
        how="left",
        validate="one_to_one",
    )
    mart_df["mention_count"] = mart_df["mention_count"].fillna(0).astype(int)
    mart_df["mean_frame_score"] = pd.to_numeric(
        mart_df["mean_frame_score"],
        errors="coerce",
    ).fillna(0.0)
    return mart_df[_FRAMING_COLUMNS].copy()


def build_mart_bias_indicators(mart_exposure_metrics_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate exposure metrics to the gender level for quick bias summaries."""
    if mart_exposure_metrics_df.empty:
        return pd.DataFrame(columns=_BIAS_COLUMNS)

    metric_specs = {
        "mean_article_count": "article_count",
        "mean_distinct_source_count": "distinct_source_count",
        "mean_exposure_per_10k_population": "exposure_per_10k_population",
        "total_headline_mention_count": "headline_mention_count",
    }
    rows = []
    for gender, group_df in mart_exposure_metrics_df.groupby("gender", dropna=False):
        for metric_name, column_name in metric_specs.items():
            if metric_name.startswith("total_"):
                metric_value = float(group_df[column_name].sum())
            else:
                metric_value = float(group_df[column_name].mean())
            rows.append(
                {
                    "gender": gender,
                    "metric_name": metric_name,
                    "metric_value": metric_value,
                }
            )
    return pd.DataFrame(rows, columns=_BIAS_COLUMNS)


def build_mart_regression_feature_base(
    sample_leaders_df: pd.DataFrame,
    mart_exposure_metrics_df: pd.DataFrame,
) -> pd.DataFrame:
    """Create the stable leader-level modeling base used by regression runs."""
    if mart_exposure_metrics_df.empty:
        return pd.DataFrame(columns=_REGRESSION_FEATURE_COLUMNS)

    sample_df = sample_leaders_df.copy()
    for optional_control_column in ("is_incumbent", "won_final_round"):
        if optional_control_column not in sample_df.columns:
            sample_df[optional_control_column] = pd.NA

    feature_df = mart_exposure_metrics_df.merge(
        sample_df[
            [
                "leader_id",
                "gender",
                "commune_insee",
                "city_size_bucket",
                "reg_code",
                "nuance_group",
                "is_incumbent",
                "won_final_round",
            ]
        ],
        on=[
            "leader_id",
            "gender",
            "commune_insee",
            "city_size_bucket",
            "reg_code",
            "nuance_group",
            "is_incumbent",
            "won_final_round",
        ],
        how="left",
        validate="one_to_one",
    )
    feature_df["gender_female"] = (feature_df["gender"] == "F").astype(int)
    feature_df["is_incumbent"] = (
        feature_df["is_incumbent"].astype("boolean").fillna(False).astype(int)
    )
    feature_df["won_final_round"] = (
        feature_df["won_final_round"].astype("boolean").fillna(False).astype(int)
    )
    return feature_df[_REGRESSION_FEATURE_COLUMNS].copy()


def build_mart_regression_results(
    regression_feature_base_df: pd.DataFrame,
) -> pd.DataFrame:
    """Fit a Poisson exposure model with documented analytical controls."""
    fitted_at = datetime.now(UTC).isoformat()
    if regression_feature_base_df.empty:
        return pd.DataFrame(columns=_REGRESSION_RESULT_COLUMNS)

    modeling_df = regression_feature_base_df.copy()
    exog_df = _build_regression_design_matrix(modeling_df)

    if sm is None:
        return _build_unfitted_regression_rows(
            exog_df=exog_df,
            status="not_fitted_missing_statsmodels",
            sample_size=len(modeling_df),
            fitted_at=fitted_at,
        )

    if modeling_df["article_count"].sum() == 0:
        return _build_unfitted_regression_rows(
            exog_df=exog_df,
            status="not_fitted_zero_articles",
            sample_size=len(modeling_df),
            fitted_at=fitted_at,
        )

    exposure_offset = np.log(modeling_df["population"].clip(lower=1).astype(float))

    try:
        model = sm.GLM(
            modeling_df["article_count"].astype(float),
            exog_df,
            family=sm.families.Poisson(),
            offset=exposure_offset,
        )
        with warnings.catch_warnings(record=True) as fit_warnings:
            warnings.simplefilter("always")
            fit_result = model.fit()
    except Exception as exc:  # pragma: no cover - exercised by runtime failure
        logger.warning("Poisson regression fit failed error=%r", exc)
        return _build_unfitted_regression_rows(
            exog_df=exog_df,
            status=f"fit_failed:{type(exc).__name__}",
            sample_size=len(modeling_df),
            fitted_at=fitted_at,
        )

    warning_types = sorted({type(warning.message).__name__ for warning in fit_warnings})
    fit_status = (
        "fitted"
        if not warning_types
        else f"fitted_with_warning:{','.join(warning_types)}"
    )
    if warning_types:
        logger.warning(
            "Poisson regression fit completed with warnings types=%s",
            warning_types,
        )

    result_rows = []
    for variable_name, coefficient in fit_result.params.items():
        result_rows.append(
            {
                "model_name": "poisson_exposure",
                "dependent_variable": "article_count",
                "variable_name": variable_name,
                "coefficient": float(coefficient),
                "std_error": float(fit_result.bse.get(variable_name, np.nan)),
                "p_value": float(fit_result.pvalues.get(variable_name, np.nan)),
                "status": fit_status,
                "sample_size": len(modeling_df),
                "fitted_at": fitted_at,
            }
        )
    return pd.DataFrame(result_rows, columns=_REGRESSION_RESULT_COLUMNS)


def run_news_corpus_quality_checks(
    *,
    sample_leaders_df: pd.DataFrame,
    fact_article_source_df: pd.DataFrame,
    fact_article_source_rejected_df: pd.DataFrame,
    fact_article_df: pd.DataFrame,
    fact_mention_df: pd.DataFrame,
    mart_exposure_metrics_df: pd.DataFrame,
    mart_regression_results_df: pd.DataFrame,
) -> dict[str, object]:
    """Validate the main contracts before the pipeline is considered successful."""
    if fact_article_source_df.empty:
        raise DataQualityError("fact_article_source is empty after normalization")

    critical_source_columns = [
        "article_source_id",
        "title_normalized",
        "body_text_hash",
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
    if rejected_ratio > max(DQ_MAX_NULL_RATE, 0.25):
        raise DataQualityError(
            f"fact_article_source rejected ratio {rejected_ratio:.1%} exceeds threshold "
            f"{max(DQ_MAX_NULL_RATE, 0.25):.1%}"
        )

    regression_warning_statuses = sorted(
        {
            str(status)
            for status in mart_regression_results_df.get("status", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .tolist()
            if str(status).startswith("fitted_with_warning:")
        }
    )
    regression_failure_statuses = sorted(
        {
            str(status)
            for status in mart_regression_results_df.get("status", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .tolist()
            if not str(status).startswith("fitted")
        }
    )

    return {
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
    }
