"""Outlier sensitivity metrics for the news exposure analysis."""

from __future__ import annotations

import numpy as np
import pandas as pd

_FEMALE_GROUP = "F"
_MALE_GROUP = "M"
_SENSITIVITY_REPORT_COLUMNS = [
    "scenario_id",
    "scenario_label",
    "statistic",
    "f_value",
    "m_value",
    "female_minus_male",
    "female_to_male_ratio",
    "f_n",
    "m_n",
    "note",
]


def _validate_exposure_metrics(
    exposure_metrics: pd.DataFrame,
    *,
    value_column: str,
    group_column: str,
    winsor_upper_quantile: float,
) -> pd.DataFrame:
    """Return a validated copy of the exposure fields needed for sensitivity."""
    if not 0 < winsor_upper_quantile <= 1:
        raise ValueError("winsor_upper_quantile must be greater than 0 and at most 1")

    missing_columns = sorted(
        {value_column, group_column} - set(exposure_metrics.columns)
    )
    if missing_columns:
        raise KeyError(
            "exposure metrics missing required columns: " + ", ".join(missing_columns)
        )

    sensitivity_base = exposure_metrics[[group_column, value_column]].copy()
    if sensitivity_base[[group_column, value_column]].isna().any().any():
        raise ValueError("outlier sensitivity inputs must not contain null values")

    sensitivity_base[value_column] = pd.to_numeric(
        sensitivity_base[value_column],
        errors="raise",
    )
    if (sensitivity_base[value_column] < 0).any():
        raise ValueError("outlier sensitivity values must be non-negative")

    return sensitivity_base


def _gender_value(
    grouped_values: pd.Series,
    group_value: str,
) -> float:
    """Return a gender value or NaN when a segment is absent."""
    if group_value not in grouped_values.index:
        return float("nan")
    return float(grouped_values.loc[group_value])


def _safe_ratio(numerator: float, denominator: float) -> float:
    """Return a finite ratio when possible, otherwise NaN."""
    if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
        return float("nan")
    return float(numerator / denominator)


def _summarize_scenario(
    sensitivity_base: pd.DataFrame,
    *,
    scenario_id: str,
    scenario_label: str,
    statistic: str,
    value_column: str,
    group_column: str,
    note: str,
) -> dict[str, object]:
    """Aggregate one outlier scenario into dashboard-ready F/M columns."""
    grouped_values = sensitivity_base.groupby(group_column, dropna=False)[value_column]
    if statistic == "mean":
        metric_values = grouped_values.mean()
    elif statistic == "median":
        metric_values = grouped_values.median()
    else:
        raise ValueError(f"Unsupported statistic: {statistic}")

    group_counts = grouped_values.count()
    female_value = _gender_value(metric_values, _FEMALE_GROUP)
    male_value = _gender_value(metric_values, _MALE_GROUP)

    return {
        "scenario_id": scenario_id,
        "scenario_label": scenario_label,
        "statistic": statistic,
        "f_value": female_value,
        "m_value": male_value,
        "female_minus_male": float(female_value - male_value),
        "female_to_male_ratio": _safe_ratio(female_value, male_value),
        "f_n": int(group_counts.get(_FEMALE_GROUP, 0)),
        "m_n": int(group_counts.get(_MALE_GROUP, 0)),
        "note": note,
    }


def build_outlier_sensitivity_report(
    exposure_metrics: pd.DataFrame,
    *,
    value_column: str = "article_count",
    group_column: str = "gender",
    winsor_upper_quantile: float = 0.95,
) -> pd.DataFrame:
    """Build mean/median exposure sensitivity scenarios by gender.

    Args:
        exposure_metrics: One-row-per-leader exposure table, typically
            ``gold.mart_exposure_metrics``.
        value_column: Numeric exposure column to audit.
        group_column: Gender segment column. The report publishes F and M
            columns because the analytical cohort is designed as a binary
            50/50 gender comparison.
        winsor_upper_quantile: Upper quantile used to cap high values for the
            winsorized mean scenario. The cap is computed across the full
            cohort so both gender segments share the same threshold.

    Returns:
        DataFrame with one row per sensitivity scenario and dashboard-ready
        gender columns.

    Raises:
        KeyError: If required input columns are missing.
        ValueError: If the quantile is invalid or the metric contains null,
            non-numeric, or negative values.
    """
    if exposure_metrics.empty:
        return pd.DataFrame(columns=_SENSITIVITY_REPORT_COLUMNS)

    sensitivity_base = _validate_exposure_metrics(
        exposure_metrics,
        value_column=value_column,
        group_column=group_column,
        winsor_upper_quantile=winsor_upper_quantile,
    )

    top_overall_index = sensitivity_base[value_column].idxmax()
    drop_top_overall = sensitivity_base.drop(index=top_overall_index)

    top_each_gender_indexes = (
        sensitivity_base.groupby(group_column, dropna=False)[value_column]
        .idxmax()
        .tolist()
    )
    drop_top_each_gender = sensitivity_base.drop(index=top_each_gender_indexes)

    # Robustness check: cap the high tail at one shared threshold so the
    # comparison does not change because each gender received a different cap.
    winsor_cap = float(sensitivity_base[value_column].quantile(winsor_upper_quantile))
    winsorized_base = sensitivity_base.copy()
    winsorized_base[value_column] = np.minimum(
        winsorized_base[value_column],
        winsor_cap,
    )

    report_rows = [
        _summarize_scenario(
            sensitivity_base,
            scenario_id="all",
            scenario_label="All candidates",
            statistic="mean",
            value_column=value_column,
            group_column=group_column,
            note="Arithmetic mean across the full sampled cohort.",
        ),
        _summarize_scenario(
            drop_top_overall,
            scenario_id="drop_top_overall",
            scenario_label="Drop top overall",
            statistic="mean",
            value_column=value_column,
            group_column=group_column,
            note="Mean after removing the single highest-exposure leader overall.",
        ),
        _summarize_scenario(
            drop_top_each_gender,
            scenario_id="drop_top_each_gender",
            scenario_label="Drop top each gender",
            statistic="mean",
            value_column=value_column,
            group_column=group_column,
            note="Mean after removing the highest-exposure leader within each gender.",
        ),
        _summarize_scenario(
            winsorized_base,
            scenario_id="winsorized_mean",
            scenario_label="Winsorized mean",
            statistic="mean",
            value_column=value_column,
            group_column=group_column,
            note=(
                "Mean after capping values at the cohort "
                f"{winsor_upper_quantile:.0%} percentile."
            ),
        ),
        _summarize_scenario(
            sensitivity_base,
            scenario_id="median",
            scenario_label="Median",
            statistic="median",
            value_column=value_column,
            group_column=group_column,
            note="Median across the full sampled cohort.",
        ),
    ]

    return pd.DataFrame(report_rows, columns=_SENSITIVITY_REPORT_COLUMNS)
