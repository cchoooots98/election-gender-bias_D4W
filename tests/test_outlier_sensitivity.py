"""Tests for outlier sensitivity exposure metrics."""

from __future__ import annotations

import pandas as pd
import pytest

from src.metrics.news.outlier_sensitivity import build_outlier_sensitivity_report


def test_build_outlier_sensitivity_report_returns_expected_scenarios():
    """Happy path: all documented sensitivity scenarios should be reproducible."""
    exposure_metrics = pd.DataFrame(
        [
            {"leader_id": "leader-f1", "gender": "F", "article_count": 10},
            {"leader_id": "leader-f2", "gender": "F", "article_count": 30},
            {"leader_id": "leader-m1", "gender": "M", "article_count": 20},
            {"leader_id": "leader-m2", "gender": "M", "article_count": 100},
        ]
    )

    report = build_outlier_sensitivity_report(
        exposure_metrics,
        winsor_upper_quantile=0.75,
    )
    report_by_scenario = report.set_index("scenario_id")

    assert report["scenario_id"].tolist() == [
        "all",
        "drop_top_overall",
        "drop_top_each_gender",
        "winsorized_mean",
        "median",
    ]
    assert report_by_scenario.loc["all", "f_value"] == pytest.approx(20.0)
    assert report_by_scenario.loc["all", "m_value"] == pytest.approx(60.0)
    assert report_by_scenario.loc["drop_top_overall", "f_value"] == pytest.approx(20.0)
    assert report_by_scenario.loc["drop_top_overall", "m_value"] == pytest.approx(20.0)
    assert report_by_scenario.loc["drop_top_each_gender", "f_value"] == pytest.approx(
        10.0
    )
    assert report_by_scenario.loc["drop_top_each_gender", "m_value"] == pytest.approx(
        20.0
    )
    assert report_by_scenario.loc["winsorized_mean", "m_value"] == pytest.approx(33.75)
    assert report_by_scenario.loc["median", "female_minus_male"] == pytest.approx(-40.0)


def test_build_outlier_sensitivity_report_returns_empty_schema_for_empty_input():
    """Boundary: missing Gold artifacts should not crash the dashboard helper."""
    report = build_outlier_sensitivity_report(pd.DataFrame())

    assert report.empty
    assert report.columns.tolist() == [
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


def test_build_outlier_sensitivity_report_raises_on_schema_drift():
    """Error path: missing required columns must fail before rendering."""
    exposure_metrics = pd.DataFrame([{"leader_id": "leader-001", "gender": "F"}])

    with pytest.raises(KeyError, match="article_count"):
        build_outlier_sensitivity_report(exposure_metrics)


@pytest.mark.parametrize(
    ("exposure_metrics", "error_pattern"),
    [
        (
            pd.DataFrame([{"gender": "F", "article_count": None}]),
            "must not contain null",
        ),
        (
            pd.DataFrame([{"gender": "F", "article_count": -1}]),
            "must be non-negative",
        ),
    ],
)
def test_build_outlier_sensitivity_report_raises_on_invalid_values(
    exposure_metrics: pd.DataFrame,
    error_pattern: str,
):
    """Error path: invalid exposure values would make the report misleading."""
    with pytest.raises(ValueError, match=error_pattern):
        build_outlier_sensitivity_report(exposure_metrics)


def test_build_outlier_sensitivity_report_raises_on_invalid_winsor_quantile():
    """Boundary: winsorization thresholds must stay in probability space."""
    exposure_metrics = pd.DataFrame([{"gender": "F", "article_count": 1}])

    with pytest.raises(ValueError, match="winsor_upper_quantile"):
        build_outlier_sensitivity_report(
            exposure_metrics,
            winsor_upper_quantile=1.5,
        )


def test_drop_top_overall_removes_only_the_single_highest_candidate():
    """Regression: drop-top-overall must not behave like drop-top-each-gender."""
    exposure_metrics = pd.DataFrame(
        [
            {"gender": "F", "article_count": 10},
            {"gender": "F", "article_count": 80},
            {"gender": "M", "article_count": 20},
            {"gender": "M", "article_count": 100},
        ]
    )

    report = build_outlier_sensitivity_report(exposure_metrics).set_index("scenario_id")

    assert report.loc["drop_top_overall", "f_n"] == 2
    assert report.loc["drop_top_overall", "m_n"] == 1
    assert report.loc["drop_top_each_gender", "f_n"] == 1
    assert report.loc["drop_top_each_gender", "m_n"] == 1
