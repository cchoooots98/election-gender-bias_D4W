"""Tests for the Streamlit dashboard helpers."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.dashboard.app import build_overview_metrics, load_dashboard_payload


def test_load_dashboard_payload_reports_missing_artifacts(tmp_path):
    """Boundary: dashboard loader should stay informative when artifacts are absent."""
    payload = load_dashboard_payload(tmp_path)

    assert payload["sample_df"].empty
    assert payload["exposure_df"].empty
    assert payload["regression_df"].empty
    assert set(payload["missing_artifacts"]) == {
        "sample_leaders",
        "mart_exposure_metrics",
        "mart_regression_results",
        "mart_bootstrap_ci",
        "mart_analysis_summary",
        "sample_manifest",
        "news_corpus_qa_report",
    }


def test_build_overview_metrics_aggregates_materialized_artifacts(tmp_path):
    """Happy path: dashboard metrics should summarize the persisted gold artifacts."""
    sample_df = pd.DataFrame(
        [
            {"leader_id": "leader-001"},
            {"leader_id": "leader-002"},
            {"leader_id": "leader-003"},
        ]
    )
    exposure_df = pd.DataFrame(
        [
            {"leader_id": "leader-001", "article_count": 2},
            {"leader_id": "leader-002", "article_count": 0},
            {"leader_id": "leader-003", "article_count": 1},
        ]
    )
    regression_df = pd.DataFrame(
        [
            {"status": "fitted"},
            {"status": "fitted_with_warning:RuntimeWarning"},
        ]
    )
    bootstrap_df = pd.DataFrame(columns=["variable_name"])
    analysis_df = pd.DataFrame(columns=["analysis_id"])
    manifest = {"triggered_warnings": [{"warning_code": "bloc"}]}
    qa_report = {"qa": {"zero_coverage_leader_count": 1}}

    sample_df.to_parquet(tmp_path / "sample_leaders.parquet", index=False)
    exposure_df.to_parquet(tmp_path / "mart_exposure_metrics.parquet", index=False)
    regression_df.to_parquet(tmp_path / "mart_regression_results.parquet", index=False)
    bootstrap_df.to_parquet(tmp_path / "mart_bootstrap_ci.parquet", index=False)
    analysis_df.to_parquet(tmp_path / "mart_analysis_summary.parquet", index=False)
    (tmp_path / "sample_manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    (tmp_path / "news_corpus_qa_report.json").write_text(
        json.dumps(qa_report),
        encoding="utf-8",
    )

    payload = load_dashboard_payload(tmp_path)
    assert payload["missing_artifacts"] == []

    metrics = {
        metric["label"]: metric["value"] for metric in build_overview_metrics(payload)
    }

    assert metrics["Sampled Leaders"] == "3"
    assert metrics["Covered Leaders"] == "2"
    assert metrics["Sampling Warnings"] == "1"
    assert metrics["Regression Issues"] == "1"
    assert metrics["Zero Coverage"] == "1"


def test_build_overview_metrics_raises_on_exposure_schema_drift():
    """Regression: present-but-invalid exposure artifacts must fail fast."""
    payload = {
        "sample_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "exposure_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "regression_df": pd.DataFrame([{"status": "fitted"}]),
        "manifest": {},
        "qa_report": {"qa": {}},
    }

    with pytest.raises(KeyError, match="article_count"):
        build_overview_metrics(payload)


def test_build_overview_metrics_raises_on_regression_schema_drift():
    """Regression: present-but-invalid regression artifacts must fail fast."""
    payload = {
        "sample_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "exposure_df": pd.DataFrame([{"article_count": 1}]),
        "regression_df": pd.DataFrame([{"model_name": "poisson_exposure"}]),
        "manifest": {},
        "qa_report": {"qa": {}},
    }

    with pytest.raises(KeyError, match="status"):
        build_overview_metrics(payload)
