"""Tests for the Streamlit dashboard helpers."""

from __future__ import annotations

import json

import pandas as pd

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
    manifest = {"triggered_warnings": [{"warning_code": "bloc"}]}
    qa_report = {"qa": {"zero_coverage_leader_count": 1}}

    sample_df.to_parquet(tmp_path / "sample_leaders.parquet", index=False)
    exposure_df.to_parquet(tmp_path / "mart_exposure_metrics.parquet", index=False)
    regression_df.to_parquet(tmp_path / "mart_regression_results.parquet", index=False)
    (tmp_path / "sample_manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    (tmp_path / "news_corpus_qa_report.json").write_text(
        json.dumps(qa_report),
        encoding="utf-8",
    )

    payload = load_dashboard_payload(tmp_path)
    metrics = {
        metric["label"]: metric["value"] for metric in build_overview_metrics(payload)
    }

    assert metrics["Sampled Leaders"] == "3"
    assert metrics["Covered Leaders"] == "2"
    assert metrics["Sampling Warnings"] == "1"
    assert metrics["Regression Issues"] == "1"
    assert metrics["Zero Coverage"] == "1"
