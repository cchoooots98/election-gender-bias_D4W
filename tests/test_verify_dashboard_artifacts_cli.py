"""Tests for dashboard artifact readiness verification."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pytest

from src.cli.verify_dashboard_artifacts import main, verify_dashboard_artifacts


def _write_dashboard_artifacts(gold_dir: Path, *, sample_rows: int = 36) -> None:
    """Write minimal dashboard artifacts for readiness tests."""
    gold_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [{"leader_id": f"leader-{index:02d}"} for index in range(sample_rows)]
    ).to_parquet(gold_dir / "sample_leaders.parquet", index=False)
    for parquet_name in [
        "mart_exposure_metrics.parquet",
        "mart_regression_results.parquet",
        "mart_bootstrap_ci.parquet",
        "mart_analysis_summary.parquet",
    ]:
        pd.DataFrame([{"status": "ok"}]).to_parquet(gold_dir / parquet_name)
    for json_name in ["sample_manifest.json", "news_corpus_qa_report.json"]:
        (gold_dir / json_name).write_text(
            json.dumps({"status": "ok"}),
            encoding="utf-8",
        )


def test_verify_dashboard_artifacts_returns_summary(tmp_path):
    """Happy path: complete artifacts should produce a readiness summary."""
    _write_dashboard_artifacts(tmp_path)

    summary = verify_dashboard_artifacts(tmp_path)

    assert summary.artifact_count == 7
    assert summary.sample_leader_count == 36
    assert summary.warning_count == 0


def test_verify_dashboard_artifacts_raises_on_missing_required_file(tmp_path):
    """Error path: missing dashboard artifacts should fail fast."""
    _write_dashboard_artifacts(tmp_path)
    (tmp_path / "mart_analysis_summary.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="mart_analysis_summary.parquet"):
        verify_dashboard_artifacts(tmp_path)


def test_verify_dashboard_artifacts_raises_on_sample_size_mismatch(tmp_path):
    """Regression: the dashboard readiness check locks the 36-leader cohort."""
    _write_dashboard_artifacts(tmp_path, sample_rows=35)

    with pytest.raises(RuntimeError, match="sample_leaders row-count mismatch"):
        verify_dashboard_artifacts(tmp_path)


def test_verify_dashboard_artifacts_returns_cache_only_warning(tmp_path):
    """Regression: non-fatal web-cache-only runs stay visible."""
    _write_dashboard_artifacts(tmp_path)
    (tmp_path / "news_corpus_qa_report.json").write_text(
        json.dumps(
            {
                "qa": {
                    "warnings": [
                        "Web enrichment ran in cache-only mode: queued URL rows were handled."
                    ]
                }
            }
        ),
        encoding="utf-8",
    )

    summary = verify_dashboard_artifacts(tmp_path)

    assert summary.warning_count == 1
    assert "cache-only" in summary.warnings[0]


def test_verify_dashboard_artifacts_raises_on_stale_regression(tmp_path):
    """Regression: stale dependent model artifacts must fail readiness."""
    _write_dashboard_artifacts(tmp_path)
    os.utime(tmp_path / "mart_regression_results.parquet", (1, 1))

    with pytest.raises(RuntimeError, match="older than"):
        verify_dashboard_artifacts(tmp_path)


def test_verify_dashboard_artifacts_raises_on_nlp_lineage_mismatch(tmp_path):
    """Regression: stale NLP input artifacts must fail readiness checks."""
    _write_dashboard_artifacts(tmp_path)
    (tmp_path / "news_corpus_qa_report.json").write_text(
        json.dumps({"qa": {"mention_count": 3}}),
        encoding="utf-8",
    )
    (tmp_path / "nlp_qa_report.json").write_text(
        json.dumps({"input_coverage": {"total_mentions": 4}}),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="NLP input row count"):
        verify_dashboard_artifacts(tmp_path)


def test_verify_dashboard_artifacts_cli_returns_status_codes(tmp_path):
    """CLI path: readiness command returns shell-friendly exit codes."""
    _write_dashboard_artifacts(tmp_path)

    assert main(["--gold-dir", str(tmp_path)]) == 0
    assert main(["--gold-dir", str(tmp_path / "missing")]) == 1
