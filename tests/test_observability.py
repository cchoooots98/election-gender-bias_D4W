"""Tests for shared observability contracts."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import pytest

from src.observability import run_logger


def _base_meta_run_kwargs(tmp_path) -> dict[str, object]:
    """Return valid log_pipeline_run_safely arguments for unit tests."""
    return {
        "run_id": "run-001",
        "flow_name": "unit_test_pipeline",
        "start_ts": datetime(2026, 4, 1, 10, 0, tzinfo=UTC),
        "end_ts": datetime(2026, 4, 1, 10, 1, tzinfo=UTC),
        "status": "success",
        "rows_ingested": 1,
        "error_count": 0,
        "artifact_paths": [tmp_path / "artifact.parquet"],
        "duckdb_path": tmp_path / "warehouse.duckdb",
        "pipeline_logger": logging.getLogger("tests.observability"),
    }


def test_log_pipeline_run_safely_returns_run_id_on_success(monkeypatch, tmp_path):
    """Happy path: the shared helper returns the run id from meta logging."""

    def _log_success(**kwargs):
        return str(kwargs["run_id"])

    monkeypatch.setattr(run_logger, "log_pipeline_run", _log_success)

    run_id = run_logger.log_pipeline_run_safely(
        **_base_meta_run_kwargs(tmp_path),
        original_error=None,
    )

    assert run_id == "run-001"


def test_log_pipeline_run_safely_preserves_original_error(monkeypatch, tmp_path):
    """Regression: meta logging failures must not hide the root cause."""

    def _raise_meta_error(**_kwargs):
        raise RuntimeError("meta_run unavailable")

    monkeypatch.setattr(run_logger, "log_pipeline_run", _raise_meta_error)

    result = run_logger.log_pipeline_run_safely(
        **_base_meta_run_kwargs(tmp_path),
        original_error=FileNotFoundError("missing input"),
    )

    assert result is None


def test_log_pipeline_run_safely_raises_meta_error_after_success(
    monkeypatch,
    tmp_path,
):
    """Regression: successful pipeline bodies must surface meta failures."""

    def _raise_meta_error(**_kwargs):
        raise RuntimeError("meta_run unavailable")

    monkeypatch.setattr(run_logger, "log_pipeline_run", _raise_meta_error)

    with pytest.raises(RuntimeError, match="meta_run unavailable"):
        run_logger.log_pipeline_run_safely(
            **_base_meta_run_kwargs(tmp_path),
            original_error=None,
        )
