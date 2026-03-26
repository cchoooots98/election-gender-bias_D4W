"""Tests for the low-frequency GDELT backfill entrypoints and policy artifacts."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import yaml

from src.cli import run_gdelt_backfill as cli_module
from src.orchestration import gdelt_backfill_pipeline


def test_news_source_policy_shortlist_yaml_is_loadable():
    """Docs contract: the shortlist should stay machine-readable for future tooling."""
    shortlist_path = Path("docs/news_source_policy_shortlist.yaml")

    with open(shortlist_path, encoding="utf-8") as file_handle:
        payload = yaml.safe_load(file_handle)

    assert "provider_policy" in payload
    assert "outlet_shortlist" in payload
    assert any(
        row["recommended_role"] == "production_candidate"
        for row in payload["outlet_shortlist"]
    )


def test_gdelt_backfill_pipeline_logs_meta_run_and_returns_runner_result(
    tmp_path,
    monkeypatch,
):
    """Happy path: orchestration wrapper should log meta_run with the generated run id."""
    logged_runs: list[dict[str, object]] = []

    monkeypatch.setattr(
        gdelt_backfill_pipeline,
        "run_gdelt_ingest",
        lambda **kwargs: SimpleNamespace(
            status="partial",
            error_count=1,
            query_count=24,
            hit_count=7,
            artifact_paths=[str(tmp_path / "bronze" / "gdelt.parquet")],
        ),
    )
    monkeypatch.setattr(
        gdelt_backfill_pipeline,
        "log_pipeline_run",
        lambda **kwargs: logged_runs.append(kwargs),
    )

    result = gdelt_backfill_pipeline.run_gdelt_backfill_pipeline(
        sample_manifest_path=tmp_path / "sample_manifest.json",
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
    )

    assert result.status == "partial"
    assert result.query_count == 24
    assert result.hit_count == 7
    assert logged_runs[0]["flow_name"] == "gdelt_backfill_pipeline"
    assert logged_runs[0]["run_id"] == result.run_id
    assert logged_runs[0]["rows_ingested"] == 7


def test_gdelt_backfill_cli_main_returns_zero_on_success(monkeypatch):
    """Happy path: CLI should convert successful runs into exit code 0."""
    monkeypatch.setattr(
        cli_module,
        "run_gdelt_backfill_pipeline",
        lambda: SimpleNamespace(
            status="success",
            run_id="run-456",
            query_count=24,
            hit_count=9,
            artifact_paths=[
                "data/bronze/news_search_results/provider=gdelt/part-0.parquet"
            ],
        ),
    )

    assert cli_module.main() == 0


def test_gdelt_backfill_cli_main_returns_one_on_failure(monkeypatch):
    """Error path: CLI should convert raised exceptions into exit code 1."""
    monkeypatch.setattr(
        cli_module,
        "run_gdelt_backfill_pipeline",
        lambda: (_ for _ in ()).throw(ValueError("gdelt backfill failed")),
    )

    assert cli_module.main() == 1


def test_gdelt_backfill_script_wrapper_reuses_cli_main():
    """Compatibility: the legacy script wrapper should point to the installable CLI."""
    from scripts import run_gdelt_backfill as script_wrapper

    assert script_wrapper.main is cli_module.main


def test_gdelt_backfill_dag_file_imports_without_airflow():
    """Smoke test: the DAG file should stay importable even when Airflow is absent."""
    dag_path = Path("airflow/dags/gdelt_backfill_dag.py")
    spec = importlib.util.spec_from_file_location("gdelt_backfill_dag", dag_path)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert hasattr(module, "gdelt_backfill_dag")
