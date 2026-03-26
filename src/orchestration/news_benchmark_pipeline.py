"""Runnable orchestration for the multi-source news benchmark slice.

This runner materializes the benchmark artifacts only:
committed benchmark cases -> provider discovery -> benchmark summary outputs.
It intentionally does not claim that the downstream NLP or mart layers are
already integrated with hybrid discovery.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.config.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    NEWS_BENCHMARK_PROVIDER_ORDER,
    PROJECT_ROOT,
    RAW_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.benchmark import BenchmarkRunResult, run_news_benchmark
from src.observability.run_logger import log_pipeline_run

logger = logging.getLogger(__name__)

_FLOW_NAME = "news_benchmark_pipeline"
_DEFAULT_BENCHMARK_MANIFEST = PROJECT_ROOT / "docs" / "news_source_benchmark_cases.yaml"


def run_news_benchmark_pipeline(
    benchmark_manifest_path: Path = _DEFAULT_BENCHMARK_MANIFEST,
    provider_order: tuple[str, ...] = NEWS_BENCHMARK_PROVIDER_ORDER,
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> BenchmarkRunResult:
    """Run the benchmark slice and record pipeline-level observability.

    Args:
        benchmark_manifest_path: YAML file containing benchmark candidates and
            manually curated truth articles.
        provider_order: Ordered provider list, typically Tier 1 -> Tier 2 -> Tier 3.
        raw_dir: Root raw-data directory.
        bronze_dir: Root bronze-data directory.
        gold_dir: Root gold-data directory.
        duckdb_path: DuckDB warehouse path.

    Returns:
        Summary object with run status, row counts, metrics, and artifact paths.

    Raises:
        Exception: Re-raises any benchmark failure after logging ``meta.meta_run``.
    """
    run_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)
    status = "failed"
    error_count = 1
    results_row_count = 0
    artifact_paths: list[Path] = []

    try:
        benchmark_result = run_news_benchmark(
            benchmark_manifest_path=benchmark_manifest_path,
            provider_order=provider_order,
            raw_dir=raw_dir,
            bronze_dir=bronze_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
            pipeline_run_id=run_id,
        )
        status = benchmark_result.status
        error_count = benchmark_result.error_count
        results_row_count = benchmark_result.results_row_count
        artifact_paths = [Path(path) for path in benchmark_result.artifact_paths]
        return benchmark_result
    except Exception:
        logger.exception("News benchmark pipeline failed run_id=%s", run_id)
        raise
    finally:
        log_pipeline_run(
            run_id=run_id,
            flow_name=_FLOW_NAME,
            start_ts=start_ts,
            end_ts=datetime.now(UTC),
            status=status,
            rows_ingested=results_row_count,
            error_count=error_count,
            artifact_paths=artifact_paths,
            duckdb_path=duckdb_path,
        )
