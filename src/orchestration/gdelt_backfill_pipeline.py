"""Runnable orchestration for low-frequency GDELT historical backfill."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from src.config.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    PROJECT_ROOT,
    RAW_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.pipeline import run_gdelt_ingest
from src.observability.run_logger import log_pipeline_run

logger = logging.getLogger(__name__)

_FLOW_NAME = "gdelt_backfill_pipeline"
_DEFAULT_SAMPLE_MANIFEST = PROJECT_ROOT / "data" / "gold" / "sample_manifest.json"


@dataclass(frozen=True)
class GdeltBackfillPipelineResult:
    """Summary of one low-frequency GDELT backfill execution."""

    run_id: str
    status: str
    error_count: int
    query_count: int
    hit_count: int
    artifact_paths: tuple[str, ...]


def run_gdelt_backfill_pipeline(
    sample_manifest_path: Path = _DEFAULT_SAMPLE_MANIFEST,
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> GdeltBackfillPipelineResult:
    """Run GDELT-only historical backfill and record pipeline observability.

    Args:
        sample_manifest_path: Candidate cohort manifest produced by the sampling slice.
        raw_dir: Root raw-data directory.
        bronze_dir: Root bronze-data directory.
        gold_dir: Root gold-data directory. Accepted for orchestration symmetry.
        duckdb_path: DuckDB warehouse path.

    Returns:
        Summary object with run metadata and persisted artifact paths.

    Raises:
        Exception: Re-raises any backfill failure after logging ``meta.meta_run``.
    """
    del gold_dir

    run_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)
    status = "failed"
    error_count = 1
    query_count = 0
    hit_count = 0
    artifact_paths: list[Path] = []

    try:
        backfill_result = run_gdelt_ingest(
            sample_manifest_path=sample_manifest_path,
            raw_dir=raw_dir,
            bronze_dir=bronze_dir,
            duckdb_path=duckdb_path,
        )
        status = backfill_result.status
        error_count = backfill_result.error_count
        query_count = backfill_result.query_count
        hit_count = backfill_result.hit_count
        artifact_paths = [Path(path) for path in backfill_result.artifact_paths]
        return GdeltBackfillPipelineResult(
            run_id=run_id,
            status=status,
            error_count=error_count,
            query_count=query_count,
            hit_count=hit_count,
            artifact_paths=tuple(str(path) for path in artifact_paths),
        )
    except Exception:
        logger.exception("GDELT backfill pipeline failed run_id=%s", run_id)
        raise
    finally:
        log_pipeline_run(
            run_id=run_id,
            flow_name=_FLOW_NAME,
            start_ts=start_ts,
            end_ts=datetime.now(UTC),
            status=status,
            rows_ingested=hit_count,
            error_count=error_count,
            artifact_paths=artifact_paths,
            duckdb_path=duckdb_path,
        )
