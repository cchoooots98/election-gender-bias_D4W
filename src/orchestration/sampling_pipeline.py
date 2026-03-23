"""Runnable orchestration for the official-data sampling slice.

This module intentionally stops at the cohort-construction stage:
Bronze official-data ingest → Silver dimensions → Gold sample_leaders cohort.
The later GDELT, NLP, and mart stages remain future work and must not be
misrepresented as implemented by this runner.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import requests

from src.config.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    RAW_DIR,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.candidates import ingest_candidates, ingest_candidates_tour2
from src.ingest.geography import ingest_geography
from src.ingest.incumbents import ingest_incumbents
from src.ingest.seats import ingest_seats_population
from src.observability.run_logger import log_pipeline_run
from src.transform.dim_candidate import build_dim_candidate_leader
from src.transform.dim_commune import build_dim_commune
from src.transform.sampling import build_sample

logger = logging.getLogger(__name__)

_FLOW_NAME = "sampling_pipeline"


@dataclass(frozen=True)
class SamplingPipelineResult:
    """Summary of one sampling pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def _count_parquet_rows(parquet_path: Path) -> int:
    """Read the row count from Parquet metadata without loading the full file."""
    return pq.read_metadata(parquet_path).num_rows


def _append_artifact(artifact_paths: list[Path], artifact_path: Path) -> None:
    """Append an artifact path once while preserving creation order."""
    if artifact_path not in artifact_paths:
        artifact_paths.append(artifact_path)


def run_sampling_pipeline(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> SamplingPipelineResult:
    """Run the currently implemented Bronze → Silver → Gold sampling slice.

    Args:
        raw_dir: Root raw-data directory.
        bronze_dir: Root bronze-data directory.
        silver_dir: Root silver-data directory.
        gold_dir: Root gold-data directory.
        duckdb_path: DuckDB warehouse path.

    Returns:
        Summary object with run metadata and artifact paths.

    Raises:
        Exception: Re-raises any required-step failure after logging meta_run.
    """
    run_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)
    artifact_paths: list[Path] = []
    rows_ingested = 0
    error_count = 0
    status = "success"

    try:
        geography_path = ingest_geography(raw_dir=raw_dir, bronze_dir=bronze_dir)
        _append_artifact(artifact_paths, geography_path)
        rows_ingested += _count_parquet_rows(geography_path)

        seats_path = ingest_seats_population(raw_dir=raw_dir, bronze_dir=bronze_dir)
        _append_artifact(artifact_paths, seats_path)
        rows_ingested += _count_parquet_rows(seats_path)

        candidates_path = ingest_candidates(raw_dir=raw_dir, bronze_dir=bronze_dir)
        _append_artifact(artifact_paths, candidates_path)
        rows_ingested += _count_parquet_rows(candidates_path)

        incumbents_path = ingest_incumbents(raw_dir=raw_dir, bronze_dir=bronze_dir)
        _append_artifact(artifact_paths, incumbents_path)
        rows_ingested += _count_parquet_rows(incumbents_path)

        try:
            tour2_path = ingest_candidates_tour2(raw_dir=raw_dir, bronze_dir=bronze_dir)
            _append_artifact(artifact_paths, tour2_path)
            rows_ingested += _count_parquet_rows(tour2_path)
        except (requests.RequestException, OSError, ValueError) as exc:
            error_count += 1
            status = "partial"
            logger.warning(
                "Optional Tour 2 ingest failed run_id=%s — continuing without a "
                "fresh Tour 2 bronze file: %s",
                run_id,
                exc,
            )

        dim_commune_df = build_dim_commune(
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
        )
        rows_ingested += len(dim_commune_df)
        _append_artifact(artifact_paths, silver_dir / "dim_commune.parquet")

        dim_candidate_df = build_dim_candidate_leader(
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
            include_tour2_flag=True,
        )
        rows_ingested += len(dim_candidate_df)
        _append_artifact(artifact_paths, silver_dir / "dim_candidate_leader.parquet")

        sample_df = build_sample(
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
            pipeline_run_id=run_id,
        )
        rows_ingested += len(sample_df)
        _append_artifact(artifact_paths, gold_dir / "sample_leaders.parquet")
        _append_artifact(artifact_paths, gold_dir / "sample_manifest.json")

    except Exception:
        status = "failed"
        error_count += 1
        logger.exception("Sampling pipeline failed run_id=%s", run_id)
        raise
    finally:
        end_ts = datetime.now(UTC)
        log_pipeline_run(
            run_id=run_id,
            flow_name=_FLOW_NAME,
            start_ts=start_ts,
            end_ts=end_ts,
            status=status,
            rows_ingested=rows_ingested,
            error_count=error_count,
            artifact_paths=artifact_paths,
            duckdb_path=duckdb_path,
        )

    return SamplingPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
