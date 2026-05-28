"""Runnable orchestration for the backup NLI agreement sample."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp.backup_agreement import (
    DEFAULT_BACKUP_RANDOM_SEED,
    DEFAULT_BACKUP_SAMPLE_SIZE,
    materialize_backup_summary_sample,
)
from src.observability.run_logger import log_pipeline_run_safely

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_backup_agreement_pipeline"


@dataclass(frozen=True)
class NlpBackupAgreementPipelineResult:
    """Summary of one backup agreement pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_backup_agreement_pipeline(
    nlp_input_path: Path = SILVER_DIR / "fact_mention_nlp_input.parquet",
    nlp_summary_path: Path = SILVER_DIR / "fact_mention_nlp_summary.parquet",
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    output_path: Path = GOLD_DIR / "nlp_backup_summary_sample.parquet",
    duckdb_path: Path = WAREHOUSE_PATH,
    sample_size: int = DEFAULT_BACKUP_SAMPLE_SIZE,
    random_seed: int = DEFAULT_BACKUP_RANDOM_SEED,
) -> NlpBackupAgreementPipelineResult:
    """Run the governed backup-model sample and log meta_run metadata.

    Args:
        nlp_input_path: Phase 0 NLP input Parquet artifact.
        nlp_summary_path: Primary NLP summary Parquet artifact.
        sample_leaders_path: Gold sample leaders artifact with candidate names.
        output_path: Backup summary sample Parquet output path.
        duckdb_path: DuckDB warehouse path for ``meta.meta_run``.
        sample_size: Maximum scoreable mentions to score.
        random_seed: Deterministic sampling seed.

    Returns:
        Pipeline summary with artifact paths.
    """
    run_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)
    status = "failed"
    rows_ingested = 0
    error_count = 1
    artifact_paths: list[Path] = []
    original_error: Exception | None = None

    try:
        nlp_input_dataframe = pd.read_parquet(nlp_input_path)
        nlp_summary_dataframe = pd.read_parquet(nlp_summary_path)
        sample_leaders_dataframe = pd.read_parquet(sample_leaders_path)
        backup_summary_dataframe = materialize_backup_summary_sample(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            sample_leaders_dataframe,
            parquet_path=output_path,
            duckdb_path=duckdb_path,
            sample_size=sample_size,
            random_seed=random_seed,
        )
        rows_ingested = int(len(backup_summary_dataframe))
        artifact_paths = [output_path]
        status = "success"
        error_count = 0
        logger.info(
            "NLP backup agreement pipeline complete run_id=%s output_path=%s",
            run_id,
            output_path,
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP backup agreement pipeline failed run_id=%s", run_id)
        raise
    finally:
        log_pipeline_run_safely(
            run_id=run_id,
            flow_name=_FLOW_NAME,
            start_ts=start_ts,
            end_ts=datetime.now(UTC),
            status=status,
            rows_ingested=rows_ingested,
            error_count=error_count,
            artifact_paths=artifact_paths,
            duckdb_path=duckdb_path,
            original_error=original_error,
            pipeline_logger=logger,
        )

    return NlpBackupAgreementPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
