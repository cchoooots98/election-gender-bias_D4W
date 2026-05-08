"""Runnable orchestration for Phase 3 tone threshold sensitivity analysis."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp.tone_sensitivity import (
    DEFAULT_TONE_SENSITIVITY_THRESHOLDS,
    materialize_tone_sensitivity_analysis,
)
from src.observability.run_logger import log_pipeline_run_safely

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_tone_sensitivity_pipeline"


@dataclass(frozen=True)
class NlpToneSensitivityPipelineResult:
    """Summary of one tone sensitivity pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_tone_sensitivity_pipeline(
    nlp_summary_path: Path = SILVER_DIR / "fact_mention_nlp_summary.parquet",
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    report_path: Path = GOLD_DIR / "nlp_tone_sensitivity_report.json",
    parquet_path: Path = GOLD_DIR / "nlp_tone_threshold_sensitivity.parquet",
    duckdb_path: Path = WAREHOUSE_PATH,
    thresholds: Sequence[float] = DEFAULT_TONE_SENSITIVITY_THRESHOLDS,
) -> NlpToneSensitivityPipelineResult:
    """Run tone threshold sensitivity analysis and record meta_run metadata.

    Args:
        nlp_summary_path: Existing Phase 3 NLP summary Parquet artifact.
        sample_leaders_path: Gold sampled leader cohort artifact.
        report_path: JSON QA report output path.
        parquet_path: Long-form threshold table output path.
        duckdb_path: DuckDB warehouse path.
        thresholds: Probability thresholds to audit.

    Returns:
        Summary object with run metadata and artifact paths.

    Raises:
        Exception: Re-raises any required-step failure after logging meta_run.
    """
    run_id = str(uuid.uuid4())
    start_ts = datetime.now(UTC)
    status = "failed"
    rows_ingested = 0
    error_count = 1
    artifact_paths: list[Path] = []
    original_error: Exception | None = None

    try:
        nlp_summary_dataframe = pd.read_parquet(nlp_summary_path)
        sample_leaders_dataframe = pd.read_parquet(sample_leaders_path)
        analysis = materialize_tone_sensitivity_analysis(
            nlp_summary_dataframe,
            sample_leaders_dataframe,
            thresholds=thresholds,
            report_path=report_path,
            parquet_path=parquet_path,
            duckdb_path=duckdb_path,
        )
        rows_ingested = len(analysis.sensitivity_table)
        artifact_paths = [report_path, parquet_path]
        status = "success"
        error_count = 0
        logger.info(
            "NLP tone sensitivity pipeline complete run_id=%s rows=%d",
            run_id,
            rows_ingested,
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP tone sensitivity pipeline failed run_id=%s", run_id)
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

    return NlpToneSensitivityPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
