"""Runnable orchestration for the Phase 5 NLP QA report."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp.qa import DEFAULT_NLP_QA_THRESHOLDS, materialize_nlp_qa_report
from src.observability.run_logger import log_pipeline_run_safely

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_qa_pipeline"


@dataclass(frozen=True)
class NlpQaPipelineResult:
    """Summary of one Phase 5 NLP QA pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_qa_pipeline(
    nlp_input_path: Path = SILVER_DIR / "fact_mention_nlp_input.parquet",
    nlp_summary_path: Path = SILVER_DIR / "fact_mention_nlp_summary.parquet",
    frame_score_path: Path = SILVER_DIR / "fact_mention_frame_score.parquet",
    stereotype_word_counts_path: Path = SILVER_DIR
    / "fact_stereotype_word_counts.parquet",
    report_path: Path = GOLD_DIR / "nlp_qa_report.json",
    duckdb_path: Path = WAREHOUSE_PATH,
    backup_summary_path: Path | None = None,
    thresholds: Sequence[float] = DEFAULT_NLP_QA_THRESHOLDS,
) -> NlpQaPipelineResult:
    """Run Phase 5 NLP QA report materialization and log meta_run metadata.

    Args:
        nlp_input_path: Phase 0 NLP input Parquet artifact.
        nlp_summary_path: Phase 2/3/4 NLP summary Parquet artifact.
        frame_score_path: Phase 4 frame-score Parquet artifact.
        stereotype_word_counts_path: Phase 1 lexicon-count Parquet artifact.
        report_path: Gold JSON QA report output path.
        duckdb_path: DuckDB warehouse path for ``meta.meta_run``.
        backup_summary_path: Optional precomputed backup-model summary artifact.
        thresholds: Probability thresholds for report sensitivity summaries.

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
        nlp_input_dataframe = pd.read_parquet(nlp_input_path)
        nlp_summary_dataframe = pd.read_parquet(nlp_summary_path)
        frame_score_dataframe = pd.read_parquet(frame_score_path)
        stereotype_word_counts_dataframe = pd.read_parquet(stereotype_word_counts_path)
        backup_summary_dataframe = (
            pd.read_parquet(backup_summary_path)
            if backup_summary_path is not None
            else None
        )
        materialize_nlp_qa_report(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            frame_score_dataframe,
            stereotype_word_counts_dataframe,
            backup_summary_dataframe=backup_summary_dataframe,
            report_path=report_path,
            thresholds=thresholds,
        )
        rows_ingested = 1
        artifact_paths = [report_path]
        status = "success"
        error_count = 0
        logger.info(
            "NLP QA pipeline complete run_id=%s report_path=%s", run_id, report_path
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP QA pipeline failed run_id=%s", run_id)
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

    return NlpQaPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
