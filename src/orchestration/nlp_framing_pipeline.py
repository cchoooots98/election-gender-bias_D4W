"""Runnable orchestration for Phase 4 NLP framing.

This entry point enriches the existing ``silver.fact_mention_nlp_summary``
table with primary frame labels and materializes
``silver.fact_mention_frame_score``. It does not activate Gold marts or
dashboard panels; those remain separate downstream milestones.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import SILVER_DIR, WAREHOUSE_PATH
from src.nlp.model_bundle import ModelBundleConfig
from src.nlp.nli import FrameRunner, materialize_fact_mention_nlp_summary_with_frames
from src.observability.run_logger import log_pipeline_run_safely

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_framing_pipeline"


@dataclass(frozen=True)
class NlpFramingPipelineResult:
    """Summary of one Phase 4 NLP framing pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_framing_pipeline(
    nlp_input_path: Path = SILVER_DIR / "fact_mention_nlp_input.parquet",
    nlp_summary_path: Path = SILVER_DIR / "fact_mention_nlp_summary.parquet",
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    frame_runner: FrameRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
) -> NlpFramingPipelineResult:
    """Run Phase 4 framing enrichment and record meta_run metadata.

    Args:
        nlp_input_path: Silver NLP input Parquet artifact.
        nlp_summary_path: Existing NLP summary Parquet artifact.
        silver_dir: Silver output root for updated NLP artifacts.
        duckdb_path: DuckDB warehouse path.
        frame_runner: Optional scorer implementation for tests.
        model_bundle_config: Optional model-bundle metadata override.

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
        _enriched_summary_dataframe, frame_score_dataframe = (
            materialize_fact_mention_nlp_summary_with_frames(
                nlp_input_dataframe,
                nlp_summary_dataframe,
                frame_runner=frame_runner,
                model_bundle_config=model_bundle_config,
                silver_dir=silver_dir,
                duckdb_path=duckdb_path,
            )
        )
        rows_ingested = len(frame_score_dataframe)
        artifact_paths = [
            silver_dir / "fact_mention_nlp_summary.parquet",
            silver_dir / "fact_mention_frame_score.parquet",
        ]
        status = "success"
        error_count = 0
        logger.info(
            "NLP framing pipeline complete run_id=%s frame_rows=%d",
            run_id,
            rows_ingested,
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP framing pipeline failed run_id=%s", run_id)
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

    return NlpFramingPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
