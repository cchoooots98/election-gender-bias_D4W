"""Runnable orchestration for Phase 3 target-aware NLP tone.

This entry point enriches the existing Phase 2
``silver.fact_mention_nlp_summary`` table. It does not activate Gold marts or
dashboard panels; those remain separate downstream milestones.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp.model_bundle import ModelBundleConfig
from src.nlp.nli import ToneRunner, materialize_fact_mention_nlp_summary_with_tone
from src.observability.run_logger import log_pipeline_run_safely

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_tone_pipeline"


@dataclass(frozen=True)
class NlpTonePipelineResult:
    """Summary of one Phase 3 NLP tone pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_tone_pipeline(
    nlp_input_path: Path = SILVER_DIR / "fact_mention_nlp_input.parquet",
    nlp_summary_path: Path = SILVER_DIR / "fact_mention_nlp_summary.parquet",
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    tone_runner: ToneRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
) -> NlpTonePipelineResult:
    """Run Phase 3 tone enrichment and record meta_run metadata.

    Args:
        nlp_input_path: Silver NLP input Parquet artifact.
        nlp_summary_path: Existing Phase 2 NLP summary Parquet artifact.
        sample_leaders_path: Gold sampled leader cohort artifact.
        silver_dir: Silver output root for the updated NLP summary artifact.
        duckdb_path: DuckDB warehouse path.
        tone_runner: Optional scorer implementation for tests.
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
        sample_leaders_dataframe = pd.read_parquet(sample_leaders_path)
        enriched_summary_dataframe = materialize_fact_mention_nlp_summary_with_tone(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            sample_leaders_dataframe,
            tone_runner=tone_runner,
            model_bundle_config=model_bundle_config,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
        )
        rows_ingested = len(enriched_summary_dataframe)
        artifact_paths = [silver_dir / "fact_mention_nlp_summary.parquet"]
        status = "success"
        error_count = 0
        logger.info(
            "NLP tone pipeline complete run_id=%s rows=%d",
            run_id,
            rows_ingested,
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP tone pipeline failed run_id=%s", run_id)
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

    return NlpTonePipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
