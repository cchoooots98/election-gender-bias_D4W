"""Runnable orchestration for the Phase 1 NLP lexicon audit.

This entry point materializes deterministic lexicon features from the Phase 0
``silver.fact_mention_nlp_input`` contract and does not run Transformer
inference.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp.lexicon import materialize_fact_stereotype_word_counts
from src.nlp.trait_lexicon import materialize_trait_metric_artifacts
from src.observability.run_logger import log_pipeline_run_safely

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_lexicon_pipeline"


@dataclass(frozen=True)
class NlpLexiconPipelineResult:
    """Summary of one Phase 1 NLP lexicon pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_lexicon_pipeline(
    nlp_input_path: Path = SILVER_DIR / "fact_mention_nlp_input.parquet",
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    exposure_metrics_path: Path = GOLD_DIR / "mart_exposure_metrics.parquet",
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    lexicon_path: Path | None = None,
    trait_lexicon_path: Path | None = None,
) -> NlpLexiconPipelineResult:
    """Run Phase 1 lexicon materialization and record meta_run metadata.

    Args:
        nlp_input_path: Silver NLP input Parquet artifact.
        sample_leaders_path: Gold sample leaders Parquet artifact.
        exposure_metrics_path: Gold exposure metrics Parquet artifact.
        silver_dir: Silver output root for the stereotype count Parquet artifact.
        gold_dir: Gold output root for trait metric Parquet artifacts.
        duckdb_path: DuckDB warehouse path.
        lexicon_path: Optional custom stereotype lexicon JSON path.
        trait_lexicon_path: Optional custom two-tier trait lexicon JSON path.

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
        sample_leaders_dataframe = pd.read_parquet(sample_leaders_path)
        exposure_metrics_dataframe = pd.read_parquet(exposure_metrics_path)
        stereotype_word_counts_dataframe = materialize_fact_stereotype_word_counts(
            nlp_input_dataframe,
            lexicon_path=lexicon_path,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
        )
        trait_artifacts = materialize_trait_metric_artifacts(
            nlp_input_dataframe=nlp_input_dataframe,
            sample_leaders_dataframe=sample_leaders_dataframe,
            exposure_metrics_dataframe=exposure_metrics_dataframe,
            trait_lexicon_path=trait_lexicon_path,
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
        )
        rows_ingested = len(stereotype_word_counts_dataframe) + len(
            trait_artifacts.word_counts
        )
        artifact_paths = [
            silver_dir / "fact_stereotype_word_counts.parquet",
            silver_dir / "fact_trait_word_counts.parquet",
            gold_dir / "mart_trait_metrics.parquet",
            gold_dir / "mart_trait_top_terms.parquet",
            gold_dir / "mart_trait_candidate_metrics.parquet",
            gold_dir / "mart_trait_qa_samples.parquet",
        ]
        status = "success"
        error_count = 0
        logger.info(
            "NLP lexicon pipeline complete run_id=%s rows=%d trait_metric_rows=%d",
            run_id,
            rows_ingested,
            len(trait_artifacts.metrics),
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP lexicon pipeline failed run_id=%s", run_id)
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

    return NlpLexiconPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
