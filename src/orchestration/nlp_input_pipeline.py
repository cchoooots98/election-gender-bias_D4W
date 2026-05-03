"""Runnable orchestration for the Phase 0 NLP input contract.

This entry point materializes only ``silver.fact_mention_nlp_input``. It does
not run lexicon scoring or Transformer inference; those later phases consume
this contract after it has passed DQ validation.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.config.settings import SILVER_DIR, WAREHOUSE_PATH
from src.nlp.input_contracts import materialize_fact_mention_nlp_input
from src.observability.run_logger import log_pipeline_run

logger = logging.getLogger(__name__)

_FLOW_NAME = "nlp_input_pipeline"


@dataclass(frozen=True)
class NlpInputPipelineResult:
    """Summary of one Phase 0 NLP input pipeline execution."""

    run_id: str
    status: str
    rows_ingested: int
    error_count: int
    artifact_paths: list[str]


def run_nlp_input_pipeline(
    fact_mention_path: Path = SILVER_DIR / "fact_mention.parquet",
    fact_article_path: Path = SILVER_DIR / "fact_article.parquet",
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> NlpInputPipelineResult:
    """Run Phase 0 NLP input materialization and record meta_run metadata.

    Args:
        fact_mention_path: Silver mention fact Parquet input.
        fact_article_path: Silver article fact Parquet input.
        silver_dir: Silver output root for the NLP input Parquet artifact.
        duckdb_path: DuckDB warehouse path.

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
        fact_mention_dataframe = pd.read_parquet(fact_mention_path)
        fact_article_dataframe = pd.read_parquet(fact_article_path)
        nlp_input_dataframe = materialize_fact_mention_nlp_input(
            fact_mention_dataframe,
            fact_article_dataframe,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
        )
        rows_ingested = len(nlp_input_dataframe)
        artifact_paths = [silver_dir / "fact_mention_nlp_input.parquet"]
        status = "success"
        error_count = 0
        logger.info(
            "NLP input pipeline complete run_id=%s rows=%d",
            run_id,
            rows_ingested,
        )
    except Exception as exc:
        original_error = exc
        logger.exception("NLP input pipeline failed run_id=%s", run_id)
        raise
    finally:
        try:
            # TODO: add flow_version when meta.meta_run supports contract versions.
            log_pipeline_run(
                run_id=run_id,
                flow_name=_FLOW_NAME,
                start_ts=start_ts,
                end_ts=datetime.now(UTC),
                status=status,
                rows_ingested=rows_ingested,
                error_count=error_count,
                artifact_paths=artifact_paths,
                duckdb_path=duckdb_path,
            )
        except Exception:
            logger.exception(
                "Failed to write meta_run for run_id=%s; original error preserved",
                run_id,
            )
            if original_error is None:
                raise

    return NlpInputPipelineResult(
        run_id=run_id,
        status=status,
        rows_ingested=rows_ingested,
        error_count=error_count,
        artifact_paths=[str(path) for path in artifact_paths],
    )
