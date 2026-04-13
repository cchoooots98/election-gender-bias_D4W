"""Runnable orchestration for the Europresse-first news corpus pipeline.

Pipeline order
--------------
1. **Manifest load** — read ``data/raw/news/news_import_manifest.json``.
2. **Corpus ETL** — parse the Europresse export, normalize article-source rows,
   deduplicate canonical articles, match sampled candidates, and build marts.
3. **Observability** — persist batch/run metadata to DuckDB ``meta`` tables.

This orchestration intentionally exposes a single authoritative news-source
path. Earlier provider-discovery experiments are not part of the supported
runnable workflow anymore.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.config.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    RAW_DIR,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.corpus import load_news_import_manifest
from src.ingest.news.corpus_pipeline import run_news_corpus_etl
from src.ingest.news.models import NewsCorpusRunResult
from src.observability.run_logger import log_news_import_batch, log_pipeline_run

logger = logging.getLogger(__name__)

_FLOW_NAME = "news_corpus_pipeline"
# Default manifest points to the primary cohort directory.
# Override via --manifest-path (CLI) or the manifest_path argument (Python).
_DEFAULT_IMPORT_MANIFEST = RAW_DIR / "news" / "cohort_36" / "news_import_manifest.json"


def run_news_corpus_pipeline(
    import_manifest_path: Path = _DEFAULT_IMPORT_MANIFEST,
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    dim_commune_path: Path = SILVER_DIR / "dim_commune.parquet",
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    enable_web_scrape: bool = False,
    bootstrap_resamples: int = 2000,
) -> NewsCorpusRunResult:
    """Run the Europresse corpus pipeline and record observability metadata.

    Args:
        import_manifest_path: Path to the Europresse import manifest.
        sample_leaders_path: Frozen analytical cohort Parquet.
        dim_commune_path: Geographic dimension for population normalization.
        bronze_dir: Bronze output root.
        silver_dir: Silver output root.
        gold_dir: Gold output root.
        duckdb_path: DuckDB warehouse path.
        enable_web_scrape: Whether this run may fetch uncached web article URLs.
        bootstrap_resamples: Number of bootstrap resamples for regression CIs.

    Returns:
        Summary object with materialized row counts and artifact paths.
    """
    start_ts = datetime.now(UTC)
    execution_run_id = str(uuid.uuid4())
    status = "failed"
    error_count = 1
    rows_ingested = 0
    artifact_paths: list[str] = []
    manifest = load_news_import_manifest(import_manifest_path)

    try:
        result = run_news_corpus_etl(
            import_manifest_path=import_manifest_path,
            sample_leaders_path=sample_leaders_path,
            dim_commune_path=dim_commune_path,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
            enable_web_scrape=enable_web_scrape,
            bootstrap_resamples=bootstrap_resamples,
        )
        status = result.status
        execution_run_id = result.run_id
        error_count = result.error_count
        rows_ingested = result.row_counts.get("fact_article", 0)
        artifact_paths = list(result.artifact_paths)

        with open(result.qa_report_path, encoding="utf-8") as file_handle:
            qa_payload = json.load(file_handle)
        log_news_import_batch(
            batch_id=result.batch_id,
            source_system=manifest.source_system,
            accepted_record_count=result.row_counts.get("fact_article_source", 0),
            rejected_record_count=result.row_counts.get(
                "fact_article_source_rejected", 0
            ),
            parser_mix=qa_payload.get("parser_mix", {}),
            coverage_start=manifest.window_start.isoformat(),
            coverage_end=manifest.window_end.isoformat(),
            language_mix=qa_payload.get("language_mix", {}),
            operator=manifest.operator,
            duckdb_path=duckdb_path,
        )
        return result
    except Exception:
        # Broad catch is intentional: any crash must be logged and then re-raised
        # so the finally block still records a failed meta_run audit row.
        logger.exception(
            "News corpus pipeline failed batch_id=%s manifest=%s",
            manifest.batch_id,
            import_manifest_path,
        )
        raise
    finally:
        log_pipeline_run(
            run_id=execution_run_id,
            flow_name=_FLOW_NAME,
            start_ts=start_ts,
            end_ts=datetime.now(UTC),
            status=status,
            rows_ingested=rows_ingested,
            error_count=error_count,
            artifact_paths=artifact_paths,
            duckdb_path=duckdb_path,
        )
