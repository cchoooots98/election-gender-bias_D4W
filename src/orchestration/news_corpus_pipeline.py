"""Runnable orchestration for the enterprise news corpus backbone.

Pipeline order
--------------
1. **(Optional) GDELT discovery** â€” ``auto_discover_gdelt=True``
   Calls GDELT for all sampled candidates and collects ``SearchHit`` objects
   in memory.  Hits are merged into the corpus ETL as supplemental discovery
   records and are **not** persisted to Bronze here.  Run ``run-news-ingest``
   separately if you need Bronze-level GDELT artifacts for auditing.

2. **Corpus ETL** â€” ``run_news_corpus_etl()``
   Parses the import manifest (Europresse or other source), merges with GDELT
   hits, deduplicates, matches candidates, builds Silver tables and Gold marts.

3. **Observability** â€” ``log_news_import_batch``, ``log_pipeline_run``
   Records batch and run metadata to DuckDB ``meta`` schema tables.

Separation from pipeline.py
----------------------------
``pipeline.py`` handles **provider discovery only** (GDELT / curated / GNews â†’
Bronze Parquet artifacts on disk).  This module handles the **corpus ETL** path
(manifest parse â†’ dedup â†’ candidate match â†’ marts).  Both can run independently:

::

    # Provider discovery only (writes Bronze Parquet)
    run-news-ingest

    # Corpus ETL with automatic GDELT supplemental hits
    run-news-corpus-pipeline --auto-discover-gdelt

    # Corpus ETL from Europresse only (no GDELT)
    run-news-corpus-pipeline
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.config.settings import (
    ANALYSIS_END_DATE,
    ANALYSIS_START_DATE,
    BRONZE_DIR,
    GOLD_DIR,
    RAW_DIR,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.corpus import load_news_import_manifest
from src.ingest.news.corpus_pipeline import run_news_corpus_etl
from src.ingest.news.models import NewsCorpusRunResult, SearchHit
from src.observability.run_logger import log_news_import_batch, log_pipeline_run

logger = logging.getLogger(__name__)

_FLOW_NAME = "news_corpus_pipeline"
_DEFAULT_IMPORT_MANIFEST = RAW_DIR / "news" / "news_import_manifest.json"
_DEFAULT_SAMPLE_MANIFEST = GOLD_DIR / "sample_manifest.json"


def _collect_gdelt_hits(
    sample_manifest_path: Path,
) -> tuple[list[SearchHit], list[dict[str, object]]]:
    """Run GDELT discovery for all sampled candidates and return in-memory hits.

    Calls the GDELT provider directly for each candidate in the sample manifest.
    Hits are NOT persisted to Bronze â€” they are merged into the corpus ETL as
    ``supplemental_search_hits``.  Run ``run-news-ingest`` separately if Bronze
    persistence of GDELT discovery artifacts is needed for auditing.

    Args:
        sample_manifest_path: Path to ``gold/sample_manifest.json``.

    Returns:
        Tuple of (search_hits, provider_query_rows) ready for corpus ETL injection.
    """
    # Lazy imports: heavy provider code not needed in thin environments (e.g. tests)
    from src.ingest.news.pipeline import _load_candidate_cases  # noqa: PLC0415
    from src.ingest.news.providers.gdelt import search_gdelt_candidate  # noqa: PLC0415

    candidate_cases = _load_candidate_cases(
        sample_manifest_path,
        query_start=ANALYSIS_START_DATE,
        query_end=ANALYSIS_END_DATE,
    )
    all_hits: list[SearchHit] = []
    provider_query_rows: list[dict[str, object]] = []

    for case in candidate_cases:
        logger.info(
            "GDELT discovery leader_id=%s name=%s window=%s/%s",
            case.leader_id,
            case.full_name,
            case.window_start,
            case.window_end,
        )
        result = search_gdelt_candidate(case)
        all_hits.extend(result.hits)
        provider_query_rows.append(
            {
                "leader_id": case.leader_id,
                "full_name": case.full_name,
                "commune_name": case.commune_name,
                "dep_code": case.dep_code,
                "city_size_bucket": case.city_size_bucket,
                "window_start": str(case.window_start),
                "window_end": str(case.window_end),
                "provider": "gdelt",
                "provider_tier": result.provider_tier,
                "provider_status": result.status,
                "provider_error_type": result.error_type,
                "provider_warning_count": result.warning_count,
                "provider_hit_count": len(result.hits),
                "request_url": None,
            }
        )
        logger.info(
            "GDELT candidate done leader_id=%s hits=%d status=%s",
            case.leader_id,
            len(result.hits),
            result.status,
        )

    logger.info(
        "GDELT discovery complete candidates=%d total_hits=%d",
        len(candidate_cases),
        len(all_hits),
    )
    return all_hits, provider_query_rows


def run_news_corpus_pipeline(
    import_manifest_path: Path = _DEFAULT_IMPORT_MANIFEST,
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    sample_manifest_path: Path = _DEFAULT_SAMPLE_MANIFEST,
    dim_commune_path: Path = SILVER_DIR / "dim_commune.parquet",
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    auto_discover_gdelt: bool = False,
) -> NewsCorpusRunResult:
    """Run the main source-agnostic corpus pipeline and record observability.

    Args:
        import_manifest_path: Path to the Europresse (or other) import manifest.
        sample_leaders_path: Frozen analytical cohort Parquet.
        sample_manifest_path: Sampling manifest JSON used for GDELT candidate
            cases when ``auto_discover_gdelt=True``.
        dim_commune_path: Geographic dimension for population normalization.
        bronze_dir: Bronze output root.
        silver_dir: Silver output root.
        gold_dir: Gold output root.
        duckdb_path: DuckDB warehouse path.
        auto_discover_gdelt: When True, run GDELT discovery for all sampled
            candidates before the corpus ETL and merge the hits as supplemental
            discovery records.  Defaults to False to keep the pipeline fast when
            Europresse coverage is sufficient.
    """
    start_ts = datetime.now(UTC)
    execution_run_id = str(uuid.uuid4())
    status = "failed"
    error_count = 1
    rows_ingested = 0
    artifact_paths: list[str] = []
    manifest = load_news_import_manifest(import_manifest_path)

    # Collect GDELT supplemental hits if requested.
    gdelt_hits: list[SearchHit] = []
    gdelt_query_rows: list[dict[str, object]] = []
    if auto_discover_gdelt:
        if not sample_manifest_path.exists():
            logger.warning(
                "auto_discover_gdelt=True but sample manifest not found: %s "
                "â€” skipping GDELT supplemental discovery",
                sample_manifest_path,
            )
        else:
            gdelt_hits, gdelt_query_rows = _collect_gdelt_hits(sample_manifest_path)

    try:
        result = run_news_corpus_etl(
            import_manifest_path=import_manifest_path,
            sample_leaders_path=sample_leaders_path,
            dim_commune_path=dim_commune_path,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
            supplemental_search_hits=gdelt_hits,
            supplemental_provider_query_rows=gdelt_query_rows,
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
