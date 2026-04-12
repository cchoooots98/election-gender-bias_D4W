"""End-to-end Europresse-first news corpus ETL pipeline.

Responsibility
--------------
This module parses a ``NewsImportManifest`` for a local Europresse export,
deduplicates canonical articles, matches sampled candidates, and materialises
all Silver and Gold analytical tables.

Typical calling sequence (from orchestration layer)
----------------------------------------------------
::

    result = run_news_corpus_etl(import_manifest_path=...)

Output tables
-------------
Bronze  : ``news_source_record``
Silver  : ``fact_article_source``, ``fact_article``, ``fact_mention``,
          ``manual_review_candidate_match``, ``_rejected/*``
Gold    : ``mart_exposure_metrics``, ``mart_framing_metrics``,
          ``mart_bias_indicators``, ``mart_regression_feature_base``,
          ``mart_regression_results``
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import pandas as pd

from src.config.settings import BRONZE_DIR, GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.ingest.news.corpus import (
    build_fact_article,
    build_fact_article_source,
    enrich_article_sources_with_web_cache,
    inspect_import_batch,
    load_news_import_manifest,
    parse_import_batch,
)
from src.ingest.news.corpus_storage import (
    write_duckdb_table,
    write_json_report,
    write_parquet_table,
)
from src.ingest.news.marts import (
    build_mart_bias_indicators,
    build_mart_exposure_metrics,
    build_mart_framing_metrics,
    build_mart_regression_feature_base,
    build_mart_regression_results,
    run_news_corpus_quality_checks,
)
from src.ingest.news.matching import build_fact_mentions
from src.ingest.news.models import NewsCorpusRunResult
from src.ingest.news.normalize import stable_md5 as _stable_md5

logger = logging.getLogger(__name__)
_PERSISTED_TEXT_PREVIEW_CHARS = 280
_REDACTED_TEXT_MARKER = "[REDACTED_FULL_TEXT_NOT_PERSISTED]"


def _prepare_persisted_text_table(
    dataframe: pd.DataFrame,
    *,
    text_column: str,
) -> pd.DataFrame:
    """Redact full text before persistence while retaining audit-friendly surrogates.

    Full article text is required transiently for matching during the ETL run,
    but the persisted Parquet and DuckDB artifacts keep only a short preview,
    length, and deterministic hash. This enforces the repository's documented
    data-minimisation contract.
    """
    if text_column not in dataframe.columns:
        return dataframe.copy()

    persisted_df = dataframe.copy()
    normalized_text = persisted_df[text_column].fillna("").astype(str)
    persisted_df[f"{text_column}_preview"] = normalized_text.apply(
        lambda value: value[:_PERSISTED_TEXT_PREVIEW_CHARS] or None
    )
    persisted_df[f"{text_column}_length"] = normalized_text.str.len().astype(int)
    hash_column = f"{text_column}_hash"
    if hash_column not in persisted_df.columns:
        persisted_df[hash_column] = normalized_text.apply(
            lambda value: _stable_md5(value) if value else None
        )
    persisted_df[text_column] = normalized_text.apply(
        lambda value: _REDACTED_TEXT_MARKER if value else None
    )
    return persisted_df


def run_news_corpus_etl(
    *,
    import_manifest_path: Path,
    sample_leaders_path: Path = GOLD_DIR / "sample_leaders.parquet",
    dim_commune_path: Path = SILVER_DIR / "dim_commune.parquet",
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    enable_web_scrape: bool = False,
) -> NewsCorpusRunResult:
    """Run the enterprise news corpus ETL and materialize all main artifacts.

    Args:
        import_manifest_path: Path to ``news_import_manifest.json``.
        sample_leaders_path: Frozen analytical cohort path.
        dim_commune_path: Geographic dimension used for population normalization.
        bronze_dir: Bronze output root.
        silver_dir: Silver output root.
        gold_dir: Gold output root.
        duckdb_path: Warehouse path.
        enable_web_scrape: Whether this run may fetch uncached web article URLs.

    Returns:
        Summary object with status, row counts, and artifact paths.
    """
    run_id = str(uuid.uuid4())
    manifest = load_news_import_manifest(import_manifest_path)
    if manifest.window_start is None or manifest.window_end is None:
        raise ValueError("News import manifest must define window_start and window_end")
    inspection = inspect_import_batch(manifest)
    bronze_source_df, unsupported_df = parse_import_batch(manifest, inspection)
    fact_article_source_df, rejected_source_df = build_fact_article_source(
        bronze_source_df,
        window_start=manifest.window_start,
        window_end=manifest.window_end,
    )
    web_fetch_cache_path = (
        bronze_dir / "news_web_fetch" / "news_web_fetch_cache.parquet"
    )
    (
        fact_article_source_df,
        web_enrichment_report,
        web_fetch_cache_written,
    ) = enrich_article_sources_with_web_cache(
        fact_article_source_df,
        cache_path=web_fetch_cache_path,
        enable_web_scrape=enable_web_scrape,
    )

    if not sample_leaders_path.exists():
        raise FileNotFoundError(
            f"Sample leaders not found: {sample_leaders_path}. "
            "Run the sampling pipeline first."
        )
    if not dim_commune_path.exists():
        raise FileNotFoundError(
            f"dim_commune not found: {dim_commune_path}. "
            "Run the sampling pipeline first."
        )

    sample_leaders_df = pd.read_parquet(sample_leaders_path)
    dim_commune_df = pd.read_parquet(dim_commune_path)
    fact_article_df = build_fact_article(fact_article_source_df)
    fact_mention_df, manual_review_df = build_fact_mentions(
        fact_article_df,
        sample_leaders_df,
    )

    mart_exposure_metrics_df = build_mart_exposure_metrics(
        sample_leaders_df,
        fact_article_df,
        fact_mention_df,
        dim_commune_df,
    )
    mart_framing_metrics_df = build_mart_framing_metrics(
        sample_leaders_df,
        fact_mention_df,
    )
    mart_bias_indicators_df = build_mart_bias_indicators(mart_exposure_metrics_df)
    mart_regression_feature_base_df = build_mart_regression_feature_base(
        sample_leaders_df,
        mart_exposure_metrics_df,
    )
    mart_regression_results_df = build_mart_regression_results(
        mart_regression_feature_base_df
    )
    qa_report = run_news_corpus_quality_checks(
        sample_leaders_df=sample_leaders_df,
        fact_article_source_df=fact_article_source_df,
        fact_article_source_rejected_df=rejected_source_df,
        fact_article_df=fact_article_df,
        fact_mention_df=fact_mention_df,
        mart_exposure_metrics_df=mart_exposure_metrics_df,
        mart_regression_results_df=mart_regression_results_df,
        web_enrichment_report=web_enrichment_report,
    )

    persisted_bronze_source_df = _prepare_persisted_text_table(
        bronze_source_df,
        text_column="raw_body_text",
    )
    persisted_fact_article_source_df = _prepare_persisted_text_table(
        fact_article_source_df,
        text_column="body_text",
    )
    persisted_fact_article_df = _prepare_persisted_text_table(
        fact_article_df,
        text_column="body_text",
    )

    artifact_paths: list[str] = [str(import_manifest_path)]
    if web_fetch_cache_written or web_fetch_cache_path.exists():
        artifact_paths.append(str(web_fetch_cache_path))
    parquet_specs = [
        (
            persisted_bronze_source_df,
            bronze_dir
            / "news_source_record"
            / f"batch_id={manifest.batch_id}"
            / f"source_system={manifest.source_system}"
            / "news_source_record.parquet",
            "bronze",
            "news_source_record",
        ),
        (
            persisted_fact_article_source_df,
            silver_dir / "fact_article_source.parquet",
            "silver",
            "fact_article_source",
        ),
        (
            persisted_fact_article_df,
            silver_dir / "fact_article.parquet",
            "silver",
            "fact_article",
        ),
        (
            fact_mention_df,
            silver_dir / "fact_mention.parquet",
            "silver",
            "fact_mention",
        ),
        (
            manual_review_df,
            silver_dir / "manual_review_candidate_match.parquet",
            "silver",
            "manual_review_candidate_match",
        ),
        (
            rejected_source_df,
            silver_dir / "_rejected" / "fact_article_source_rejected.parquet",
            "silver",
            "fact_article_source_rejected",
        ),
        (
            unsupported_df,
            silver_dir / "_rejected" / "news_import_unsupported.parquet",
            "silver",
            "news_import_unsupported",
        ),
        (
            mart_exposure_metrics_df,
            gold_dir / "mart_exposure_metrics.parquet",
            "gold",
            "mart_exposure_metrics",
        ),
        (
            mart_framing_metrics_df,
            gold_dir / "mart_framing_metrics.parquet",
            "gold",
            "mart_framing_metrics",
        ),
        (
            mart_bias_indicators_df,
            gold_dir / "mart_bias_indicators.parquet",
            "gold",
            "mart_bias_indicators",
        ),
        (
            mart_regression_feature_base_df,
            gold_dir / "mart_regression_feature_base.parquet",
            "gold",
            "mart_regression_feature_base",
        ),
        (
            mart_regression_results_df,
            gold_dir / "mart_regression_results.parquet",
            "gold",
            "mart_regression_results",
        ),
    ]

    for dataframe, parquet_path, schema_name, table_name in parquet_specs:
        write_parquet_table(dataframe, parquet_path)
        write_duckdb_table(
            dataframe=dataframe,
            schema_name=schema_name,
            table_name=table_name,
            duckdb_path=duckdb_path,
        )
        artifact_paths.append(str(parquet_path))

    qa_report_path = write_json_report(
        {
            "run_id": run_id,
            "batch_id": manifest.batch_id,
            "source_system": manifest.source_system,
            "parser_mix": inspection.parser_mix,
            "language_mix": {
                str(language): int(count)
                for language, count in fact_article_source_df["language"]
                .value_counts(dropna=False)
                .to_dict()
                .items()
            },
            "qa": qa_report,
        },
        gold_dir / "news_corpus_qa_report.json",
    )
    artifact_paths.append(str(qa_report_path))

    row_counts = {
        "bronze_news_source_record": int(len(persisted_bronze_source_df)),
        "fact_article_source": int(len(fact_article_source_df)),
        "fact_article_source_rejected": int(len(rejected_source_df)),
        "fact_article": int(len(fact_article_df)),
        "fact_mention": int(len(fact_mention_df)),
        "manual_review_candidate_match": int(len(manual_review_df)),
        "mart_exposure_metrics": int(len(mart_exposure_metrics_df)),
    }
    partial_signal_count = sum(
        1
        for dataframe in (unsupported_df, rejected_source_df, manual_review_df)
        if not dataframe.empty
    )
    partial_signal_count += qa_report["regression_warning_count"]
    partial_signal_count += qa_report["regression_failure_count"]
    status = "partial" if partial_signal_count else "success"
    logger.info(
        "News corpus ETL complete run_id=%s batch_id=%s status=%s canonical_articles=%d mentions=%d",
        run_id,
        manifest.batch_id,
        status,
        len(fact_article_df),
        len(fact_mention_df),
    )
    return NewsCorpusRunResult(
        run_id=run_id,
        batch_id=manifest.batch_id,
        status=status,
        error_count=partial_signal_count,
        row_counts=row_counts,
        artifact_paths=tuple(artifact_paths),
        qa_report_path=str(qa_report_path),
    )
