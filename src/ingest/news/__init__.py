"""Hybrid news ingestion package.

This package replaces the original single-file GDELT prototype with a
source-agnostic discovery framework. The public import path remains
``src.ingest.news`` so downstream code keeps working while the implementation
grows into providers, storage, normalization, benchmarking, and orchestration.
"""

from src.ingest.news.benchmark import load_benchmark_manifest, run_news_benchmark
from src.ingest.news.corpus import (
    build_article_source_from_search_hits,
    build_fact_article,
    build_fact_article_discovery,
    build_fact_article_source,
    inspect_import_batch,
    load_news_import_manifest,
    parse_import_batch,
    write_news_import_manifest,
)
from src.ingest.news.corpus_pipeline import run_news_corpus_etl
from src.ingest.news.marts import (
    build_mart_bias_indicators,
    build_mart_exposure_metrics,
    build_mart_framing_metrics,
    build_mart_regression_feature_base,
    build_mart_regression_results,
)
from src.ingest.news.matching import build_fact_mentions
from src.ingest.news.models import (
    BenchmarkCase,
    BenchmarkRunResult,
    CandidateQueryCase,
    ImportBatchFile,
    ImportBatchInspection,
    NewsCorpusRunResult,
    NewsImportManifest,
    NewsIngestRunResult,
    ProviderQueryResult,
    RawDocument,
    SearchHit,
)
from src.ingest.news.normalize import canonicalize_url
from src.ingest.news.pipeline import run_gdelt_ingest, run_news_ingest
from src.ingest.news.providers.gdelt import build_gdelt_query
from src.ingest.news.queries import (
    build_candidate_aliases,
    build_candidate_query_case,
    normalize_text_for_match,
    parse_candidate_full_name,
)

__all__ = [
    "BenchmarkCase",
    "BenchmarkRunResult",
    "CandidateQueryCase",
    "ImportBatchFile",
    "ImportBatchInspection",
    "NewsCorpusRunResult",
    "NewsIngestRunResult",
    "NewsImportManifest",
    "ProviderQueryResult",
    "RawDocument",
    "SearchHit",
    "build_article_source_from_search_hits",
    "build_candidate_aliases",
    "build_candidate_query_case",
    "build_fact_article",
    "build_fact_article_discovery",
    "build_fact_article_source",
    "build_fact_mentions",
    "build_gdelt_query",
    "build_mart_bias_indicators",
    "build_mart_exposure_metrics",
    "build_mart_framing_metrics",
    "build_mart_regression_feature_base",
    "build_mart_regression_results",
    "canonicalize_url",
    "inspect_import_batch",
    "load_benchmark_manifest",
    "load_news_import_manifest",
    "normalize_text_for_match",
    "parse_import_batch",
    "parse_candidate_full_name",
    "run_gdelt_ingest",
    "run_news_benchmark",
    "run_news_corpus_etl",
    "run_news_ingest",
    "write_news_import_manifest",
]
