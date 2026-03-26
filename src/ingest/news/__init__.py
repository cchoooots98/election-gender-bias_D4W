"""Hybrid news ingestion package.

This package replaces the original single-file GDELT prototype with a
source-agnostic discovery framework. The public import path remains
``src.ingest.news`` so downstream code keeps working while the implementation
grows into providers, storage, normalization, benchmarking, and orchestration.
"""

from src.ingest.news.benchmark import load_benchmark_manifest, run_news_benchmark
from src.ingest.news.models import (
    BenchmarkCase,
    BenchmarkRunResult,
    CandidateQueryCase,
    NewsIngestRunResult,
    ProviderQueryResult,
    RawDocument,
    SearchHit,
)
from src.ingest.news.normalize import build_fact_article_frames, canonicalize_url
from src.ingest.news.pipeline import run_gdelt_ingest, run_news_ingest
from src.ingest.news.providers.gdelt import build_gdelt_query
from src.ingest.news.queries import (
    build_candidate_aliases,
    build_candidate_query_case,
    normalize_text_for_match,
)

__all__ = [
    "BenchmarkCase",
    "BenchmarkRunResult",
    "CandidateQueryCase",
    "NewsIngestRunResult",
    "ProviderQueryResult",
    "RawDocument",
    "SearchHit",
    "build_candidate_aliases",
    "build_candidate_query_case",
    "build_fact_article_frames",
    "build_gdelt_query",
    "canonicalize_url",
    "load_benchmark_manifest",
    "normalize_text_for_match",
    "run_gdelt_ingest",
    "run_news_benchmark",
    "run_news_ingest",
]
