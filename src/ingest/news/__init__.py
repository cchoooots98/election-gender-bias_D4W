"""Public package surface for the active Europresse-first news workflow.

The supported runnable path is:

``Europresse export -> news import manifest -> corpus ETL -> dbt marts``

Legacy provider-discovery experiments are intentionally excluded from the
default import surface so the package API mirrors the project's current
production-grade story.
"""

from src.ingest.news.corpus import (
    build_fact_article,
    build_fact_article_source,
    inspect_import_batch,
    load_news_import_manifest,
    parse_import_batch,
    write_news_import_manifest,
)
from src.ingest.news.corpus_pipeline import run_news_corpus_etl
from src.ingest.news.matching import build_fact_mentions
from src.ingest.news.models import (
    ImportBatchFile,
    ImportBatchInspection,
    NewsCorpusRunResult,
    NewsImportManifest,
)
from src.ingest.news.normalize import canonicalize_url
from src.ingest.news.queries import normalize_text_for_match, parse_candidate_full_name

__all__ = [
    "ImportBatchFile",
    "ImportBatchInspection",
    "NewsCorpusRunResult",
    "NewsImportManifest",
    "build_fact_article",
    "build_fact_article_source",
    "build_fact_mentions",
    "canonicalize_url",
    "inspect_import_batch",
    "load_news_import_manifest",
    "normalize_text_for_match",
    "parse_candidate_full_name",
    "parse_import_batch",
    "run_news_corpus_etl",
    "write_news_import_manifest",
]
