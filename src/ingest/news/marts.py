"""Compatibility imports for legacy news mart callers.

SQL-friendly Gold marts are now owned by dbt. Python-only regression and data
quality helpers live under ``src.metrics.news`` because they are analytical
metrics responsibilities rather than ingest responsibilities.
"""

from src.metrics.news.quality import run_news_corpus_quality_checks
from src.metrics.news.regression import (
    build_mart_bootstrap_ci,
    build_mart_regression_results,
)

__all__ = [
    "build_mart_bootstrap_ci",
    "build_mart_regression_results",
    "run_news_corpus_quality_checks",
]
