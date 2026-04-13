"""Console script entrypoint for the enterprise news corpus pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import GOLD_DIR, SILVER_DIR
from src.orchestration.news_corpus_pipeline import (
    _DEFAULT_IMPORT_MANIFEST,
    run_news_corpus_pipeline,
)

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the Europresse news corpus ETL pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
--------
  Primary cohort (default):
    python -m src.cli.run_news_corpus_pipeline

  Sensitivity analysis — expanded cohort (48 candidates):
    python -m src.cli.run_news_corpus_pipeline \\
        --manifest-path data/raw/news/cohort_sa_48/news_import_manifest.json \\
        --sample-leaders-path data/gold/sample_leaders_sa48.parquet

  Sensitivity analysis — relaxed constraints:
    python -m src.cli.run_news_corpus_pipeline \\
        --manifest-path data/raw/news/cohort_sa_relaxed/news_import_manifest.json \\
        --sample-leaders-path data/gold/sample_leaders_sa_relaxed.parquet
""",
    )
    parser.add_argument(
        "--manifest-path",
        type=Path,
        default=_DEFAULT_IMPORT_MANIFEST,
        metavar="PATH",
        help=(
            "Path to news_import_manifest.json. " f"Default: {_DEFAULT_IMPORT_MANIFEST}"
        ),
    )
    parser.add_argument(
        "--sample-leaders-path",
        type=Path,
        default=GOLD_DIR / "sample_leaders.parquet",
        metavar="PATH",
        help=(
            "Path to the frozen sample_leaders.parquet for this cohort. "
            f"Default: {GOLD_DIR / 'sample_leaders.parquet'}"
        ),
    )
    parser.add_argument(
        "--dim-commune-path",
        type=Path,
        default=SILVER_DIR / "dim_commune.parquet",
        metavar="PATH",
        help="Path to dim_commune.parquet. Rarely needs overriding.",
    )
    parser.add_argument(
        "--enable-web-scrape",
        action="store_true",
        help=(
            "Allow fetching uncached article URLs. Cached web fetches are reused "
            "even when this flag is omitted."
        ),
    )
    parser.add_argument(
        "--bootstrap-resamples",
        type=int,
        default=2000,
        metavar="N",
        help="Number of bootstrap resamples for regression confidence intervals.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the news corpus pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_news_corpus_pipeline(
            import_manifest_path=args.manifest_path,
            sample_leaders_path=args.sample_leaders_path,
            dim_commune_path=args.dim_commune_path,
            enable_web_scrape=args.enable_web_scrape,
            bootstrap_resamples=args.bootstrap_resamples,
        )
    except Exception:
        logger.exception("News corpus pipeline exited with failure")
        return 1

    logger.info(
        "News corpus pipeline finished status=%s batch_id=%s artifacts=%d",
        result.status,
        result.batch_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
