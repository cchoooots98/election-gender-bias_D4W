"""Daily news collection entry point — Windows-compatible alternative to the Airflow DAG.

Run once per day (e.g., via Windows Task Scheduler or manually) to collect
articles for yesterday's 24-hour window across all three providers.

Usage:
    python scripts/run_daily_news_collection.py
    python scripts/run_daily_news_collection.py --date 2026-03-25     # specific date
    python scripts/run_daily_news_collection.py --providers curated gdelt  # subset

This script mirrors the logic in airflow/dags/daily_news_collection_dag.py
but runs in a single process, making it suitable for Windows Task Scheduler
or ad-hoc execution without an Airflow installation.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

# Ensure src/ is importable when running from the project root
sys.path.insert(0, str(Path(__file__).parents[1]))

from src.config.settings import GOLD_DIR
from src.ingest.news.pipeline import run_news_ingest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logger = logging.getLogger(__name__)

_DEFAULT_MANIFEST = GOLD_DIR / "sample_manifest.json"
# GDELT indexes with a lag; extend its window by one extra day for overlap safety.
_GDELT_LOOKBACK_EXTRA_DAYS = 1
_ALL_PROVIDERS = ("curated", "gdelt", "gnews")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run daily news collection for one 24h window."
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="Window start date in YYYY-MM-DD format. Defaults to yesterday (UTC).",
    )
    parser.add_argument(
        "--providers",
        nargs="+",
        choices=list(_ALL_PROVIDERS),
        default=list(_ALL_PROVIDERS),
        help="Providers to run. Defaults to all three: curated gdelt gnews.",
    )
    return parser.parse_args()


def run_daily_collection(
    window_date: date | None = None,
    providers: tuple[str, ...] = _ALL_PROVIDERS,
    sample_manifest_path: Path = _DEFAULT_MANIFEST,
) -> dict:
    """Run one day of news collection across the requested providers.

    Args:
        window_date: The start of the 24h window to collect. Defaults to yesterday (UTC).
        providers: Provider names to run. Subset of ("curated", "gdelt", "gnews").
        sample_manifest_path: Path to the 24-candidate sample manifest JSON.

    Returns:
        Summary dict with keys: date, total_hits, total_errors, per_provider.
    """
    if window_date is None:
        window_date = (datetime.now(UTC) - timedelta(days=1)).date()

    window_end = window_date + timedelta(days=1)

    logger.info(
        "Daily collection: date=%s providers=%s window=[%s, %s)",
        window_date,
        providers,
        window_date,
        window_end,
    )

    per_provider: dict[str, dict] = {}
    total_hits = 0
    total_errors = 0

    for provider in providers:
        # GDELT gets an extra day of lookback to compensate for indexing lag.
        if provider == "gdelt":
            provider_start = window_date - timedelta(days=_GDELT_LOOKBACK_EXTRA_DAYS)
        else:
            provider_start = window_date

        logger.info(
            "Running provider=%s window=[%s, %s)", provider, provider_start, window_end
        )
        result = run_news_ingest(
            sample_manifest_path=sample_manifest_path,
            provider_order=(provider,),
            window_start=provider_start,
            window_end=window_end,
        )
        per_provider[provider] = {
            "status": result.status,
            "hit_count": result.hit_count,
            "error_count": result.error_count,
        }
        total_hits += result.hit_count
        total_errors += result.error_count
        logger.info(
            "Provider complete: provider=%s status=%s hits=%d errors=%d",
            provider,
            result.status,
            result.hit_count,
            result.error_count,
        )

    summary = {
        "date": str(window_date),
        "total_hits": total_hits,
        "total_errors": total_errors,
        "per_provider": per_provider,
    }
    logger.info(
        "Daily collection complete: date=%s total_hits=%d total_errors=%d",
        window_date,
        total_hits,
        total_errors,
    )
    return summary


if __name__ == "__main__":
    args = _parse_args()
    summary = run_daily_collection(
        window_date=args.date,
        providers=tuple(args.providers),
    )
    # Exit non-zero if any provider had errors — lets Task Scheduler detect failures.
    if summary["total_errors"] > 0:
        logger.error("Collection finished with errors: %s", summary)
        sys.exit(1)
    logger.info("Collection finished cleanly: %s", summary)
