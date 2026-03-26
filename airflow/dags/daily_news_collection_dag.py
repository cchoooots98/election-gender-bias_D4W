"""Daily news collection DAG for the election gender bias analysis project.

Orchestrates two parallel provider tasks (curated RSS/sitemap, GNews) followed
by output validation and observability logging. Runs daily from 2026-03-27 to
2026-04-30 with catchup=True so missed days self-heal.

## Why GDELT is excluded from this DAG

GDELT DOC 2.0 enforces a strict inter-query rate limit: consecutive queries to
the same endpoint must be separated by at least 2 hours. With 24 candidates,
a full sweep requires 24 × 2h = 48 hours minimum — incompatible with a daily
cadence. Additionally, empirical testing (2026-03-25 backfill: 24 candidates,
analysis window 2026-02-01 to 2026-04-30) showed 0 hits, confirming that local
French municipal candidates are not indexed at meaningful density in GDELT's
source corpus. GDELT is retained as an on-demand one-time backfill tool via
``airflow/dags/gdelt_backfill_dag.py`` and ``scripts/run_gdelt_backfill.py``.

## Architecture note

    Historical backfill (one-time)           Daily collection (this DAG)
    ─────────────────────────────            ─────────────────────────────
    run_gdelt_backfill_pipeline()  →         fetch_curated (24h window)
    covers 2026-02-01 to now                 fetch_gnews   (24h window)
                                             ↓
                                             validate_output
                                             ↓
                                             log_run_meta

## Deployment

Requires Apache Airflow on a POSIX system (Linux, macOS, or WSL2/Docker on Windows).
For local development:

    export AIRFLOW_HOME=$(pwd)/airflow
    airflow db migrate
    airflow standalone          # UI at http://localhost:8080

The DAG uses a 07:00 UTC schedule (09:00 Paris time, after overnight articles settle).
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

logger = logging.getLogger(__name__)

# ── Analysis window constants ─────────────────────────────────────────────────
# Matches ANALYSIS_START_DATE / ANALYSIS_END_DATE in src/config/settings.py.
_DAG_START_DATE = datetime(2026, 3, 27, tzinfo=UTC)
_DAG_END_DATE = datetime(2026, 4, 30, 23, 59, tzinfo=UTC)

_DEFAULT_MANIFEST = Path(__file__).parents[2] / "data" / "gold" / "sample_manifest.json"


@dag(
    dag_id="daily_news_collection",
    description="Daily curated + GNews news collection for 24 French municipal election candidates.",
    schedule="0 7 * * *",  # 07:00 UTC = 09:00 Paris time
    start_date=_DAG_START_DATE,
    end_date=_DAG_END_DATE,
    catchup=True,  # Backfill missed days automatically when DAG is first activated
    max_active_runs=1,  # Prevent overlapping runs that would race on the DuckDB file
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
    },
    tags=["news", "ingest", "election-gender-bias"],
)
def daily_news_collection_dag():
    """Daily news ingestion pipeline for election gender bias analysis.

    Each run covers a 24-hour window anchored to the Airflow logical date (ds),
    which represents the start of the period being processed (yesterday's articles).
    """

    @task(task_id="fetch_curated")
    def fetch_curated(logical_date: datetime | None = None, **context) -> dict:
        """Fetch articles from curated RSS/sitemap outlets for the 24h window.

        Uses the realtime_only and historical_capable outlets in ACTIVE_CURATED_OUTLET_KEYS.
        Window: [logical_date, logical_date + 1 day) — strictly yesterday.
        """
        # Airflow injects logical_date (= ds as a datetime) via context
        execution_date = logical_date or context.get("logical_date")
        if execution_date is None:
            raise ValueError("logical_date not injected by Airflow context")

        window_start = execution_date.date()
        window_end = (execution_date + timedelta(days=1)).date()

        logger.info("fetch_curated: window=[%s, %s)", window_start, window_end)

        # Import here so Airflow's DAG parser does not execute src/ on import
        from src.ingest.news.pipeline import run_news_ingest

        result = run_news_ingest(
            sample_manifest_path=_DEFAULT_MANIFEST,
            provider_order=("curated",),
            window_start=window_start,
            window_end=window_end,
        )
        logger.info(
            "fetch_curated: status=%s hits=%d errors=%d",
            result.status,
            result.hit_count,
            result.error_count,
        )
        return {
            "provider": "curated",
            "status": result.status,
            "hit_count": result.hit_count,
            "error_count": result.error_count,
        }

    @task(task_id="fetch_gnews")
    def fetch_gnews(logical_date: datetime | None = None, **context) -> dict:
        """Fetch articles from GNews API for the 24h window.

        GNews free tier: 100 req/day. 24 candidates × 1 req = 24 req/day — fits comfortably.
        Window: [logical_date, logical_date + 1 day) — same as curated.
        """
        execution_date = logical_date or context.get("logical_date")
        if execution_date is None:
            raise ValueError("logical_date not injected by Airflow context")

        window_start = execution_date.date()
        window_end = (execution_date + timedelta(days=1)).date()

        logger.info("fetch_gnews: window=[%s, %s)", window_start, window_end)

        from src.ingest.news.pipeline import run_news_ingest

        result = run_news_ingest(
            sample_manifest_path=_DEFAULT_MANIFEST,
            provider_order=("gnews",),
            window_start=window_start,
            window_end=window_end,
        )
        logger.info(
            "fetch_gnews: status=%s hits=%d errors=%d",
            result.status,
            result.hit_count,
            result.error_count,
        )
        return {
            "provider": "gnews",
            "status": result.status,
            "hit_count": result.hit_count,
            "error_count": result.error_count,
        }

    @task(task_id="validate_output")
    def validate_output(
        curated_result: dict,
        gnews_result: dict,
        logical_date: datetime | None = None,
        **context,
    ) -> dict:
        """Validate that at least one provider returned hits for the window.

        Raises AirflowSkipException if all providers returned zero hits — this is
        expected on days with no qualifying articles and should not fail the DAG.
        Raises ValueError if any provider reported errors, so the retry policy fires.
        """
        total_hits = curated_result["hit_count"] + gnews_result["hit_count"]
        total_errors = curated_result["error_count"] + gnews_result["error_count"]

        execution_date = logical_date or context.get("logical_date")
        logger.info(
            "validate_output: date=%s total_hits=%d total_errors=%d",
            execution_date,
            total_hits,
            total_errors,
        )

        if total_errors > 0:
            raise ValueError(
                f"Provider errors detected: curated={curated_result['error_count']} "
                f"gnews={gnews_result['error_count']}"
            )

        if total_hits == 0:
            # Not a failure — some days legitimately have no new election coverage.
            raise AirflowSkipException(
                f"All providers returned zero hits for {execution_date}. "
                "No articles to log."
            )

        return {
            "total_hits": total_hits,
            "providers_with_hits": sum(
                1 for r in (curated_result, gnews_result) if r["hit_count"] > 0
            ),
        }

    @task(task_id="log_run_meta")
    def log_run_meta(
        validation_result: dict,
        logical_date: datetime | None = None,
        **context,
    ) -> None:
        """Log run metadata to the meta_run DuckDB table for observability.

        This is separate from Airflow's own task logs — it writes a row to
        the project's meta_run table so the Streamlit dashboard can display
        pipeline health alongside the analysis results.
        """
        execution_date = logical_date or context.get("logical_date")
        logger.info(
            "log_run_meta: date=%s total_hits=%d providers_with_hits=%d",
            execution_date,
            validation_result["total_hits"],
            validation_result["providers_with_hits"],
        )
        # Observability write is already handled inside run_news_ingest via
        # run_logger.log_pipeline_run — this task just emits a summary log line
        # for the Airflow UI task log viewer.

    # ── DAG wiring ────────────────────────────────────────────────────────────
    # Two fetch tasks run in parallel (independent providers, separate rate limits).
    # validate_output fans in both results before log_run_meta.
    curated = fetch_curated()
    gnews = fetch_gnews()
    validation = validate_output(curated, gnews)
    log_run_meta(validation)


daily_news_collection_dag()
