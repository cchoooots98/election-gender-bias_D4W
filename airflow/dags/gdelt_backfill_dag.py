"""Airflow DAG for low-frequency GDELT historical backfill.

This DAG is intentionally isolated from the main benchmark path. GDELT is useful
for backfilling older windows, but its public API is rate-limited enough that it
should run on a slow schedule rather than in every interactive benchmark.
"""

from __future__ import annotations

from datetime import datetime

try:
    from airflow.operators.python import PythonOperator

    from airflow import DAG
except ImportError:  # pragma: no cover - exercised by import-smoke tests
    gdelt_backfill_dag = None
else:

    def _run_gdelt_backfill() -> None:
        """Import lazily so local development does not require Airflow installed."""
        from src.orchestration.gdelt_backfill_pipeline import (
            run_gdelt_backfill_pipeline,
        )

        run_gdelt_backfill_pipeline()

    with DAG(
        dag_id="gdelt_backfill_pipeline",
        description="Low-frequency GDELT historical backfill for sampled candidates.",
        start_date=datetime(2026, 3, 25),
        schedule="0 6,18 * * *",
        catchup=False,
        tags=["news", "gdelt", "backfill"],
    ) as gdelt_backfill_dag:
        PythonOperator(
            task_id="run_gdelt_backfill",
            python_callable=_run_gdelt_backfill,
        )
