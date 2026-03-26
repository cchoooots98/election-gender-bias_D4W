"""Pipeline run observability: record data source snapshots and run metadata.

This module writes two meta tables to DuckDB:
  meta.meta_source_snapshot -- one row per source file downloaded, recording
      URL, MD5 hash, row count, and timestamp. Enables change detection:
      if source_hash changes between runs, the source file was updated.
  meta.meta_run -- one row per pipeline run with start/end time,
      status, row counts, and artifact paths.

Why observability matters for a portfolio project:
  A hiring manager reviewing this repo will check whether the pipeline can
  answer \"when was this data last fetched and was it different from last time?\"
  Without meta_source_snapshot, the answer is \"I don't know.\" With it, the
  pipeline has a complete audit trail -- the same capability that dbt's
  source freshness checks and Airflow's XCom provide in production systems.

Industry pattern: this is the \"pipeline lineage\" and \"data freshness\" layer
that platforms like Databricks Unity Catalog, Snowflake Data Lineage, and
Atlan provide as managed services. We implement a minimal version ourselves.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.config.settings import WAREHOUSE_PATH

logger = logging.getLogger(__name__)

_CREATE_META_SCHEMA = "CREATE SCHEMA IF NOT EXISTS meta"

_CREATE_SOURCE_SNAPSHOT_TABLE = """
CREATE TABLE IF NOT EXISTS meta.meta_source_snapshot (
    snapshot_id      VARCHAR     PRIMARY KEY,
    source_key       VARCHAR     NOT NULL,
    source_url       VARCHAR     NOT NULL,
    source_hash      CHAR(32)    NOT NULL,
    raw_file_path    VARCHAR     NOT NULL,
    row_count        INTEGER     NOT NULL,
    fetched_at       TIMESTAMPTZ NOT NULL
)
"""

_CREATE_RUN_TABLE = """
CREATE TABLE IF NOT EXISTS meta.meta_run (
    run_id         VARCHAR     PRIMARY KEY,
    flow_name      VARCHAR     NOT NULL,
    start_ts       TIMESTAMPTZ NOT NULL,
    end_ts         TIMESTAMPTZ NOT NULL,
    status         VARCHAR     NOT NULL,
    rows_ingested  INTEGER     NOT NULL,
    error_count    INTEGER     NOT NULL,
    artifact_paths VARCHAR     NOT NULL
)
"""


def _import_duckdb():
    """Import DuckDB lazily so modules remain importable in thin environments."""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "duckdb is required to write observability metadata. "
            "Install project dependencies before running pipeline entrypoints."
        ) from exc
    return duckdb


def _build_snapshot_id(source_key: str, fetched_at: datetime) -> str:
    """Generate a deterministic snapshot primary key.

    MD5(source_key + fetched_at ISO string) is stable: the same source fetched
    at the same second always produces the same ID.

    Args:
        source_key: Identifier from DATA_SOURCES (for example 'candidates_tour1').
        fetched_at: UTC timestamp of the fetch.

    Returns:
        32-character hex MD5 string.
    """
    raw = f"{source_key}:{fetched_at.isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()


def _ensure_meta_tables(conn: Any) -> None:
    """Create meta schema and tables if they do not already exist.

    Idempotent: safe to call on every pipeline run.

    Args:
        conn: Open DuckDB connection.
    """
    conn.execute(_CREATE_META_SCHEMA)
    conn.execute(_CREATE_SOURCE_SNAPSHOT_TABLE)
    conn.execute(_CREATE_RUN_TABLE)


def log_source_snapshot(
    source_key: str,
    source_url: str,
    source_hash: str,
    raw_file_path: Path,
    row_count: int,
    fetched_at: datetime | None = None,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> str:
    """Record a data source snapshot to meta.meta_source_snapshot.

    Called by every ingest function after successfully writing to bronze.
    If a snapshot with the same snapshot_id already exists (same source_key
    + same second), the insert is skipped (idempotent).

    Args:
        source_key: Identifier from DATA_SOURCES (for example 'candidates_tour1').
        source_url: URL the file was downloaded from.
        source_hash: MD5 hex of the downloaded raw file.
        raw_file_path: Path to the saved raw file.
        row_count: Number of rows in the bronze Parquet.
        fetched_at: UTC timestamp of the download. Defaults to now().
        duckdb_path: Path to the DuckDB warehouse file.

    Returns:
        The snapshot_id that was written.
    """
    if fetched_at is None:
        fetched_at = datetime.now(UTC)

    snapshot_id = _build_snapshot_id(source_key, fetched_at)

    duckdb = _import_duckdb()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))

    try:
        _ensure_meta_tables(conn)

        existing = conn.execute(
            "SELECT snapshot_id FROM meta.meta_source_snapshot WHERE snapshot_id = ?",
            [snapshot_id],
        ).fetchone()

        if existing:
            logger.info(
                "Snapshot already recorded snapshot_id=%s source_key=%s -- skipping",
                snapshot_id,
                source_key,
            )
            return snapshot_id

        conn.execute(
            """
            INSERT INTO meta.meta_source_snapshot
                (snapshot_id, source_key, source_url, source_hash,
                 raw_file_path, row_count, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snapshot_id,
                source_key,
                source_url,
                source_hash,
                str(raw_file_path),
                row_count,
                fetched_at,
            ],
        )

        logger.info(
            "Source snapshot recorded source_key=%s rows=%d hash=%s snapshot_id=%s",
            source_key,
            row_count,
            source_hash[:8] + "...",
            snapshot_id[:8] + "...",
        )

    finally:
        conn.close()

    return snapshot_id


def get_last_source_hash(
    source_key: str,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> str | None:
    """Return the most recently recorded MD5 hash for a source.

    Used by ingest modules to detect whether a source file has changed
    since the last pipeline run -- enabling incremental ingest decisions.

    Args:
        source_key: Identifier from DATA_SOURCES.
        duckdb_path: Path to the DuckDB warehouse file.

    Returns:
        The last recorded source_hash string, or None if no snapshot exists.
    """
    if not duckdb_path.exists():
        return None

    duckdb = _import_duckdb()
    conn = duckdb.connect(str(duckdb_path))
    try:
        _ensure_meta_tables(conn)
        result = conn.execute(
            """
            SELECT source_hash
            FROM meta.meta_source_snapshot
            WHERE source_key = ?
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            [source_key],
        ).fetchone()
        return result[0] if result else None
    finally:
        conn.close()


def log_pipeline_run(
    run_id: str,
    flow_name: str,
    start_ts: datetime,
    end_ts: datetime,
    status: str,
    rows_ingested: int,
    error_count: int,
    artifact_paths: list[str | Path],
    duckdb_path: Path = WAREHOUSE_PATH,
) -> str:
    """Record one pipeline run to meta.meta_run.

    Args:
        run_id: Unique identifier for this pipeline run.
        flow_name: Logical pipeline name.
        start_ts: UTC timestamp when the run started.
        end_ts: UTC timestamp when the run finished.
        status: Final run state ('success', 'partial', or 'failed').
        rows_ingested: Total rows materialized by this runnable slice.
        error_count: Count of non-successful steps in this run.
        artifact_paths: Ordered list of output artifact paths.
        duckdb_path: Path to the DuckDB warehouse file.

    Returns:
        The run_id that was written.
    """
    artifact_paths_json = json.dumps(
        [str(path) for path in artifact_paths],
        ensure_ascii=False,
    )

    duckdb = _import_duckdb()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        _ensure_meta_tables(conn)
        conn.execute("DELETE FROM meta.meta_run WHERE run_id = ?", [run_id])
        conn.execute(
            """
            INSERT INTO meta.meta_run
                (run_id, flow_name, start_ts, end_ts, status,
                 rows_ingested, error_count, artifact_paths)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                flow_name,
                start_ts,
                end_ts,
                status,
                rows_ingested,
                error_count,
                artifact_paths_json,
            ],
        )
        logger.info(
            "Pipeline run recorded run_id=%s flow=%s status=%s rows=%d errors=%d",
            run_id,
            flow_name,
            status,
            rows_ingested,
            error_count,
        )
    finally:
        conn.close()

    return run_id
