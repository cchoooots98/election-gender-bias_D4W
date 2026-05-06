"""Shared table and report persistence helpers.

This module owns cross-layer storage primitives. Ingest, NLP, and future metric
pipelines can depend on these helpers without importing each other's internal
modules.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def _import_duckdb() -> Any:
    """Import DuckDB lazily so pure parsing tests stay lightweight."""
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError("duckdb is required to persist analytical tables.") from exc
    return duckdb


def write_parquet_table(dataframe: Any, parquet_path: Path) -> Path:
    """Write one DataFrame to Parquet with snappy compression.

    Args:
        dataframe: DataFrame-like object accepted by ``pyarrow.Table.from_pandas``.
        parquet_path: Output path for the Parquet artifact.

    Returns:
        The written Parquet path.
    """
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dataframe), parquet_path, compression="snappy")
    return parquet_path


def write_duckdb_table(
    *,
    dataframe: Any,
    schema_name: str,
    table_name: str,
    duckdb_path: Path,
) -> None:
    """Materialize one DataFrame into DuckDB using replace semantics.

    Args:
        dataframe: DataFrame-like object registered as a DuckDB staging table.
        schema_name: Trusted internal DuckDB schema constant. Do not pass
            user-provided values because DuckDB parameter binding does not
            apply to object identifiers.
        table_name: Trusted internal DuckDB table constant. Do not pass
            user-provided values because DuckDB parameter binding does not
            apply to object identifiers.
        duckdb_path: Path to the DuckDB database file.

    Raises:
        RuntimeError: If DuckDB is not installed in the runtime environment.
    """
    duckdb = _import_duckdb()
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        conn.register("staged_dataframe", dataframe)
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema_name}.{table_name} "
            "AS SELECT * FROM staged_dataframe"
        )
        conn.unregister("staged_dataframe")
    finally:
        conn.close()


def write_json_report(payload: dict[str, object], report_path: Path) -> Path:
    """Write a JSON QA or summary artifact.

    Args:
        payload: JSON-serializable report payload.
        report_path: Output path for the JSON artifact.

    Returns:
        The written report path.
    """
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, ensure_ascii=False, indent=2)
    return report_path
