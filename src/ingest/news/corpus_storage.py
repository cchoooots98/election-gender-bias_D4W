"""Silver / Gold persistence helpers for the Europresse corpus pipeline.

Layer responsibility
--------------------
This module handles **Silver and Gold** writes produced by the corpus ETL
(``corpus_pipeline.py``). Bronze writes for raw Europresse imports happen in
the corpus pipeline itself because the supported runnable flow now has a single
authoritative archive source.

Calling sequence
----------------
::

    corpus_pipeline.run_news_corpus_etl()
        → corpus_storage.write_parquet_table()   # Silver / Gold Parquet
        → corpus_storage.write_duckdb_table()    # Silver / Gold DuckDB
        → corpus_storage.write_json_report()     # QA / summary JSON

Do not use these helpers for Bronze-layer imports; the Bronze landing write is
owned by ``corpus_pipeline.run_news_corpus_etl()``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def _import_duckdb():
    """Import DuckDB lazily so pure parsing tests stay lightweight."""
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError(
            "duckdb is required to persist the news corpus tables."
        ) from exc
    return duckdb


def write_parquet_table(dataframe, parquet_path: Path) -> Path:
    """Write one DataFrame to Parquet with snappy compression."""
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
    """Materialize one DataFrame into DuckDB using replace semantics."""
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
    """Write a JSON QA or summary artifact."""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, ensure_ascii=False, indent=2)
    return report_path
