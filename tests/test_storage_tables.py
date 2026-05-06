"""Tests for shared table persistence helpers."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.storage.tables import (
    write_duckdb_table,
    write_json_report,
    write_parquet_table,
)


def test_write_parquet_table_materializes_dataframe(tmp_path):
    """Happy path: a DataFrame is written to a readable Parquet artifact."""
    parquet_path = tmp_path / "silver" / "example.parquet"
    source_dataframe = pd.DataFrame([{"id": "row-1", "value": 3}])

    written_path = write_parquet_table(source_dataframe, parquet_path)

    assert written_path == parquet_path
    assert pd.read_parquet(parquet_path).to_dict("records") == [
        {"id": "row-1", "value": 3}
    ]


def test_write_duckdb_table_replaces_existing_rows(tmp_path):
    """Regression: shared DuckDB writes keep replace semantics."""
    duckdb = pytest.importorskip("duckdb")
    duckdb_path = tmp_path / "warehouse.duckdb"

    write_duckdb_table(
        dataframe=pd.DataFrame([{"id": "old-row"}]),
        schema_name="silver",
        table_name="example",
        duckdb_path=duckdb_path,
    )
    write_duckdb_table(
        dataframe=pd.DataFrame([{"id": "new-row"}]),
        schema_name="silver",
        table_name="example",
        duckdb_path=duckdb_path,
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        rows = conn.execute("SELECT id FROM silver.example").fetchall()
    finally:
        conn.close()
    assert rows == [("new-row",)]


def test_write_json_report_materializes_payload(tmp_path):
    """Happy path: QA report payloads are written as UTF-8 JSON."""
    report_path = tmp_path / "reports" / "qa.json"
    payload = {"status": "success", "rows": 2}

    written_path = write_json_report(payload, report_path)

    assert written_path == report_path
    assert json.loads(report_path.read_text(encoding="utf-8")) == payload
