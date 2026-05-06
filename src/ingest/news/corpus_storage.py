"""Backward-compatible storage imports for the news corpus pipeline.

Shared persistence helpers live in ``src.storage.tables``. This module remains
as a compatibility shim for older imports while new code should depend on the
shared storage layer directly.
"""

from src.storage.tables import (
    write_duckdb_table,
    write_json_report,
    write_parquet_table,
)

__all__ = [
    "write_duckdb_table",
    "write_json_report",
    "write_parquet_table",
]
