"""Helpers for running dbt-owned news marts."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

import pandas as pd

from src.config.settings import WAREHOUSE_PATH

logger = logging.getLogger(__name__)

_DEFAULT_DBT_PROJECT_DIR = Path("dbt")
_DEFAULT_DBT_PROFILES_DIR = Path("dbt")


def _import_duckdb():
    """Import DuckDB lazily so unit tests can stub dbt without warehouse IO."""
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError("duckdb is required for dbt mart exports") from exc
    return duckdb


def _find_dbt_executable() -> Path:
    """Resolve the dbt console script from the active virtual environment."""
    executable_name = "dbt.exe" if os.name == "nt" else "dbt"
    venv_candidate = Path(sys.executable).with_name(executable_name)
    if venv_candidate.exists():
        return venv_candidate

    path_candidate = shutil.which("dbt")
    if path_candidate:
        return Path(path_candidate)

    raise RuntimeError(
        "dbt executable not found. Install the default requirements before "
        "running the news corpus pipeline."
    )


def run_dbt_news_marts(
    *,
    duckdb_path: Path = WAREHOUSE_PATH,
    project_dir: Path = _DEFAULT_DBT_PROJECT_DIR,
    profiles_dir: Path = _DEFAULT_DBT_PROFILES_DIR,
    select: str = "marts.news",
) -> None:
    """Run dbt models that own SQL-friendly Gold news marts.

    Args:
        duckdb_path: DuckDB warehouse file used by dbt-duckdb.
        project_dir: dbt project directory.
        profiles_dir: Directory containing the non-sensitive dbt profile.
        select: dbt selector. Defaults to the news mart package.

    Raises:
        RuntimeError: If dbt is not installed.
        subprocess.CalledProcessError: If dbt exits with a non-zero status.
    """
    dbt_executable = _find_dbt_executable()
    env = os.environ.copy()
    env["DBT_DUCKDB_PATH"] = str(duckdb_path)

    command = [
        str(dbt_executable),
        "run",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--select",
        select,
    ]
    logger.info(
        "Running dbt news marts duckdb_path=%s project_dir=%s select=%s",
        duckdb_path,
        project_dir,
        select,
    )
    subprocess.run(command, check=True, env=env)


def read_duckdb_table(
    *,
    duckdb_path: Path,
    schema_name: str,
    table_name: str,
) -> pd.DataFrame:
    """Read a DuckDB table into a pandas DataFrame.

    Args:
        duckdb_path: DuckDB warehouse file.
        schema_name: Source schema name.
        table_name: Source table name.

    Returns:
        DataFrame containing the requested table.
    """
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        return conn.execute(f"SELECT * FROM {schema_name}.{table_name}").fetchdf()
    finally:
        conn.close()


def export_duckdb_tables_to_parquet(
    *,
    duckdb_path: Path,
    schema_name: str,
    table_names: Sequence[str],
    output_dir: Path,
) -> dict[str, Path]:
    """Export dbt-owned DuckDB tables to Parquet compatibility artifacts.

    Args:
        duckdb_path: DuckDB warehouse file.
        schema_name: Schema containing the tables.
        table_names: Table names to export.
        output_dir: Destination directory.

    Returns:
        Mapping of table name to written Parquet path.
    """
    duckdb = _import_duckdb()
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    written_paths: dict[str, Path] = {}
    try:
        for table_name in table_names:
            parquet_path = output_dir / f"{table_name}.parquet"
            escaped_path = parquet_path.as_posix().replace("'", "''")
            conn.execute(
                f"COPY (SELECT * FROM {schema_name}.{table_name}) "
                f"TO '{escaped_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)"
            )
            written_paths[table_name] = parquet_path
    finally:
        conn.close()
    return written_paths
