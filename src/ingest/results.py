"""Ingest official municipal election results to Bronze Parquet.

Bronze keeps the source schema faithful. The wide commune-level CSV is
normalized later in ``src/transform/fact_election_result.py``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.errors import EmptyDataError

from src.config.settings import BRONZE_DIR, DATA_SOURCES, RAW_DIR
from src.ingest._base import (
    build_provenance_columns,
    compute_file_md5,
    download_raw_file,
)
from src.observability.run_logger import log_source_snapshot

logger = logging.getLogger(__name__)

_ROUND1_SOURCE_KEY = "results_tour1"
_ROUND1_SOURCE_CFG = DATA_SOURCES[_ROUND1_SOURCE_KEY]

_ROUND2_SOURCE_KEY = "results_tour2"
_ROUND2_SOURCE_CFG = DATA_SOURCES[_ROUND2_SOURCE_KEY]


def _read_results_csv(raw_csv_path: Path) -> pd.DataFrame:
    """Read an official municipal-results CSV with encoding fallback.

    Args:
        raw_csv_path: Path to the downloaded raw CSV.

    Returns:
        Raw results DataFrame with all columns preserved as strings.

    Raises:
        FileNotFoundError: If the raw CSV does not exist.
        ValueError: If the CSV is empty.
    """
    if not raw_csv_path.exists():
        raise FileNotFoundError(f"Raw municipal results CSV not found: {raw_csv_path}")

    try:
        results_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="utf-8",
            sep=";",
        )
    except UnicodeDecodeError:
        logger.warning(
            "utf-8 decode failed for %s - retrying with latin-1", raw_csv_path.name
        )
        results_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="latin-1",
            sep=";",
        )
    except EmptyDataError as exc:
        raise ValueError(f"Municipal results CSV is empty: {raw_csv_path}") from exc

    if results_df.empty:
        raise ValueError(f"Municipal results CSV is empty: {raw_csv_path}")

    logger.info(
        "Loaded municipal results rows=%d columns=%d source=%s",
        len(results_df),
        len(results_df.columns),
        raw_csv_path.name,
    )
    return results_df


def _download_results(raw_dir: Path, source_cfg: dict[str, str]) -> tuple[Path, str]:
    """Download one official municipal-results CSV."""
    dest_path = raw_dir / "gouv" / source_cfg["raw_filename"]
    return download_raw_file(url=source_cfg["url"], dest_path=dest_path)


def _load_results_to_bronze(
    raw_csv_path: Path,
    bronze_dir: Path,
    source_cfg: dict[str, str],
    bronze_filename: str,
) -> tuple[Path, int]:
    """Write one results CSV to Bronze Parquet with provenance columns."""
    results_df = _read_results_csv(raw_csv_path)

    source_hash = compute_file_md5(raw_csv_path)
    provenance = build_provenance_columns(
        source_url=source_cfg["url"],
        source_hash=source_hash,
    )
    for column_name, column_value in provenance.items():
        results_df[column_name] = column_value

    bronze_path = bronze_dir / "results" / bronze_filename
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(results_df), bronze_path, compression="snappy")

    logger.info(
        "Bronze municipal results written path=%s rows=%d",
        bronze_path,
        len(results_df),
    )
    return bronze_path, len(results_df)


def ingest_results_tour1(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Download and bronze-load first-round municipal results."""
    raw_path, source_hash = _download_results(
        raw_dir=raw_dir, source_cfg=_ROUND1_SOURCE_CFG
    )
    bronze_path, row_count = _load_results_to_bronze(
        raw_csv_path=raw_path,
        bronze_dir=bronze_dir,
        source_cfg=_ROUND1_SOURCE_CFG,
        bronze_filename="results_tour1.parquet",
    )
    log_source_snapshot(
        source_key=_ROUND1_SOURCE_KEY,
        source_url=_ROUND1_SOURCE_CFG["url"],
        source_hash=source_hash,
        raw_file_path=raw_path,
        row_count=row_count,
    )
    return bronze_path


def ingest_results_tour2(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Download and bronze-load second-round municipal results."""
    raw_path, source_hash = _download_results(
        raw_dir=raw_dir, source_cfg=_ROUND2_SOURCE_CFG
    )
    bronze_path, row_count = _load_results_to_bronze(
        raw_csv_path=raw_path,
        bronze_dir=bronze_dir,
        source_cfg=_ROUND2_SOURCE_CFG,
        bronze_filename="results_tour2.parquet",
    )
    log_source_snapshot(
        source_key=_ROUND2_SOURCE_KEY,
        source_url=_ROUND2_SOURCE_CFG["url"],
        source_hash=source_hash,
        raw_file_path=raw_path,
        row_count=row_count,
    )
    return bronze_path
