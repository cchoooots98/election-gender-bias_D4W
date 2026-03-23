"""Ingest the INSEE COG 2026 commune reference file to bronze Parquet.

COG = Code Officiel Géographique — INSEE's official geographic nomenclature.
The commune file (v_commune_2026.csv) provides the canonical mapping between:
  - commune INSEE code (COM field, e.g. '75056' for Paris)
  - department code (DEP)
  - region code (REG)
  - commune name (LIBELLE)
  - commune type (TYPECOM: 'COM', 'ARM', 'COMA', 'COMD', 'COMS')

This file is the Join Key standard for all French administrative data.
It is used in dim_commune.py to enrich the seats/population data with region
and department codes needed for geographic stratification.

Bronze layer rule: faithful copy, all rows and columns preserved.
Filtering to TYPECOM == 'COM' (actual communes, not sub-units) happens in silver.
"""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import BRONZE_DIR, DATA_SOURCES, RAW_DIR
from src.ingest._base import (
    build_provenance_columns,
    compute_file_md5,
    download_raw_file,
)
from src.observability.run_logger import log_source_snapshot

logger = logging.getLogger(__name__)

_SOURCE_KEY = "insee_cog_communes"
_SOURCE_CFG = DATA_SOURCES[_SOURCE_KEY]


def download_cog_communes(raw_dir: Path = RAW_DIR) -> tuple[Path, str]:
    """Download the INSEE COG 2026 commune CSV.

    Args:
        raw_dir: Directory where the raw CSV will be saved.

    Returns:
        Tuple of (path_to_raw_csv, md5_hex).

    Raises:
        requests.HTTPError: On non-2xx HTTP response.
        requests.Timeout: If download exceeds 120 seconds.
    """
    dest_path = raw_dir / "insee" / _SOURCE_CFG["raw_filename"]
    return download_raw_file(url=_SOURCE_CFG["url"], dest_path=dest_path)


def load_cog_to_bronze(
    raw_csv_path: Path,
    bronze_dir: Path = BRONZE_DIR,
) -> tuple[Path, int]:
    """Read the COG commune CSV and write it as a bronze Parquet file.

    INSEE COG v_commune_2026.csv uses UTF-8 encoding and comma separators.
    dtype=str preserves leading zeros in COM/DEP/REG codes
    (e.g. DEP '01' would be parsed as integer 1 without this).

    Args:
        raw_csv_path: Path to the downloaded COG CSV.
        bronze_dir: Root of the bronze data layer.

    Returns:
        Tuple of (path_to_bronze_parquet, row_count).

    Raises:
        FileNotFoundError: If raw_csv_path does not exist.
        ValueError: If the CSV is empty after reading.
    """
    if not raw_csv_path.exists():
        raise FileNotFoundError(f"Raw COG CSV not found: {raw_csv_path}")

    logger.info("Reading COG communes CSV path=%s", raw_csv_path.name)

    # INSEE COG v_commune_2026.csv uses UTF-8 and commas as the field separator.
    # dtype=str preserves leading zeros in COM/DEP/REG codes (e.g. DEP '01' must
    # not become integer 1). Explicit sep=',' avoids csv.Sniffer, which can
    # corrupt accented characters on Windows when used with the C engine.
    try:
        cog_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="utf-8",
            sep=",",
        )
    except UnicodeDecodeError:
        logger.warning(
            "UTF-8 decode failed for %s — retrying with latin-1", raw_csv_path.name
        )
        cog_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="latin-1",
            sep=",",
        )

    if cog_df.empty:
        raise ValueError(f"COG CSV is empty: {raw_csv_path}")

    logger.info(
        "Loaded COG rows=%d columns=%d typecom_values=%s",
        len(cog_df),
        len(cog_df.columns),
        (
            cog_df.get("TYPECOM", cog_df.get("typecom", pd.Series(dtype=str)))
            .unique()
            .tolist()
            if any(c.upper() == "TYPECOM" for c in cog_df.columns)
            else "column_name_unknown_until_EDA"
        ),
    )
    logger.info("Columns: %s", cog_df.columns.tolist())

    source_hash = compute_file_md5(raw_csv_path)
    provenance = build_provenance_columns(
        source_url=_SOURCE_CFG["url"], source_hash=source_hash
    )
    for col, val in provenance.items():
        cog_df[col] = val

    bronze_path = bronze_dir / "geography" / "cog_communes.parquet"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(cog_df)
    pq.write_table(table, bronze_path, compression="snappy")

    logger.info(
        "Bronze Parquet written path=%s rows=%d size_mb=%.1f",
        bronze_path,
        len(cog_df),
        bronze_path.stat().st_size / 1_048_576,
    )
    return bronze_path, len(cog_df)


def ingest_geography(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Orchestrate COG geography ingest: download → bronze Parquet.

    Args:
        raw_dir: Directory for the raw CSV download.
        bronze_dir: Root bronze directory.

    Returns:
        Path to the bronze Parquet file.
    """
    raw_path, source_hash = download_cog_communes(raw_dir=raw_dir)
    bronze_path, row_count = load_cog_to_bronze(
        raw_csv_path=raw_path, bronze_dir=bronze_dir
    )
    log_source_snapshot(
        source_key=_SOURCE_KEY,
        source_url=_SOURCE_CFG["url"],
        source_hash=source_hash,
        raw_file_path=raw_path,
        row_count=row_count,
    )
    return bronze_path
