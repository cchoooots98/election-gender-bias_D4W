"""Ingest the Interior Ministry seats/population XLSX file to bronze Parquet.

This file provides two pieces of data per commune:
  NBRE_SAP_COM  — number of municipal council seats to fill
  NBRE_SAP_EPCI — number of community council (EPCI) seats to fill
  population    — resident population (from INSEE; used for city-size stratification)

It is the only source file in this pipeline that is XLSX rather than CSV.
openpyxl is used as the engine (required by pandas for .xlsx; listed in requirements.in).

Why population from this file, not from INSEE 5359146:
  The Interior Ministry embeds the relevant INSEE population figure directly.
  The full Dossier Complet (197 MB, 1900 indicators) is overkill for this step.
  We document this choice in README as the "seats proxy population" approach.

Bronze layer rule: faithful copy, dtype=str to preserve commune codes.
"""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import BRONZE_DIR, DATA_SOURCES, RAW_DIR
from src.ingest._base import build_provenance_columns, compute_file_md5, download_raw_file

logger = logging.getLogger(__name__)

_SOURCE_KEY = "seats_population"
_SOURCE_CFG = DATA_SOURCES[_SOURCE_KEY]


def download_seats_population(raw_dir: Path = RAW_DIR) -> tuple[Path, str]:
    """Download the seats/population XLSX from data.gouv.fr.

    Args:
        raw_dir: Directory where the raw XLSX will be saved.

    Returns:
        Tuple of (path_to_raw_xlsx, md5_hex).

    Raises:
        requests.HTTPError: On non-2xx HTTP response.
        requests.Timeout: If download exceeds 60 seconds.
    """
    dest_path = raw_dir / "gouv" / _SOURCE_CFG["raw_filename"]
    return download_raw_file(url=_SOURCE_CFG["url"], dest_path=dest_path)


def load_seats_to_bronze(
    raw_xlsx_path: Path,
    bronze_dir: Path = BRONZE_DIR,
    sheet_name: str | int = 0,
) -> Path:
    """Read the seats/population XLSX and write it as a bronze Parquet file.

    dtype=str is critical here: without it, commune codes like '01001' are
    parsed as integer 1001, losing the leading zero and breaking all downstream
    joins on commune_insee. This is the most common ingest bug for French
    administrative data.

    sheet_name is a parameter because XLSX files frequently have a metadata
    or documentation sheet before the data. The actual sheet index is
    confirmed in notebooks/02_source_schema_exploration.ipynb.

    Args:
        raw_xlsx_path: Path to the XLSX file.
        bronze_dir: Root of the bronze data layer.
        sheet_name: Sheet name or 0-based integer index. Default 0 (first sheet).

    Returns:
        Path to the written bronze Parquet file.

    Raises:
        FileNotFoundError: If raw_xlsx_path does not exist.
        ValueError: If the specified sheet is empty or does not exist.
    """
    if not raw_xlsx_path.exists():
        raise FileNotFoundError(f"Raw seats XLSX not found: {raw_xlsx_path}")

    logger.info(
        "Reading seats XLSX path=%s sheet=%s", raw_xlsx_path.name, sheet_name
    )

    try:
        seats_df = pd.read_excel(
            raw_xlsx_path,
            sheet_name=sheet_name,
            dtype=str,        # Preserve commune codes with leading zeros
            engine="openpyxl",
        )
    except Exception as exc:
        raise ValueError(
            f"Failed to read XLSX sheet={sheet_name} from {raw_xlsx_path}: {exc}"
        ) from exc

    if seats_df.empty:
        raise ValueError(
            f"Seats XLSX sheet={sheet_name} is empty in {raw_xlsx_path}"
        )

    logger.info(
        "Loaded seats rows=%d columns=%d", len(seats_df), len(seats_df.columns)
    )
    logger.info("Columns: %s", seats_df.columns.tolist())

    source_hash = compute_file_md5(raw_xlsx_path)
    provenance = build_provenance_columns(
        source_url=_SOURCE_CFG["url"], source_hash=source_hash
    )
    for col, val in provenance.items():
        seats_df[col] = val

    bronze_path = bronze_dir / "seats" / "seats_population.parquet"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(seats_df)
    pq.write_table(table, bronze_path, compression="snappy")

    logger.info(
        "Bronze Parquet written path=%s rows=%d size_mb=%.1f",
        bronze_path,
        len(seats_df),
        bronze_path.stat().st_size / 1_048_576,
    )
    return bronze_path


def ingest_seats_population(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    sheet_name: str | int = 0,
) -> Path:
    """Orchestrate seats/population XLSX ingest: download → bronze Parquet.

    Args:
        raw_dir: Directory for the raw XLSX download.
        bronze_dir: Root bronze directory.
        sheet_name: Sheet name or index to read. Default 0.

    Returns:
        Path to the bronze Parquet file.
    """
    raw_path, _md5 = download_seats_population(raw_dir=raw_dir)
    bronze_path = load_seats_to_bronze(
        raw_xlsx_path=raw_path, bronze_dir=bronze_dir, sheet_name=sheet_name
    )
    return bronze_path
