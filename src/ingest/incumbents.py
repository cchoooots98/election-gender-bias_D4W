"""Ingest the Interior Ministry RNE incumbent mayors file to bronze Parquet.

RNE = Répertoire National des Élus — the national elected officials registry.
This extract (as of 2026-02-27) contains outgoing mayors from the 2020–2026
municipal term. It is used in dim_candidate.py to derive the is_incumbent flag
for each analysed tête de liste via fuzzy name matching.

Why we need this:
  Incumbent status is a key control variable in the exposure regression model.
  Incumbents typically receive more media coverage regardless of gender —
  if we do not control for it, a gender effect may actually be an incumbency
  effect. Including is_incumbent as a covariate isolates the pure gender signal.

Bronze layer rule: faithful copy, no filtering or renaming.
"""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import BRONZE_DIR, DATA_SOURCES, RAW_DIR
from src.ingest._base import build_provenance_columns, compute_file_md5, download_raw_file

logger = logging.getLogger(__name__)

_SOURCE_KEY = "rne_incumbents"
_SOURCE_CFG = DATA_SOURCES[_SOURCE_KEY]


def download_rne_incumbents(raw_dir: Path = RAW_DIR) -> tuple[Path, str]:
    """Download the RNE current mayors CSV from data.gouv.fr.

    Args:
        raw_dir: Directory where the raw CSV will be saved.

    Returns:
        Tuple of (path_to_raw_csv, md5_hex).

    Raises:
        requests.HTTPError: On non-2xx HTTP response.
        requests.Timeout: If download exceeds 120 seconds.
    """
    dest_path = raw_dir / "rne" / _SOURCE_CFG["raw_filename"]
    return download_raw_file(url=_SOURCE_CFG["url"], dest_path=dest_path)


def load_incumbents_to_bronze(
    raw_csv_path: Path,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Read the RNE mayors CSV and write it as a bronze Parquet file.

    The RNE file is 3.9 MB — small enough to load into memory without chunking.
    dtype=str is used to preserve commune codes with leading zeros.

    Args:
        raw_csv_path: Path to the downloaded RNE CSV.
        bronze_dir: Root of the bronze data layer.

    Returns:
        Path to the written bronze Parquet file.

    Raises:
        FileNotFoundError: If raw_csv_path does not exist.
        ValueError: If the CSV is empty after reading.
    """
    if not raw_csv_path.exists():
        raise FileNotFoundError(f"Raw RNE incumbents CSV not found: {raw_csv_path}")

    logger.info("Reading RNE incumbents CSV path=%s", raw_csv_path.name)

    try:
        incumbents_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="utf-8",
            sep=";",        # French government CSVs use semicolons (confirmed by EDA).
        )
    except UnicodeDecodeError:
        logger.warning(
            "UTF-8 decode failed for %s — retrying with latin-1", raw_csv_path.name
        )
        incumbents_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="latin-1",
            sep=";",
        )

    if incumbents_df.empty:
        raise ValueError(f"RNE incumbents CSV is empty: {raw_csv_path}")

    logger.info(
        "Loaded RNE incumbents rows=%d columns=%d",
        len(incumbents_df),
        len(incumbents_df.columns),
    )
    logger.info("Columns: %s", incumbents_df.columns.tolist())

    source_hash = compute_file_md5(raw_csv_path)
    provenance = build_provenance_columns(
        source_url=_SOURCE_CFG["url"], source_hash=source_hash
    )
    for col, val in provenance.items():
        incumbents_df[col] = val

    bronze_path = bronze_dir / "rne" / "rne_incumbents.parquet"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(incumbents_df)
    pq.write_table(table, bronze_path, compression="snappy")

    logger.info(
        "Bronze Parquet written path=%s rows=%d size_mb=%.1f",
        bronze_path,
        len(incumbents_df),
        bronze_path.stat().st_size / 1_048_576,
    )
    return bronze_path


def ingest_incumbents(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Orchestrate RNE incumbents ingest: download → bronze Parquet.

    Args:
        raw_dir: Directory for the raw CSV download.
        bronze_dir: Root bronze directory.

    Returns:
        Path to the bronze Parquet file.
    """
    raw_path, _md5 = download_rne_incumbents(raw_dir=raw_dir)
    bronze_path = load_incumbents_to_bronze(raw_csv_path=raw_path, bronze_dir=bronze_dir)
    return bronze_path
