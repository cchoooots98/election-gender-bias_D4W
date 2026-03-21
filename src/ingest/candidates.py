"""Ingest the Interior Ministry Tour 1 and Tour 2 candidate lists to bronze Parquet.

Bronze layer rule (CLAUDE.md §6): these modules are faithful copies of the
source — no cleaning, no renaming, no filtering. Schema transformation
happens in src/transform/dim_candidate.py (silver layer).

Tour 1 source (137.8 MB): all candidates across all communes. Filtering
to tête de liste (position == 1) happens downstream.

Tour 2 source (20.5 MB): second-round candidate lists. Used only to derive
the advanced_to_tour2 flag in dim_candidate_leader — it is NOT a second
analysis source. Rationale: sampling from Tour 1 avoids survivorship bias
(candidates who advanced partly benefited from media coverage we are measuring).
The advanced_to_tour2 flag is then used as a control variable in regression.

Portfolio signal: the skip-if-unchanged download pattern and per-row provenance
columns demonstrate production-grade ingest reliability.
"""

import logging
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import BRONZE_DIR, DATA_SOURCES, RAW_DIR
from src.ingest._base import build_provenance_columns, download_raw_file

logger = logging.getLogger(__name__)

_SOURCE_KEY = "candidates_tour1"
_SOURCE_CFG = DATA_SOURCES[_SOURCE_KEY]


def download_candidates(raw_dir: Path = RAW_DIR) -> tuple[Path, str]:
    """Download the Tour 1 candidate CSV from data.gouv.fr.

    Args:
        raw_dir: Directory where the raw CSV will be saved.

    Returns:
        Tuple of (path_to_raw_csv, md5_hex).

    Raises:
        requests.HTTPError: On non-2xx HTTP response.
        requests.Timeout: If download exceeds 120 seconds.
    """
    dest_path = raw_dir / "gouv" / _SOURCE_CFG["raw_filename"]
    return download_raw_file(url=_SOURCE_CFG["url"], dest_path=dest_path)


def load_candidates_to_bronze(
    raw_csv_path: Path,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Read the raw candidates CSV and write it as a bronze Parquet file.

    Adds provenance columns (_source_url, _ingested_at, _source_hash).
    All source columns are preserved as-is — bronze is an exact replica.

    Encoding note: Interior Ministry CSVs are typically latin-1 (ISO-8859-1).
    If the file fails to read with latin-1, utf-8 is tried as a fallback.
    The actual encoding is confirmed in notebooks/02_source_schema_exploration.ipynb.

    dtype=str on read: prevents commune codes like "01001" from being parsed
    as integer 1001 and losing the leading zero — a common ingest bug.

    Args:
        raw_csv_path: Path to the downloaded raw CSV.
        bronze_dir: Root of the bronze data layer.

    Returns:
        Path to the written bronze Parquet file.

    Raises:
        FileNotFoundError: If raw_csv_path does not exist.
        ValueError: If the CSV is empty after reading.
    """
    if not raw_csv_path.exists():
        raise FileNotFoundError(f"Raw candidates CSV not found: {raw_csv_path}")

    logger.info("Reading candidates CSV path=%s", raw_csv_path.name)

    try:
        candidates_df = pd.read_csv(
            raw_csv_path,
            dtype=str,      # Preserve leading zeros in commune codes
            encoding="utf-8",
            sep=";",        # Interior Ministry CSVs use semicolons (confirmed by hex dump).
                            # Explicit sep avoids csv.Sniffer path which can corrupt accented
                            # characters on Windows when sep=None is used with the C engine.
        )
    except UnicodeDecodeError:
        logger.warning(
            "utf-8 decode failed for %s — retrying with latin-1", raw_csv_path.name
        )
        candidates_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="latin-1",
            sep=";",
        )

    if candidates_df.empty:
        raise ValueError(f"Candidates CSV is empty: {raw_csv_path}")

    logger.info(
        "Loaded candidates rows=%d columns=%d", len(candidates_df), len(candidates_df.columns)
    )
    logger.info("Columns: %s", candidates_df.columns.tolist())

    # Attach mandatory bronze provenance columns.
    from src.ingest._base import compute_file_md5
    source_hash = compute_file_md5(raw_csv_path)
    provenance = build_provenance_columns(
        source_url=_SOURCE_CFG["url"], source_hash=source_hash
    )
    for col, val in provenance.items():
        candidates_df[col] = val

    # Write to bronze Parquet — idempotent (overwrites on re-run).
    bronze_path = bronze_dir / "candidates" / "candidates_tour1.parquet"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(candidates_df)
    pq.write_table(table, bronze_path, compression="snappy")

    logger.info(
        "Bronze Parquet written path=%s rows=%d size_mb=%.1f",
        bronze_path,
        len(candidates_df),
        bronze_path.stat().st_size / 1_048_576,
    )
    return bronze_path


def ingest_candidates(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Orchestrate full Tour 1 candidates ingest: download → bronze Parquet.

    This is the public entry point called by the pipeline runner or Airflow task.

    Args:
        raw_dir: Directory for the raw CSV download.
        bronze_dir: Root bronze directory.

    Returns:
        Path to the bronze Parquet file.
    """
    raw_path, _md5 = download_candidates(raw_dir=raw_dir)
    bronze_path = load_candidates_to_bronze(raw_csv_path=raw_path, bronze_dir=bronze_dir)
    return bronze_path


# ── Tour 2 ────────────────────────────────────────────────────────────────────

_TOUR2_SOURCE_KEY = "candidates_tour2"
_TOUR2_SOURCE_CFG = DATA_SOURCES[_TOUR2_SOURCE_KEY]


def download_candidates_tour2(raw_dir: Path = RAW_DIR) -> tuple[Path, str]:
    """Download the Tour 2 candidate CSV from data.gouv.fr.

    Tour 2 data is used only to derive the advanced_to_tour2 flag in
    dim_candidate_leader. It is NOT a separate set of analysis subjects.

    Args:
        raw_dir: Directory where the raw CSV will be saved.

    Returns:
        Tuple of (path_to_raw_csv, md5_hex).

    Raises:
        requests.HTTPError: On non-2xx HTTP response.
        requests.Timeout: If download exceeds 120 seconds.
    """
    dest_path = raw_dir / "gouv" / _TOUR2_SOURCE_CFG["raw_filename"]
    return download_raw_file(url=_TOUR2_SOURCE_CFG["url"], dest_path=dest_path)


def load_candidates_tour2_to_bronze(
    raw_csv_path: Path,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Read the Tour 2 candidates CSV and write it as a bronze Parquet file.

    Same encoding-fallback and dtype=str strategy as Tour 1.

    Args:
        raw_csv_path: Path to the downloaded raw CSV.
        bronze_dir: Root of the bronze data layer.

    Returns:
        Path to the written bronze Parquet file.

    Raises:
        FileNotFoundError: If raw_csv_path does not exist.
        ValueError: If the CSV is empty after reading.
    """
    if not raw_csv_path.exists():
        raise FileNotFoundError(f"Raw Tour 2 candidates CSV not found: {raw_csv_path}")

    logger.info("Reading Tour 2 candidates CSV path=%s", raw_csv_path.name)

    try:
        tour2_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="utf-8",
            sep=";",        # Same format as Tour 1 — semicolon separator, confirmed.
        )
    except UnicodeDecodeError:
        logger.warning(
            "utf-8 decode failed for %s — retrying with latin-1", raw_csv_path.name
        )
        tour2_df = pd.read_csv(
            raw_csv_path,
            dtype=str,
            encoding="latin-1",
            sep=";",
        )

    if tour2_df.empty:
        raise ValueError(f"Tour 2 candidates CSV is empty: {raw_csv_path}")

    logger.info(
        "Loaded Tour 2 candidates rows=%d columns=%d",
        len(tour2_df),
        len(tour2_df.columns),
    )
    logger.info("Columns: %s", tour2_df.columns.tolist())

    from src.ingest._base import compute_file_md5
    source_hash = compute_file_md5(raw_csv_path)
    provenance = build_provenance_columns(
        source_url=_TOUR2_SOURCE_CFG["url"], source_hash=source_hash
    )
    for col, val in provenance.items():
        tour2_df[col] = val

    bronze_path = bronze_dir / "candidates" / "candidates_tour2.parquet"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pandas(tour2_df)
    pq.write_table(table, bronze_path, compression="snappy")

    logger.info(
        "Bronze Parquet written path=%s rows=%d size_mb=%.1f",
        bronze_path,
        len(tour2_df),
        bronze_path.stat().st_size / 1_048_576,
    )
    return bronze_path


def ingest_candidates_tour2(
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> Path:
    """Orchestrate Tour 2 candidates ingest: download → bronze Parquet.

    Args:
        raw_dir: Directory for the raw CSV download.
        bronze_dir: Root bronze directory.

    Returns:
        Path to the bronze Parquet file.
    """
    raw_path, _md5 = download_candidates_tour2(raw_dir=raw_dir)
    bronze_path = load_candidates_tour2_to_bronze(
        raw_csv_path=raw_path, bronze_dir=bronze_dir
    )
    return bronze_path
