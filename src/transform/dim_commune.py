"""Build the dim_commune silver table from bronze COG and seats sources.

dim_commune is the geographic reference dimension in the star schema.
Grain: one row per commune (TYPECOM == 'COM').

Sources (bronze):
  data/bronze/geography/cog_communes.parquet   — commune codes, names, dep, reg
  data/bronze/seats/seats_population.parquet   — seats to fill, population

Outputs (silver):
  data/silver/dim_commune.parquet
  DuckDB: silver.dim_commune
  data/silver/_rejected/dim_commune_rejected.parquet  (quarantine)

Key column added here: city_size_bucket ('large' / 'medium' / 'small' / 'excluded')
This bucket is later used by the sampling algorithm to ensure gender balance
within each city-size stratum.

Column name note:
  The actual column names in the source files are UNKNOWN until
  notebooks/02_source_schema_exploration.ipynb is run. The column_map
  parameters allow callers to pass the real names discovered during EDA.
  Default maps in settings.py are placeholders that will be filled in
  after the first pipeline run with real data.

Industry pattern: this "config-driven transform" approach (column maps as
parameters, not hard-coded strings) is how dbt handles source column names
in sources.yml — the mapping is explicit and auditable.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    BRONZE_DIR,
    CITY_SIZE_LARGE_THRESHOLD,
    CITY_SIZE_MEDIUM_THRESHOLD,
    CITY_SIZE_SMALL_THRESHOLD,
    COG_COLUMN_MAP,
    DQ_MAX_NULL_RATE,
    SEATS_COLUMN_MAP,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

# Canonical output columns for dim_commune.
# Any downstream table that references this table uses these names.
_OUTPUT_COLUMNS = [
    "commune_insee",
    "commune_name",
    "dep_code",
    "reg_code",
    "seats_municipal",
    "seats_epci",
    "population",
    "city_size_bucket",
]


def _assign_city_size_bucket(population: int | None) -> str:
    """Map a commune's resident population to an analytical size bucket.

    Buckets are defined in settings.py (CITY_SIZE_* thresholds) so changing
    a threshold requires editing exactly one place.

    Communes below CITY_SIZE_SMALL_THRESHOLD are labelled 'excluded' rather
    than being dropped here — the exclusion decision happens at sampling time,
    not at dim_commune build time, so the full table remains a general-purpose
    geographic reference usable by other analyses.

    Args:
        population: Resident population (population municipale). May be None
            if the seats file has no entry for this commune.

    Returns:
        One of: 'large', 'medium', 'small', 'excluded'.
    """
    if population is None or population <= 0:
        return "excluded"
    if population >= CITY_SIZE_LARGE_THRESHOLD:
        return "large"
    if population >= CITY_SIZE_MEDIUM_THRESHOLD:
        return "medium"
    if population >= CITY_SIZE_SMALL_THRESHOLD:
        return "small"
    return "excluded"


def _validate_dim_commune(
    commune_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run DQ checks on the pre-write dim_commune DataFrame.

    Quarantine strategy: bad rows are moved to rejected_df with a
    _rejection_reason column rather than silently dropped. This follows the
    "quarantine, not delete" principle from CLAUDE.md §6.

    Pipeline-level failure (DataQualityError) is raised only when the null
    rate on commune_insee exceeds DQ_MAX_NULL_RATE — meaning the entire
    column is unreliable, not just a few rows.

    DQ checks performed:
      1. commune_insee is not null
      2. No duplicate commune_insee values (keep first, quarantine rest)
      3. population is not null and > 0 (quarantine if missing, but do not fail)
      4. city_size_bucket is one of the four valid values

    Args:
        commune_df: DataFrame to validate.

    Returns:
        Tuple of (clean_df, rejected_df). rejected_df has a _rejection_reason
        column. If all rows pass, rejected_df is empty.

    Raises:
        DataQualityError: If null rate on commune_insee > DQ_MAX_NULL_RATE.
    """
    rejected_rows: list[pd.DataFrame] = []
    clean_df = commune_df.copy()

    # ── Check 1: commune_insee null rate ──────────────────────────────────────
    null_count = clean_df["commune_insee"].isna().sum()
    null_rate = null_count / max(len(clean_df), 1)

    if null_rate > DQ_MAX_NULL_RATE:
        raise DataQualityError(
            f"commune_insee null rate {null_rate:.1%} exceeds threshold "
            f"{DQ_MAX_NULL_RATE:.1%} ({null_count}/{len(clean_df)} rows)"
        )

    if null_count > 0:
        mask = clean_df["commune_insee"].isna()
        bad = clean_df[mask].copy()
        bad["_rejection_reason"] = "commune_insee is null"
        rejected_rows.append(bad)
        clean_df = clean_df[~mask]
        logger.warning("Quarantined %d rows with null commune_insee", null_count)

    # ── Check 2: duplicate commune_insee ──────────────────────────────────────
    duplicated_mask = clean_df.duplicated(subset=["commune_insee"], keep="first")
    dup_count = duplicated_mask.sum()
    if dup_count > 0:
        bad = clean_df[duplicated_mask].copy()
        bad["_rejection_reason"] = "duplicate commune_insee"
        rejected_rows.append(bad)
        clean_df = clean_df[~duplicated_mask]
        logger.warning("Quarantined %d duplicate commune_insee rows", dup_count)

    # ── Check 3: invalid city_size_bucket ─────────────────────────────────────
    valid_buckets = {"large", "medium", "small", "excluded"}
    invalid_bucket_mask = ~clean_df["city_size_bucket"].isin(valid_buckets)
    if invalid_bucket_mask.sum() > 0:
        bad = clean_df[invalid_bucket_mask].copy()
        bad["_rejection_reason"] = "invalid city_size_bucket"
        rejected_rows.append(bad)
        clean_df = clean_df[~invalid_bucket_mask]
        logger.warning(
            "Quarantined %d rows with invalid city_size_bucket",
            invalid_bucket_mask.sum(),
        )

    rejected_df = (
        pd.concat(rejected_rows, ignore_index=True)
        if rejected_rows
        else pd.DataFrame(columns=list(commune_df.columns) + ["_rejection_reason"])
    )

    logger.info(
        "DQ complete: clean_rows=%d rejected_rows=%d", len(clean_df), len(rejected_df)
    )
    return clean_df, rejected_df


def build_dim_commune(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    cog_column_map: dict[str, str] | None = None,
    seats_column_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Build the dim_commune silver table from bronze COG and seats sources.

    Data flow:
      bronze/geography/cog_communes.parquet   (filtered to TYPECOM == 'COM')
        LEFT JOIN
      bronze/seats/seats_population.parquet   (on commune_insee)
        → add city_size_bucket
        → DQ validation
        → write silver/dim_commune.parquet
        → write DuckDB silver.dim_commune (idempotent)

    LEFT JOIN rationale: not every commune has seats data (very small communes,
    or EPCI-only entries). We keep all COG communes so that the table is a
    complete geographic reference — communes with no seats data get
    city_size_bucket = 'excluded'.

    column_map parameters: the raw source column names are unknown until
    Notebook EDA confirms them. Pass the real names discovered there. Defaults
    come from settings.py and will be updated after EDA.

    Args:
        bronze_dir: Root bronze directory.
        silver_dir: Root silver directory.
        duckdb_path: Path to the DuckDB warehouse file.
        cog_column_map: Maps COG raw column names → canonical names.
            Example: {'COM': 'commune_insee', 'LIBELLE': 'commune_name'}.
            Defaults to settings.COG_COLUMN_MAP.
        seats_column_map: Maps seats XLSX raw column names → canonical names.
            Example: {'NBRE_SAP_COM': 'seats_municipal', 'POPULATION': 'population'}.
            Defaults to settings.SEATS_COLUMN_MAP.

    Returns:
        The clean dim_commune DataFrame (silver rows only, not rejected).

    Raises:
        FileNotFoundError: If bronze Parquet files do not exist.
        DataQualityError: If null rate on commune_insee exceeds DQ_MAX_NULL_RATE.
    """
    effective_cog_map = cog_column_map if cog_column_map is not None else COG_COLUMN_MAP
    effective_seats_map = seats_column_map if seats_column_map is not None else SEATS_COLUMN_MAP

    # ── Load bronze sources ───────────────────────────────────────────────────
    cog_bronze_path = bronze_dir / "geography" / "cog_communes.parquet"
    seats_bronze_path = bronze_dir / "seats" / "seats_population.parquet"

    for path in (cog_bronze_path, seats_bronze_path):
        if not path.exists():
            raise FileNotFoundError(
                f"Bronze Parquet not found: {path}. "
                "Run the ingest step first."
            )

    cog_df = pd.read_parquet(cog_bronze_path)
    seats_df = pd.read_parquet(seats_bronze_path)

    logger.info(
        "Loaded bronze: cog_rows=%d seats_rows=%d", len(cog_df), len(seats_df)
    )

    # ── Rename COG columns to canonical names ─────────────────────────────────
    if effective_cog_map:
        cog_df = cog_df.rename(columns=effective_cog_map)
        logger.info("Applied COG column map: %s", effective_cog_map)
    else:
        logger.warning(
            "COG_COLUMN_MAP is empty — column names not yet confirmed by EDA. "
            "Run notebooks/02_source_schema_exploration.ipynb first."
        )

    # Filter to actual communes only (TYPECOM == 'COM').
    # 'ARM' = arrondissement municipal (Paris/Lyon/Marseille sub-units)
    # 'COMA' = commune associée — not standalone communes
    if "typecom" in cog_df.columns:
        before = len(cog_df)
        cog_df = cog_df[cog_df["typecom"] == "COM"].copy()
        logger.info(
            "Filtered TYPECOM == 'COM': before=%d after=%d", before, len(cog_df)
        )
    else:
        logger.warning(
            "typecom column not found after rename — skipping TYPECOM filter. "
            "All %d COG rows will be kept.",
            len(cog_df),
        )

    # ── Rename seats columns to canonical names ───────────────────────────────
    if effective_seats_map:
        seats_df = seats_df.rename(columns=effective_seats_map)
        logger.info("Applied seats column map: %s", effective_seats_map)
    else:
        logger.warning(
            "SEATS_COLUMN_MAP is empty — seats columns not renamed. "
            "Downstream joins may fail until EDA is complete."
        )

    # ── Left join COG + seats on commune_insee ────────────────────────────────
    # commune_insee must exist in COG after rename; seats may not have a
    # 'commune_insee' column until EDA confirms the correct source column name.
    seats_join_key = "commune_insee" if "commune_insee" in seats_df.columns else None

    if seats_join_key:
        commune_df = cog_df.merge(
            seats_df[[col for col in seats_df.columns if not col.startswith("_")]],
            on="commune_insee",
            how="left",
        )
        logger.info(
            "Joined COG + seats: result_rows=%d", len(commune_df)
        )
    else:
        logger.warning(
            "Cannot join seats — 'commune_insee' not in seats columns after rename. "
            "Proceeding with COG only; population will be null."
        )
        commune_df = cog_df.copy()

    # ── Assign city_size_bucket ───────────────────────────────────────────────
    if "population" in commune_df.columns:
        # Convert population to numeric (it was read as str from bronze).
        commune_df["population"] = pd.to_numeric(
            commune_df["population"], errors="coerce"
        )
        commune_df["city_size_bucket"] = commune_df["population"].apply(
            lambda pop: _assign_city_size_bucket(
                int(pop) if pop is not None and not pd.isna(pop) else None
            )
        )
    else:
        logger.warning(
            "population column not found — all communes assigned 'excluded'. "
            "Update SEATS_COLUMN_MAP in settings.py after EDA."
        )
        commune_df["city_size_bucket"] = "excluded"

    # ── Ensure required output columns exist ──────────────────────────────────
    for col in _OUTPUT_COLUMNS:
        if col not in commune_df.columns:
            commune_df[col] = None
            logger.warning("Output column '%s' not present — filled with None", col)

    commune_df = commune_df[_OUTPUT_COLUMNS].copy()

    # ── DQ validation ─────────────────────────────────────────────────────────
    clean_df, rejected_df = _validate_dim_commune(commune_df)

    # ── Write silver Parquet ──────────────────────────────────────────────────
    silver_path = silver_dir / "dim_commune.parquet"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(clean_df), silver_path, compression="snappy")
    logger.info("Silver Parquet written path=%s rows=%d", silver_path, len(clean_df))

    # Write rejected rows to quarantine sub-table.
    if not rejected_df.empty:
        rejected_path = silver_dir / "_rejected" / "dim_commune_rejected.parquet"
        rejected_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(rejected_df), rejected_path, compression="snappy"
        )
        logger.warning(
            "Quarantine written path=%s rows=%d", rejected_path, len(rejected_df)
        )

    # ── Write to DuckDB (idempotent) ──────────────────────────────────────────
    # Idempotent write: DROP + CREATE ensures re-runs never produce duplicates.
    # This is the most common data reliability interview question for DE roles.
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("DROP TABLE IF EXISTS silver.dim_commune")
        conn.execute(
            "CREATE TABLE silver.dim_commune AS SELECT * FROM clean_df"
        )
        row_count = conn.execute(
            "SELECT count(*) FROM silver.dim_commune"
        ).fetchone()[0]
        logger.info("DuckDB silver.dim_commune written rows=%d", row_count)
    finally:
        conn.close()

    return clean_df
