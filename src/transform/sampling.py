"""Stratified matched sampling: select 20–24 analysis subjects from dim_candidate_leader.

This module implements the core methodological design decision of the project.

WHY NOT SIMPLE RANDOM SAMPLING:
  Female list leaders are under-represented in larger cities. A simple random
  sample of 12F + 12M would produce mostly small-city women paired with large-city
  men — confounding gender with city size. Any observed media coverage difference
  could be explained by city size rather than gender bias.

WHY MATCHED STRATIFIED SAMPLING:
  We require gender balance WITHIN each city-size stratum. This ensures that
  every male candidate has a female counterpart of similar city size, making
  gender comparisons within strata valid. This is the same logic behind
  matched-pair experimental design in social science research.

Stratum quotas (from settings.py):
  large   (≥100k pop)   → SAMPLE_LARGE_TOTAL  = 4   (2F + 2M)
  medium  (20k–100k)    → SAMPLE_MEDIUM_TOTAL = 8   (4F + 4M)
  small   (3.5k–20k)    → SAMPLE_SMALL_TOTAL  = 12  (6F + 6M)

Additional soft constraint: ≥ SAMPLE_MIN_REGION_COUNT distinct INSEE region codes.
  If the initial sample under-represents regions, a swap-based resampling
  attempts to improve diversity without breaking gender balance.

Outputs:
  data/gold/sample_leaders.parquet    — the 24-person sample
  data/gold/sample_manifest.json      — audit trail: seed, quotas, per-candidate details
  DuckDB: gold.sample_leaders         — queryable via DuckDB
"""

import json
import logging
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    CANDIDATE_SAMPLE_SIZE,
    GOLD_DIR,
    SAMPLE_LARGE_TOTAL,
    SAMPLE_MEDIUM_TOTAL,
    SAMPLE_MIN_REGION_COUNT,
    SAMPLE_SMALL_TOTAL,
    SAMPLING_RANDOM_SEED,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.transform._exceptions import SamplingError

logger = logging.getLogger(__name__)

# Derived per-gender quotas from total stratum quotas.
# Each stratum total must be even (50/50 gender split).
_STRATUM_CONFIG: dict[str, dict[str, int]] = {
    "large": {"total": SAMPLE_LARGE_TOTAL, "per_gender": SAMPLE_LARGE_TOTAL // 2},
    "medium": {"total": SAMPLE_MEDIUM_TOTAL, "per_gender": SAMPLE_MEDIUM_TOTAL // 2},
    "small": {"total": SAMPLE_SMALL_TOTAL, "per_gender": SAMPLE_SMALL_TOTAL // 2},
}

_REQUIRED_SAMPLE_COLUMNS = {
    "leader_id",
    "gender",
    "commune_insee",
    "city_size_bucket",
    "reg_code",
    "nuance_group",
    "same_name_candidate_count",
}

_MANIFEST_DIM_COMMUNE_COLUMNS = [
    "commune_insee",
    "commune_name",
    "dep_code",
    "population",
]


def _prioritize_candidate_pool(
    candidate_pool_df: pd.DataFrame,
    per_gender: int,
    random_seed: int,
) -> pd.DataFrame:
    """Prefer low-collision names while keeping deterministic tie-breaking.

    The candidate pool is first shuffled with the configured random seed to
    produce stable but unbiased tie-breaking. A stable sort on
    same_name_candidate_count then prioritises candidates whose names are less
    ambiguous in the broader Tour 1 candidate universe.

    Args:
        candidate_pool_df: Single-gender pool for one city-size stratum.
        per_gender: Number of rows to select.
        random_seed: Seed used for deterministic shuffling.

    Returns:
        Prioritised sample of size per_gender.

    Raises:
        SamplingError: If same_name_candidate_count is missing or null.
    """
    if "same_name_candidate_count" not in candidate_pool_df.columns:
        raise SamplingError(
            "same_name_candidate_count is required for sampling priority. "
            "Rebuild dim_candidate_leader with the current transform code."
        )

    if candidate_pool_df["same_name_candidate_count"].isna().any():
        raise SamplingError(
            "same_name_candidate_count contains NULL values. "
            "The candidate dimension must provide non-null collision counts."
        )

    shuffled_pool_df = candidate_pool_df.sample(frac=1, random_state=random_seed)
    prioritised_pool_df = shuffled_pool_df.sort_values(
        by="same_name_candidate_count",
        ascending=True,
        kind="stable",
    )
    return prioritised_pool_df.head(per_gender).copy()


def _sample_stratum(
    pool_df: pd.DataFrame,
    bucket: str,
    per_gender: int,
    random_seed: int,
) -> pd.DataFrame:
    """Sample the required number of F and M candidates from one size stratum.

    Args:
        pool_df: Full dim_candidate_leader, already filtered to non-excluded communes.
        bucket: One of 'large', 'medium', 'small'.
        per_gender: Number of candidates to sample per gender.
        random_seed: Random state for reproducibility.

    Returns:
        DataFrame with (2 × per_gender) rows, balanced M/F.

    Raises:
        SamplingError: If the female or male pool in this bucket is too small.
    """
    bucket_pool = pool_df[pool_df["city_size_bucket"] == bucket]
    female_pool = bucket_pool[bucket_pool["gender"] == "F"]
    male_pool = bucket_pool[bucket_pool["gender"] == "M"]

    if len(female_pool) < per_gender:
        raise SamplingError(
            f"Insufficient female candidates in '{bucket}' bucket: "
            f"need {per_gender}, found {len(female_pool)}"
        )
    if len(male_pool) < per_gender:
        raise SamplingError(
            f"Insufficient male candidates in '{bucket}' bucket: "
            f"need {per_gender}, found {len(male_pool)}"
        )

    sampled_f = _prioritize_candidate_pool(
        candidate_pool_df=female_pool,
        per_gender=per_gender,
        random_seed=random_seed,
    )
    sampled_m = _prioritize_candidate_pool(
        candidate_pool_df=male_pool,
        per_gender=per_gender,
        random_seed=random_seed + 1,
    )

    logger.info(
        "Sampled bucket=%s female=%d male=%d", bucket, len(sampled_f), len(sampled_m)
    )
    return pd.concat([sampled_f, sampled_m], ignore_index=True)


def _attempt_geographic_resampling(
    initial_sample_df: pd.DataFrame,
    full_pool_df: pd.DataFrame,
    random_seed: int,
) -> pd.DataFrame:
    """Attempt to improve geographic diversity via gender-neutral swaps.

    Strategy:
    1. Find which regions are ABSENT from the sample.
    2. For each absent region, find a candidate from that region in the pool.
    3. Swap them with a candidate from an OVER-REPRESENTED region in the same
       city-size bucket AND same gender — preserving both gender balance and
       stratum quotas.
    4. If no feasible swap exists, log a WARNING and proceed — geographic
       diversity is a SOFT constraint; it should not crash the pipeline.

    Args:
        initial_sample_df: Output of per-stratum sampling.
        full_pool_df: All eligible leaders (pre-sampling pool).
        random_seed: Random state for tie-breaking.

    Returns:
        Improved sample (may be identical to initial_sample_df if no swap is feasible).
    """
    represented_regions = set(initial_sample_df["reg_code"].dropna().unique())
    all_pool_regions = set(full_pool_df["reg_code"].dropna().unique())
    absent_regions = all_pool_regions - represented_regions

    if not absent_regions:
        logger.info(
            "Geographic constraint met: %d distinct regions in sample",
            len(represented_regions),
        )
        return initial_sample_df

    # Count how many times each region appears in the sample.
    region_counts = initial_sample_df["reg_code"].value_counts()
    sample_df = initial_sample_df.copy()

    for absent_reg in list(absent_regions):
        # Find an eligible candidate from the absent region.
        absent_candidates = full_pool_df[
            (full_pool_df["reg_code"] == absent_reg)
            & (~full_pool_df["leader_id"].isin(sample_df["leader_id"]))
        ]
        if absent_candidates.empty:
            logger.warning(
                "No unsampled candidates in region %s — cannot add this region",
                absent_reg,
            )
            continue

        swap_in = absent_candidates.sample(n=1, random_state=random_seed).iloc[0]
        swap_gender = swap_in["gender"]
        swap_bucket = swap_in["city_size_bucket"]

        # Find a same-gender, same-bucket candidate from an over-represented region.
        over_rep_region = region_counts[region_counts == region_counts.max()].index[0]
        swap_out_candidates = sample_df[
            (sample_df["reg_code"] == over_rep_region)
            & (sample_df["gender"] == swap_gender)
            & (sample_df["city_size_bucket"] == swap_bucket)
        ]

        if swap_out_candidates.empty:
            logger.warning(
                "Cannot swap region %s → %s: no same-gender/bucket candidate "
                "in over-represented region %s",
                over_rep_region,
                absent_reg,
                over_rep_region,
            )
            continue

        swap_out_id = swap_out_candidates.iloc[0]["leader_id"]
        sample_df = sample_df[sample_df["leader_id"] != swap_out_id]
        sample_df = pd.concat([sample_df, swap_in.to_frame().T], ignore_index=True)
        region_counts = sample_df["reg_code"].value_counts()
        logger.info(
            "Geographic swap: removed region=%s, added region=%s (gender=%s bucket=%s)",
            over_rep_region,
            absent_reg,
            swap_gender,
            swap_bucket,
        )

    final_regions = sample_df["reg_code"].nunique()
    logger.info(
        "After geographic resampling: distinct_regions=%d (target≥%d)",
        final_regions,
        SAMPLE_MIN_REGION_COUNT,
    )
    return sample_df


def _validate_sample(sample_df: pd.DataFrame) -> None:
    """Assert that the sample satisfies all hard constraints.

    Hard constraints (will raise SamplingError if violated):
      - Total rows == CANDIDATE_SAMPLE_SIZE
      - Exactly CANDIDATE_SAMPLE_SIZE / 2 female rows
      - Exactly CANDIDATE_SAMPLE_SIZE / 2 male rows

    Soft constraint (WARNING only, no exception):
      - ≥ SAMPLE_MIN_REGION_COUNT distinct region codes

    Args:
        sample_df: The final sampled DataFrame to validate.

    Raises:
        SamplingError: If hard constraints are not met.
    """
    total = len(sample_df)
    if total != CANDIDATE_SAMPLE_SIZE:
        raise SamplingError(f"Sample size {total} != target {CANDIDATE_SAMPLE_SIZE}")

    female_count = (sample_df["gender"] == "F").sum()
    male_count = (sample_df["gender"] == "M").sum()
    target_per_gender = CANDIDATE_SAMPLE_SIZE // 2

    if female_count != target_per_gender:
        raise SamplingError(
            f"Female count {female_count} != target {target_per_gender}"
        )
    if male_count != target_per_gender:
        raise SamplingError(f"Male count {male_count} != target {target_per_gender}")

    duplicate_ids = sample_df["leader_id"].duplicated().sum()
    if duplicate_ids > 0:
        raise SamplingError(
            f"Sample contains {duplicate_ids} duplicate leader_id values"
        )

    distinct_regions = sample_df["reg_code"].nunique()
    if distinct_regions < SAMPLE_MIN_REGION_COUNT:
        logger.warning(
            "Geographic diversity constraint not fully met: "
            "distinct_regions=%d < target=%d. "
            "Proceeding — this is a soft constraint.",
            distinct_regions,
            SAMPLE_MIN_REGION_COUNT,
        )

    logger.info(
        "Sample validation passed: total=%d female=%d male=%d regions=%d",
        total,
        female_count,
        male_count,
        distinct_regions,
    )


def _build_manifest_sample_df(
    sample_df: pd.DataFrame,
    silver_dir: Path,
) -> pd.DataFrame:
    """Join manifest-only commune audit fields onto the sampled leaders.

    sample_leaders.parquet intentionally keeps the lean candidate schema from
    dim_candidate_leader. The manifest, however, is an audit artifact and must
    include human-readable commune context. Those fields are therefore joined
    from dim_commune only at manifest-write time.

    Args:
        sample_df: Final sampled leaders DataFrame.
        silver_dir: Root silver directory.

    Returns:
        Sample DataFrame enriched with manifest-only commune audit columns.

    Raises:
        FileNotFoundError: If dim_commune.parquet does not exist.
        SamplingError: If manifest audit columns are missing or null after join.
    """
    dim_commune_path = silver_dir / "dim_commune.parquet"
    if not dim_commune_path.exists():
        raise FileNotFoundError(
            f"dim_commune silver file not found: {dim_commune_path}. "
            "Run build_dim_commune() before build_sample()."
        )

    dim_commune_df = pd.read_parquet(dim_commune_path)
    missing_dim_columns = sorted(
        set(_MANIFEST_DIM_COMMUNE_COLUMNS) - set(dim_commune_df.columns)
    )
    if missing_dim_columns:
        raise SamplingError(
            "dim_commune is missing required manifest audit columns: "
            f"{missing_dim_columns}"
        )

    manifest_df = sample_df.merge(
        dim_commune_df[_MANIFEST_DIM_COMMUNE_COLUMNS],
        on="commune_insee",
        how="left",
        validate="many_to_one",
    )

    missing_audit_mask = (
        manifest_df[["commune_name", "dep_code", "population"]].isna().any(axis=1)
    )
    if missing_audit_mask.any():
        missing_communes = sorted(
            manifest_df.loc[missing_audit_mask, "commune_insee"].astype(str).unique()
        )
        raise SamplingError(
            "Manifest audit join produced null commune attributes for sampled "
            f"communes: {missing_communes}"
        )

    return manifest_df


def _write_sample_manifest(
    manifest_df: pd.DataFrame,
    gold_dir: Path,
    pipeline_run_id: str,
    random_seed: int,
) -> Path:
    """Write a JSON audit trail documenting the sampling decisions.

    The manifest answers: "How was this sample constructed, and can it be
    reproduced?" — a question any peer reviewer or hiring manager will ask.

    Manifest structure:
      run_id, created_at, random_seed, total_sampled,
      by_gender, by_city_size, by_nuance_group, distinct_regions,
      region_codes, candidates (per-person details)

    Args:
        manifest_df: Final sampled DataFrame enriched with manifest-only fields.
        gold_dir: Root gold directory.
        pipeline_run_id: Unique identifier for this pipeline run.
        random_seed: The random seed used for sampling.

    Returns:
        Path to the written JSON manifest file.
    """
    by_gender = manifest_df["gender"].value_counts().to_dict()
    by_city_size = manifest_df["city_size_bucket"].value_counts().to_dict()
    by_nuance = manifest_df["nuance_group"].value_counts().to_dict()
    region_codes = sorted(manifest_df["reg_code"].dropna().unique().tolist())

    candidates_list = []
    for _, row in manifest_df.iterrows():
        candidates_list.append(
            {
                "leader_id": str(row.get("leader_id", "")),
                "full_name": str(row.get("full_name", "")),
                "gender": str(row.get("gender", "")),
                "commune_name": str(row.get("commune_name", "")),
                "commune_insee": str(row.get("commune_insee", "")),
                "city_size_bucket": str(row.get("city_size_bucket", "")),
                "nuance_group": str(row.get("nuance_group", "")),
                "is_incumbent": (
                    bool(row.get("is_incumbent"))
                    if row.get("is_incumbent") is not None
                    else None
                ),
                "reg_code": str(row.get("reg_code", "")),
                "dep_code": str(row.get("dep_code", "")),
                "population": (
                    int(row["population"]) if pd.notna(row.get("population")) else None
                ),
                "same_name_candidate_count": (
                    int(row["same_name_candidate_count"])
                    if pd.notna(row.get("same_name_candidate_count"))
                    else None
                ),
            }
        )

    manifest = {
        "run_id": pipeline_run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "random_seed": random_seed,
        "total_sampled": len(manifest_df),
        "by_gender": by_gender,
        "by_city_size": by_city_size,
        "by_nuance_group": by_nuance,
        "distinct_regions": len(region_codes),
        "region_codes": region_codes,
        "candidates": candidates_list,
    }

    manifest_path = gold_dir / "sample_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    logger.info("Sample manifest written path=%s", manifest_path)
    return manifest_path


def build_sample(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    random_seed: int = SAMPLING_RANDOM_SEED,
    pipeline_run_id: str | None = None,
) -> pd.DataFrame:
    """Execute stratified matched sampling and write outputs.

    Reads dim_candidate_leader from silver, applies the sampling algorithm,
    validates constraints, and writes:
      - gold/sample_leaders.parquet
      - gold/sample_manifest.json
      - DuckDB: gold.sample_leaders

    Args:
        silver_dir: Root silver directory.
        gold_dir: Root gold directory.
        duckdb_path: Path to DuckDB warehouse.
        random_seed: Random seed for reproducibility. Stored in manifest.
        pipeline_run_id: Optional shared pipeline run identifier for the
            manifest. If omitted, a UUID is generated for standalone use.

    Returns:
        The sampled leaders DataFrame (24 rows).

    Raises:
        FileNotFoundError: If dim_candidate_leader silver file does not exist.
        SamplingError: If gender balance or minimum sample size cannot be met.
    """
    silver_path = silver_dir / "dim_candidate_leader.parquet"
    dim_commune_path = silver_dir / "dim_commune.parquet"
    if not silver_path.exists():
        raise FileNotFoundError(
            f"dim_candidate_leader silver file not found: {silver_path}. "
            "Run build_dim_candidate_leader() first."
        )
    if not dim_commune_path.exists():
        raise FileNotFoundError(
            f"dim_commune silver file not found: {dim_commune_path}. "
            "Run build_dim_commune() first."
        )

    all_leaders_df = pd.read_parquet(silver_path)
    logger.info("Loaded dim_candidate_leader rows=%d", len(all_leaders_df))

    missing_required_columns = sorted(
        _REQUIRED_SAMPLE_COLUMNS - set(all_leaders_df.columns)
    )
    if missing_required_columns:
        raise SamplingError(
            "dim_candidate_leader is missing required sampling columns: "
            f"{missing_required_columns}"
        )

    # Eligible pool: only non-excluded communes with known gender.
    eligible_pool_df = all_leaders_df[
        (all_leaders_df["city_size_bucket"] != "excluded")
        & (all_leaders_df["gender"].isin(["M", "F"]))
    ].copy()

    logger.info(
        "Eligible pool (non-excluded, known gender): rows=%d female=%d male=%d",
        len(eligible_pool_df),
        (eligible_pool_df["gender"] == "F").sum(),
        (eligible_pool_df["gender"] == "M").sum(),
    )

    # ── Per-stratum stratified sampling ───────────────────────────────────────
    stratum_samples: list[pd.DataFrame] = []
    for bucket, config in _STRATUM_CONFIG.items():
        stratum_sample = _sample_stratum(
            pool_df=eligible_pool_df,
            bucket=bucket,
            per_gender=config["per_gender"],
            random_seed=random_seed,
        )
        stratum_samples.append(stratum_sample)

    initial_sample_df = pd.concat(stratum_samples, ignore_index=True)

    # ── Geographic diversity check + soft resampling ──────────────────────────
    distinct_regions_initial = initial_sample_df["reg_code"].nunique()
    if distinct_regions_initial < SAMPLE_MIN_REGION_COUNT:
        logger.info(
            "Geographic resampling: initial sample has %d regions, target ≥ %d",
            distinct_regions_initial,
            SAMPLE_MIN_REGION_COUNT,
        )
        final_sample_df = _attempt_geographic_resampling(
            initial_sample_df=initial_sample_df,
            full_pool_df=eligible_pool_df,
            random_seed=random_seed,
        )
    else:
        final_sample_df = initial_sample_df

    # ── Validate sample ───────────────────────────────────────────────────────
    _validate_sample(final_sample_df)

    # ── Write gold Parquet ────────────────────────────────────────────────────
    gold_parquet_path = gold_dir / "sample_leaders.parquet"
    gold_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(final_sample_df), gold_parquet_path, compression="snappy"
    )
    logger.info(
        "Gold Parquet written path=%s rows=%d", gold_parquet_path, len(final_sample_df)
    )

    # ── Write manifest ────────────────────────────────────────────────────────
    manifest_df = _build_manifest_sample_df(
        sample_df=final_sample_df,
        silver_dir=silver_dir,
    )
    effective_pipeline_run_id = pipeline_run_id or str(uuid.uuid4())
    _write_sample_manifest(
        manifest_df=manifest_df,
        gold_dir=gold_dir,
        pipeline_run_id=effective_pipeline_run_id,
        random_seed=random_seed,
    )

    # ── Idempotent DuckDB write ────────────────────────────────────────────────
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("DROP TABLE IF EXISTS gold.sample_leaders")
        conn.execute(
            "CREATE TABLE gold.sample_leaders AS SELECT * FROM final_sample_df"
        )
        row_count_result = conn.execute(
            "SELECT count(*) FROM gold.sample_leaders"
        ).fetchone()
        if row_count_result is None:
            raise RuntimeError("Expected one row from gold.sample_leaders count query")
        logger.info(
            "DuckDB gold.sample_leaders written rows=%d",
            row_count_result[0],
        )
    finally:
        conn.close()

    return final_sample_df
