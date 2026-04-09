"""Stratified matched sampling for the active metropolitan analysis cohort.

This module materializes the Gold cohort used to scope downstream news
collection and bias analysis.

Why not simple random sampling:
  Female list leaders are scarcer in the largest communes. A simple random
  draw would over-sample small-commune women and large-commune men, confounding
  gender with city size and local media ecology.

Why matched stratified sampling:
  We enforce gender balance within each city-size stratum so that comparisons
  remain interpretable after controlling for commune scale. This mirrors the
  matched-design logic used in social-science studies.

Current hard cohort contract (configured in settings.py):
  large   -> SAMPLE_LARGE_TOTAL
  medium  -> SAMPLE_MEDIUM_TOTAL
  small   -> SAMPLE_SMALL_TOTAL
  total   -> CANDIDATE_SAMPLE_SIZE
  max     -> one sampled candidate per commune
  region  -> max four sampled candidates per region
  scope   -> metropolitan France only when EXCLUDE_DOM_TOM=True
  frame   -> round-1 viable candidates only

Soft diagnostics:
  - region diversity is encouraged via an adaptive tie-break and audited in the
    manifest
  - political bloc concentration is audited overall, by city size, and by
    city-size Ã— gender subgroup
  - department concentration within each region is audited in the manifest

Outputs:
  data/gold/sample_leaders.parquet
  data/gold/sample_manifest.json
  DuckDB: gold.sample_leaders
"""

import json
import logging
import math
import uuid
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    CANDIDATE_SAMPLE_SIZE,
    DOM_TOM_REG_CODES,
    EXCLUDE_DOM_TOM,
    GOLD_DIR,
    SAMPLE_LARGE_TOTAL,
    SAMPLE_MAX_BLOC_SHARE_LIFT_VS_POOL_SHARE,
    SAMPLE_MAX_CANDIDATES_PER_REGION,
    SAMPLE_MAX_GENDER_BLOC_SHARE_GAP,
    SAMPLE_MAX_GENDER_WIN_RATE_GAP,
    SAMPLE_MAX_RANK_TOUR1_FOR_VIABILITY,
    SAMPLE_MAX_SINGLE_BLOC_RATIO,
    SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER,
    SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_STRATUM,
    SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION,
    SAMPLE_MEDIUM_TOTAL,
    SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE,
    SAMPLE_MIN_NUANCE_GROUP_COUNT_PER_GENDER,
    SAMPLE_MIN_REGION_COUNT,
    SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1,
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
    "commune_name",
    "dep_code",
    "city_size_bucket",
    "reg_code",
    "nuance_group",
    "same_name_candidate_count",
    "is_viable",
    "score_tour1_pct_expressed",
    "score_tour1_rank",
}

# Only population is joined exclusively for the manifest audit artifact.
# commune_name and dep_code already live in the gold sample DataFrame itself.
_MANIFEST_DIM_COMMUNE_COLUMNS = ["commune_insee", "population"]

# Explicit maximum candidates from any single region for the active cohort.
_MAX_CANDIDATES_PER_REGION: int = SAMPLE_MAX_CANDIDATES_PER_REGION

_SAMPLING_RULE_VERSION = (
    "v11_metropolitan_36_regioncap4_blocbalance_before_deptdiversity"
)
_SAMPLING_HARD_CONSTRAINTS = {
    "total_candidates": CANDIDATE_SAMPLE_SIZE,
    "gender_balance": {
        "F": CANDIDATE_SAMPLE_SIZE // 2,
        "M": CANDIDATE_SAMPLE_SIZE // 2,
    },
    "city_size_totals": {
        "large": SAMPLE_LARGE_TOTAL,
        "medium": SAMPLE_MEDIUM_TOTAL,
        "small": SAMPLE_SMALL_TOTAL,
    },
    "max_candidates_per_commune": 1,
    "max_candidates_per_region": _MAX_CANDIDATES_PER_REGION,
    "exclude_dom_tom": EXCLUDE_DOM_TOM,
    "round1_viability": {
        "logic": "score_tour1_pct_expressed >= min_vote_share_pct OR score_tour1_rank <= max_rank",
        "min_vote_share_pct": SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1,
        "max_rank": SAMPLE_MAX_RANK_TOUR1_FOR_VIABILITY,
    },
}
_SAMPLING_SELECTION_PRIORITY = [
    "lowest_same_name_candidate_count",
    "commune_not_already_sampled",
    "region_below_capacity",
    "improves_region_diversity_recomputed_after_each_selection",
    "stays_within_bucket_gender_nuance_soft_cap_recomputed_after_each_selection",
    "stays_close_to_bucket_gender_pool_nuance_share_recomputed_after_each_selection",
    "reduces_bucket_gender_target_deficit_recomputed_after_each_selection",
    "improves_department_diversity_within_region_recomputed_after_each_selection",
    "deterministic_random_seed_tie_break",
]
_SAMPLING_SELECTION_PARAMETERS = {
    "max_single_bloc_ratio_per_bucket_gender_soft_cap": (
        SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER
    ),
    "max_bloc_share_lift_vs_pool_share": SAMPLE_MAX_BLOC_SHARE_LIFT_VS_POOL_SHARE,
}
_SAMPLING_WARNING_THRESHOLDS = {
    "max_single_bloc_ratio_overall": SAMPLE_MAX_SINGLE_BLOC_RATIO,
    "max_single_bloc_ratio_per_stratum": SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_STRATUM,
    "max_single_bloc_ratio_per_bucket_gender": SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER,
    "max_single_department_ratio_per_region": SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION,
    "max_gender_bloc_share_gap": SAMPLE_MAX_GENDER_BLOC_SHARE_GAP,
    "max_gender_win_rate_gap": SAMPLE_MAX_GENDER_WIN_RATE_GAP,
    "min_bucket_gender_subgroup_size": SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE,
    "min_nuance_group_count_per_gender": SAMPLE_MIN_NUANCE_GROUP_COUNT_PER_GENDER,
}


def _validate_sampling_configuration() -> None:
    """Fail fast when cohort-size settings drift out of sync.

    The 36-person cohort contract is defined by two layers of configuration:
    the overall ``CANDIDATE_SAMPLE_SIZE`` and the per-stratum totals in
    ``_STRATUM_CONFIG``. If someone changes only one side, the sampler can
    still import successfully but later produce contradictory manifests or
    quota checks. This guard surfaces that drift before any data is read.
    """
    if CANDIDATE_SAMPLE_SIZE <= 0:
        raise SamplingError(
            "CANDIDATE_SAMPLE_SIZE must be a positive integer. "
            f"Got {CANDIDATE_SAMPLE_SIZE}."
        )

    if CANDIDATE_SAMPLE_SIZE % 2 != 0:
        raise SamplingError(
            "CANDIDATE_SAMPLE_SIZE must be even for a 50/50 gender split. "
            f"Got {CANDIDATE_SAMPLE_SIZE}."
        )

    invalid_strata = {
        bucket: config
        for bucket, config in _STRATUM_CONFIG.items()
        if config["total"] <= 0
        or config["total"] % 2 != 0
        or config["per_gender"] * 2 != config["total"]
    }
    if invalid_strata:
        raise SamplingError(
            "Sampling stratum totals must be positive even numbers with "
            "per_gender equal to total / 2. "
            f"Invalid config: {invalid_strata}"
        )

    total_from_strata = sum(config["total"] for config in _STRATUM_CONFIG.values())
    if total_from_strata != CANDIDATE_SAMPLE_SIZE:
        raise SamplingError(
            "Sampling configuration inconsistent: city-size totals sum to "
            f"{total_from_strata}, but CANDIDATE_SAMPLE_SIZE is "
            f"{CANDIDATE_SAMPLE_SIZE}."
        )


def _apply_primary_cohort_eligibility(candidate_pool_df: pd.DataFrame) -> pd.DataFrame:
    """Filter the candidate pool to the viable-candidate study population.

    The Gold candidate_universe mart computes the viability flag once from the
    round-1 results contract. Sampling then consumes that flag directly instead
    of re-joining Silver tables or re-implementing the same logic.

    Args:
        candidate_pool_df: Base eligible pool after geography and stratum filters.

    Returns:
        Filtered viable-candidate pool.

    Raises:
        SamplingError: If the candidate_universe viability contract is broken.
    """
    if "is_viable" not in candidate_pool_df.columns:
        raise SamplingError(
            "candidate_universe is missing is_viable. "
            "Rebuild gold.candidate_universe before sampling."
        )

    missing_viability_mask = candidate_pool_df["is_viable"].isna()
    if missing_viability_mask.any():
        missing_leaders = sorted(
            candidate_pool_df.loc[missing_viability_mask, "leader_id"]
            .astype(str)
            .unique()
            .tolist()
        )
        raise SamplingError(
            "candidate_universe contains null is_viable values for leaders: "
            f"{missing_leaders[:10]}"
        )

    viability_mask = candidate_pool_df["is_viable"].astype(bool)
    viable_pool_df = candidate_pool_df.loc[viability_mask].copy()

    logger.info(
        "Primary cohort viability filter applied from candidate_universe: "
        "before=%d after=%d min_vote_share_pct=%.2f max_rank=%d",
        len(candidate_pool_df),
        len(viable_pool_df),
        SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1,
        SAMPLE_MAX_RANK_TOUR1_FOR_VIABILITY,
    )

    by_bucket_gender = (
        viable_pool_df.groupby(["city_size_bucket", "gender"]).size().to_dict()
        if not viable_pool_df.empty
        else {}
    )
    logger.info("Viable candidate pool distribution: %s", by_bucket_gender)
    return viable_pool_df


def _prepare_candidate_pool(
    candidate_pool_df: pd.DataFrame,
    random_seed: int,
) -> pd.DataFrame:
    """Assign a deterministic tie-break order for one candidate pool.

    Region coverage and commune uniqueness depend on the current state of the
    running sample, so they must be recomputed after every pick. The random
    seed therefore only establishes a stable baseline order inside otherwise
    tied candidates; it must not freeze the dynamic priorities themselves.
    """
    prepared_pool_df = candidate_pool_df.sample(frac=1, random_state=random_seed).copy()
    prepared_pool_df["_tie_break_order"] = range(len(prepared_pool_df))
    return prepared_pool_df


def _department_not_yet_sampled_within_region(
    reg_code: object,
    dep_code: object,
    used_departments_by_region: dict[str, set[str]],
) -> bool:
    """Return whether a candidate adds a new department within its region."""
    if pd.isna(reg_code) or pd.isna(dep_code):
        return False

    normalized_region = str(reg_code)
    normalized_department = str(dep_code)
    return normalized_department not in used_departments_by_region.get(
        normalized_region, set()
    )


def _bucket_gender_nuance_soft_cap_count(per_gender: int) -> int:
    """Return the preferred max count for one bloc inside a bucket x gender cell."""
    return max(
        1,
        math.ceil(per_gender * SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER),
    )


def _bucket_gender_pool_share_upper_count(
    per_gender: int,
    pool_share: float,
) -> int:
    """Return the preferred max count relative to the observed pool share."""
    return max(
        1,
        math.ceil(per_gender * pool_share * SAMPLE_MAX_BLOC_SHARE_LIFT_VS_POOL_SHARE),
    )


def _prioritize_candidate_pool(
    candidate_pool_df: pd.DataFrame,
    used_communes: set[str],
    used_regions: set[str],
    used_region_counts: dict[str, int],
    used_departments_by_region: dict[str, set[str]],
    selected_nuance_counts: dict[str, int],
    bucket_gender_nuance_soft_cap: int,
    bucket_gender_pool_share_by_nuance: dict[str, float],
    per_gender: int,
) -> pd.DataFrame:
    """Sort a candidate pool by active sampling constraints and tie-breaks.

    Industry rationale: identification-risk controls should outrank "nice to
    have" geography polish. We therefore apply ambiguity, commune uniqueness,
    region-cap, and bloc-balance priorities before intra-region department
    diversity. Department spread still matters, but only after the cohort stays
    closer to the viable pool's political composition inside each
    city-size x gender cell.
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

    if "nuance_group" not in candidate_pool_df.columns:
        raise SamplingError(
            "nuance_group is required for sampling priority. "
            "Rebuild dim_candidate_leader with the current transform code."
        )

    if "_tie_break_order" not in candidate_pool_df.columns:
        raise SamplingError(
            "Candidate pool missing _tie_break_order. "
            "Prepare the pool before prioritisation."
        )

    prioritised_pool_df = candidate_pool_df.copy()
    prioritised_pool_df["commune_not_sampled"] = ~prioritised_pool_df[
        "commune_insee"
    ].isin(used_communes)
    # True when region has not yet reached _MAX_CANDIDATES_PER_REGION.
    # Candidates from capped regions are deprioritised but not excluded here;
    # the eligibility filter in _select_unique_commune_candidates enforces the cap.
    prioritised_pool_df["region_below_capacity"] = prioritised_pool_df["reg_code"].map(
        lambda reg: used_region_counts.get(reg, 0) < _MAX_CANDIDATES_PER_REGION
    )
    prioritised_pool_df["improves_region_diversity"] = ~prioritised_pool_df[
        "reg_code"
    ].isin(used_regions)
    prioritised_pool_df["department_not_sampled_within_region"] = [
        _department_not_yet_sampled_within_region(
            reg_code=reg_code,
            dep_code=dep_code,
            used_departments_by_region=used_departments_by_region,
        )
        for reg_code, dep_code in zip(
            prioritised_pool_df["reg_code"],
            prioritised_pool_df["dep_code"],
            strict=False,
        )
    ]
    prioritised_pool_df["bucket_gender_selected_nuance_count"] = prioritised_pool_df[
        "nuance_group"
    ].map(lambda nuance_group: selected_nuance_counts.get(str(nuance_group), 0))
    prioritised_pool_df["bucket_gender_pool_share"] = prioritised_pool_df[
        "nuance_group"
    ].map(lambda nuance_group: bucket_gender_pool_share_by_nuance[str(nuance_group)])
    prioritised_pool_df["bucket_gender_within_nuance_soft_cap"] = prioritised_pool_df[
        "bucket_gender_selected_nuance_count"
    ].map(lambda current_count: current_count + 1 <= bucket_gender_nuance_soft_cap)
    prioritised_pool_df["bucket_gender_within_pool_share_lift_limit"] = [
        current_count + 1
        <= _bucket_gender_pool_share_upper_count(
            per_gender=per_gender,
            pool_share=float(pool_share),
        )
        for current_count, pool_share in zip(
            prioritised_pool_df["bucket_gender_selected_nuance_count"],
            prioritised_pool_df["bucket_gender_pool_share"],
            strict=False,
        )
    ]
    prioritised_pool_df["bucket_gender_target_count"] = (
        prioritised_pool_df["bucket_gender_pool_share"] * per_gender
    )
    prioritised_pool_df["bucket_gender_remaining_target_deficit_after_selection"] = (
        prioritised_pool_df["bucket_gender_target_count"]
        - (prioritised_pool_df["bucket_gender_selected_nuance_count"] + 1)
    )
    prioritised_pool_df = prioritised_pool_df.sort_values(
        by=[
            "same_name_candidate_count",
            "commune_not_sampled",
            "region_below_capacity",
            "improves_region_diversity",
            "bucket_gender_within_nuance_soft_cap",
            "bucket_gender_within_pool_share_lift_limit",
            "bucket_gender_remaining_target_deficit_after_selection",
            "department_not_sampled_within_region",
            "_tie_break_order",
        ],
        ascending=[True, False, False, False, False, False, False, False, True],
        kind="stable",
    )
    return prioritised_pool_df.drop(
        columns=[
            "commune_not_sampled",
            "region_below_capacity",
            "improves_region_diversity",
            "department_not_sampled_within_region",
            "bucket_gender_selected_nuance_count",
            "bucket_gender_pool_share",
            "bucket_gender_within_nuance_soft_cap",
            "bucket_gender_within_pool_share_lift_limit",
            "bucket_gender_target_count",
            "bucket_gender_remaining_target_deficit_after_selection",
        ]
    )


def _select_unique_commune_candidates(
    candidate_pool_df: pd.DataFrame,
    bucket: str,
    gender: str,
    per_gender: int,
    random_seed: int,
    used_communes: set[str],
    used_regions: set[str],
    used_region_counts: dict[str, int],
    used_departments_by_region: dict[str, set[str]],
) -> tuple[pd.DataFrame, set[str], set[str], dict[str, int], dict[str, set[str]]]:
    """Greedily select candidates enforcing commune uniqueness and region cap."""
    prepared_pool_df = _prepare_candidate_pool(
        candidate_pool_df=candidate_pool_df,
        random_seed=random_seed,
    )
    selected_indexes: list[int] = []
    current_used_communes = set(used_communes)
    current_used_regions = set(used_regions)
    current_region_counts = dict(used_region_counts)
    current_used_departments_by_region = {
        region_code: set(department_codes)
        for region_code, department_codes in used_departments_by_region.items()
    }
    current_selected_nuance_counts: dict[str, int] = {}
    bucket_gender_nuance_soft_cap = _bucket_gender_nuance_soft_cap_count(per_gender)
    bucket_gender_pool_share_by_nuance = (
        candidate_pool_df["nuance_group"]
        .astype(str)
        .value_counts(normalize=True)
        .to_dict()
    )
    remaining_pool_df = prepared_pool_df.copy()

    while len(selected_indexes) < per_gender:
        # Enforce commune uniqueness and region cap simultaneously.
        eligible_remaining_df = remaining_pool_df.loc[
            ~remaining_pool_df["commune_insee"].isin(current_used_communes)
            & remaining_pool_df["reg_code"].map(
                lambda reg: current_region_counts.get(reg, 0)
                < _MAX_CANDIDATES_PER_REGION
            )
        ]
        if eligible_remaining_df.empty:
            break

        prioritised_pool_df = _prioritize_candidate_pool(
            candidate_pool_df=eligible_remaining_df,
            used_communes=current_used_communes,
            used_regions=current_used_regions,
            used_region_counts=current_region_counts,
            used_departments_by_region=current_used_departments_by_region,
            selected_nuance_counts=current_selected_nuance_counts,
            bucket_gender_nuance_soft_cap=bucket_gender_nuance_soft_cap,
            bucket_gender_pool_share_by_nuance=bucket_gender_pool_share_by_nuance,
            per_gender=per_gender,
        )
        selected_row = prioritised_pool_df.iloc[0]
        selected_indexes.append(int(selected_row.name))
        current_used_communes.add(str(selected_row["commune_insee"]))
        selected_nuance_group = selected_row["nuance_group"]
        if pd.notna(selected_nuance_group):
            current_selected_nuance_counts[selected_nuance_group] = (
                current_selected_nuance_counts.get(selected_nuance_group, 0) + 1
            )
        reg = selected_row["reg_code"] if pd.notna(selected_row["reg_code"]) else None
        dep = selected_row["dep_code"] if pd.notna(selected_row["dep_code"]) else None
        if reg:
            current_used_regions.add(reg)
            current_region_counts[reg] = current_region_counts.get(reg, 0) + 1
            if dep:
                current_used_departments_by_region.setdefault(reg, set()).add(dep)

        remaining_pool_df = prepared_pool_df.loc[
            ~prepared_pool_df.index.isin(selected_indexes)
        ]

    if len(selected_indexes) != per_gender:
        available = (
            candidate_pool_df.loc[
                ~candidate_pool_df["commune_insee"].isin(used_communes)
                & candidate_pool_df["reg_code"].map(
                    lambda reg: used_region_counts.get(reg, 0)
                    < _MAX_CANDIDATES_PER_REGION
                ),
                "commune_insee",
            ]
            .dropna()
            .nunique()
        )
        raise SamplingError(
            "Commune uniqueness + region cap made the stratum/gender quota infeasible: "
            f"bucket={bucket} gender={gender} need={per_gender} "
            f"available_unique_eligible_communes={available} "
            f"max_per_region={_MAX_CANDIDATES_PER_REGION}"
        )

    return (
        prepared_pool_df.loc[selected_indexes]
        .drop(columns=["_tie_break_order"])
        .copy(),
        current_used_communes,
        current_used_regions,
        current_region_counts,
        current_used_departments_by_region,
    )


def _sample_stratum(
    pool_df: pd.DataFrame,
    bucket: str,
    per_gender: int,
    random_seed: int,
    used_communes: set[str],
    used_regions: set[str],
    used_region_counts: dict[str, int],
    used_departments_by_region: dict[str, set[str]],
) -> tuple[pd.DataFrame, set[str], set[str], dict[str, int], dict[str, set[str]]]:
    """Sample one size stratum under commune-uniqueness and region-cap rules."""
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

    gender_pools = {"F": female_pool, "M": male_pool}
    gender_order = sorted(
        gender_pools,
        key=lambda gender_code: (
            gender_pools[gender_code]["commune_insee"].dropna().nunique(),
            gender_code,
        ),
    )

    sampled_by_gender: dict[str, pd.DataFrame] = {}
    current_used_communes = set(used_communes)
    current_used_regions = set(used_regions)
    current_region_counts = dict(used_region_counts)
    current_used_departments_by_region = {
        region_code: set(department_codes)
        for region_code, department_codes in used_departments_by_region.items()
    }

    for offset, gender_code in enumerate(gender_order):
        (
            sampled_gender_df,
            current_used_communes,
            current_used_regions,
            current_region_counts,
            current_used_departments_by_region,
        ) = _select_unique_commune_candidates(
            candidate_pool_df=gender_pools[gender_code],
            bucket=bucket,
            gender=gender_code,
            per_gender=per_gender,
            random_seed=random_seed + offset,
            used_communes=current_used_communes,
            used_regions=current_used_regions,
            used_region_counts=current_region_counts,
            used_departments_by_region=current_used_departments_by_region,
        )
        sampled_by_gender[gender_code] = sampled_gender_df

    logger.info(
        "Sampled bucket=%s female=%d male=%d unique_communes=%d",
        bucket,
        len(sampled_by_gender["F"]),
        len(sampled_by_gender["M"]),
        pd.concat([sampled_by_gender["F"], sampled_by_gender["M"]])[
            "commune_insee"
        ].nunique(),
    )
    return (
        pd.concat([sampled_by_gender["F"], sampled_by_gender["M"]], ignore_index=True),
        current_used_communes,
        current_used_regions,
        current_region_counts,
        current_used_departments_by_region,
    )


def _validate_sample(sample_df: pd.DataFrame) -> None:
    """Assert that the sample satisfies all hard constraints.

    Hard constraints (will raise SamplingError if violated):
      - Total rows == CANDIDATE_SAMPLE_SIZE
      - Exactly CANDIDATE_SAMPLE_SIZE / 2 female rows
      - Exactly CANDIDATE_SAMPLE_SIZE / 2 male rows
      - Per-stratum row counts match _STRATUM_CONFIG totals exactly
      - No duplicate leader_id values
      - No duplicate commune_insee values (max one candidate per commune)
      - No region exceeds _MAX_CANDIDATES_PER_REGION sampled candidates

    Soft constraint (WARNING only, no exception):
      - â‰¥ SAMPLE_MIN_REGION_COUNT distinct region codes

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

    # Enforce stratum quotas explicitly. The manifest documents these as hard
    # constraints â€” the validator must enforce what it claims to guarantee.
    # Without this check, a bug in _sample_stratum could produce the right
    # total but wrong distribution (e.g. 8 large + 0 medium + 28 small).
    for bucket, config in _STRATUM_CONFIG.items():
        actual = (sample_df["city_size_bucket"] == bucket).sum()
        expected = config["total"]
        if actual != expected:
            raise SamplingError(
                f"Stratum quota violated: bucket={bucket} "
                f"actual={actual} expected={expected}"
            )

    duplicate_ids = sample_df["leader_id"].duplicated().sum()
    if duplicate_ids > 0:
        raise SamplingError(
            f"Sample contains {duplicate_ids} duplicate leader_id values"
        )

    duplicate_communes = sample_df["commune_insee"].duplicated().sum()
    if duplicate_communes > 0:
        raise SamplingError(
            "Sample violates max one candidate per commune: "
            f"duplicate_commune_count={duplicate_communes}"
        )

    region_counts = sample_df["reg_code"].value_counts(dropna=True)
    over_cap_regions = region_counts[region_counts > _MAX_CANDIDATES_PER_REGION]
    if not over_cap_regions.empty:
        raise SamplingError(
            f"Sample violates max {_MAX_CANDIDATES_PER_REGION} candidates per region: "
            f"{over_cap_regions.to_dict()}"
        )

    distinct_regions = sample_df["reg_code"].nunique()
    if distinct_regions < SAMPLE_MIN_REGION_COUNT:
        logger.warning(
            "Geographic diversity constraint not fully met: "
            "distinct_regions=%d < target=%d. "
            "Proceeding â€” this is a soft constraint.",
            distinct_regions,
            SAMPLE_MIN_REGION_COUNT,
        )

    logger.info(
        "Sample validation passed: total=%d female=%d male=%d "
        "large=%d medium=%d small=%d regions=%d",
        total,
        female_count,
        male_count,
        (sample_df["city_size_bucket"] == "large").sum(),
        (sample_df["city_size_bucket"] == "medium").sum(),
        (sample_df["city_size_bucket"] == "small").sum(),
        distinct_regions,
    )


def _build_warning_event(
    *,
    warning_code: str,
    scope: str,
    dimension: str,
    value: str,
    count: int,
    denominator: int,
    threshold: float,
    recommended_action: str,
    bucket: str | None = None,
    gender: str | None = None,
    reg_code: str | None = None,
    dep_code: str | None = None,
) -> dict[str, object]:
    """Build one structured warning payload for logs and the manifest."""
    share = count / denominator
    return {
        "warning_code": warning_code,
        "scope": scope,
        "bucket": bucket,
        "gender": gender,
        "reg_code": reg_code,
        "dep_code": dep_code,
        "dimension": dimension,
        "value": value,
        "count": int(count),
        "denominator": int(denominator),
        "share": float(share),
        "threshold": float(threshold),
        "recommended_action": recommended_action,
    }


def _build_distribution_summary(
    *,
    scope: str,
    counts: pd.Series,
    denominator: int,
    threshold: float,
    bucket: str | None = None,
    gender: str | None = None,
    reg_code: str | None = None,
) -> dict[str, object]:
    """Build a consistent summary payload for one categorical distribution."""
    counts_dict = {str(key): int(value) for key, value in counts.items()}
    if counts.empty or denominator <= 0:
        top_value = None
        top_count = 0
        top_share = 0.0
    else:
        top_value = str(counts.index[0])
        top_count = int(counts.iloc[0])
        top_share = float(top_count / denominator)

    return {
        "scope": scope,
        "bucket": bucket,
        "gender": gender,
        "reg_code": reg_code,
        "counts": counts_dict,
        "denominator": int(denominator),
        "top_value": top_value,
        "top_count": int(top_count),
        "top_share": float(top_share),
        "threshold": float(threshold),
    }


def _build_political_bloc_concentration_warnings(
    bloc_counts: pd.Series,
    *,
    scope: str,
    denominator: int,
    threshold: float,
) -> list[dict[str, object]]:
    """Build warnings when one political bloc dominates a diagnostic scope."""
    if denominator <= 0 or bloc_counts.empty:
        return []

    warnings: list[dict[str, object]] = []
    for bloc, count in bloc_counts.items():
        share = count / denominator
        if share >= threshold:
            warning_event = _build_warning_event(
                warning_code="political_bloc_concentration",
                scope=scope,
                dimension="nuance_group",
                value=str(bloc),
                count=int(count),
                denominator=denominator,
                threshold=threshold,
                recommended_action=(
                    "Include nuance_group as a regression control variable."
                ),
            )
            warnings.append(warning_event)
            logger.warning(
                "Political bloc over-represented: scope=%s nuance_group=%s "
                "count=%d share=%.0f%% (threshold=%.0f%%). "
                "Include nuance_group as a regression control variable.",
                scope,
                bloc,
                count,
                share * 100,
                threshold * 100,
            )
    return warnings


def _build_political_bloc_diagnostics(sample_df: pd.DataFrame) -> dict[str, object]:
    """Build overall, stratum, and bucket-gender bloc diagnostics.

    This is a soft diagnostic only. Political affiliation is not used to define
    the cohort, but concentrated blocs can still confound downstream media
    comparisons and should therefore be surfaced explicitly in logs.
    """
    if "nuance_group" not in sample_df.columns:
        return {
            "overall": {},
            "by_city_size": [],
            "by_city_size_gender": [],
            "triggered_warnings": [],
        }

    overall_counts = sample_df["nuance_group"].value_counts(dropna=True)
    overall_summary = _build_distribution_summary(
        scope="overall",
        counts=overall_counts,
        denominator=len(sample_df),
        threshold=SAMPLE_MAX_SINGLE_BLOC_RATIO,
    )
    triggered_warnings = _build_political_bloc_concentration_warnings(
        overall_counts,
        scope="overall",
        denominator=len(sample_df),
        threshold=SAMPLE_MAX_SINGLE_BLOC_RATIO,
    )
    logger.info(
        "Political bloc distribution scope=overall data=%s",
        overall_counts.to_dict(),
    )

    by_city_size: list[dict[str, object]] = []
    for bucket, bucket_df in sample_df.groupby("city_size_bucket", dropna=False):
        bucket_counts = bucket_df["nuance_group"].value_counts(dropna=True)
        bucket_summary = _build_distribution_summary(
            scope=f"city_size_bucket:{bucket}",
            bucket=str(bucket),
            counts=bucket_counts,
            denominator=len(bucket_df),
            threshold=SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_STRATUM,
        )
        by_city_size.append(bucket_summary)
        triggered_warnings.extend(
            _build_political_bloc_concentration_warnings(
                bucket_counts,
                scope=f"city_size_bucket:{bucket}",
                denominator=len(bucket_df),
                threshold=SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_STRATUM,
            )
        )
        logger.info(
            "Political bloc distribution scope=city_size_bucket:%s data=%s",
            bucket,
            bucket_counts.to_dict(),
        )

    by_city_size_gender: list[dict[str, object]] = []
    for (bucket, gender), subgroup_df in sample_df.groupby(
        ["city_size_bucket", "gender"],
        dropna=False,
    ):
        subgroup_counts = subgroup_df["nuance_group"].value_counts(dropna=True)
        subgroup_scope = f"city_size_bucket_gender:{bucket}:{gender}"
        subgroup_summary = _build_distribution_summary(
            scope=subgroup_scope,
            bucket=str(bucket),
            gender=str(gender),
            counts=subgroup_counts,
            denominator=len(subgroup_df),
            threshold=SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER,
        )
        by_city_size_gender.append(subgroup_summary)
        triggered_warnings.extend(
            _build_political_bloc_concentration_warnings(
                subgroup_counts,
                scope=subgroup_scope,
                denominator=len(subgroup_df),
                threshold=SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER,
            )
        )
        logger.info(
            "Political bloc distribution scope=city_size_bucket_gender:%s:%s data=%s",
            bucket,
            gender,
            subgroup_counts.to_dict(),
        )

    return {
        "overall": overall_summary,
        "by_city_size": by_city_size,
        "by_city_size_gender": by_city_size_gender,
        "triggered_warnings": triggered_warnings,
    }


def _build_geographic_diagnostics(sample_df: pd.DataFrame) -> dict[str, object]:
    """Build region and department concentration diagnostics for manifest audit."""
    if "reg_code" not in sample_df.columns:
        return {
            "regions": [],
            "departments_by_region": [],
            "triggered_warnings": [],
        }

    region_counts = sample_df["reg_code"].value_counts(dropna=True)
    region_summaries = [
        {
            "reg_code": str(reg_code),
            "count": int(count),
            "at_hard_cap": bool(count == _MAX_CANDIDATES_PER_REGION),
            "hard_cap": int(_MAX_CANDIDATES_PER_REGION),
        }
        for reg_code, count in region_counts.items()
    ]
    for reg_code, count in region_counts.items():
        if count == _MAX_CANDIDATES_PER_REGION:
            logger.info(
                "Region reached hard cap: reg_code=%s count=%d max_per_region=%d",
                reg_code,
                count,
                _MAX_CANDIDATES_PER_REGION,
            )
    logger.info(
        "Region distribution: distinct_regions=%d max_region_count=%d data=%s",
        region_counts.size,
        int(region_counts.max()) if not region_counts.empty else 0,
        region_counts.to_dict(),
    )

    department_summaries: list[dict[str, object]] = []
    triggered_warnings: list[dict[str, object]] = []
    if "dep_code" not in sample_df.columns:
        return {
            "regions": region_summaries,
            "departments_by_region": department_summaries,
            "triggered_warnings": triggered_warnings,
        }

    for reg_code, region_df in sample_df.groupby("reg_code", dropna=False):
        department_counts = region_df["dep_code"].value_counts(dropna=True)
        region_summary = _build_distribution_summary(
            scope=f"region:{reg_code}",
            reg_code=str(reg_code),
            counts=department_counts,
            denominator=len(region_df),
            threshold=SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION,
        )
        department_summaries.append(region_summary)

        if len(region_df) < 3:
            logger.info(
                "Department concentration warning skipped for small regional sample "
                "reg_code=%s region_count=%d",
                reg_code,
                len(region_df),
            )
            continue

        for dep_code, count in department_counts.items():
            share = count / len(region_df)
            if share >= SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION:
                warning_event = _build_warning_event(
                    warning_code="department_concentration_within_region",
                    scope=f"region:{reg_code}",
                    reg_code=str(reg_code),
                    dep_code=str(dep_code),
                    dimension="dep_code",
                    value=str(dep_code),
                    count=int(count),
                    denominator=len(region_df),
                    threshold=SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION,
                    recommended_action=(
                        "Review department concentration within the region and "
                        "treat the cohort as geographically clustered."
                    ),
                )
                triggered_warnings.append(warning_event)
                logger.warning(
                    "Department over-represented within region: reg_code=%s dep_code=%s "
                    "count=%d share=%.0f%% (threshold=%.0f%%).",
                    reg_code,
                    dep_code,
                    count,
                    share * 100,
                    SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION * 100,
                )

        logger.info(
            "Department distribution scope=region:%s data=%s",
            reg_code,
            department_counts.to_dict(),
        )

    return {
        "regions": region_summaries,
        "departments_by_region": department_summaries,
        "triggered_warnings": triggered_warnings,
    }


def _gender_share_stats(
    scope_df: pd.DataFrame,
    *,
    value_column: str,
    target_value: object,
    gender: str,
) -> dict[str, float | int]:
    """Return count/share stats for one gender-specific categorical value."""
    gender_scope_df = scope_df.loc[scope_df["gender"] == gender]
    denominator = len(gender_scope_df)
    if denominator == 0:
        return {"count": 0, "denominator": 0, "share": 0.0}

    matching_mask = gender_scope_df[value_column].eq(target_value)
    if hasattr(matching_mask, "fillna"):
        matching_mask = matching_mask.fillna(False)
    count = int(matching_mask.sum())
    return {
        "count": count,
        "denominator": denominator,
        "share": float(count / denominator),
    }


def _build_gender_bloc_gap_diagnostics(
    sample_df: pd.DataFrame,
    eligible_pool_df: pd.DataFrame,
) -> dict[str, object]:
    """Audit whether bloc composition differs materially between sampled women and men."""
    if (
        "nuance_group" not in sample_df.columns
        or "nuance_group" not in eligible_pool_df.columns
    ):
        return {
            "overall": [],
            "by_city_size": [],
            "triggered_warnings": [],
        }

    def _build_scope_rows(
        *,
        scope: str,
        sample_scope_df: pd.DataFrame,
        pool_scope_df: pd.DataFrame,
        bucket: str | None = None,
    ) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
        scope_rows: list[dict[str, object]] = []
        scope_warnings: list[dict[str, object]] = []
        bloc_values = sorted(
            {
                *sample_scope_df["nuance_group"].dropna().astype(str).tolist(),
                *pool_scope_df["nuance_group"].dropna().astype(str).tolist(),
            }
        )
        for nuance_group in bloc_values:
            female_sample = _gender_share_stats(
                sample_scope_df,
                value_column="nuance_group",
                target_value=nuance_group,
                gender="F",
            )
            male_sample = _gender_share_stats(
                sample_scope_df,
                value_column="nuance_group",
                target_value=nuance_group,
                gender="M",
            )
            female_pool = _gender_share_stats(
                pool_scope_df,
                value_column="nuance_group",
                target_value=nuance_group,
                gender="F",
            )
            male_pool = _gender_share_stats(
                pool_scope_df,
                value_column="nuance_group",
                target_value=nuance_group,
                gender="M",
            )
            sample_gap = abs(
                float(female_sample["share"]) - float(male_sample["share"])
            )
            pool_gap = abs(float(female_pool["share"]) - float(male_pool["share"]))
            scope_rows.append(
                {
                    "scope": scope,
                    "bucket": bucket,
                    "nuance_group": nuance_group,
                    "female_count": int(female_sample["count"]),
                    "female_denominator": int(female_sample["denominator"]),
                    "female_share": float(female_sample["share"]),
                    "male_count": int(male_sample["count"]),
                    "male_denominator": int(male_sample["denominator"]),
                    "male_share": float(male_sample["share"]),
                    "absolute_gap": float(sample_gap),
                    "pool_female_share": float(female_pool["share"]),
                    "pool_male_share": float(male_pool["share"]),
                    "pool_absolute_gap": float(pool_gap),
                }
            )
            if sample_gap >= SAMPLE_MAX_GENDER_BLOC_SHARE_GAP:
                warning_event = {
                    "warning_code": "gender_bloc_share_gap",
                    "scope": scope,
                    "bucket": bucket,
                    "gender": None,
                    "reg_code": None,
                    "dep_code": None,
                    "dimension": "nuance_group",
                    "value": nuance_group,
                    "count": int(female_sample["count"]) + int(male_sample["count"]),
                    "denominator": len(sample_scope_df),
                    "share": float(sample_gap),
                    "threshold": float(SAMPLE_MAX_GENDER_BLOC_SHARE_GAP),
                    "female_share": float(female_sample["share"]),
                    "male_share": float(male_sample["share"]),
                    "pool_female_share": float(female_pool["share"]),
                    "pool_male_share": float(male_pool["share"]),
                    "pool_absolute_gap": float(pool_gap),
                    "recommended_action": (
                        "Treat political bloc as a mandatory regression control and "
                        "avoid subgroup claims when gender and bloc composition diverge."
                    ),
                }
                scope_warnings.append(warning_event)
                logger.warning(
                    "Gender/bloc share gap detected: scope=%s nuance_group=%s "
                    "female_share=%.1f%% male_share=%.1f%% sample_gap=%.1fpp "
                    "pool_gap=%.1fpp threshold=%.1fpp",
                    scope,
                    nuance_group,
                    float(female_sample["share"]) * 100,
                    float(male_sample["share"]) * 100,
                    sample_gap * 100,
                    pool_gap * 100,
                    SAMPLE_MAX_GENDER_BLOC_SHARE_GAP * 100,
                )
        return scope_rows, scope_warnings

    overall_rows, triggered_warnings = _build_scope_rows(
        scope="overall",
        sample_scope_df=sample_df,
        pool_scope_df=eligible_pool_df,
    )
    by_city_size: list[dict[str, object]] = []
    for bucket, bucket_df in sample_df.groupby("city_size_bucket", dropna=False):
        pool_bucket_df = eligible_pool_df.loc[
            eligible_pool_df["city_size_bucket"] == bucket
        ]
        bucket_rows, bucket_warnings = _build_scope_rows(
            scope=f"city_size_bucket:{bucket}",
            sample_scope_df=bucket_df,
            pool_scope_df=pool_bucket_df,
            bucket=str(bucket),
        )
        by_city_size.extend(bucket_rows)
        triggered_warnings.extend(bucket_warnings)

    return {
        "overall": overall_rows,
        "by_city_size": by_city_size,
        "triggered_warnings": triggered_warnings,
    }


def _build_gender_win_rate_diagnostics(
    sample_df: pd.DataFrame,
    eligible_pool_df: pd.DataFrame,
) -> dict[str, object]:
    """Audit whether sampled win-rate imbalance could confound exposure results."""
    if (
        "won_final_round" not in sample_df.columns
        or "won_final_round" not in eligible_pool_df.columns
    ):
        return {"overall": {}, "by_city_size": [], "triggered_warnings": []}

    def _build_scope_summary(
        *,
        scope: str,
        sample_scope_df: pd.DataFrame,
        pool_scope_df: pd.DataFrame,
        bucket: str | None = None,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        female_sample = _gender_share_stats(
            sample_scope_df,
            value_column="won_final_round",
            target_value=True,
            gender="F",
        )
        male_sample = _gender_share_stats(
            sample_scope_df,
            value_column="won_final_round",
            target_value=True,
            gender="M",
        )
        female_pool = _gender_share_stats(
            pool_scope_df,
            value_column="won_final_round",
            target_value=True,
            gender="F",
        )
        male_pool = _gender_share_stats(
            pool_scope_df,
            value_column="won_final_round",
            target_value=True,
            gender="M",
        )
        sample_gap = abs(float(female_sample["share"]) - float(male_sample["share"]))
        pool_gap = abs(float(female_pool["share"]) - float(male_pool["share"]))
        summary = {
            "scope": scope,
            "bucket": bucket,
            "female_winner_count": int(female_sample["count"]),
            "female_denominator": int(female_sample["denominator"]),
            "female_win_rate": float(female_sample["share"]),
            "male_winner_count": int(male_sample["count"]),
            "male_denominator": int(male_sample["denominator"]),
            "male_win_rate": float(male_sample["share"]),
            "absolute_gap": float(sample_gap),
            "pool_female_win_rate": float(female_pool["share"]),
            "pool_male_win_rate": float(male_pool["share"]),
            "pool_absolute_gap": float(pool_gap),
        }
        scope_warnings: list[dict[str, object]] = []
        if sample_gap >= SAMPLE_MAX_GENDER_WIN_RATE_GAP:
            warning_event = {
                "warning_code": "gender_win_rate_gap",
                "scope": scope,
                "bucket": bucket,
                "gender": None,
                "reg_code": None,
                "dep_code": None,
                "dimension": "won_final_round",
                "value": "True",
                "count": int(female_sample["count"]) + int(male_sample["count"]),
                "denominator": len(sample_scope_df),
                "share": float(sample_gap),
                "threshold": float(SAMPLE_MAX_GENDER_WIN_RATE_GAP),
                "female_share": float(female_sample["share"]),
                "male_share": float(male_sample["share"]),
                "pool_female_share": float(female_pool["share"]),
                "pool_male_share": float(male_pool["share"]),
                "pool_absolute_gap": float(pool_gap),
                "recommended_action": (
                    "Include won_final_round as a regression control for any "
                    "analysis window that includes post-election coverage."
                ),
            }
            scope_warnings.append(warning_event)
            logger.warning(
                "Gender win-rate gap detected: scope=%s female_rate=%.1f%% "
                "male_rate=%.1f%% sample_gap=%.1fpp pool_gap=%.1fpp "
                "threshold=%.1fpp",
                scope,
                float(female_sample["share"]) * 100,
                float(male_sample["share"]) * 100,
                sample_gap * 100,
                pool_gap * 100,
                SAMPLE_MAX_GENDER_WIN_RATE_GAP * 100,
            )
        return summary, scope_warnings

    overall_summary, triggered_warnings = _build_scope_summary(
        scope="overall",
        sample_scope_df=sample_df,
        pool_scope_df=eligible_pool_df,
    )
    by_city_size: list[dict[str, object]] = []
    for bucket, bucket_df in sample_df.groupby("city_size_bucket", dropna=False):
        pool_bucket_df = eligible_pool_df.loc[
            eligible_pool_df["city_size_bucket"] == bucket
        ]
        bucket_summary, bucket_warnings = _build_scope_summary(
            scope=f"city_size_bucket:{bucket}",
            sample_scope_df=bucket_df,
            pool_scope_df=pool_bucket_df,
            bucket=str(bucket),
        )
        by_city_size.append(bucket_summary)
        triggered_warnings.extend(bucket_warnings)

    return {
        "overall": overall_summary,
        "by_city_size": by_city_size,
        "triggered_warnings": triggered_warnings,
    }


def _build_subgroup_size_diagnostics(sample_df: pd.DataFrame) -> dict[str, object]:
    """Audit bucket x gender cell sizes before reviewers over-interpret them."""
    subgroup_rows: list[dict[str, object]] = []
    triggered_warnings: list[dict[str, object]] = []
    for (bucket, gender), subgroup_df in sample_df.groupby(
        ["city_size_bucket", "gender"],
        dropna=False,
    ):
        count = len(subgroup_df)
        subgroup_rows.append(
            {
                "scope": f"city_size_bucket_gender:{bucket}:{gender}",
                "bucket": str(bucket),
                "gender": str(gender),
                "count": int(count),
                "minimum_recommended": int(SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE),
            }
        )
        if count < SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE:
            warning_event = {
                "warning_code": "subgroup_small_n",
                "scope": f"city_size_bucket_gender:{bucket}:{gender}",
                "bucket": str(bucket),
                "gender": str(gender),
                "reg_code": None,
                "dep_code": None,
                "dimension": "sample_size",
                "value": "n",
                "count": int(count),
                "denominator": int(SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE),
                "share": float(count),
                "threshold": float(SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE),
                "recommended_action": (
                    "Treat this subgroup as descriptive only; do not make "
                    "bucket-specific inferential claims from such a small cell."
                ),
            }
            triggered_warnings.append(warning_event)
            logger.warning(
                "Small subgroup cell detected: scope=city_size_bucket_gender:%s:%s "
                "count=%d minimum_recommended=%d",
                bucket,
                gender,
                count,
                SAMPLE_MIN_BUCKET_GENDER_SUBGROUP_SIZE,
            )
    return {
        "by_city_size_gender": subgroup_rows,
        "triggered_warnings": triggered_warnings,
    }


def _build_rare_nuance_group_diagnostics(sample_df: pd.DataFrame) -> dict[str, object]:
    """Warn when one bloc has too few examples for stable gender-wise interpretation."""
    subgroup_rows: list[dict[str, object]] = []
    triggered_warnings: list[dict[str, object]] = []
    for gender, gender_df in sample_df.groupby("gender", dropna=False):
        nuance_counts = gender_df["nuance_group"].value_counts(dropna=True)
        for nuance_group, count in nuance_counts.items():
            subgroup_rows.append(
                {
                    "scope": f"gender:{gender}",
                    "gender": str(gender),
                    "nuance_group": str(nuance_group),
                    "count": int(count),
                    "minimum_recommended": int(
                        SAMPLE_MIN_NUANCE_GROUP_COUNT_PER_GENDER
                    ),
                }
            )
            if count < SAMPLE_MIN_NUANCE_GROUP_COUNT_PER_GENDER:
                warning_event = {
                    "warning_code": "rare_nuance_group_count",
                    "scope": f"gender:{gender}",
                    "bucket": None,
                    "gender": str(gender),
                    "reg_code": None,
                    "dep_code": None,
                    "dimension": "nuance_group",
                    "value": str(nuance_group),
                    "count": int(count),
                    "denominator": int(len(gender_df)),
                    "share": float(count / len(gender_df)),
                    "threshold": float(SAMPLE_MIN_NUANCE_GROUP_COUNT_PER_GENDER),
                    "recommended_action": (
                        "Keep this bloc as a main-effect control only; do not "
                        "interpret gender x bloc interactions with such a small cell."
                    ),
                }
                triggered_warnings.append(warning_event)
                logger.warning(
                    "Rare nuance-group cell detected: gender=%s nuance_group=%s "
                    "count=%d minimum_recommended=%d",
                    gender,
                    nuance_group,
                    count,
                    SAMPLE_MIN_NUANCE_GROUP_COUNT_PER_GENDER,
                )
    return {"by_gender": subgroup_rows, "triggered_warnings": triggered_warnings}


def _build_control_missingness_diagnostics(
    sample_df: pd.DataFrame,
) -> dict[str, object]:
    """Audit nullable modeling controls that could otherwise disappear silently."""
    control_rows: list[dict[str, object]] = []
    triggered_warnings: list[dict[str, object]] = []
    for column_name in ("is_incumbent", "won_final_round"):
        if column_name not in sample_df.columns:
            continue
        missing_count = int(sample_df[column_name].isna().sum())
        control_rows.append(
            {
                "column_name": column_name,
                "missing_count": missing_count,
                "denominator": len(sample_df),
            }
        )
        if missing_count > 0:
            warning_event = {
                "warning_code": "control_missingness",
                "scope": "overall",
                "bucket": None,
                "gender": None,
                "reg_code": None,
                "dep_code": None,
                "dimension": column_name,
                "value": "missing",
                "count": missing_count,
                "denominator": len(sample_df),
                "share": float(missing_count / len(sample_df)),
                "threshold": 0.0,
                "recommended_action": (
                    "Audit or impute this control explicitly before modeling so "
                    "rows are not dropped or misclassified silently."
                ),
            }
            triggered_warnings.append(warning_event)
            logger.warning(
                "Modeling control missingness detected: column=%s missing=%d/%d",
                column_name,
                missing_count,
                len(sample_df),
            )
    return {"controls": control_rows, "triggered_warnings": triggered_warnings}


def _build_region_singleton_diagnostics(sample_df: pd.DataFrame) -> dict[str, object]:
    """Warn when one region is represented by only one sampled candidate."""
    if "reg_code" not in sample_df.columns:
        return {"regions": [], "triggered_warnings": []}

    region_rows: list[dict[str, object]] = []
    triggered_warnings: list[dict[str, object]] = []
    region_counts = sample_df["reg_code"].value_counts(dropna=True)
    for reg_code, count in region_counts.items():
        region_rows.append({"reg_code": str(reg_code), "count": int(count)})
        if count == 1:
            warning_event = {
                "warning_code": "singleton_region_representation",
                "scope": f"region:{reg_code}",
                "bucket": None,
                "gender": None,
                "reg_code": str(reg_code),
                "dep_code": None,
                "dimension": "reg_code",
                "value": str(reg_code),
                "count": 1,
                "denominator": len(sample_df),
                "share": float(1 / len(sample_df)),
                "threshold": 1.0,
                "recommended_action": (
                    "Treat this region as a potential outlier in interpretation; "
                    "a singleton region cannot support region-specific inference."
                ),
            }
            triggered_warnings.append(warning_event)
            logger.warning(
                "Singleton region detected: reg_code=%s count=1 total_sample=%d",
                reg_code,
                len(sample_df),
            )
    return {"regions": region_rows, "triggered_warnings": triggered_warnings}


def _build_sampling_diagnostics(
    sample_df: pd.DataFrame,
    eligible_pool_df: pd.DataFrame,
) -> dict[str, object]:
    """Build the full manifest-ready diagnostics package for the sampled cohort."""
    political_bloc = _build_political_bloc_diagnostics(sample_df)
    geography = _build_geographic_diagnostics(sample_df)
    gender_bloc_balance = _build_gender_bloc_gap_diagnostics(
        sample_df=sample_df,
        eligible_pool_df=eligible_pool_df,
    )
    gender_win_rate = _build_gender_win_rate_diagnostics(
        sample_df=sample_df,
        eligible_pool_df=eligible_pool_df,
    )
    subgroup_size = _build_subgroup_size_diagnostics(sample_df)
    rare_nuance_group = _build_rare_nuance_group_diagnostics(sample_df)
    control_missingness = _build_control_missingness_diagnostics(sample_df)
    region_singleton = _build_region_singleton_diagnostics(sample_df)

    triggered_warnings = [
        *political_bloc["triggered_warnings"],
        *geography["triggered_warnings"],
        *gender_bloc_balance["triggered_warnings"],
        *gender_win_rate["triggered_warnings"],
        *subgroup_size["triggered_warnings"],
        *rare_nuance_group["triggered_warnings"],
        *control_missingness["triggered_warnings"],
        *region_singleton["triggered_warnings"],
    ]

    return {
        "political_bloc": political_bloc,
        "geography": geography,
        "gender_bloc_balance": gender_bloc_balance,
        "gender_win_rate": gender_win_rate,
        "subgroup_size": subgroup_size,
        "rare_nuance_group": rare_nuance_group,
        "control_missingness": control_missingness,
        "region_singleton": region_singleton,
        "triggered_warnings": triggered_warnings,
    }


def _build_manifest_sample_df(
    sample_df: pd.DataFrame,
    silver_dir: Path,
) -> pd.DataFrame:
    """Join the manifest-only population field onto the enriched gold sample.

    commune_name and dep_code are already present in sample_df (joined into the
    gold schema by build_sample before this function is called). Only population
    is fetched here exclusively for the audit manifest â€” it is not stored in
    gold.sample_leaders because city_size_bucket already captures the size
    stratum needed for downstream modelling.

    Args:
        sample_df: Gold sample DataFrame already containing commune_name and dep_code.
        silver_dir: Root silver directory.

    Returns:
        Sample DataFrame enriched with population for manifest-only audit output.

    Raises:
        FileNotFoundError: If dim_commune.parquet does not exist.
        SamplingError: If population or any commune field is null after join.
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
            "dim_commune is missing required population column for manifest: "
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


def _serialize_optional_bool(value: object) -> bool | None:
    """Serialize nullable boolean-like scalars safely for JSON artifacts.

    Pandas may store nullable booleans as ``pd.NA`` rather than ``None``. Using
    ``bool(pd.NA)`` raises ``TypeError``, so manifest serialization must guard
    with ``pd.isna`` before coercing to a plain Python ``bool``.
    """
    if value is None or pd.isna(value):
        return None
    return bool(value)


def _build_temp_output_path(final_path: Path) -> Path:
    """Create a unique sibling temp path for one artifact publish cycle."""
    final_path.parent.mkdir(parents=True, exist_ok=True)
    return final_path.with_suffix(final_path.suffix + f".tmp.{uuid.uuid4().hex}")


def _cleanup_temp_artifacts(temp_paths: list[Path], pipeline_run_id: str) -> None:
    """Best-effort cleanup for staged artifacts after failed publishes."""
    for temp_path in temp_paths:
        if not temp_path.exists():
            continue
        try:
            temp_path.unlink()
        except OSError as exc:
            logger.warning(
                "Failed to clean staged artifact run_id=%s path=%s error=%r",
                pipeline_run_id,
                temp_path,
                exc,
            )


def _write_temp_sample_parquet(
    sample_df: pd.DataFrame,
    final_path: Path,
    pipeline_run_id: str,
) -> Path:
    """Stage the sample Parquet beside the final destination before publish."""
    temp_path = _build_temp_output_path(final_path)
    pq.write_table(pa.Table.from_pandas(sample_df), temp_path, compression="snappy")
    logger.info(
        "Staged sample Parquet run_id=%s path=%s rows=%d",
        pipeline_run_id,
        temp_path,
        len(sample_df),
    )
    return temp_path


def _write_sample_to_duckdb(
    sample_df: pd.DataFrame,
    duckdb_path: Path,
    pipeline_run_id: str,
) -> None:
    """Replace gold.sample_leaders inside one DuckDB transaction."""
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute("DROP TABLE IF EXISTS gold.sample_leaders")
            conn.execute("CREATE TABLE gold.sample_leaders AS SELECT * FROM sample_df")
            row_count_result = conn.execute(
                "SELECT count(*) FROM gold.sample_leaders"
            ).fetchone()
            if row_count_result is None:
                raise RuntimeError(
                    "Expected one row from gold.sample_leaders count query"
                )
            conn.execute("COMMIT")
            logger.info(
                "DuckDB gold.sample_leaders written run_id=%s rows=%d",
                pipeline_run_id,
                row_count_result[0],
            )
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def _write_sample_manifest(
    manifest_df: pd.DataFrame,
    gold_dir: Path,
    pipeline_run_id: str,
    random_seed: int,
    sampling_diagnostics: dict[str, object],
    output_path: Path | None = None,
) -> Path:
    """Write a JSON audit trail documenting the sampling decisions.

    The manifest answers: "How was this sample constructed, and can it be
    reproduced?" â€” a question any peer reviewer or hiring manager will ask.

    Manifest structure:
      run_id, created_at, random_seed, total_sampled,
      selection_priority, selection_parameters, by_gender, by_city_size,
      by_nuance_group, distinct_regions, region_codes, triggered_warnings,
      diagnostics, candidates (per-person details)

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
                "is_incumbent": _serialize_optional_bool(row.get("is_incumbent")),
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
                "score_tour1_votes": (
                    int(row["score_tour1_votes"])
                    if pd.notna(row.get("score_tour1_votes"))
                    else None
                ),
                "score_tour1_pct_expressed": (
                    float(row["score_tour1_pct_expressed"])
                    if pd.notna(row.get("score_tour1_pct_expressed"))
                    else None
                ),
                "score_tour1_rank": (
                    int(row["score_tour1_rank"])
                    if pd.notna(row.get("score_tour1_rank"))
                    else None
                ),
                "score_tour2_votes": (
                    int(row["score_tour2_votes"])
                    if pd.notna(row.get("score_tour2_votes"))
                    else None
                ),
                "score_tour2_pct_expressed": (
                    float(row["score_tour2_pct_expressed"])
                    if pd.notna(row.get("score_tour2_pct_expressed"))
                    else None
                ),
                "score_tour2_rank": (
                    int(row["score_tour2_rank"])
                    if pd.notna(row.get("score_tour2_rank"))
                    else None
                ),
                "vote_share_band_tour1": (
                    str(row.get("vote_share_band_tour1", ""))
                    if pd.notna(row.get("vote_share_band_tour1"))
                    else None
                ),
                "won_final_round": _serialize_optional_bool(row.get("won_final_round")),
            }
        )

    manifest = {
        "run_id": pipeline_run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "random_seed": random_seed,
        "sampling_rule_version": _SAMPLING_RULE_VERSION,
        "hard_constraints": _SAMPLING_HARD_CONSTRAINTS,
        "selection_priority": _SAMPLING_SELECTION_PRIORITY,
        "selection_parameters": _SAMPLING_SELECTION_PARAMETERS,
        "warning_thresholds": _SAMPLING_WARNING_THRESHOLDS,
        "triggered_warnings": sampling_diagnostics["triggered_warnings"],
        "diagnostics": {
            "political_bloc": sampling_diagnostics["political_bloc"],
            "geography": sampling_diagnostics["geography"],
            "gender_bloc_balance": sampling_diagnostics["gender_bloc_balance"],
            "gender_win_rate": sampling_diagnostics["gender_win_rate"],
            "subgroup_size": sampling_diagnostics["subgroup_size"],
            "rare_nuance_group": sampling_diagnostics["rare_nuance_group"],
            "control_missingness": sampling_diagnostics["control_missingness"],
            "region_singleton": sampling_diagnostics["region_singleton"],
        },
        "total_sampled": len(manifest_df),
        "by_gender": by_gender,
        "by_city_size": by_city_size,
        "by_nuance_group": by_nuance,
        "distinct_regions": len(region_codes),
        "region_codes": region_codes,
        "candidates": candidates_list,
    }

    manifest_path = output_path or (gold_dir / "sample_manifest.json")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    logger.info(
        "Sample manifest written run_id=%s path=%s", pipeline_run_id, manifest_path
    )
    return manifest_path


def build_sample(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    random_seed: int = SAMPLING_RANDOM_SEED,
    pipeline_run_id: str | None = None,
) -> pd.DataFrame:
    """Execute stratified matched sampling and write outputs.

    Reads gold.candidate_universe, applies the sampling algorithm, validates
    constraints, and writes:
      - gold/sample_leaders.parquet
      - gold/sample_manifest.json
      - DuckDB: gold.sample_leaders

    Args:
        silver_dir: Root silver directory used for manifest-only population audit.
        gold_dir: Root gold directory containing candidate_universe input and
            sample_leaders outputs.
        duckdb_path: Path to DuckDB warehouse.
        random_seed: Random seed for reproducibility. Stored in manifest.
        pipeline_run_id: Optional shared pipeline run identifier for the
            manifest. If omitted, a UUID is generated for standalone use.

    Returns:
        The sampled leaders DataFrame sourced directly from candidate_universe.

    Raises:
        FileNotFoundError: If candidate_universe or dim_commune does not exist.
        SamplingError: If gender balance or minimum sample size cannot be met.
    """
    _validate_sampling_configuration()
    effective_pipeline_run_id = pipeline_run_id or str(uuid.uuid4())

    candidate_universe_path = gold_dir / "candidate_universe.parquet"
    dim_commune_path = silver_dir / "dim_commune.parquet"
    if not candidate_universe_path.exists():
        raise FileNotFoundError(
            f"candidate_universe gold file not found: {candidate_universe_path}. "
            "Run build_candidate_universe() before build_sample()."
        )
    if not dim_commune_path.exists():
        raise FileNotFoundError(
            f"dim_commune silver file not found: {dim_commune_path}. "
            "Run build_dim_commune() first."
        )

    all_leaders_df = pd.read_parquet(candidate_universe_path)
    logger.info(
        "Loaded candidate_universe run_id=%s rows=%d",
        effective_pipeline_run_id,
        len(all_leaders_df),
    )

    missing_required_columns = sorted(
        _REQUIRED_SAMPLE_COLUMNS - set(all_leaders_df.columns)
    )
    if missing_required_columns:
        raise SamplingError(
            "candidate_universe is missing required sampling columns: "
            f"{missing_required_columns}"
        )

    missing_commune_mask = (
        all_leaders_df[["commune_name", "dep_code"]].isna().any(axis=1)
    )
    if missing_commune_mask.any():
        missing_communes = sorted(
            all_leaders_df.loc[missing_commune_mask, "commune_insee"]
            .astype(str)
            .unique()
        )
        raise SamplingError(
            "candidate_universe contains null commune_name/dep_code for leaders: "
            f"{missing_communes}"
        )

    # Eligible pool: known city-size strata, known gender, and (if configured)
    # metropolitan France only. candidate_universe already carries the
    # pre-joined geography and viability contract used here.
    eligible_pool_df = all_leaders_df[
        all_leaders_df["city_size_bucket"].isin({"large", "medium", "small"})
        & all_leaders_df["gender"].isin(["M", "F"])
    ].copy()

    if EXCLUDE_DOM_TOM:
        before_dom_tom = len(eligible_pool_df)
        eligible_pool_df = eligible_pool_df[
            ~eligible_pool_df["reg_code"].isin(DOM_TOM_REG_CODES)
        ].copy()
        dom_tom_excluded = before_dom_tom - len(eligible_pool_df)
        logger.info(
            "DOM-TOM exclusion applied: removed=%d remaining=%d "
            "(EXCLUDE_DOM_TOM=True, scope=metropolitan France only)",
            dom_tom_excluded,
            len(eligible_pool_df),
        )

    eligible_pool_df = _apply_primary_cohort_eligibility(eligible_pool_df)

    logger.info(
        "Eligible pool: rows=%d female=%d male=%d",
        len(eligible_pool_df),
        (eligible_pool_df["gender"] == "F").sum(),
        (eligible_pool_df["gender"] == "M").sum(),
    )

    # â”€â”€ Per-stratum stratified sampling â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    stratum_samples: list[pd.DataFrame] = []
    used_communes: set[str] = set()
    used_regions: set[str] = set()
    used_region_counts: dict[str, int] = {}
    used_departments_by_region: dict[str, set[str]] = {}
    for bucket, config in _STRATUM_CONFIG.items():
        (
            stratum_sample,
            used_communes,
            used_regions,
            used_region_counts,
            used_departments_by_region,
        ) = _sample_stratum(
            pool_df=eligible_pool_df,
            bucket=bucket,
            per_gender=config["per_gender"],
            random_seed=random_seed,
            used_communes=used_communes,
            used_regions=used_regions,
            used_region_counts=used_region_counts,
            used_departments_by_region=used_departments_by_region,
        )
        stratum_samples.append(stratum_sample)

    final_sample_df = pd.concat(stratum_samples, ignore_index=True)

    # â”€â”€ Geographic diversity audit â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    distinct_regions_initial = final_sample_df["reg_code"].nunique()
    logger.info(
        "Primary selection region coverage: distinct_regions=%d target_min=%d",
        distinct_regions_initial,
        SAMPLE_MIN_REGION_COUNT,
    )

    # â”€â”€ Validate sample â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    _validate_sample(final_sample_df)

    sampling_diagnostics = _build_sampling_diagnostics(
        sample_df=final_sample_df,
        eligible_pool_df=eligible_pool_df,
    )
    logger.info(
        "Sampling diagnostics built run_id=%s triggered_warnings=%d",
        effective_pipeline_run_id,
        len(sampling_diagnostics["triggered_warnings"]),
    )

    # â”€â”€ Write gold Parquet â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    manifest_df = _build_manifest_sample_df(
        sample_df=final_sample_df,
        silver_dir=silver_dir,
    )
    gold_parquet_path = gold_dir / "sample_leaders.parquet"
    manifest_path = gold_dir / "sample_manifest.json"
    staged_paths: list[Path] = []

    # â”€â”€ Write manifest â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

    # â”€â”€ Idempotent DuckDB write â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        staged_sample_path = _write_temp_sample_parquet(
            final_sample_df,
            gold_parquet_path,
            effective_pipeline_run_id,
        )
        staged_paths.append(staged_sample_path)
        staged_manifest_path = _write_sample_manifest(
            manifest_df=manifest_df,
            gold_dir=gold_dir,
            pipeline_run_id=effective_pipeline_run_id,
            random_seed=random_seed,
            sampling_diagnostics=sampling_diagnostics,
            output_path=_build_temp_output_path(manifest_path),
        )
        staged_paths.append(staged_manifest_path)
        _write_sample_to_duckdb(
            final_sample_df,
            duckdb_path,
            effective_pipeline_run_id,
        )

        staged_sample_path.replace(gold_parquet_path)
        staged_paths.remove(staged_sample_path)
        logger.info(
            "Gold Parquet published run_id=%s path=%s rows=%d",
            effective_pipeline_run_id,
            gold_parquet_path,
            len(final_sample_df),
        )

        # Publish the manifest last so it acts as the visible run marker.
        staged_manifest_path.replace(manifest_path)
        staged_paths.remove(staged_manifest_path)
        logger.info(
            "Sample manifest published run_id=%s path=%s",
            effective_pipeline_run_id,
            manifest_path,
        )
    except Exception:
        _cleanup_temp_artifacts(staged_paths, effective_pipeline_run_id)
        raise

    return final_sample_df
