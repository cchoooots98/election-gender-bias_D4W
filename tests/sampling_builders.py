"""Shared builders for sampling-related tests.

Keeping one canonical test-data builder prevents schema drift between
``test_sampling.py`` and ``test_transform.py`` when the Gold sampling contract
adds new required fields.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    CANDIDATE_SAMPLE_SIZE,
    SAMPLE_LARGE_TOTAL,
    SAMPLE_MAX_RANK_TOUR1_FOR_VIABILITY,
    SAMPLE_MEDIUM_TOTAL,
    SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1,
    SAMPLE_SMALL_TOTAL,
)

TARGET_PER_GENDER = CANDIDATE_SAMPLE_SIZE // 2
LARGE_PER_GENDER = SAMPLE_LARGE_TOTAL // 2
MEDIUM_PER_GENDER = SAMPLE_MEDIUM_TOTAL // 2
SMALL_PER_GENDER = SAMPLE_SMALL_TOTAL // 2
_COMMUNE_ENRICHMENT_COLUMNS = [
    "commune_insee",
    "commune_name",
    "dep_code",
    "reg_code",
    "city_size_bucket",
]


def write_parquet_frame(dataframe: pd.DataFrame, path: Path) -> None:
    """Write one test DataFrame to Parquet, creating parents as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dataframe), path, compression="snappy")


def build_candidate_and_commune_frames(
    *,
    include_extra_large_female: bool = False,
    insufficient_large_female: bool = False,
    extra_candidates_per_slot: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create deterministic dim_candidate_leader + dim_commune sampling fixtures.

    Args:
        include_extra_large_female: Add one extra large-F candidate with a high
            same-name collision count to test ambiguity-based deprioritisation.
        insufficient_large_female: Remove one large-F candidate so the quota
            cannot be met.
        extra_candidates_per_slot: Extra candidates generated beyond the active
            quota per city-size x gender slot.

    Returns:
        Tuple of (leader_df, commune_df).
    """
    quota_by_bucket: dict[str, int] = {
        "large": LARGE_PER_GENDER,
        "medium": MEDIUM_PER_GENDER,
        "small": SMALL_PER_GENDER,
    }
    population_by_bucket = {"large": 150_000, "medium": 50_000, "small": 10_000}

    leader_rows: list[dict[str, object]] = []
    commune_rows: list[dict[str, object]] = []
    commune_counter = 1
    leader_counter = 1

    for bucket, per_gender in quota_by_bucket.items():
        for gender in ("F", "M"):
            count = per_gender + extra_candidates_per_slot
            if insufficient_large_female and bucket == "large" and gender == "F":
                count = per_gender - 1

            for _ in range(count):
                commune_insee = f"{commune_counter:05d}"
                commune_counter += 1
                leader_rows.append(
                    {
                        "leader_id": f"leader-{leader_counter:03d}",
                        "full_name": f"Candidate {leader_counter}",
                        "gender": gender,
                        "commune_insee": commune_insee,
                        "reg_code": f"R{(leader_counter % 20) + 1:02d}",
                        "city_size_bucket": bucket,
                        "same_name_candidate_count": 1,
                        "list_nuance": "DVG",
                        "nuance_group": "gauche",
                        "is_incumbent": False,
                        "incumbent_match_score": None,
                        "incumbent_match_auditable": False,
                        "advanced_to_tour2": None,
                        "score_tour1_votes": 100 + leader_counter,
                        "score_tour1_pct_expressed": 20.0 + (leader_counter % 7),
                        "score_tour1_rank": 1 + (leader_counter % 2),
                        "won_final_round": leader_counter % 4 == 0,
                    }
                )
                commune_rows.append(
                    {
                        "commune_insee": commune_insee,
                        "commune_name": f"Commune {leader_counter}",
                        "dep_code": f"D{(leader_counter % 9) + 1:02d}",
                        "reg_code": f"R{(leader_counter % 20) + 1:02d}",
                        "city_size_bucket": bucket,
                        "population": population_by_bucket[bucket],
                    }
                )
                leader_counter += 1

    if include_extra_large_female:
        commune_insee = f"{commune_counter:05d}"
        leader_rows.append(
            {
                "leader_id": "leader-extra-large-f",
                "full_name": "Highly Ambiguous Name",
                "gender": "F",
                "commune_insee": commune_insee,
                "reg_code": "R09",
                "city_size_bucket": "large",
                "same_name_candidate_count": 5,
                "list_nuance": "DVG",
                "nuance_group": "gauche",
                "is_incumbent": False,
                "incumbent_match_score": None,
                "incumbent_match_auditable": False,
                "advanced_to_tour2": None,
                "score_tour1_votes": 999,
                "score_tour1_pct_expressed": 22.0,
                "score_tour1_rank": 1,
                "won_final_round": False,
            }
        )
        commune_rows.append(
            {
                "commune_insee": commune_insee,
                "commune_name": "Commune Extra",
                "dep_code": "D09",
                "reg_code": "R09",
                "city_size_bucket": "large",
                "population": population_by_bucket["large"],
            }
        )

    return pd.DataFrame(leader_rows), pd.DataFrame(commune_rows)


def build_candidate_universe_frame(
    candidate_df: pd.DataFrame,
    commune_df: pd.DataFrame,
) -> pd.DataFrame:
    """Join fixture candidates with commune attributes and compute is_viable.

    Test fixtures mimic the production Gold mart contract so sampling tests can
    stay focused on cohort logic rather than on reconstructing the join shape
    inside every test.
    """
    candidate_universe_df = candidate_df.merge(
        commune_df[_COMMUNE_ENRICHMENT_COLUMNS],
        on="commune_insee",
        how="left",
        suffixes=("", "__commune"),
        validate="many_to_one",
    )
    for column_name in _COMMUNE_ENRICHMENT_COLUMNS[1:]:
        commune_column_name = f"{column_name}__commune"
        if commune_column_name not in candidate_universe_df.columns:
            continue

        if column_name in candidate_universe_df.columns:
            candidate_universe_df[column_name] = candidate_universe_df[
                column_name
            ].combine_first(candidate_universe_df[commune_column_name])
            candidate_universe_df = candidate_universe_df.drop(
                columns=[commune_column_name]
            )
        else:
            candidate_universe_df[column_name] = candidate_universe_df.pop(
                commune_column_name
            )

    candidate_universe_df["is_viable"] = (
        (
            candidate_universe_df["score_tour1_pct_expressed"]
            >= SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1
        )
        | (
            candidate_universe_df["score_tour1_rank"]
            <= SAMPLE_MAX_RANK_TOUR1_FOR_VIABILITY
        )
    ).astype("boolean")
    return candidate_universe_df
