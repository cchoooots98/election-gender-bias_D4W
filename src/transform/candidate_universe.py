"""Materialize the Gold candidate_universe wide table.

This table is the single pre-joined consumer mart for candidate-level
analytics. Silver remains Kimball-clean:

- ``silver.dim_candidate_leader`` keeps descriptive candidate attributes only
- ``silver.fact_election_result`` keeps vote/result measures only
- ``silver.dim_commune`` keeps geographic attributes only

Downstream consumers such as sampling, dashboards, and regression features read
this Gold table instead of repeating joins ad hoc.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    GOLD_DIR,
    SAMPLE_MAX_RANK_TOUR1_FOR_VIABILITY,
    SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.transform._exceptions import DataQualityError
from src.transform.fact_election_result import summarize_election_results

logger = logging.getLogger(__name__)

_REQUIRED_CANDIDATE_COLUMNS = {
    "leader_id",
    "full_name",
    "gender",
    "commune_insee",
    "same_name_candidate_count",
    "list_nuance",
    "nuance_group",
    "is_incumbent",
    "incumbent_match_score",
    "incumbent_match_auditable",
    "advanced_to_tour2",
}
_FORBIDDEN_CANDIDATE_COLUMNS = {
    "commune_name",
    "dep_code",
    "reg_code",
    "city_size_bucket",
    "score_tour1_votes",
    "score_tour1_pct_expressed",
    "score_tour1_rank",
    "score_tour2_votes",
    "score_tour2_pct_expressed",
    "score_tour2_rank",
    "vote_share_band_tour1",
    "won_final_round",
}
_COMMUNE_JOIN_COLUMNS = [
    "commune_insee",
    "commune_name",
    "dep_code",
    "reg_code",
    "city_size_bucket",
]
_REQUIRED_COMMUNE_COLUMNS = set(_COMMUNE_JOIN_COLUMNS)
_OUTPUT_COLUMNS = [
    "leader_id",
    "full_name",
    "gender",
    "commune_insee",
    "commune_name",
    "dep_code",
    "reg_code",
    "city_size_bucket",
    "same_name_candidate_count",
    "list_nuance",
    "nuance_group",
    "is_incumbent",
    "incumbent_match_score",
    "incumbent_match_auditable",
    "advanced_to_tour2",
    "score_tour1_votes",
    "score_tour1_pct_expressed",
    "score_tour1_rank",
    "score_tour2_votes",
    "score_tour2_pct_expressed",
    "score_tour2_rank",
    "vote_share_band_tour1",
    "won_final_round",
    "is_viable",
]


def _validate_columns(
    dataframe: pd.DataFrame,
    required_columns: set[str],
    dataset_name: str,
) -> None:
    """Fail fast when an upstream table violates the expected contract."""
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise DataQualityError(
            f"{dataset_name} is missing required columns: {missing_columns}"
        )


def _validate_absent_columns(
    dataframe: pd.DataFrame,
    forbidden_columns: set[str],
    dataset_name: str,
) -> None:
    """Fail fast when an upstream table still carries deprecated wide columns."""
    present_forbidden_columns = sorted(forbidden_columns & set(dataframe.columns))
    if present_forbidden_columns:
        raise DataQualityError(
            f"{dataset_name} contains stale denormalized columns from an older "
            "silver contract: "
            f"{present_forbidden_columns}. Rebuild dim_candidate_leader before "
            "materializing candidate_universe."
        )


def build_candidate_universe(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> pd.DataFrame:
    """Build the Gold candidate_universe mart.

    Args:
        silver_dir: Root silver directory containing the conformed inputs.
        gold_dir: Root gold directory where the mart Parquet is written.
        duckdb_path: DuckDB warehouse path.

    Returns:
        Materialized Gold candidate_universe DataFrame.

    Raises:
        FileNotFoundError: If a required Silver input is missing.
        DataQualityError: If the Gold join would emit an invalid analytical base.
    """
    dim_candidate_path = silver_dir / "dim_candidate_leader.parquet"
    fact_result_path = silver_dir / "fact_election_result.parquet"
    dim_commune_path = silver_dir / "dim_commune.parquet"

    for required_path in (dim_candidate_path, fact_result_path, dim_commune_path):
        if not required_path.exists():
            raise FileNotFoundError(
                f"Required silver file not found: {required_path}. "
                "Build dim_candidate_leader, fact_election_result, and dim_commune "
                "before materializing candidate_universe."
            )

    candidate_df = pd.read_parquet(dim_candidate_path)
    fact_result_df = pd.read_parquet(fact_result_path)
    commune_df = pd.read_parquet(dim_commune_path)

    _validate_columns(
        dataframe=candidate_df,
        required_columns=_REQUIRED_CANDIDATE_COLUMNS,
        dataset_name="dim_candidate_leader",
    )
    _validate_absent_columns(
        dataframe=candidate_df,
        forbidden_columns=_FORBIDDEN_CANDIDATE_COLUMNS,
        dataset_name="dim_candidate_leader",
    )
    _validate_columns(
        dataframe=commune_df,
        required_columns=_REQUIRED_COMMUNE_COLUMNS,
        dataset_name="dim_commune",
    )

    if candidate_df["leader_id"].duplicated().any():
        duplicate_leaders = (
            candidate_df.loc[candidate_df["leader_id"].duplicated(), "leader_id"]
            .astype(str)
            .unique()
            .tolist()
        )
        raise DataQualityError(
            "dim_candidate_leader is not unique on leader_id. "
            f"Examples: {duplicate_leaders[:5]}"
        )

    result_summary_df = summarize_election_results(fact_result_df)
    candidate_universe_df = candidate_df.merge(
        result_summary_df,
        on="leader_id",
        how="left",
        validate="one_to_one",
    ).merge(
        commune_df[_COMMUNE_JOIN_COLUMNS],
        on="commune_insee",
        how="left",
        validate="many_to_one",
    )

    missing_round1_mask = (
        candidate_universe_df[["score_tour1_pct_expressed", "score_tour1_rank"]]
        .isna()
        .any(axis=1)
    )
    if missing_round1_mask.any():
        missing_candidates = candidate_universe_df.loc[
            missing_round1_mask, ["leader_id", "commune_insee", "full_name"]
        ].to_dict("records")
        raise DataQualityError(
            "candidate_universe is missing round-1 result fields for one or more "
            f"leaders. Examples: {missing_candidates[:5]}"
        )

    missing_commune_mask = (
        candidate_universe_df[
            ["commune_name", "dep_code", "reg_code", "city_size_bucket"]
        ]
        .isna()
        .any(axis=1)
    )
    if missing_commune_mask.any():
        missing_communes = (
            candidate_universe_df.loc[missing_commune_mask, "commune_insee"]
            .astype(str)
            .unique()
            .tolist()
        )
        raise DataQualityError(
            "candidate_universe is missing commune attributes for one or more "
            f"leaders. Examples: {missing_communes[:5]}"
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
    candidate_universe_df = candidate_universe_df[_OUTPUT_COLUMNS].copy()

    gold_path = gold_dir / "candidate_universe.parquet"
    gold_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(candidate_universe_df),
        gold_path,
        compression="snappy",
    )
    logger.info(
        "Gold Parquet written path=%s rows=%d viable_rows=%d",
        gold_path,
        len(candidate_universe_df),
        int(candidate_universe_df["is_viable"].fillna(False).sum()),
    )

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("DROP TABLE IF EXISTS gold.candidate_universe")
        conn.execute(
            "CREATE TABLE gold.candidate_universe AS SELECT * FROM candidate_universe_df"
        )
        row_count_result = conn.execute(
            "SELECT count(*) FROM gold.candidate_universe"
        ).fetchone()
        if row_count_result is None:
            raise RuntimeError(
                "Expected one row from gold.candidate_universe count query"
            )
        logger.info(
            "DuckDB gold.candidate_universe written rows=%d",
            row_count_result[0],
        )
    finally:
        conn.close()

    return candidate_universe_df
