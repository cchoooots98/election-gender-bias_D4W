"""Tests for the Gold candidate_universe mart."""

import pandas as pd
import pytest

from src.transform._exceptions import DataQualityError
from src.transform.candidate_universe import build_candidate_universe
from tests.sampling_builders import write_parquet_frame


def test_build_candidate_universe_joins_geography_scores_and_viability(tmp_path):
    """Happy path: Gold mart should materialize the single consumer-wide join."""
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"

    candidate_df = pd.DataFrame(
        {
            "leader_id": ["leader-1"],
            "full_name": ["Alice Martin"],
            "gender": ["F"],
            "commune_insee": ["11111"],
            "same_name_candidate_count": [1],
            "list_nuance": ["DVG"],
            "nuance_group": ["gauche"],
            "is_incumbent": pd.Series([True], dtype="boolean"),
            "incumbent_match_score": [97.0],
            "incumbent_match_auditable": [True],
            "advanced_to_tour2": pd.Series([True], dtype="boolean"),
        }
    )
    commune_df = pd.DataFrame(
        {
            "commune_insee": ["11111"],
            "commune_name": ["Rennes"],
            "dep_code": ["35"],
            "reg_code": ["53"],
            "city_size_bucket": ["large"],
            "population": [220_000],
        }
    )
    fact_df = pd.DataFrame(
        {
            "leader_id": ["leader-1", "leader-1"],
            "commune_insee": ["11111", "11111"],
            "round_number": [1, 2],
            "votes": [600, 700],
            "vote_share_pct_expressed": [12.0, 70.0],
            "rank_in_commune_round": [2, 1],
        }
    )

    write_parquet_frame(candidate_df, silver_dir / "dim_candidate_leader.parquet")
    write_parquet_frame(commune_df, silver_dir / "dim_commune.parquet")
    write_parquet_frame(fact_df, silver_dir / "fact_election_result.parquet")

    candidate_universe_df = build_candidate_universe(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=tmp_path / "warehouse.duckdb",
    )

    assert candidate_universe_df.loc[0, "commune_name"] == "Rennes"
    assert candidate_universe_df.loc[0, "dep_code"] == "35"
    assert candidate_universe_df.loc[0, "reg_code"] == "53"
    assert candidate_universe_df.loc[0, "score_tour1_pct_expressed"] == 12.0
    assert candidate_universe_df.loc[0, "score_tour2_pct_expressed"] == 70.0
    assert candidate_universe_df.loc[0, "won_final_round"] == True  # noqa: E712
    assert candidate_universe_df.loc[0, "is_viable"] == True  # noqa: E712


def test_build_candidate_universe_raises_when_fact_file_missing(tmp_path):
    """Regression: missing fact_election_result must fail at the mart boundary."""
    silver_dir = tmp_path / "silver"

    candidate_df = pd.DataFrame(
        {
            "leader_id": ["leader-1"],
            "full_name": ["Alice Martin"],
            "gender": ["F"],
            "commune_insee": ["11111"],
            "same_name_candidate_count": [1],
            "list_nuance": ["DVG"],
            "nuance_group": ["gauche"],
            "is_incumbent": pd.Series([True], dtype="boolean"),
            "incumbent_match_score": [97.0],
            "incumbent_match_auditable": [True],
            "advanced_to_tour2": pd.Series([False], dtype="boolean"),
        }
    )
    commune_df = pd.DataFrame(
        {
            "commune_insee": ["11111"],
            "commune_name": ["Rennes"],
            "dep_code": ["35"],
            "reg_code": ["53"],
            "city_size_bucket": ["large"],
            "population": [220_000],
        }
    )

    write_parquet_frame(candidate_df, silver_dir / "dim_candidate_leader.parquet")
    write_parquet_frame(commune_df, silver_dir / "dim_commune.parquet")

    with pytest.raises(FileNotFoundError, match="fact_election_result"):
        build_candidate_universe(
            silver_dir=silver_dir,
            gold_dir=tmp_path / "gold",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )


def test_build_candidate_universe_fails_when_round1_scores_missing(tmp_path):
    """Regression: every candidate in the mart must have round-1 results."""
    silver_dir = tmp_path / "silver"

    candidate_df = pd.DataFrame(
        {
            "leader_id": ["leader-1"],
            "full_name": ["Alice Martin"],
            "gender": ["F"],
            "commune_insee": ["11111"],
            "same_name_candidate_count": [1],
            "list_nuance": ["DVG"],
            "nuance_group": ["gauche"],
            "is_incumbent": pd.Series([False], dtype="boolean"),
            "incumbent_match_score": [None],
            "incumbent_match_auditable": [False],
            "advanced_to_tour2": pd.Series([False], dtype="boolean"),
        }
    )
    commune_df = pd.DataFrame(
        {
            "commune_insee": ["11111"],
            "commune_name": ["Rennes"],
            "dep_code": ["35"],
            "reg_code": ["53"],
            "city_size_bucket": ["large"],
            "population": [220_000],
        }
    )
    fact_df = pd.DataFrame(
        {
            "leader_id": ["other-leader"],
            "commune_insee": ["11111"],
            "round_number": [1],
            "votes": [600],
            "vote_share_pct_expressed": [12.0],
            "rank_in_commune_round": [2],
        }
    )

    write_parquet_frame(candidate_df, silver_dir / "dim_candidate_leader.parquet")
    write_parquet_frame(commune_df, silver_dir / "dim_commune.parquet")
    write_parquet_frame(fact_df, silver_dir / "fact_election_result.parquet")

    with pytest.raises(DataQualityError, match="missing round-1 result fields"):
        build_candidate_universe(
            silver_dir=silver_dir,
            gold_dir=tmp_path / "gold",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )


def test_build_candidate_universe_fails_when_dim_candidate_has_stale_wide_columns(
    tmp_path,
):
    """Regression: stale Silver artifacts must fail with a contract-level error.

    ``gold.candidate_universe`` assumes ``silver.dim_candidate_leader`` is a
    narrow candidate dimension. If an older artifact still carries geography or
    score columns, the Gold join can otherwise degrade into suffix collisions
    and confusing pandas KeyErrors.
    """
    silver_dir = tmp_path / "silver"

    candidate_df = pd.DataFrame(
        {
            "leader_id": ["leader-1"],
            "full_name": ["Alice Martin"],
            "gender": ["F"],
            "commune_insee": ["11111"],
            "same_name_candidate_count": [1],
            "list_nuance": ["DVG"],
            "nuance_group": ["gauche"],
            "is_incumbent": pd.Series([False], dtype="boolean"),
            "incumbent_match_score": [None],
            "incumbent_match_auditable": [False],
            "advanced_to_tour2": pd.Series([False], dtype="boolean"),
            "reg_code": ["53"],
            "score_tour1_pct_expressed": [12.0],
        }
    )
    commune_df = pd.DataFrame(
        {
            "commune_insee": ["11111"],
            "commune_name": ["Rennes"],
            "dep_code": ["35"],
            "reg_code": ["53"],
            "city_size_bucket": ["large"],
        }
    )
    fact_df = pd.DataFrame(
        {
            "leader_id": ["leader-1"],
            "commune_insee": ["11111"],
            "round_number": [1],
            "votes": [600],
            "vote_share_pct_expressed": [12.0],
            "rank_in_commune_round": [2],
        }
    )

    write_parquet_frame(candidate_df, silver_dir / "dim_candidate_leader.parquet")
    write_parquet_frame(commune_df, silver_dir / "dim_commune.parquet")
    write_parquet_frame(fact_df, silver_dir / "fact_election_result.parquet")

    with pytest.raises(DataQualityError, match="stale denormalized columns"):
        build_candidate_universe(
            silver_dir=silver_dir,
            gold_dir=tmp_path / "gold",
            duckdb_path=tmp_path / "warehouse.duckdb",
        )
