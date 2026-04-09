"""Tests for the normalized municipal election-results fact table."""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.transform.fact_election_result import (
    build_fact_election_result,
    summarize_election_results,
)


def _write_parquet(dataframe: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to Parquet for a temporary test warehouse."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dataframe), path, compression="snappy")


def _candidate_rows(commune_insee: str) -> list[dict[str, object]]:
    """Build a minimal canonical candidate fixture."""
    return [
        {
            "commune_insee": commune_insee,
            "list_id": "1",
            "family_name": "Martin",
            "given_name": "Alice",
            "is_list_leader": "Oui",
        },
        {
            "commune_insee": commune_insee,
            "list_id": "2",
            "family_name": "Durand",
            "given_name": "Bob",
            "is_list_leader": "Oui",
        },
    ]


def _results_round1_row(commune_insee: str) -> dict[str, object]:
    """Build one commune-level official results row with two list groups."""
    return {
        "Code commune": commune_insee,
        "Numéro de panneau 1": "1",
        "Nom candidat 1": "Martin",
        "Prénom candidat 1": "Alice",
        "Sexe candidat 1": "F",
        "Nuance liste 1": "LDVG",
        "Voix 1": "600",
        "% Voix/inscrits 1": "60,00%",
        "% Voix/exprimés 1": "60,00%",
        "Sièges au CM 1": "20",
        "Sièges au CC 1": "5",
        "Numéro de panneau 2": "2",
        "Nom candidat 2": "Durand",
        "Prénom candidat 2": "Bob",
        "Sexe candidat 2": "M",
        "Nuance liste 2": "LDVD",
        "Voix 2": "400",
        "% Voix/inscrits 2": "40,00%",
        "% Voix/exprimés 2": "40,00%",
        "Sièges au CM 2": "5",
        "Sièges au CC 2": "1",
        "_source_url": "https://example.com/results_tour1.csv",
        "_ingested_at": "2026-04-08T10:00:00+00:00",
        "_source_hash": "a" * 32,
    }


def _results_round2_row(
    commune_insee: str, family_name: str, given_name: str
) -> dict[str, object]:
    """Build one round-2 official results row with a single list group."""
    return {
        "Code commune": commune_insee,
        "Numéro de panneau 1": "1",
        "Nom candidat 1": family_name,
        "Prénom candidat 1": given_name,
        "Sexe candidat 1": "F",
        "Nuance liste 1": "LDVG",
        "Voix 1": "700",
        "% Voix/inscrits 1": "70,00%",
        "% Voix/exprimés 1": "70,00%",
        "Sièges au CM 1": "22",
        "Sièges au CC 1": "5",
        "_source_url": "https://example.com/results_tour2.csv",
        "_ingested_at": "2026-04-08T10:00:00+00:00",
        "_source_hash": "b" * 32,
    }


def test_build_fact_election_result_normalizes_rounds_and_summary(tmp_path):
    """Happy path: round 1 and round 2 rows should normalize and summarize cleanly."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    _write_parquet(
        pd.DataFrame(_candidate_rows("11111")),
        bronze_dir / "candidates" / "candidates_tour1.parquet",
    )
    _write_parquet(
        pd.DataFrame(
            [
                {
                    "commune_insee": "11111",
                    "list_id": "1",
                    "family_name": "Martin",
                    "given_name": "Alice",
                    "is_list_leader": "Oui",
                }
            ]
        ),
        bronze_dir / "candidates" / "candidates_tour2.parquet",
    )
    _write_parquet(
        pd.DataFrame([_results_round1_row("11111")]),
        bronze_dir / "results" / "results_tour1.parquet",
    )
    _write_parquet(
        pd.DataFrame([_results_round2_row("11111", "Martin", "Alice")]),
        bronze_dir / "results" / "results_tour2.parquet",
    )

    fact_df = build_fact_election_result(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )
    summary_df = summarize_election_results(fact_df)

    assert len(fact_df) == 3
    round1_df = fact_df[fact_df["round_number"] == 1].sort_values(
        "rank_in_commune_round"
    )
    assert round1_df["rank_in_commune_round"].tolist() == [1, 2]
    assert round1_df["vote_share_pct_expressed"].tolist() == [60.0, 40.0]

    alice_summary = summary_df.sort_values("score_tour1_rank").iloc[0]
    bob_summary = summary_df.sort_values("score_tour1_rank").iloc[1]
    assert alice_summary["score_tour1_pct_expressed"] == 60.0
    assert alice_summary["score_tour2_pct_expressed"] == 70.0
    assert alice_summary["vote_share_band_tour1"] == "50+"
    assert alice_summary["won_final_round"] == True  # noqa: E712
    assert bob_summary["won_final_round"] == False  # noqa: E712


def test_build_fact_election_result_ignores_blank_non_leader_candidate_rows(tmp_path):
    """Regression: blank raw rows must not fail the leader lookup if they are not leaders."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    candidate_rows = _candidate_rows("11111")
    candidate_rows.append(
        {
            "commune_insee": "11111",
            "list_id": None,
            "family_name": None,
            "given_name": None,
            "is_list_leader": None,
        }
    )
    _write_parquet(
        pd.DataFrame(candidate_rows),
        bronze_dir / "candidates" / "candidates_tour1.parquet",
    )
    _write_parquet(
        pd.DataFrame([_results_round1_row("11111")]),
        bronze_dir / "results" / "results_tour1.parquet",
    )

    fact_df = build_fact_election_result(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )

    assert len(fact_df) == 2
    assert fact_df["leader_id"].nunique() == 2


def test_build_fact_election_result_rejects_name_mismatch(tmp_path):
    """Boundary: same commune/list_id with a contradictory leader name must quarantine."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    _write_parquet(
        pd.DataFrame(_candidate_rows("22222")),
        bronze_dir / "candidates" / "candidates_tour1.parquet",
    )
    mismatch_row = _results_round1_row("22222")
    mismatch_row["Nom candidat 1"] = "Wrong"
    mismatch_row["Prénom candidat 1"] = "Person"
    _write_parquet(
        pd.DataFrame([mismatch_row]),
        bronze_dir / "results" / "results_tour1.parquet",
    )

    fact_df = build_fact_election_result(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )
    rejected_df = pq.read_table(
        silver_dir / "_rejected" / "fact_election_result_rejected.parquet"
    ).to_pandas()

    assert len(fact_df) == 1
    assert "round1_leader_name_mismatch" in rejected_df["_rejection_reason"].tolist()


def test_build_fact_election_result_quarantines_round2_leader_missing_from_tour1(
    tmp_path,
):
    """Regression: round-2 leaders not resolvable back to Tour 1 must not map silently."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    _write_parquet(
        pd.DataFrame(
            [
                {
                    "commune_insee": "33333",
                    "list_id": "1",
                    "family_name": "Martin",
                    "given_name": "Alice",
                    "is_list_leader": "Oui",
                }
            ]
        ),
        bronze_dir / "candidates" / "candidates_tour1.parquet",
    )
    _write_parquet(
        pd.DataFrame(
            [
                {
                    "commune_insee": "33333",
                    "list_id": "1",
                    "family_name": "Lefevre",
                    "given_name": "Chloe",
                    "is_list_leader": "Oui",
                }
            ]
        ),
        bronze_dir / "candidates" / "candidates_tour2.parquet",
    )
    _write_parquet(
        pd.DataFrame([_results_round1_row("33333")]),
        bronze_dir / "results" / "results_tour1.parquet",
    )
    _write_parquet(
        pd.DataFrame([_results_round2_row("33333", "Lefevre", "Chloe")]),
        bronze_dir / "results" / "results_tour2.parquet",
    )

    fact_df = build_fact_election_result(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )
    rejected_df = pq.read_table(
        silver_dir / "_rejected" / "fact_election_result_rejected.parquet"
    ).to_pandas()

    assert (fact_df["round_number"] == 2).sum() == 0
    assert (
        "round2_leader_not_resolvable_back_to_tour1_universe"
        in rejected_df["_rejection_reason"].tolist()
    )


def test_build_rejected_output_no_futurewarning_when_rejected_parts_contains_empty_df():
    """Regression: _build_rejected_output must not trigger FutureWarning when the
    rejected_parts list contains empty DataFrames mixed with populated ones.

    Bug: pd.concat([empty_df, populated_df]) raised FutureWarning in pandas ≥ 2.1
    because dtype inference on all-NA entries changes in a future release. The fix
    filters empty frames before calling concat.
    """
    import warnings

    from src.transform.fact_election_result import _build_rejected_output

    empty_df = pd.DataFrame(columns=["leader_id", "round_number", "_rejection_reason"])
    populated_df = pd.DataFrame(
        [
            {
                "leader_id": "abc123",
                "round_number": 1,
                "_rejection_reason": "round1_leader_name_mismatch",
            }
        ]
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        # Must not raise FutureWarning even though one part is an empty DataFrame.
        result_df = _build_rejected_output([empty_df, populated_df])

    assert len(result_df) == 1
    assert result_df.iloc[0]["_rejection_reason"] == "round1_leader_name_mismatch"


def test_build_rejected_output_handles_round_specific_columns_without_futurewarning():
    """Regression: round-specific rejected schemas must not trigger concat warnings.

    Real runs combine quarantine rows from round-1 and round-2 mapping stages.
    Those frames carry different helper columns, so some columns are entirely
    missing/NA in one part. Building the quarantine output must remain stable
    as pandas tightens concat dtype inference rules.
    """
    import warnings

    from src.transform.fact_election_result import _build_rejected_output

    round1_rejected_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-1",
                "round_number": 1,
                "leader_name_from_results": "Alice Martin",
                "_rejection_reason": "round1_leader_name_mismatch",
            }
        ]
    )
    round2_rejected_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-2",
                "round_number": 2,
                "tour2_full_name": "Chloe Lefevre",
                "leader_name_from_results": pd.NA,
                "_rejection_reason": (
                    "round2_leader_not_resolvable_back_to_tour1_universe"
                ),
            }
        ]
    )

    with warnings.catch_warnings():
        warnings.simplefilter("error", FutureWarning)
        result_df = _build_rejected_output([round1_rejected_df, round2_rejected_df])

    assert len(result_df) == 2
    assert "tour2_full_name" in result_df.columns
    assert "leader_name_from_results" in result_df.columns
    assert set(result_df["_rejection_reason"]) == {
        "round1_leader_name_mismatch",
        "round2_leader_not_resolvable_back_to_tour1_universe",
    }


def test_build_rejected_output_returns_empty_frame_when_all_parts_are_empty():
    """Boundary: all-empty rejected_parts must return a valid empty DataFrame."""
    from src.transform.fact_election_result import _build_rejected_output

    empty_a = pd.DataFrame(columns=["leader_id", "_rejection_reason"])
    empty_b = pd.DataFrame(columns=["leader_id", "_rejection_reason"])

    result_df = _build_rejected_output([empty_a, empty_b])
    assert result_df.empty
    assert "_rejection_reason" in result_df.columns
