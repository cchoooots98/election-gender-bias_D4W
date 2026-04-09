"""Normalize official municipal results into one leader x commune x round fact.

The official results files are wide commune-level exports with repeated list
groups. This module converts them into an auditable fact table and quarantines
rows that cannot be mapped safely back to the Tour 1 leader universe.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    BRONZE_DIR,
    CANDIDATES_COLUMN_MAP,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.transform._exceptions import DataQualityError
from src.transform._leader_keys import (
    build_full_name,
    build_full_name_columns,
    generate_leader_id,
    normalize_leader_name,
)

logger = logging.getLogger(__name__)

_LIST_GROUP_PATTERN = re.compile(r"^(?P<base>.+?) (?P<index>\d+)$")

_LIST_GROUP_FIELD_MAP = {
    "Numéro de panneau": "list_id",
    "Nom candidat": "family_name",
    "Prénom candidat": "given_name",
    "Sexe candidat": "gender",
    "Nuance liste": "list_nuance",
    "Libellé abrégé de liste": "list_label_short",
    "Libellé de liste": "list_label",
    "Voix": "votes",
    "% Voix/inscrits": "vote_share_pct_registered",
    "% Voix/exprimés": "vote_share_pct_expressed",
    "Elu": "elected_flag",
    "Sièges au CM": "seats_municipal_won",
    "Sièges au CC": "seats_epci_won",
}

_RESULT_OUTPUT_COLUMNS = [
    "leader_id",
    "commune_insee",
    "round_number",
    "list_id",
    "leader_full_name_official",
    "votes",
    "vote_share_pct_expressed",
    "vote_share_pct_registered",
    "rank_in_commune_round",
    "seats_municipal_won",
    "seats_epci_won",
    "list_nuance",
    "_source_url",
    "_ingested_at",
    "_source_hash",
]

_SUMMARY_OUTPUT_COLUMNS = [
    "leader_id",
    "score_tour1_votes",
    "score_tour1_pct_expressed",
    "score_tour1_rank",
    "score_tour2_votes",
    "score_tour2_pct_expressed",
    "score_tour2_rank",
    "vote_share_band_tour1",
    "won_final_round",
]


def _normalize_string(value: object) -> str:
    """Normalize a raw scalar to a stripped string."""
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_commune_insee(value: object) -> str:
    """Normalize a commune code to its 5-character canonical form."""
    raw_value = _normalize_string(value)
    if not raw_value:
        return ""
    if raw_value.isdigit() and len(raw_value) < 5:
        return raw_value.zfill(5)
    return raw_value


def _normalize_list_id(value: object) -> str:
    """Normalize panel/list identifiers used as the primary mapping key."""
    raw_value = _normalize_string(value)
    if not raw_value:
        return ""
    if raw_value.endswith(".0"):
        raw_value = raw_value[:-2]
    return raw_value


def _parse_int_field(value: object) -> int | None:
    """Parse an integer-like official results value safely."""
    raw_value = _normalize_string(value).replace("\u00a0", "").replace(" ", "")
    if not raw_value:
        return None
    return int(raw_value)


def _parse_percent_field(value: object) -> float | None:
    """Parse a French-formatted percent value such as ``55,29%``."""
    raw_value = (
        _normalize_string(value)
        .replace("\u00a0", "")
        .replace(" ", "")
        .replace("%", "")
        .replace(",", ".")
    )
    if not raw_value:
        return None
    return float(raw_value)


def _is_blank_list_group(record: dict[str, object]) -> bool:
    """Return True when a repeated list-group contains no usable values."""
    relevant_values = [
        _normalize_string(record.get("list_id")),
        _normalize_string(record.get("family_name")),
        _normalize_string(record.get("given_name")),
        _normalize_string(record.get("list_label")),
        _normalize_string(record.get("votes")),
    ]
    return all(value == "" for value in relevant_values)


def _identify_list_groups(column_names: list[str]) -> dict[int, dict[str, str]]:
    """Find all repeated result-list column groups by suffix index."""
    groups: dict[int, dict[str, str]] = {}
    for column_name in column_names:
        match = _LIST_GROUP_PATTERN.match(column_name)
        if not match:
            continue
        raw_base = match.group("base")
        list_index = int(match.group("index"))
        if raw_base not in _LIST_GROUP_FIELD_MAP:
            continue
        groups.setdefault(list_index, {})[raw_base] = column_name
    return dict(sorted(groups.items()))


def _load_round_candidate_leaders(
    bronze_candidates_path: Path,
    candidates_column_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Load one round of candidate leaders and prepare a stable lookup."""
    if not bronze_candidates_path.exists():
        raise FileNotFoundError(
            f"Required candidate bronze file not found: {bronze_candidates_path}"
        )

    effective_map = (
        candidates_column_map
        if candidates_column_map is not None
        else CANDIDATES_COLUMN_MAP
    )
    candidate_df = pd.read_parquet(bronze_candidates_path)
    candidate_df = candidate_df.rename(columns=effective_map)

    if "is_list_leader" in candidate_df.columns:
        leader_df = candidate_df[
            candidate_df["is_list_leader"].fillna("").str.strip().str.lower() == "oui"
        ].copy()
    elif "position_on_list" in candidate_df.columns:
        leader_df = candidate_df[
            candidate_df["position_on_list"].astype(str).str.strip() == "1"
        ].copy()
    else:
        raise DataQualityError(
            "Candidate leader extraction failed: neither is_list_leader nor "
            "position_on_list is available after applying CANDIDATES_COLUMN_MAP."
        )

    # Some official candidate exports contain fully blank non-leader rows.
    # We only need the leader lookup here, so validate name completeness after
    # the leader filter rather than letting unrelated blanks fail the whole fact build.
    leader_df = build_full_name_columns(leader_df)

    required_columns = {"commune_insee", "list_id", "full_name", "full_name_normalized"}
    missing_columns = sorted(required_columns - set(leader_df.columns))
    if missing_columns:
        raise DataQualityError(
            "Candidate leader lookup is missing required columns: "
            f"{missing_columns}. Check CANDIDATES_COLUMN_MAP."
        )

    blank_name_mask = leader_df["full_name_normalized"].astype(str).str.strip().eq("")
    if blank_name_mask.any():
        invalid_rows = (
            leader_df.loc[blank_name_mask, ["commune_insee", "list_id"]]
            .head(10)
            .to_dict("records")
        )
        raise DataQualityError(
            "Candidate leader lookup contains blank leader names after filtering. "
            f"Examples: {invalid_rows}"
        )

    leader_df["commune_insee"] = leader_df["commune_insee"].apply(
        _normalize_commune_insee
    )
    leader_df["list_id"] = leader_df["list_id"].apply(_normalize_list_id)

    # Drop leaders without a list_id — they cannot be matched to results records
    # and are typically overseas-territory candidates (e.g. Wallis-et-Futuna
    # communes 98xxx) where the official export omits the panel number.
    # These communes are outside the sampling pool, so excluding them here is safe.
    empty_list_id_mask = leader_df["list_id"] == ""
    if empty_list_id_mask.any():
        dropped_count = empty_list_id_mask.sum()
        affected_communes = sorted(
            leader_df.loc[empty_list_id_mask, "commune_insee"].unique().tolist()
        )
        logger.warning(
            "Dropped %d leader rows with empty list_id from results lookup "
            "(communes outside sampling pool: %s)",
            dropped_count,
            affected_communes,
        )
        leader_df = leader_df.loc[~empty_list_id_mask].copy()

    leader_df["leader_id"] = leader_df.apply(
        lambda row: generate_leader_id(
            full_name=str(row["full_name"]),
            commune_insee=str(row["commune_insee"]),
        ),
        axis=1,
    )

    duplicate_key_mask = leader_df.duplicated(subset=["commune_insee", "list_id"])
    if duplicate_key_mask.any():
        duplicate_keys = leader_df.loc[
            duplicate_key_mask, ["commune_insee", "list_id"]
        ].to_dict("records")
        raise DataQualityError(
            "Candidate leader lookup is not unique on commune_insee + list_id: "
            f"{duplicate_keys[:5]}"
        )

    return leader_df[
        [
            "leader_id",
            "commune_insee",
            "list_id",
            "full_name",
            "full_name_normalized",
        ]
    ].copy()


def _normalize_round_results(
    results_df: pd.DataFrame,
    round_number: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert one wide official results export into a normalized row set."""
    list_groups = _identify_list_groups(results_df.columns.tolist())
    if not list_groups:
        raise DataQualityError(
            "Municipal results CSV does not contain repeated list groups. "
            "Verify the official schema before parsing."
        )

    normalized_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []

    for row in results_df.to_dict(orient="records"):
        commune_insee = _normalize_commune_insee(row.get("Code commune"))

        for _list_index, group_columns in list_groups.items():
            list_record = {
                "commune_insee": commune_insee,
                "round_number": round_number,
                "_source_url": row.get("_source_url"),
                "_ingested_at": row.get("_ingested_at"),
                "_source_hash": row.get("_source_hash"),
            }
            for raw_base, normalized_name in _LIST_GROUP_FIELD_MAP.items():
                source_column = group_columns.get(raw_base)
                list_record[normalized_name] = (
                    row.get(source_column) if source_column else None
                )

            if _is_blank_list_group(list_record):
                continue

            list_record["list_id"] = _normalize_list_id(list_record["list_id"])
            list_record["leader_name_from_results"] = build_full_name(
                _normalize_string(list_record.get("family_name")),
                _normalize_string(list_record.get("given_name")),
            )
            list_record["leader_name_normalized"] = normalize_leader_name(
                str(list_record["leader_name_from_results"])
            )

            try:
                list_record["votes"] = _parse_int_field(list_record["votes"])
                list_record["vote_share_pct_registered"] = _parse_percent_field(
                    list_record["vote_share_pct_registered"]
                )
                list_record["vote_share_pct_expressed"] = _parse_percent_field(
                    list_record["vote_share_pct_expressed"]
                )
                list_record["seats_municipal_won"] = _parse_int_field(
                    list_record["seats_municipal_won"]
                )
                list_record["seats_epci_won"] = _parse_int_field(
                    list_record["seats_epci_won"]
                )
            except ValueError as exc:
                rejected_row = dict(list_record)
                rejected_row["_rejection_reason"] = (
                    f"malformed_numeric_value_round_{round_number}: {exc}"
                )
                rejected_rows.append(rejected_row)
                continue

            if not list_record["commune_insee"]:
                rejected_row = dict(list_record)
                rejected_row["_rejection_reason"] = "missing_commune_insee"
                rejected_rows.append(rejected_row)
                continue

            if not list_record["list_id"]:
                rejected_row = dict(list_record)
                rejected_row["_rejection_reason"] = (
                    f"missing_list_id_round_{round_number}"
                )
                rejected_rows.append(rejected_row)
                continue

            normalized_rows.append(list_record)

    normalized_df = pd.DataFrame(normalized_rows)
    rejected_df = pd.DataFrame(rejected_rows)
    return normalized_df, rejected_df


def _map_round1_results(
    normalized_results_df: pd.DataFrame,
    round1_leader_lookup_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map first-round results to the Tour 1 leader universe."""
    mapped_df = normalized_results_df.merge(
        round1_leader_lookup_df,
        on=["commune_insee", "list_id"],
        how="left",
        validate="many_to_one",
    )

    rejected_parts: list[pd.DataFrame] = []

    missing_lookup_mask = mapped_df["leader_id"].isna()
    if missing_lookup_mask.any():
        rejected_df = mapped_df.loc[missing_lookup_mask].copy()
        rejected_df["_rejection_reason"] = (
            "round1_candidate_leader_not_found_by_commune_insee_and_list_id"
        )
        rejected_parts.append(rejected_df)
        mapped_df = mapped_df.loc[~missing_lookup_mask].copy()

    name_mismatch_mask = mapped_df["leader_name_normalized"].ne("") & mapped_df[
        "full_name_normalized"
    ].ne(mapped_df["leader_name_normalized"])
    if name_mismatch_mask.any():
        rejected_df = mapped_df.loc[name_mismatch_mask].copy()
        rejected_df["_rejection_reason"] = "round1_leader_name_mismatch"
        rejected_parts.append(rejected_df)
        mapped_df = mapped_df.loc[~name_mismatch_mask].copy()

    mapped_df["leader_full_name_official"] = mapped_df[
        "leader_name_from_results"
    ].where(
        mapped_df["leader_name_from_results"].ne(""),
        mapped_df["full_name"],
    )

    rejected_df = (
        pd.concat(rejected_parts, ignore_index=True)
        if rejected_parts
        else pd.DataFrame(columns=list(mapped_df.columns) + ["_rejection_reason"])
    )
    return mapped_df, rejected_df


def _map_round2_results(
    normalized_results_df: pd.DataFrame,
    round2_leader_lookup_df: pd.DataFrame,
    round1_leader_lookup_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Map second-round results through Tour 2 leaders back to Tour 1 IDs."""
    round1_name_lookup_df = round1_leader_lookup_df[
        ["leader_id", "commune_insee", "full_name_normalized"]
    ].drop_duplicates()

    mapped_df = normalized_results_df.merge(
        round2_leader_lookup_df.rename(
            columns={
                "leader_id": "tour2_leader_id",
                "full_name": "tour2_full_name",
                "full_name_normalized": "tour2_full_name_normalized",
            }
        ),
        on=["commune_insee", "list_id"],
        how="left",
        validate="many_to_one",
    )

    rejected_parts: list[pd.DataFrame] = []

    missing_round2_lookup_mask = mapped_df["tour2_leader_id"].isna()
    if missing_round2_lookup_mask.any():
        rejected_df = mapped_df.loc[missing_round2_lookup_mask].copy()
        rejected_df["_rejection_reason"] = (
            "round2_candidate_leader_not_found_by_commune_insee_and_list_id"
        )
        rejected_parts.append(rejected_df)
        mapped_df = mapped_df.loc[~missing_round2_lookup_mask].copy()

    round2_name_mismatch_mask = mapped_df["leader_name_normalized"].ne("") & mapped_df[
        "tour2_full_name_normalized"
    ].ne(mapped_df["leader_name_normalized"])
    if round2_name_mismatch_mask.any():
        rejected_df = mapped_df.loc[round2_name_mismatch_mask].copy()
        rejected_df["_rejection_reason"] = "round2_leader_name_mismatch"
        rejected_parts.append(rejected_df)
        mapped_df = mapped_df.loc[~round2_name_mismatch_mask].copy()

    mapped_df = mapped_df.merge(
        round1_name_lookup_df.rename(
            columns={
                "leader_id": "leader_id",
                "full_name_normalized": "tour1_full_name_normalized",
            }
        ),
        left_on=["commune_insee", "tour2_full_name_normalized"],
        right_on=["commune_insee", "tour1_full_name_normalized"],
        how="left",
        validate="many_to_one",
    )

    unresolved_round2_mask = mapped_df["leader_id"].isna()
    if unresolved_round2_mask.any():
        rejected_df = mapped_df.loc[unresolved_round2_mask].copy()
        rejected_df["_rejection_reason"] = (
            "round2_leader_not_resolvable_back_to_tour1_universe"
        )
        rejected_parts.append(rejected_df)
        mapped_df = mapped_df.loc[~unresolved_round2_mask].copy()

    mapped_df["leader_full_name_official"] = mapped_df[
        "leader_name_from_results"
    ].where(
        mapped_df["leader_name_from_results"].ne(""),
        mapped_df["tour2_full_name"],
    )

    rejected_df = (
        pd.concat(rejected_parts, ignore_index=True)
        if rejected_parts
        else pd.DataFrame(columns=list(mapped_df.columns) + ["_rejection_reason"])
    )
    return mapped_df, rejected_df


def _finalize_clean_fact(mapped_df: pd.DataFrame) -> pd.DataFrame:
    """Finalize the clean fact table schema and derived ranking."""
    clean_df = mapped_df.copy()
    clean_df["rank_in_commune_round"] = (
        clean_df.groupby(["commune_insee", "round_number"])["vote_share_pct_expressed"]
        .rank(method="dense", ascending=False)
        .astype("Int64")
    )

    for column_name in _RESULT_OUTPUT_COLUMNS:
        if column_name not in clean_df.columns:
            clean_df[column_name] = None

    clean_df = clean_df[_RESULT_OUTPUT_COLUMNS].copy()

    duplicate_key_mask = clean_df.duplicated(
        subset=["leader_id", "commune_insee", "round_number"]
    )
    if duplicate_key_mask.any():
        duplicate_keys = clean_df.loc[
            duplicate_key_mask, ["leader_id", "commune_insee", "round_number"]
        ].to_dict("records")
        raise DataQualityError(
            "fact_election_result duplicate keys detected: " f"{duplicate_keys[:5]}"
        )

    if clean_df["leader_id"].isna().any():
        raise DataQualityError("fact_election_result contains null leader_id values.")

    if clean_df["vote_share_pct_expressed"].isna().any():
        raise DataQualityError(
            "fact_election_result contains null vote_share_pct_expressed values."
        )

    return clean_df


def _build_rejected_output(
    rejected_parts: list[pd.DataFrame],
) -> pd.DataFrame:
    """Combine rejected result rows into one quarantine DataFrame."""
    # Round-specific rejected frames do not share an identical schema. Building
    # the quarantine table from explicit row records avoids pandas concat dtype
    # inference on empty/all-NA columns, which emits a FutureWarning in pandas
    # ≥ 2.1 and will become stricter in a future release.
    non_empty_parts = [df for df in rejected_parts if not df.empty]
    if not non_empty_parts:
        return pd.DataFrame(columns=["_rejection_reason"])
    ordered_columns: list[str] = []
    seen_columns: set[str] = set()
    row_records: list[dict[str, object]] = []

    for rejected_df in non_empty_parts:
        for column_name in rejected_df.columns:
            if column_name not in seen_columns:
                ordered_columns.append(column_name)
                seen_columns.add(column_name)
        row_records.extend(rejected_df.to_dict("records"))

    if "_rejection_reason" not in seen_columns:
        ordered_columns.append("_rejection_reason")

    return pd.DataFrame.from_records(row_records, columns=ordered_columns)


def _vote_share_band_from_pct(vote_share_pct: float | None) -> str | None:
    """Bucket Tour 1 vote share into stable analytical bands."""
    if vote_share_pct is None or pd.isna(vote_share_pct):
        return None
    if vote_share_pct < 5:
        return "<5"
    if vote_share_pct < 10:
        return "5-10"
    if vote_share_pct < 25:
        return "10-25"
    if vote_share_pct < 50:
        return "25-50"
    return "50+"


def summarize_election_results(
    fact_result_df: pd.DataFrame,
) -> pd.DataFrame:
    """Build one derived score summary row per Tour 1 leader."""
    required_columns = {
        "leader_id",
        "commune_insee",
        "round_number",
        "votes",
        "vote_share_pct_expressed",
        "rank_in_commune_round",
    }
    missing_columns = sorted(required_columns - set(fact_result_df.columns))
    if missing_columns:
        raise DataQualityError(
            "fact_election_result summary is missing required columns: "
            f"{missing_columns}"
        )

    summary_df = fact_result_df[["leader_id", "commune_insee"]].drop_duplicates()

    tour1_df = fact_result_df[fact_result_df["round_number"] == 1][
        ["leader_id", "votes", "vote_share_pct_expressed", "rank_in_commune_round"]
    ].rename(
        columns={
            "votes": "score_tour1_votes",
            "vote_share_pct_expressed": "score_tour1_pct_expressed",
            "rank_in_commune_round": "score_tour1_rank",
        }
    )
    tour2_df = fact_result_df[fact_result_df["round_number"] == 2][
        ["leader_id", "votes", "vote_share_pct_expressed", "rank_in_commune_round"]
    ].rename(
        columns={
            "votes": "score_tour2_votes",
            "vote_share_pct_expressed": "score_tour2_pct_expressed",
            "rank_in_commune_round": "score_tour2_rank",
        }
    )

    summary_df = summary_df.merge(
        tour1_df, on="leader_id", how="left", validate="one_to_one"
    )
    summary_df = summary_df.merge(
        tour2_df, on="leader_id", how="left", validate="one_to_one"
    )

    final_round_df = fact_result_df.groupby("commune_insee", as_index=False)[
        "round_number"
    ].max()
    final_round_winners_df = fact_result_df.merge(
        final_round_df,
        on=["commune_insee", "round_number"],
        how="inner",
        validate="many_to_one",
    )
    winner_ids = set(
        final_round_winners_df.loc[
            final_round_winners_df["rank_in_commune_round"] == 1, "leader_id"
        ]
    )

    summary_df["vote_share_band_tour1"] = summary_df["score_tour1_pct_expressed"].apply(
        _vote_share_band_from_pct
    )
    summary_df["won_final_round"] = summary_df["leader_id"].isin(winner_ids)

    for column_name in _SUMMARY_OUTPUT_COLUMNS:
        if column_name not in summary_df.columns:
            summary_df[column_name] = None

    return summary_df[_SUMMARY_OUTPUT_COLUMNS].copy()


def build_fact_election_result(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    candidates_column_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Build the normalized official election-results fact table.

    Args:
        bronze_dir: Root bronze directory.
        silver_dir: Root silver directory.
        duckdb_path: DuckDB warehouse path.
        candidates_column_map: Optional override for candidate column mapping.

    Returns:
        Clean fact_election_result DataFrame.

    Raises:
        FileNotFoundError: If required Bronze inputs are missing.
        DataQualityError: If the fact violates its required contracts.
    """
    round1_results_path = bronze_dir / "results" / "results_tour1.parquet"
    round2_results_path = bronze_dir / "results" / "results_tour2.parquet"
    round1_candidates_path = bronze_dir / "candidates" / "candidates_tour1.parquet"
    round2_candidates_path = bronze_dir / "candidates" / "candidates_tour2.parquet"

    for required_path in (round1_results_path, round1_candidates_path):
        if not required_path.exists():
            raise FileNotFoundError(
                f"Required file not found: {required_path}. "
                "Run the official Bronze ingest steps first."
            )

    rejected_parts: list[pd.DataFrame] = []

    round1_results_df = pd.read_parquet(round1_results_path)
    round1_lookup_df = _load_round_candidate_leaders(
        bronze_candidates_path=round1_candidates_path,
        candidates_column_map=candidates_column_map,
    )
    normalized_round1_df, rejected_round1_parse_df = _normalize_round_results(
        results_df=round1_results_df,
        round_number=1,
    )
    clean_round1_df, rejected_round1_map_df = _map_round1_results(
        normalized_results_df=normalized_round1_df,
        round1_leader_lookup_df=round1_lookup_df,
    )
    rejected_parts.extend([rejected_round1_parse_df, rejected_round1_map_df])

    clean_fact_parts = [clean_round1_df]

    if round2_results_path.exists() and round2_candidates_path.exists():
        round2_results_df = pd.read_parquet(round2_results_path)
        round2_lookup_df = _load_round_candidate_leaders(
            bronze_candidates_path=round2_candidates_path,
            candidates_column_map=candidates_column_map,
        )
        normalized_round2_df, rejected_round2_parse_df = _normalize_round_results(
            results_df=round2_results_df,
            round_number=2,
        )
        clean_round2_df, rejected_round2_map_df = _map_round2_results(
            normalized_results_df=normalized_round2_df,
            round2_leader_lookup_df=round2_lookup_df,
            round1_leader_lookup_df=round1_lookup_df,
        )
        rejected_parts.extend([rejected_round2_parse_df, rejected_round2_map_df])
        clean_fact_parts.append(clean_round2_df)
    else:
        logger.warning(
            "Round 2 results or Tour 2 candidate bronze is missing - "
            "fact_election_result will contain round 1 rows only."
        )

    clean_df = _finalize_clean_fact(pd.concat(clean_fact_parts, ignore_index=True))
    rejected_df = _build_rejected_output(rejected_parts)

    silver_path = silver_dir / "fact_election_result.parquet"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(clean_df), silver_path, compression="snappy")
    logger.info(
        "Silver fact_election_result written path=%s rows=%d",
        silver_path,
        len(clean_df),
    )

    if not rejected_df.empty:
        rejected_path = (
            silver_dir / "_rejected" / "fact_election_result_rejected.parquet"
        )
        rejected_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(rejected_df),
            rejected_path,
            compression="snappy",
        )
        logger.warning(
            "Election results quarantine written path=%s rows=%d",
            rejected_path,
            len(rejected_df),
        )

    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("DROP TABLE IF EXISTS silver.fact_election_result")
        conn.execute(
            "CREATE TABLE silver.fact_election_result AS SELECT * FROM clean_df"
        )
    finally:
        conn.close()

    return clean_df
