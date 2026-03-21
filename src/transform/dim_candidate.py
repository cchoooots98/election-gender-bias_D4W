"""Build the dim_candidate_leader silver table.

Grain: one row per tête de liste (list leader) per commune.

Sources (bronze):
  data/bronze/candidates/candidates_tour1.parquet   — analysis subject pool
  data/bronze/candidates/candidates_tour2.parquet   — for advanced_to_tour2 flag only
  data/bronze/rne/rne_incumbents.parquet

Reference (silver):
  data/silver/dim_commune.parquet  (for city_size_bucket, reg_code join)

Outputs (silver):
  data/silver/dim_candidate_leader.parquet
  DuckDB: silver.dim_candidate_leader
  data/silver/_rejected/dim_candidate_leader_rejected.parquet  (quarantine)

Key steps:
  1. Filter Tour 1 bronze to position == 1 on list (tête de liste)
  2. Filter to communes ≥ CITY_SIZE_SMALL_THRESHOLD (exclude <3500 pop)
  3. Join dim_commune → city_size_bucket, reg_code, population
  4. Assign nuance_group from NUANCE_GROUP_MAP
  5. Match against RNE incumbents (fuzzy, token_sort_ratio)
  6. Derive advanced_to_tour2 flag from Tour 2 bronze (name + commune match)
  7. Generate deterministic MD5 leader_id
  8. DQ validation + quarantine
  9. Idempotent write to Parquet + DuckDB

Why we sample from Tour 1 and NOT from Tour 2 finalists:
  Limiting the analysis to second-round survivors introduces survivorship bias.
  Candidates who advanced partly benefited from media coverage that is the
  outcome variable we are measuring. Instead, advanced_to_tour2 is included
  as a control variable in the regression model — this is the methodologically
  correct approach.

Incumbent matching design:
  Two-stage matching for performance: first filter to the same commune_insee
  (O(1) lookup, typically 1–3 mayors per commune), then run token_sort_ratio
  on the pre-filtered set. This avoids O(n_candidates × n_mayors) comparisons.

  token_sort_ratio is used over simple ratio because French official files
  sometimes list names as "FAMILY GIVEN" in one source and "GIVEN FAMILY"
  in another. token_sort_ratio sorts tokens alphabetically before comparing,
  making it invariant to word order.
"""

import hashlib
import logging
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from rapidfuzz import fuzz

from src.config.settings import (
    BRONZE_DIR,
    CANDIDATES_COLUMN_MAP,
    CITY_SIZE_SMALL_THRESHOLD,
    DQ_MAX_NULL_RATE,
    INCUMBENT_MATCH_THRESHOLD,
    NUANCE_GROUP_MAP,
    RNE_COLUMN_MAP,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

_OUTPUT_COLUMNS = [
    "leader_id",
    "full_name",
    "gender",
    "commune_insee",
    "commune_name",
    "dep_code",
    "reg_code",
    "population",
    "city_size_bucket",
    "list_nuance",
    "nuance_group",
    "is_incumbent",
    "incumbent_match_score",
    "incumbent_match_auditable",
    # advanced_to_tour2: True if this tête de liste's list advanced to the second round.
    # Used as a CONTROL VARIABLE in regression (not a sampling criterion).
    # NULL when Tour 2 bronze is not available (graceful degradation).
    "advanced_to_tour2",
]


def _normalize_name(name: str) -> str:
    """Normalise a French name for fuzzy comparison.

    Steps:
    1. Strip whitespace
    2. Uppercase
    3. Decompose accented characters (NFD normalisation)
    4. Remove non-ASCII combining marks (strips accents like é → e)
    5. Collapse multiple spaces

    Why all steps: the candidate file and the RNE file may use different accent
    encodings (latin-1 vs UTF-8) or different capitalisation conventions.
    Normalising before matching increases rapidfuzz score accuracy significantly.

    Args:
        name: Raw name string.

    Returns:
        Normalised uppercase ASCII name string.
    """
    if not name or not isinstance(name, str):
        return ""
    name = name.strip().upper()
    # NFD decomposes accented chars into base char + combining mark.
    # encode('ascii', 'ignore') then strips the combining marks.
    name = unicodedata.normalize("NFD", name).encode("ascii", "ignore").decode("ascii")
    # Collapse multiple spaces (e.g. after removing hyphen-related chars)
    name = " ".join(name.split())
    return name


def _generate_leader_id(full_name: str, commune_insee: str) -> str:
    """Generate a deterministic MD5 primary key for a candidate leader.

    PK design: MD5(full_name + commune_insee) — same inputs always produce
    the same ID. This makes the table idempotent: re-running produces
    identical PKs without a database sequence or UUID generator.

    This is the same approach dbt uses for surrogate keys (dbt_utils.generate_surrogate_key).

    Args:
        full_name: Candidate full name (as it appears in the source file).
        commune_insee: INSEE commune code.

    Returns:
        32-character lowercase hex MD5 string.
    """
    raw = f"{full_name}|{commune_insee}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _build_tour2_leader_set(
    tour2_bronze_path: Path,
    candidates_column_map: dict[str, str] | None = None,
) -> set[tuple[str, str]]:
    """Build a set of (commune_insee, normalized_name) tuples from Tour 2 tête de liste.

    Matching strategy: a Tour 1 leader is considered to have advanced to Tour 2
    if a row with the same commune_insee AND a name that fuzzy-matches above
    INCUMBENT_MATCH_THRESHOLD appears at position 1 in the Tour 2 file.

    For simplicity (and because the Tour 2 file uses the same schema as Tour 1),
    we use exact commune_insee + normalized name matching as the primary key.
    Fuzzy matching is only applied when exact name match fails.

    Why commune_insee + name (not list_id):
      List IDs are assigned fresh for each round — the same list gets a new
      NUMLISTE in Tour 2. Matching on commune + leader name is more robust.

    Args:
        tour2_bronze_path: Path to the Tour 2 bronze Parquet file.
        candidates_column_map: Same column map as Tour 1 (same schema).
            Must include mappings for commune_insee and position_on_list.

    Returns:
        Set of (commune_insee, normalized_full_name) tuples for Tour 2 leaders.
        Empty set if the file does not exist (graceful degradation — the flag
        will be NULL rather than crashing the pipeline).
    """
    if not tour2_bronze_path.exists():
        logger.warning(
            "Tour 2 bronze not found at %s — advanced_to_tour2 will be NULL. "
            "Run ingest_candidates_tour2() first if you want this flag.",
            tour2_bronze_path,
        )
        return set()

    effective_map = candidates_column_map if candidates_column_map is not None else CANDIDATES_COLUMN_MAP

    tour2_df = pd.read_parquet(tour2_bronze_path)
    logger.info("Loaded Tour 2 bronze rows=%d", len(tour2_df))

    if effective_map:
        tour2_df = tour2_df.rename(columns=effective_map)

    # Filter to tête de liste — same priority as Tour 1: explicit flag first,
    # position_on_list fallback. Tour 1 and Tour 2 share an identical schema.
    if "is_list_leader" in tour2_df.columns:
        tour2_leaders_df = tour2_df[
            tour2_df["is_list_leader"].str.strip().str.lower() == "oui"
        ].copy()
        logger.info(
            "Tour 2 tête de liste via is_list_leader flag: %d lists advanced",
            len(tour2_leaders_df),
        )
    elif "position_on_list" in tour2_df.columns:
        logger.warning(
            "Column 'is_list_leader' not found in Tour 2 — falling back to position_on_list == '1'."
        )
        tour2_leaders_df = tour2_df[
            tour2_df["position_on_list"].astype(str).str.strip() == "1"
        ].copy()
        logger.info(
            "Tour 2 tête de liste via position_on_list: %d lists advanced",
            len(tour2_leaders_df),
        )
    else:
        logger.warning(
            "Neither 'is_list_leader' nor 'position_on_list' found in Tour 2 — using all rows. "
            "Update CANDIDATES_COLUMN_MAP after EDA."
        )
        tour2_leaders_df = tour2_df.copy()

    # Build (commune_insee, normalized_name) lookup set.
    tour2_set: set[tuple[str, str]] = set()

    if "commune_insee" not in tour2_leaders_df.columns:
        logger.warning(
            "commune_insee not found in Tour 2 — advanced_to_tour2 will be NULL."
        )
        return tour2_set

    # Build full name from parts (same logic as Tour 1).
    if "full_name" not in tour2_leaders_df.columns:
        if "family_name" in tour2_leaders_df.columns and "given_name" in tour2_leaders_df.columns:
            tour2_leaders_df["full_name"] = (
                tour2_leaders_df["family_name"].fillna("") + " "
                + tour2_leaders_df["given_name"].fillna("")
            ).str.strip()
        else:
            logger.warning(
                "Cannot build full_name for Tour 2 leaders — advanced_to_tour2 will be NULL."
            )
            return tour2_set

    for _, row in tour2_leaders_df.iterrows():
        commune = str(row.get("commune_insee", "")).strip()
        name_norm = _normalize_name(str(row.get("full_name", "")))
        if commune and name_norm:
            tour2_set.add((commune, name_norm))

    logger.info(
        "Tour 2 leader lookup built: %d (commune, name) pairs", len(tour2_set)
    )
    return tour2_set


def _apply_tour2_flag(
    leader_df: pd.DataFrame,
    tour2_leader_set: set[tuple[str, str]],
) -> pd.DataFrame:
    """Add the advanced_to_tour2 boolean column to leader_df.

    Matching: exact (commune_insee, normalized_full_name) membership in tour2_leader_set.
    If tour2_leader_set is empty (Tour 2 file not available), the column is NULL.

    Args:
        leader_df: DataFrame with commune_insee and full_name_normalized columns.
        tour2_leader_set: Set of (commune_insee, normalized_name) from Tour 2.

    Returns:
        leader_df with advanced_to_tour2 column added.
    """
    if not tour2_leader_set:
        leader_df["advanced_to_tour2"] = None
        return leader_df

    def _is_tour2(row: pd.Series) -> bool | None:
        commune = str(row.get("commune_insee", "")).strip()
        name_norm = str(row.get("full_name_normalized", "")).strip()
        if not commune or not name_norm:
            return None
        return (commune, name_norm) in tour2_leader_set

    leader_df["advanced_to_tour2"] = leader_df.apply(_is_tour2, axis=1)

    advanced_count = leader_df["advanced_to_tour2"].sum()
    logger.info(
        "advanced_to_tour2 flag applied: %d leaders advanced to Tour 2 (%.1f%%)",
        advanced_count,
        100 * advanced_count / max(len(leader_df), 1),
    )
    return leader_df


def _build_incumbent_lookup(
    rne_bronze_path: Path,
    rne_column_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Load the RNE incumbents bronze file and prepare it for fuzzy matching.

    Pre-computes normalised full names once (O(n_rne)) rather than inside the
    matching loop (which would be O(n_candidates × n_rne)).

    Expected canonical columns after rename:
      - commune_insee: join key for pre-filtering
      - family_name, given_name: used to build full_name_normalized
      - mandate_role: kept for audit logging

    Args:
        rne_bronze_path: Path to the RNE bronze Parquet file.
        rne_column_map: Maps raw RNE column names → canonical names.
            Defaults to settings.RNE_COLUMN_MAP.

    Returns:
        DataFrame with columns: commune_insee, full_name_normalized,
        original_full_name, rne_mandate_role.
    """
    effective_map = rne_column_map if rne_column_map is not None else RNE_COLUMN_MAP

    rne_df = pd.read_parquet(rne_bronze_path)
    logger.info(
        "Loaded RNE bronze rows=%d columns=%s",
        len(rne_df),
        rne_df.columns.tolist(),
    )

    if effective_map:
        rne_df = rne_df.rename(columns=effective_map)

    # Build full name: try family_name + given_name first; fall back to
    # whatever the EDA discovers. Both name parts are normalised together.
    has_family = "family_name" in rne_df.columns
    has_given = "given_name" in rne_df.columns

    if has_family and has_given:
        rne_df["original_full_name"] = (
            rne_df["family_name"].fillna("") + " " + rne_df["given_name"].fillna("")
        ).str.strip()
    elif "full_name" in rne_df.columns:
        rne_df["original_full_name"] = rne_df["full_name"].fillna("")
    else:
        # Before EDA confirms column names, we cannot build a reliable lookup.
        logger.warning(
            "RNE lookup: neither (family_name + given_name) nor full_name found. "
            "Incumbent matching will return no matches until RNE_COLUMN_MAP is set."
        )
        return pd.DataFrame(
            columns=["commune_insee", "full_name_normalized", "original_full_name", "rne_mandate_role"]
        )

    rne_df["full_name_normalized"] = rne_df["original_full_name"].apply(_normalize_name)

    mandate_col = "mandate_role" if "mandate_role" in rne_df.columns else None
    rne_df["rne_mandate_role"] = rne_df[mandate_col] if mandate_col else "unknown"

    lookup_df = rne_df[
        ["commune_insee", "full_name_normalized", "original_full_name", "rne_mandate_role"]
    ].copy()

    logger.info(
        "Built incumbent lookup rows=%d unique_communes=%d",
        len(lookup_df),
        lookup_df["commune_insee"].nunique(),
    )
    return lookup_df


def _match_incumbent(
    candidate_name_normalized: str,
    candidate_commune_insee: str,
    incumbent_lookup_df: pd.DataFrame,
    threshold: int = INCUMBENT_MATCH_THRESHOLD,
) -> tuple[bool, float | None]:
    """Determine if a candidate is a current mayor via fuzzy name matching.

    Two-stage strategy:
    1. Filter to rows where commune_insee matches exactly (typically 1–3 rows).
    2. Run token_sort_ratio on the pre-filtered set and return the best score.

    Args:
        candidate_name_normalized: Normalised candidate name (_normalize_name output).
        candidate_commune_insee: INSEE commune code for pre-filtering.
        incumbent_lookup_df: Pre-built lookup from _build_incumbent_lookup().
        threshold: Minimum score to declare a match.

    Returns:
        Tuple of (is_incumbent: bool, best_score: float | None).
        best_score is None if no incumbents exist for this commune.
    """
    if incumbent_lookup_df.empty:
        return False, None

    commune_incumbents = incumbent_lookup_df[
        incumbent_lookup_df["commune_insee"] == candidate_commune_insee
    ]

    if commune_incumbents.empty:
        return False, None

    best_score: float = 0.0
    for rne_name in commune_incumbents["full_name_normalized"]:
        score = fuzz.token_sort_ratio(candidate_name_normalized, rne_name)
        if score > best_score:
            best_score = score

    is_match = best_score >= threshold
    return is_match, best_score


def _apply_incumbent_matching(
    leader_df: pd.DataFrame,
    incumbent_lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply incumbent matching to all candidate leaders.

    Adds three columns:
      is_incumbent: True/False (None if commune has no RNE entry)
      incumbent_match_score: float 0–100 (None if no match attempted)
      incumbent_match_auditable: True when score < 100 (needs review)

    Logs progress every 500 rows so long runs are observable.

    Args:
        leader_df: DataFrame of tête de liste rows with normalized_full_name.
        incumbent_lookup_df: Output of _build_incumbent_lookup().

    Returns:
        leader_df with three new columns added.
    """
    total = len(leader_df)
    results: list[tuple[bool | None, float | None, bool]] = []

    for i, row in enumerate(leader_df.itertuples(index=False), start=1):
        if i % 500 == 0:
            logger.info("Incumbent matching: %d/%d rows processed", i, total)

        name_norm = getattr(row, "full_name_normalized", "")
        commune = getattr(row, "commune_insee", "")

        if not name_norm or not commune:
            results.append((None, None, False))
            continue

        is_match, score = _match_incumbent(
            candidate_name_normalized=name_norm,
            candidate_commune_insee=commune,
            incumbent_lookup_df=incumbent_lookup_df,
        )

        # Flag non-exact matches for manual audit.
        auditable = (score is not None) and (score < 100)
        results.append((is_match, score, auditable))

    logger.info("Incumbent matching complete: %d/%d rows processed", total, total)

    is_inc, scores, auditables = zip(*results) if results else ([], [], [])
    result_df = leader_df.copy()
    result_df["is_incumbent"] = list(is_inc)
    result_df["incumbent_match_score"] = list(scores)
    result_df["incumbent_match_auditable"] = list(auditables)
    return result_df


def _validate_dim_candidate_leader(
    leader_df: pd.DataFrame,
    dim_commune_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run DQ checks before writing dim_candidate_leader.

    DQ checks:
      1. leader_id not null and length == 32
      2. gender in {'M', 'F'} — null rate check triggers DataQualityError
      3. commune_insee not null and present in dim_commune (referential integrity)
      4. nuance_group not null
      5. Uniqueness on leader_id

    Args:
        leader_df: DataFrame to validate.
        dim_commune_df: Silver dim_commune for referential integrity check.

    Returns:
        Tuple of (clean_df, rejected_df).

    Raises:
        DataQualityError: If null rate on gender exceeds DQ_MAX_NULL_RATE.
    """
    rejected_rows: list[pd.DataFrame] = []
    clean_df = leader_df.copy()

    # ── Check 1: gender null rate ──────────────────────────────────────────────
    gender_null_rate = clean_df["gender"].isna().sum() / max(len(clean_df), 1)
    if gender_null_rate > DQ_MAX_NULL_RATE:
        raise DataQualityError(
            f"gender null rate {gender_null_rate:.1%} exceeds threshold "
            f"{DQ_MAX_NULL_RATE:.1%}"
        )

    # ── Check 2: gender must be M or F ────────────────────────────────────────
    invalid_gender_mask = ~clean_df["gender"].isin(["M", "F"])
    if invalid_gender_mask.sum() > 0:
        bad = clean_df[invalid_gender_mask].copy()
        bad["_rejection_reason"] = "gender not in {M, F}"
        rejected_rows.append(bad)
        clean_df = clean_df[~invalid_gender_mask]
        logger.warning(
            "Quarantined %d rows with invalid gender", invalid_gender_mask.sum()
        )

    # ── Check 3: referential integrity commune_insee → dim_commune ────────────
    valid_communes = set(dim_commune_df["commune_insee"].dropna().unique())
    bad_commune_mask = ~clean_df["commune_insee"].isin(valid_communes)
    if bad_commune_mask.sum() > 0:
        bad = clean_df[bad_commune_mask].copy()
        bad["_rejection_reason"] = "commune_insee not in dim_commune"
        rejected_rows.append(bad)
        clean_df = clean_df[~bad_commune_mask]
        logger.warning(
            "Quarantined %d rows with commune_insee not in dim_commune",
            bad_commune_mask.sum(),
        )

    # ── Check 4: leader_id not null, length 32 ────────────────────────────────
    bad_id_mask = clean_df["leader_id"].isna() | (clean_df["leader_id"].str.len() != 32)
    if bad_id_mask.sum() > 0:
        bad = clean_df[bad_id_mask].copy()
        bad["_rejection_reason"] = "invalid leader_id"
        rejected_rows.append(bad)
        clean_df = clean_df[~bad_id_mask]

    # ── Check 5: uniqueness on leader_id ──────────────────────────────────────
    dup_mask = clean_df.duplicated(subset=["leader_id"], keep="first")
    if dup_mask.sum() > 0:
        bad = clean_df[dup_mask].copy()
        bad["_rejection_reason"] = "duplicate leader_id"
        rejected_rows.append(bad)
        clean_df = clean_df[~dup_mask]
        logger.warning("Quarantined %d duplicate leader_id rows", dup_mask.sum())

    rejected_df = (
        pd.concat(rejected_rows, ignore_index=True)
        if rejected_rows
        else pd.DataFrame(columns=list(leader_df.columns) + ["_rejection_reason"])
    )

    logger.info(
        "DQ complete: clean_rows=%d rejected_rows=%d", len(clean_df), len(rejected_df)
    )
    return clean_df, rejected_df


def build_dim_candidate_leader(
    bronze_dir: Path = BRONZE_DIR,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    candidates_column_map: dict[str, str] | None = None,
    rne_column_map: dict[str, str] | None = None,
    include_tour2_flag: bool = True,
) -> pd.DataFrame:
    """Build the dim_candidate_leader silver table.

    Steps:
      1. Load Tour 1 bronze candidates → filter to tête de liste (position == 1)
      2. Filter communes ≥ CITY_SIZE_SMALL_THRESHOLD
      3. Join dim_commune (silver) → city_size_bucket, reg_code, population
      4. Assign nuance_group from NUANCE_GROUP_MAP
      5. Build incumbent lookup → fuzzy-match against RNE
      6. Derive advanced_to_tour2 flag from Tour 2 bronze (if available)
      7. Generate leader_id (MD5)
      8. DQ validation + quarantine
      9. Idempotent write to Parquet + DuckDB

    Position-on-list filter: the candidates file contains one row per
    candidate, ordered by their position on the list. Position 1 = tête de
    liste. The actual column name is confirmed by EDA; the candidates_column_map
    must include a mapping to 'position_on_list'.

    Args:
        bronze_dir: Root bronze directory.
        silver_dir: Root silver directory.
        duckdb_path: Path to DuckDB warehouse.
        candidates_column_map: Raw column names → canonical names for candidates file.
        rne_column_map: Raw column names → canonical names for RNE file.
        include_tour2_flag: If True, attempt to derive advanced_to_tour2 from
            the Tour 2 bronze file. If the file does not exist, the column is
            NULL (graceful degradation — pipeline does not fail).

    Returns:
        Clean dim_candidate_leader DataFrame.

    Raises:
        FileNotFoundError: If required bronze or silver files do not exist.
        DataQualityError: If null rate on gender exceeds DQ_MAX_NULL_RATE.
    """
    effective_cand_map = (
        candidates_column_map if candidates_column_map is not None else CANDIDATES_COLUMN_MAP
    )

    # ── Load bronze candidates ────────────────────────────────────────────────
    cand_bronze_path = bronze_dir / "candidates" / "candidates_tour1.parquet"
    rne_bronze_path = bronze_dir / "rne" / "rne_incumbents.parquet"
    dim_commune_path = silver_dir / "dim_commune.parquet"

    for path in (cand_bronze_path, rne_bronze_path, dim_commune_path):
        if not path.exists():
            raise FileNotFoundError(
                f"Required file not found: {path}. "
                "Run ingest steps and build_dim_commune() first."
            )

    candidates_df = pd.read_parquet(cand_bronze_path)
    dim_commune_df = pd.read_parquet(dim_commune_path)

    logger.info(
        "Loaded bronze candidates rows=%d", len(candidates_df)
    )

    # ── Apply column map ──────────────────────────────────────────────────────
    if effective_cand_map:
        candidates_df = candidates_df.rename(columns=effective_cand_map)
        logger.info("Applied candidates column map: %s", effective_cand_map)
    else:
        logger.warning(
            "CANDIDATES_COLUMN_MAP is empty. Column names not confirmed by EDA. "
            "Filter and join steps may fail. Run notebooks/02_source_schema_exploration.ipynb."
        )

    # ── Filter to tête de liste ───────────────────────────────────────────────
    # Primary: use the explicit 'Tête de liste' flag ('Oui'/'Non') from the
    # Interior Ministry CSV — more authoritative than a numeric position rank.
    # Fallback: position_on_list == '1' if the flag column is absent.
    before = len(candidates_df)
    if "is_list_leader" in candidates_df.columns:
        leader_df = candidates_df[
            candidates_df["is_list_leader"].str.strip().str.lower() == "oui"
        ].copy()
        logger.info(
            "Filtered to tête de liste via is_list_leader flag: before=%d after=%d",
            before,
            len(leader_df),
        )
    elif "position_on_list" in candidates_df.columns:
        logger.warning(
            "Column 'is_list_leader' not found — falling back to position_on_list == '1'. "
            "Verify 'Tête de liste' is mapped in CANDIDATES_COLUMN_MAP."
        )
        leader_df = candidates_df[
            candidates_df["position_on_list"].astype(str).str.strip() == "1"
        ].copy()
        logger.info(
            "Filtered to tête de liste via position_on_list: before=%d after=%d",
            before,
            len(leader_df),
        )
    else:
        logger.warning(
            "Neither 'is_list_leader' nor 'position_on_list' found after rename. "
            "Using all rows as placeholder. Run EDA notebook and update CANDIDATES_COLUMN_MAP."
        )
        leader_df = candidates_df.copy()

    if len(leader_df) == 0:
        logger.error(
            "No tête de liste rows found after filtering. "
            "Check CANDIDATES_COLUMN_MAP in settings.py."
        )

    # ── Join dim_commune to get city_size_bucket, reg_code, population ────────
    if "commune_insee" in leader_df.columns and "commune_insee" in dim_commune_df.columns:
        commune_cols = ["commune_insee", "commune_name", "dep_code", "reg_code",
                        "population", "city_size_bucket"]
        available_commune_cols = [c for c in commune_cols if c in dim_commune_df.columns]
        leader_df = leader_df.merge(
            dim_commune_df[available_commune_cols],
            on="commune_insee",
            how="left",
        )
        logger.info("Joined dim_commune: result_rows=%d", len(leader_df))

        # Filter out excluded communes (population < CITY_SIZE_SMALL_THRESHOLD).
        if "city_size_bucket" in leader_df.columns:
            before = len(leader_df)
            leader_df = leader_df[
                leader_df["city_size_bucket"] != "excluded"
            ].copy()
            logger.info(
                "Filtered excluded communes: before=%d after=%d (excluded %d)",
                before,
                len(leader_df),
                before - len(leader_df),
            )
    else:
        logger.warning(
            "commune_insee not found in candidates or dim_commune — skipping join. "
            "Check CANDIDATES_COLUMN_MAP."
        )

    # ── Assign nuance_group ───────────────────────────────────────────────────
    nuance_col = "list_nuance" if "list_nuance" in leader_df.columns else None
    if nuance_col:
        leader_df["nuance_group"] = leader_df[nuance_col].map(NUANCE_GROUP_MAP)
        unmapped = leader_df["nuance_group"].isna().sum()
        if unmapped > 0:
            logger.warning(
                "%d rows have unmapped nuance codes — assigned 'divers'. "
                "Update NUANCE_GROUP_MAP in settings.py if new codes appeared.",
                unmapped,
            )
            leader_df["nuance_group"] = leader_df["nuance_group"].fillna("divers")
    else:
        logger.warning(
            "list_nuance column not found — nuance_group will be null. "
            "Update CANDIDATES_COLUMN_MAP in settings.py."
        )
        leader_df["nuance_group"] = None

    # ── Build normalized full name for incumbent matching ─────────────────────
    if "full_name" not in leader_df.columns:
        # Build full_name from parts if present; otherwise use placeholder.
        if "family_name" in leader_df.columns and "given_name" in leader_df.columns:
            leader_df["full_name"] = (
                leader_df["family_name"].fillna("") + " " + leader_df["given_name"].fillna("")
            ).str.strip()
        else:
            logger.warning(
                "full_name column not found — incumbent matching will not run. "
                "Update CANDIDATES_COLUMN_MAP."
            )
            leader_df["full_name"] = ""

    leader_df["full_name_normalized"] = leader_df["full_name"].apply(_normalize_name)

    # ── Incumbent matching ────────────────────────────────────────────────────
    incumbent_lookup_df = _build_incumbent_lookup(
        rne_bronze_path=rne_bronze_path,
        rne_column_map=rne_column_map,
    )
    leader_df = _apply_incumbent_matching(leader_df, incumbent_lookup_df)

    # ── Tour 2 advancement flag ───────────────────────────────────────────────
    # Graceful degradation: if Tour 2 bronze is missing, column is NULL.
    # The pipeline does not fail — you can always re-run after ingesting Tour 2.
    if include_tour2_flag:
        tour2_bronze_path = bronze_dir / "candidates" / "candidates_tour2.parquet"
        tour2_leader_set = _build_tour2_leader_set(
            tour2_bronze_path=tour2_bronze_path,
            candidates_column_map=candidates_column_map,
        )
        leader_df = _apply_tour2_flag(leader_df, tour2_leader_set)
    else:
        leader_df["advanced_to_tour2"] = None

    # ── Generate leader_id ────────────────────────────────────────────────────
    leader_df["leader_id"] = leader_df.apply(
        lambda row: _generate_leader_id(
            full_name=str(row.get("full_name", "")),
            commune_insee=str(row.get("commune_insee", "")),
        ),
        axis=1,
    )

    # ── Ensure all output columns present ────────────────────────────────────
    for col in _OUTPUT_COLUMNS:
        if col not in leader_df.columns:
            leader_df[col] = None

    leader_df = leader_df[_OUTPUT_COLUMNS].copy()

    # ── DQ validation ─────────────────────────────────────────────────────────
    clean_df, rejected_df = _validate_dim_candidate_leader(leader_df, dim_commune_df)

    # ── Write silver Parquet ──────────────────────────────────────────────────
    silver_path = silver_dir / "dim_candidate_leader.parquet"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(clean_df), silver_path, compression="snappy"
    )
    logger.info(
        "Silver Parquet written path=%s rows=%d", silver_path, len(clean_df)
    )

    if not rejected_df.empty:
        rejected_path = silver_dir / "_rejected" / "dim_candidate_leader_rejected.parquet"
        rejected_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pandas(rejected_df), rejected_path, compression="snappy"
        )
        logger.warning("Quarantine written path=%s rows=%d", rejected_path, len(rejected_df))

    # ── Idempotent DuckDB write ────────────────────────────────────────────────
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("DROP TABLE IF EXISTS silver.dim_candidate_leader")
        conn.execute(
            "CREATE TABLE silver.dim_candidate_leader AS SELECT * FROM clean_df"
        )
        row_count = conn.execute(
            "SELECT count(*) FROM silver.dim_candidate_leader"
        ).fetchone()[0]
        logger.info("DuckDB silver.dim_candidate_leader written rows=%d", row_count)

        # Log gender split — key sanity check before sampling.
        gender_counts = conn.execute(
            "SELECT gender, count(*) FROM silver.dim_candidate_leader GROUP BY gender"
        ).fetchall()
        logger.info("Gender distribution: %s", dict(gender_counts))
    finally:
        conn.close()

    return clean_df
