"""Tests for src/transform/dim_commune.py, src/transform/dim_candidate.py,
and src/transform/sampling.py.

Regression tests encode the bug they prevent in their name. Schema tests
verify layer contracts (silver stays lean; gold is self-contained).

Hermetic design:
- No network calls, no GPU, no writes to real data directories.
- build_dim_commune tests write Parquets to pytest's tmp_path fixture (auto-cleaned).
- Private functions (_normalize_name, _match_incumbent, _apply_tour2_flag) are
  imported directly — testing private functions from the same package is standard
  Python practice when the logic is critical and the function is a shared utility.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.transform._exceptions import DataQualityError
from src.transform.dim_candidate import (
    _OUTPUT_COLUMNS,
    _apply_tour2_flag,
    _compute_same_name_candidate_counts,
    _match_incumbent,
    _normalize_list_nuance_code,
    _normalize_name,
)
from src.transform.dim_commune import build_dim_commune
from src.transform.sampling import build_sample


def test_normalize_list_nuance_code_strips_leading_list_prefix():
    """Regression: official list-level nuance codes include an ``L`` prefix.

    The candidate source uses codes such as LDVG and LRN, while
    NUANCE_GROUP_MAP is keyed by the base nuance code (DVG, RN).
    Without this normalization every row misses the map and is backfilled
    to ``divers``, which corrupts the political-bloc stratification.
    """
    assert _normalize_list_nuance_code("LDVG") == "DVG"
    assert _normalize_list_nuance_code("LRN") == "RN"
    assert _normalize_list_nuance_code("DVG") == "DVG"
    assert _normalize_list_nuance_code(None) == ""


# ── Helpers ───────────────────────────────────────────────────────────────────


def _write_parquet(df: pd.DataFrame, path) -> None:
    """Write a DataFrame to Parquet, creating parent directories as needed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df), path, compression="snappy")


# Minimal column maps using the actual source column names confirmed by EDA.
# Passing these explicitly keeps tests independent of settings.py defaults,
# so a settings change cannot silently break tests.
_COG_MAP = {
    "COM": "commune_insee",
    "LIBELLE": "commune_name",
    "DEP": "dep_code",
    "REG": "reg_code",
    "TYPECOM": "typecom",
}
_SEATS_MAP = {
    "CODE_COMMUNE": "commune_insee",
    "LIB_COMMUNE": "commune_name",  # deliberate overlap with COG — exercises the fix
    "CODE_DPT": "dep_code",  # deliberate overlap with COG — exercises the fix
    "LIB_DPT": "dep_name",
    "POPULATION": "population",
    "NBRE_SAP_COM": "seats_municipal",
    "NBRE_SAP_EPCI": "seats_epci",
}


@pytest.fixture
def bronze_commune_parquets(tmp_path):
    """Write minimal COG + seats bronze Parquets to a temp bronze directory.

    The seats fixture deliberately includes LIB_COMMUNE and CODE_DPT — columns
    that also exist in COG after rename. This is the exact condition that
    triggered the commune_name / dep_code → None bug before the fix.
    """
    cog_df = pd.DataFrame(
        {
            "COM": ["75056", "69123"],
            "LIBELLE": ["Paris", "Lyon"],
            "DEP": ["75", "69"],
            "REG": ["11", "84"],
            "TYPECOM": ["COM", "COM"],
            "_source_url": ["http://example.com", "http://example.com"],
            "_ingested_at": ["2026-01-01", "2026-01-01"],
            "_source_hash": ["aaa", "aaa"],
        }
    )
    seats_df = pd.DataFrame(
        {
            "CODE_COMMUNE": ["75056", "69123"],
            "LIB_COMMUNE": [
                "Paris",
                "Lyon",
            ],  # overlaps with COG.LIBELLE → commune_name
            "CODE_DPT": ["75", "69"],  # overlaps with COG.DEP → dep_code
            "LIB_DPT": ["Paris dept", "Rhône"],
            "POPULATION": ["2161000", "522000"],
            "NBRE_SAP_COM": ["163", "73"],
            "NBRE_SAP_EPCI": ["0", "44"],
            "_source_url": ["http://example.com", "http://example.com"],
            "_ingested_at": ["2026-01-01", "2026-01-01"],
            "_source_hash": ["bbb", "bbb"],
        }
    )
    bronze_dir = tmp_path / "bronze"
    _write_parquet(cog_df, bronze_dir / "geography" / "cog_communes.parquet")
    _write_parquet(seats_df, bronze_dir / "seats" / "seats_population.parquet")
    return bronze_dir


# ── _normalize_name ───────────────────────────────────────────────────────────


def test_normalize_name_strips_hyphens():
    """Regression: JEAN-LUC was not normalised to JEAN LUC before the fix.

    token_sort_ratio("ANNE-SOPHIE MARTIN", "ANNE SOPHIE MARTIN") scored ~61
    because after alphabetical token sort the strings diverge: "ANNE-SOPHIE MARTIN"
    vs "ANNE MARTIN SOPHIE". After replacing hyphens with spaces both tokenise
    identically and score 100.
    """
    assert _normalize_name("JEAN-LUC") == "JEAN LUC"


def test_normalize_name_strips_apostrophe():
    """Regression: O'BRIEN was not normalised to O BRIEN before the fix.

    Apostrophes (including the typographic variant U+2019) appear in French
    names like D'Alembert. Without unification, the RNE and candidate files
    could use different apostrophe characters, causing missed matches.
    """
    assert _normalize_name("O'BRIEN") == "O BRIEN"
    assert _normalize_name("O\u2019BRIEN") == "O BRIEN"  # typographic apostrophe


def test_normalize_name_handles_none():
    """Boundary: None input must return empty string, not raise."""
    assert _normalize_name(None) == ""  # type: ignore[arg-type]


def test_normalize_name_strips_accents():
    """Happy path: accented French characters are stripped to ASCII equivalents."""
    assert _normalize_name("Élise") == "ELISE"
    assert _normalize_name("François") == "FRANCOIS"


# ── _match_incumbent ──────────────────────────────────────────────────────────


def test_match_incumbent_hyphenated_name_above_threshold():
    """Regression: Anne-Sophie vs Anne Sophie scored ~61 (below threshold 85) before fix.

    Both names go through _normalize_name before matching. After the hyphen fix,
    both normalise to "ANNE SOPHIE MARTIN" and score 100.
    """
    rne_normalized = _normalize_name(
        "Anne-Sophie Martin"
    )  # was "ANNE-SOPHIE MARTIN"; now "ANNE SOPHIE MARTIN"
    lookup_df = pd.DataFrame(
        {
            "commune_insee": ["01001"],
            "full_name_normalized": [rne_normalized],
            "original_full_name": ["Anne-Sophie Martin"],
            "rne_mandate_role": ["Maire"],
        }
    )
    candidate_normalized = _normalize_name("Anne Sophie Martin")  # "ANNE SOPHIE MARTIN"

    is_match, score = _match_incumbent(candidate_normalized, "01001", lookup_df)

    assert is_match is True, (
        f"Expected is_incumbent=True for 'Anne Sophie Martin' vs RNE 'Anne-Sophie Martin', "
        f"got score={score}"
    )
    assert score is not None and score >= 85


# ── _apply_tour2_flag ─────────────────────────────────────────────────────────


def test_apply_tour2_flag_matches_hyphenated_variant():
    """Regression: Tour 2 'ANNE-SOPHIE MARTIN' missed Tour 1 'ANNE SOPHIE MARTIN' before fix.

    _build_tour2_leader_set uses _normalize_name on Tour 2 names.
    _apply_tour2_flag checks (commune, full_name_normalized) set membership.
    Before the fix, "ANNE-SOPHIE MARTIN" ≠ "ANNE SOPHIE MARTIN" as strings.
    After the fix, both produce "ANNE SOPHIE MARTIN" and the lookup succeeds.
    """
    # Simulate Tour 2 set as built by _build_tour2_leader_set
    tour2_set = {("75056", _normalize_name("Anne-Sophie Martin"))}

    leader_df = pd.DataFrame(
        {
            "commune_insee": ["75056"],
            # Tour 1 name without hyphen — as built by build_dim_candidate_leader
            "full_name_normalized": [_normalize_name("Anne Sophie Martin")],
        }
    )

    result_df = _apply_tour2_flag(leader_df, tour2_set)

    assert result_df.loc[0, "advanced_to_tour2"] == True, (  # noqa: E712
        "Tour 1 leader 'Anne Sophie Martin' should match Tour 2 'Anne-Sophie Martin' "
        "in the same commune after hyphen normalisation"
    )


# ── dim_candidate _OUTPUT_COLUMNS ─────────────────────────────────────────────


def test_dim_candidate_output_columns_match_data_model():
    """Regression: _OUTPUT_COLUMNS previously included commune_name, dep_code, population.

    Per data-model.md §dim_candidate_leader, these three columns are intentionally
    excluded — they are fully derivable via JOIN dim_commune. Including them caused
    the merge suffix bug (commune_name became None after join with dim_commune).
    The denormalised columns required for sampling and regression must still be present.
    """
    for forbidden in ("commune_name", "dep_code", "population"):
        assert (
            forbidden not in _OUTPUT_COLUMNS
        ), f"'{forbidden}' must not be in _OUTPUT_COLUMNS — see data-model.md Gap Analysis"

    for required in ("city_size_bucket", "reg_code"):
        assert (
            required in _OUTPUT_COLUMNS
        ), f"'{required}' must be in _OUTPUT_COLUMNS — it is a direct regression input"

    assert "same_name_candidate_count" in _OUTPUT_COLUMNS, (
        "'same_name_candidate_count' must be in _OUTPUT_COLUMNS — "
        "sampling priority depends on this auditable collision metric"
    )


def test_same_name_candidate_count_uses_normalized_full_name():
    """Regression: hyphen and accent variants must collapse to one collision count.

    The sampling step prefers lower same_name_candidate_count values. If the
    count logic treats "JEAN-LUC DUPONT" and "JEAN LUC DUPONT" as different
    names, we understate ambiguity and bias the prioritisation.
    """
    candidate_df = pd.DataFrame(
        {
            "family_name": ["Dupont", "Dupont", "Durand"],
            "given_name": ["Jean-Luc", "Jean Luc", "Marie"],
        }
    )

    result_df = _compute_same_name_candidate_counts(candidate_df)

    assert result_df.loc[0, "same_name_candidate_count"] == 2
    assert result_df.loc[1, "same_name_candidate_count"] == 2
    assert result_df.loc[2, "same_name_candidate_count"] == 1


# ── build_dim_commune ─────────────────────────────────────────────────────────


def test_build_dim_commune_commune_name_not_none_after_merge(
    bronze_commune_parquets, tmp_path
):
    """Regression: commune_name was None when seats also had a LIB_COMMUNE column.

    Without the fix, the merge produced commune_name_x/commune_name_y suffixes.
    The _OUTPUT_COLUMNS guard then found neither and filled commune_name with None.
    """
    result_df = build_dim_commune(
        bronze_dir=bronze_commune_parquets,
        silver_dir=tmp_path / "silver",
        duckdb_path=tmp_path / "warehouse.duckdb",
        cog_column_map=_COG_MAP,
        seats_column_map=_SEATS_MAP,
    )
    assert result_df["commune_name"].notna().all(), (
        "commune_name must not be None after merge — "
        "COG is authoritative and must not be overridden by seats suffixes"
    )


def test_build_dim_commune_dep_code_not_none_after_merge(
    bronze_commune_parquets, tmp_path
):
    """Regression: dep_code was None for the same suffix-collision reason as commune_name."""
    result_df = build_dim_commune(
        bronze_dir=bronze_commune_parquets,
        silver_dir=tmp_path / "silver",
        duckdb_path=tmp_path / "warehouse.duckdb",
        cog_column_map=_COG_MAP,
        seats_column_map=_SEATS_MAP,
    )
    assert (
        result_df["dep_code"].notna().all()
    ), "dep_code must not be None — suffix collision with seats was not handled"


def test_build_dim_commune_no_suffix_columns_in_output(
    bronze_commune_parquets, tmp_path
):
    """Regression: merge must not leak _x/_y suffix columns into the output DataFrame."""
    result_df = build_dim_commune(
        bronze_dir=bronze_commune_parquets,
        silver_dir=tmp_path / "silver",
        duckdb_path=tmp_path / "warehouse.duckdb",
        cog_column_map=_COG_MAP,
        seats_column_map=_SEATS_MAP,
    )
    suffix_cols = [c for c in result_df.columns if c.endswith("_x") or c.endswith("_y")]
    assert (
        not suffix_cols
    ), f"Unexpected suffix columns leaked into output: {suffix_cols}"


def test_build_dim_commune_raises_on_missing_seats_join_key(
    bronze_commune_parquets, tmp_path
):
    """Regression: empty seats_column_map must raise DataQualityError, not silently continue.

    Before the fix, this set city_size_bucket='excluded' for ALL communes —
    the pipeline appeared to succeed while all downstream sampling was broken.
    """
    with pytest.raises(DataQualityError, match="seats join failed"):
        build_dim_commune(
            bronze_dir=bronze_commune_parquets,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
            cog_column_map=_COG_MAP,
            seats_column_map={},  # empty map → no rename → commune_insee absent → must fail
        )


# ── build_sample (gold schema) ────────────────────────────────────────────────

# Region codes: 4 distinct values to satisfy SAMPLE_MIN_REGION_COUNT=4.
_SAMPLE_REG_CODES = ["11", "84", "44", "24"]


def _make_leader_rows(bucket: str, n_per_gender: int, dep_prefix: str) -> list[dict]:
    """Build the minimum viable candidate rows for one city-size stratum.

    Candidates are assigned cycling reg_codes so the 24-person sample always
    contains ≥ 4 distinct regions without triggering geographic resampling.
    INSEE commune codes follow the standard format: 2-digit dep + 3-digit number.
    """
    rows = []
    idx = 0
    for gender in ["F", "M"]:
        for _j in range(n_per_gender):
            commune = f"{dep_prefix}{idx + 1:03d}"
            rows.append(
                {
                    "leader_id": f"{commune}{gender}",
                    "full_name": f"Leader {commune} {gender}",
                    "gender": gender,
                    "commune_insee": commune,
                    "city_size_bucket": bucket,
                    "reg_code": _SAMPLE_REG_CODES[idx % len(_SAMPLE_REG_CODES)],
                    "nuance_group": "divers",
                    "same_name_candidate_count": 1,
                    "list_nuance": "LDVC",
                    "is_incumbent": False,
                    "incumbent_match_score": 0.0,
                    "incumbent_match_auditable": True,
                    "advanced_to_tour2": False,
                }
            )
            idx += 1
    return rows


@pytest.fixture
def silver_parquets_for_sampling(tmp_path):
    """Write minimal dim_candidate_leader + dim_commune silver Parquets for sampling tests.

    Constructs exactly the minimum viable pool: 2F+2M large, 4F+4M medium,
    6F+6M small — matching settings.py quotas exactly. With pool size == quota
    per stratum, all candidates are selected deterministically, making the
    test output stable across random seeds.

    Commune codes:
      large:  75001–75004  (dep 75)
      medium: 69001–69008  (dep 69)
      small:  35001–35012  (dep 35)
    """
    silver_dir = tmp_path / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    leader_rows = (
        _make_leader_rows("large", 2, "75")
        + _make_leader_rows("medium", 4, "69")
        + _make_leader_rows("small", 6, "35")
    )
    leader_df = pd.DataFrame(leader_rows)
    _write_parquet(leader_df, silver_dir / "dim_candidate_leader.parquet")

    # dim_commune: one row per unique commune in the leader pool.
    # dep_code derived from the first two characters of the INSEE code,
    # matching the real INSEE encoding convention.
    population_by_bucket = {"large": 150_000, "medium": 50_000, "small": 10_000}
    commune_rows = [
        {
            "commune_insee": row["commune_insee"],
            "commune_name": f"Commune {row['commune_insee']}",
            "dep_code": row["commune_insee"][:2],
            "reg_code": row["reg_code"],
            "population": population_by_bucket[row["city_size_bucket"]],
            "city_size_bucket": row["city_size_bucket"],
            "seats_municipal": 63,
            "seats_epci": 44,
        }
        for row in leader_rows
    ]
    commune_df = pd.DataFrame(commune_rows).drop_duplicates(subset="commune_insee")
    _write_parquet(commune_df, silver_dir / "dim_commune.parquet")

    return silver_dir


def test_build_sample_gold_schema_includes_commune_name(
    silver_parquets_for_sampling, tmp_path
):
    """Happy path: commune_name must be present in gold sample.

    GDELT DOC 2.0 is a full-text search engine. News articles contain "Rennes",
    not "35238". Without commune_name the news ingest module cannot build
    valid search queries and would return empty or unrelated results.
    """
    result_df = build_sample(
        silver_dir=silver_parquets_for_sampling,
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
        random_seed=42,
    )
    assert "commune_name" in result_df.columns, (
        "gold.sample_leaders must contain commune_name — "
        "GDELT text queries require the human-readable commune label"
    )


def test_build_sample_gold_schema_includes_dep_code(
    silver_parquets_for_sampling, tmp_path
):
    """Happy path: dep_code must be present in gold sample.

    France contains many same-name communes (e.g. multiple "Saint-Martin").
    dep_code narrows the GDELT search scope to the correct administrative area
    and is also used as a covariate in regression models.
    """
    result_df = build_sample(
        silver_dir=silver_parquets_for_sampling,
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
        random_seed=42,
    )
    assert "dep_code" in result_df.columns, (
        "gold.sample_leaders must contain dep_code — "
        "needed to disambiguate same-name communes in GDELT queries"
    )


def test_build_sample_commune_fields_are_non_null(
    silver_parquets_for_sampling, tmp_path
):
    """Boundary: commune_name and dep_code must be non-null for every sampled candidate.

    A null here means a sampled commune_insee has no match in dim_commune,
    which would silently produce an empty GDELT query string and zero articles
    for that candidate — corrupting the exposure metric.
    """
    result_df = build_sample(
        silver_dir=silver_parquets_for_sampling,
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
        random_seed=42,
    )
    null_commune_names = result_df["commune_name"].isna().sum()
    null_dep_codes = result_df["dep_code"].isna().sum()
    assert null_commune_names == 0, (
        f"commune_name has {null_commune_names} null values — "
        "all sampled communes must be present in dim_commune"
    )
    assert null_dep_codes == 0, (
        f"dep_code has {null_dep_codes} null values — "
        "all sampled communes must be present in dim_commune"
    )
