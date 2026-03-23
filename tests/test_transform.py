"""Regression tests for src/transform/dim_commune.py and src/transform/dim_candidate.py.

Every test corresponds to a confirmed bug found during code review. The test name
encodes the bug so future readers know what regression is being prevented.

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
    _normalize_name,
)
from src.transform.dim_commune import build_dim_commune

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
