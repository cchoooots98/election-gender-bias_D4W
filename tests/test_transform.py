"""Tests for src/transform/dim_commune.py, src/transform/dim_candidate.py,
and src/transform/sampling.py.

Regression tests encode the bug they prevent in their name. Schema tests
verify layer contracts (silver stays lean; gold is self-contained).

Hermetic design:
- No network calls, no GPU, no writes to real data directories.
- build_dim_commune tests write Parquets to pytest's tmp_path fixture (auto-cleaned).
- Private functions (_normalize_name, _match_incumbent, _apply_tour2_flag) are
  imported directly â€” testing private functions from the same package is standard
  Python practice when the logic is critical and the function is a shared utility.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.config.settings import (
    SAMPLE_LARGE_TOTAL,
    SAMPLE_MEDIUM_TOTAL,
    SAMPLE_SMALL_TOTAL,
)
from src.transform._exceptions import DataQualityError
from src.transform._leader_keys import build_full_name_columns
from src.transform.dim_candidate import (
    _OUTPUT_COLUMNS,
    _apply_incumbent_matching,
    _apply_tour2_flag,
    _build_tour2_leader_set,
    _compute_same_name_candidate_counts,
    _match_incumbent,
    _normalize_list_nuance_code,
    _normalize_name,
    build_dim_candidate_leader,
)
from src.transform.dim_commune import build_dim_commune
from src.transform.sampling import build_sample
from tests.sampling_builders import (
    build_candidate_and_commune_frames,
    build_candidate_universe_frame,
    write_parquet_frame,
)


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


def test_build_full_name_columns_raises_when_name_component_columns_missing():
    """Regression: full_name construction must fail fast when source columns are absent."""
    candidate_df = pd.DataFrame(
        {
            "commune_insee": ["75056"],
            "gender": ["F"],
        }
    )

    with pytest.raises(
        ValueError, match="Cannot build full_name without required columns"
    ):
        build_full_name_columns(candidate_df)


def test_build_full_name_columns_raises_when_name_components_are_blank():
    """Regression: blank family/given names must not silently collapse to empty full_name."""
    candidate_df = pd.DataFrame(
        {
            "family_name": ["DUPONT", ""],
            "given_name": ["Alice", "Bob"],
        }
    )

    with pytest.raises(
        ValueError,
        match="Cannot build full_name from blank family_name/given_name rows",
    ):
        build_full_name_columns(candidate_df)


# â”€â”€ Helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


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
    "LIB_COMMUNE": "commune_name",  # deliberate overlap with COG â€” exercises the fix
    "CODE_DPT": "dep_code",  # deliberate overlap with COG â€” exercises the fix
    "LIB_DPT": "dep_name",
    "POPULATION": "population",
    "NBRE_SAP_COM": "seats_municipal",
    "NBRE_SAP_EPCI": "seats_epci",
}
_LARGE_PER_GENDER = SAMPLE_LARGE_TOTAL // 2
_MEDIUM_PER_GENDER = SAMPLE_MEDIUM_TOTAL // 2
_SMALL_PER_GENDER = SAMPLE_SMALL_TOTAL // 2
_CANDIDATE_IDENTITY_MAP = {
    "full_name": "full_name",
    "family_name": "family_name",
    "given_name": "given_name",
    "gender": "gender",
    "commune_insee": "commune_insee",
    "is_list_leader": "is_list_leader",
    "position_on_list": "position_on_list",
    "list_nuance": "list_nuance",
}


def _write_dim_candidate_inputs(
    tmp_path,
    *,
    candidate_rows: list[dict[str, object]],
    dim_commune_rows: list[dict[str, object]] | None = None,
    rne_rows: list[dict[str, object]] | None = None,
) -> tuple:
    """Write the minimum viable inputs needed by build_dim_candidate_leader."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    _write_parquet(
        pd.DataFrame(candidate_rows),
        bronze_dir / "candidates" / "candidates_tour1.parquet",
    )
    _write_parquet(
        pd.DataFrame(rne_rows or []),
        bronze_dir / "rne" / "rne_incumbents.parquet",
    )
    _write_parquet(
        pd.DataFrame(
            dim_commune_rows
            or [
                {
                    "commune_insee": "01001",
                    "commune_name": "Commune 01001",
                    "dep_code": "01",
                    "reg_code": "84",
                    "population": 12_000,
                    "city_size_bucket": "small",
                }
            ]
        ),
        silver_dir / "dim_commune.parquet",
    )
    return bronze_dir, silver_dir, duckdb_path


@pytest.fixture
def bronze_commune_parquets(tmp_path):
    """Write minimal COG + seats bronze Parquets to a temp bronze directory.

    The seats fixture deliberately includes LIB_COMMUNE and CODE_DPT â€” columns
    that also exist in COG after rename. This is the exact condition that
    triggered the commune_name / dep_code â†’ None bug before the fix.
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
            ],  # overlaps with COG.LIBELLE â†’ commune_name
            "CODE_DPT": ["75", "69"],  # overlaps with COG.DEP â†’ dep_code
            "LIB_DPT": ["Paris dept", "RhÃ´ne"],
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


# â”€â”€ _normalize_name â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


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


# â”€â”€ _match_incumbent â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


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


def test_apply_incumbent_matching_preserves_non_contiguous_index_alignment():
    """Regression: nullable-boolean assignment must align to the leader index.

    After filtering the full candidate table, leader_df keeps the original row
    index. Assigning a default RangeIndex-backed Series caused pandas to align on
    mismatched labels, silently converting real True/False values into <NA>.
    """
    leader_df = pd.DataFrame(
        {
            "commune_insee": ["01001", "01002"],
            "full_name_normalized": [
                _normalize_name("Anne-Sophie Martin"),
                _normalize_name("Louis Durand"),
            ],
        },
        index=[101, 205],
    )
    lookup_df = pd.DataFrame(
        {
            "commune_insee": ["01001", "01002"],
            "full_name_normalized": [
                _normalize_name("Anne Sophie Martin"),
                _normalize_name("Someone Else"),
            ],
            "original_full_name": ["Anne Sophie Martin", "Someone Else"],
            "rne_mandate_role": ["Maire", "Maire"],
        }
    )

    result_df = _apply_incumbent_matching(leader_df, lookup_df)

    assert str(result_df["is_incumbent"].dtype) == "boolean"
    assert result_df.loc[101, "is_incumbent"] == True  # noqa: E712
    assert result_df.loc[205, "is_incumbent"] == False  # noqa: E712


# â”€â”€ _apply_tour2_flag â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_apply_tour2_flag_matches_hyphenated_variant():
    """Regression: Tour 2 'ANNE-SOPHIE MARTIN' missed Tour 1 'ANNE SOPHIE MARTIN' before fix.

    _build_tour2_leader_set uses _normalize_name on Tour 2 names.
    _apply_tour2_flag checks (commune, full_name_normalized) set membership.
    Before the fix, "ANNE-SOPHIE MARTIN" â‰  "ANNE SOPHIE MARTIN" as strings.
    After the fix, both produce "ANNE SOPHIE MARTIN" and the lookup succeeds.
    """
    # Simulate Tour 2 set as built by _build_tour2_leader_set
    tour2_set = {("75056", _normalize_name("Anne-Sophie Martin"))}

    leader_df = pd.DataFrame(
        {
            "commune_insee": ["75056"],
            # Tour 1 name without hyphen â€” as built by build_dim_candidate_leader
            "full_name_normalized": [_normalize_name("Anne Sophie Martin")],
        }
    )

    result_df = _apply_tour2_flag(leader_df, tour2_set)

    assert result_df.loc[0, "advanced_to_tour2"] == True, (  # noqa: E712
        "Tour 1 leader 'Anne Sophie Martin' should match Tour 2 'Anne-Sophie Martin' "
        "in the same commune after hyphen normalisation"
    )


def test_apply_tour2_flag_uses_nullable_boolean_and_preserves_missing_keys():
    """Boundary: missing commune or name keys must remain NULL, not false.

    The advanced_to_tour2 contract distinguishes "not in Tour 2" from
    "comparison could not be attempted". Using pandas' nullable boolean dtype
    preserves that difference while avoiding an object-typed column.
    """
    tour2_set = {("75056", _normalize_name("Anne-Sophie Martin"))}
    leader_df = pd.DataFrame(
        {
            "commune_insee": ["75056", "75057", "75058", ""],
            "full_name_normalized": [
                _normalize_name("Anne Sophie Martin"),
                _normalize_name("Marie Durand"),
                "",
                _normalize_name("Alice Martin"),
            ],
        }
    )

    result_df = _apply_tour2_flag(leader_df, tour2_set)

    assert str(result_df["advanced_to_tour2"].dtype) == "boolean"
    assert result_df.loc[0, "advanced_to_tour2"] == True  # noqa: E712
    assert result_df.loc[1, "advanced_to_tour2"] == False  # noqa: E712
    assert pd.isna(result_df.loc[2, "advanced_to_tour2"])
    assert pd.isna(result_df.loc[3, "advanced_to_tour2"])


def test_build_tour2_leader_set_deduplicates_and_skips_blank_keys(tmp_path):
    """Boundary: Tour 2 lookup should keep only valid unique (commune, name) pairs."""
    tour2_df = pd.DataFrame(
        {
            "is_list_leader": ["Oui", "Oui", "Oui", "Oui"],
            "commune_insee": ["75056", "75056", "75057", ""],
            "full_name": [
                "ANNE-SOPHIE MARTIN",
                "ANNE SOPHIE MARTIN",
                "",
                "ALICE MARTIN",
            ],
        }
    )
    bronze_path = tmp_path / "bronze" / "candidates_tour2.parquet"
    _write_parquet(tour2_df, bronze_path)

    tour2_set = _build_tour2_leader_set(bronze_path, candidates_column_map={})

    assert tour2_set == {("75056", _normalize_name("ANNE SOPHIE MARTIN"))}


# â”€â”€ dim_candidate _OUTPUT_COLUMNS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_dim_candidate_output_columns_match_data_model():
    """Regression: Silver dim_candidate_leader must stay Kimball-clean.

    Geography belongs in dim_commune and election outcomes belong in
    fact_election_result. Keeping those columns out of the candidate dimension
    prevents early denormalisation and forces the pre-join to happen once in
    gold.candidate_universe.
    """
    for forbidden in (
        "commune_name",
        "dep_code",
        "population",
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
    ):
        assert (
            forbidden not in _OUTPUT_COLUMNS
        ), f"'{forbidden}' must not be in _OUTPUT_COLUMNS â€” see data-model.md Gap Analysis"

    for required in (
        "commune_insee",
        "same_name_candidate_count",
        "list_nuance",
        "nuance_group",
        "is_incumbent",
        "incumbent_match_score",
        "advanced_to_tour2",
    ):
        assert (
            required in _OUTPUT_COLUMNS
        ), f"'{required}' must stay in _OUTPUT_COLUMNS â€” it is a candidate attribute"

    assert "same_name_candidate_count" in _OUTPUT_COLUMNS, (
        "'same_name_candidate_count' must be in _OUTPUT_COLUMNS â€” "
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


def test_same_name_candidate_count_ignores_blank_non_candidate_rows():
    """Regression: blank raw rows should not fail the ambiguity feature build."""
    candidate_df = pd.DataFrame(
        {
            "family_name": ["Dupont", "", None],
            "given_name": ["Jean-Luc", " ", None],
        }
    )

    result_df = _compute_same_name_candidate_counts(candidate_df)

    assert result_df.loc[0, "same_name_candidate_count"] == 1
    assert pd.isna(result_df.loc[1, "same_name_candidate_count"])
    assert pd.isna(result_df.loc[2, "same_name_candidate_count"])


# â”€â”€ build_dim_commune â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_build_dim_candidate_leader_returns_valid_row(tmp_path):
    """Happy path: build_dim_candidate_leader should emit a clean one-row dimension."""
    bronze_dir, silver_dir, duckdb_path = _write_dim_candidate_inputs(
        tmp_path,
        candidate_rows=[
            {
                "full_name": "DUPONT Alice",
                "family_name": "DUPONT",
                "given_name": "Alice",
                "gender": "F",
                "commune_insee": "01001",
                "is_list_leader": "Oui",
                "position_on_list": "1",
                "list_nuance": "DVG",
            }
        ],
    )

    result_df = build_dim_candidate_leader(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        candidates_column_map=_CANDIDATE_IDENTITY_MAP,
        rne_column_map={},
        include_tour2_flag=False,
    )

    assert len(result_df) == 1
    assert result_df.loc[0, "nuance_group"] == "gauche"
    assert "city_size_bucket" not in result_df.columns
    assert "score_tour1_rank" not in result_df.columns
    assert str(result_df["advanced_to_tour2"].dtype) == "boolean"
    assert pd.isna(result_df.loc[0, "advanced_to_tour2"])


def test_build_dim_candidate_leader_filters_out_excluded_communes_before_nuance_mapping(
    tmp_path,
):
    """Regression: excluded communes must not fail the build when their nuance is missing."""
    bronze_dir, silver_dir, duckdb_path = _write_dim_candidate_inputs(
        tmp_path,
        candidate_rows=[
            {
                "full_name": "DUPONT Alice",
                "family_name": "DUPONT",
                "given_name": "Alice",
                "gender": "F",
                "commune_insee": "01001",
                "is_list_leader": "Oui",
                "position_on_list": "1",
                "list_nuance": "DVG",
            },
            {
                "full_name": "MARTIN Bob",
                "family_name": "MARTIN",
                "given_name": "Bob",
                "gender": "M",
                "commune_insee": "01002",
                "is_list_leader": "Oui",
                "position_on_list": "1",
                "list_nuance": None,
            },
        ],
        dim_commune_rows=[
            {
                "commune_insee": "01001",
                "commune_name": "Eligible Commune",
                "dep_code": "01",
                "reg_code": "84",
                "population": 12_000,
                "city_size_bucket": "small",
            },
            {
                "commune_insee": "01002",
                "commune_name": "Excluded Commune",
                "dep_code": "01",
                "reg_code": "84",
                "population": 1_200,
                "city_size_bucket": "excluded",
            },
        ],
    )

    result_df = build_dim_candidate_leader(
        bronze_dir=bronze_dir,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        candidates_column_map=_CANDIDATE_IDENTITY_MAP,
        rne_column_map={},
        include_tour2_flag=False,
    )

    assert result_df["commune_insee"].tolist() == ["01001"]
    assert result_df["full_name"].tolist() == ["DUPONT Alice"]


def test_build_dim_candidate_leader_raises_when_leader_columns_missing(tmp_path):
    """Regression: missing leader-identification columns must fail fast."""
    bronze_dir, silver_dir, duckdb_path = _write_dim_candidate_inputs(
        tmp_path,
        candidate_rows=[
            {
                "full_name": "DUPONT Alice",
                "family_name": "DUPONT",
                "given_name": "Alice",
                "gender": "F",
                "commune_insee": "01001",
                "list_nuance": "DVG",
            }
        ],
    )

    with pytest.raises(
        DataQualityError,
        match="neither 'is_list_leader' nor 'position_on_list'",
    ):
        build_dim_candidate_leader(
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
            candidates_column_map=_CANDIDATE_IDENTITY_MAP,
            rne_column_map={},
            include_tour2_flag=False,
        )


def test_build_dim_candidate_leader_raises_when_list_nuance_missing(tmp_path):
    """Regression: nuance_group must fail fast when source nuance is absent."""
    bronze_dir, silver_dir, duckdb_path = _write_dim_candidate_inputs(
        tmp_path,
        candidate_rows=[
            {
                "full_name": "DUPONT Alice",
                "family_name": "DUPONT",
                "given_name": "Alice",
                "gender": "F",
                "commune_insee": "01001",
                "is_list_leader": "Oui",
                "position_on_list": "1",
            }
        ],
    )

    with pytest.raises(DataQualityError, match="list_nuance column not found"):
        build_dim_candidate_leader(
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
            candidates_column_map=_CANDIDATE_IDENTITY_MAP,
            rne_column_map={},
            include_tour2_flag=False,
        )


def test_build_dim_candidate_leader_raises_when_full_name_is_unusable(tmp_path):
    """Regression: empty leader names must fail before surrogate-key generation."""
    bronze_dir, silver_dir, duckdb_path = _write_dim_candidate_inputs(
        tmp_path,
        candidate_rows=[
            {
                "full_name": "",
                "family_name": "",
                "given_name": "",
                "gender": "F",
                "commune_insee": "01001",
                "is_list_leader": "Oui",
                "position_on_list": "1",
                "list_nuance": "DVG",
            }
        ],
    )

    with pytest.raises(DataQualityError, match="could not build usable leader names"):
        build_dim_candidate_leader(
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
            candidates_column_map=_CANDIDATE_IDENTITY_MAP,
            rne_column_map={},
            include_tour2_flag=False,
        )


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
        "commune_name must not be None after merge â€” "
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
    ), "dep_code must not be None â€” suffix collision with seats was not handled"


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

    Before the fix, this set city_size_bucket='excluded' for ALL communes â€”
    the pipeline appeared to succeed while all downstream sampling was broken.
    """
    with pytest.raises(DataQualityError, match="seats join failed"):
        build_dim_commune(
            bronze_dir=bronze_commune_parquets,
            silver_dir=tmp_path / "silver",
            duckdb_path=tmp_path / "warehouse.duckdb",
            cog_column_map=_COG_MAP,
            seats_column_map={},  # empty map â†’ no rename â†’ commune_insee absent â†’ must fail
        )


# â”€â”€ build_sample (gold schema) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

# Region codes cycled globally across the full fixture so the schema tests also
# satisfy the active region-cap contract.
_SAMPLE_REG_CODES = [
    "11",
    "24",
    "27",
    "28",
    "32",
    "44",
    "52",
    "53",
    "75",
    "76",
    "84",
    "93",
    "94",
]


@pytest.fixture
def silver_parquets_for_sampling(tmp_path):
    """Write minimal candidate_universe + dim_commune inputs for sampling tests.

    Constructs exactly the minimum viable pool for the active cohort contract:
    one full stratum quota per bucket with a 50/50 gender split. Because the
    pool size equals the quota in every bucket, all candidates are selected
    deterministically and the schema tests stay stable across random seeds.
    """
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    silver_dir.mkdir(parents=True, exist_ok=True)

    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    write_parquet_frame(commune_df, silver_dir / "dim_commune.parquet")
    write_parquet_frame(
        build_candidate_universe_frame(leader_df, commune_df),
        gold_dir / "candidate_universe.parquet",
    )

    return silver_dir, gold_dir


def test_build_sample_gold_schema_includes_commune_name(
    silver_parquets_for_sampling, tmp_path
):
    """Happy path: commune_name must be present in gold sample.

    GDELT DOC 2.0 is a full-text search engine. News articles contain "Rennes",
    not "35238". Without commune_name the news ingest module cannot build
    valid search queries and would return empty or unrelated results.
    """
    silver_dir, gold_dir = silver_parquets_for_sampling
    result_df = build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=tmp_path / "warehouse.duckdb",
        random_seed=42,
    )
    assert "commune_name" in result_df.columns, (
        "gold.sample_leaders must contain commune_name â€” "
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
    silver_dir, gold_dir = silver_parquets_for_sampling
    result_df = build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=tmp_path / "warehouse.duckdb",
        random_seed=42,
    )
    assert "dep_code" in result_df.columns, (
        "gold.sample_leaders must contain dep_code â€” "
        "needed to disambiguate same-name communes in GDELT queries"
    )


def test_build_sample_commune_fields_are_non_null(
    silver_parquets_for_sampling, tmp_path
):
    """Boundary: commune_name and dep_code must be non-null for every sampled candidate.

    A null here means a sampled commune_insee has no match in dim_commune,
    which would silently produce an empty GDELT query string and zero articles
    for that candidate â€” corrupting the exposure metric.
    """
    silver_dir, gold_dir = silver_parquets_for_sampling
    result_df = build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=tmp_path / "warehouse.duckdb",
        random_seed=42,
    )
    null_commune_names = result_df["commune_name"].isna().sum()
    null_dep_codes = result_df["dep_code"].isna().sum()
    assert null_commune_names == 0, (
        f"commune_name has {null_commune_names} null values â€” "
        "all sampled communes must be present in dim_commune"
    )
    assert null_dep_codes == 0, (
        f"dep_code has {null_dep_codes} null values â€” "
        "all sampled communes must be present in dim_commune"
    )
