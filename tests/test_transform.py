"""Tests for src/transform/dim_candidate.py and shared leader-key helpers."""

import pandas as pd
import pytest

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
from tests.sampling_builders import write_parquet_frame

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

    write_parquet_frame(
        pd.DataFrame(candidate_rows),
        bronze_dir / "candidates" / "candidates_tour1.parquet",
    )
    write_parquet_frame(
        pd.DataFrame(rne_rows or []),
        bronze_dir / "rne" / "rne_incumbents.parquet",
    )
    write_parquet_frame(
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


def test_normalize_list_nuance_code_strips_leading_list_prefix():
    """Regression: official list-level nuance codes include an ``L`` prefix."""
    assert _normalize_list_nuance_code("LDVG") == "DVG"
    assert _normalize_list_nuance_code("LRN") == "RN"
    assert _normalize_list_nuance_code("DVG") == "DVG"
    assert _normalize_list_nuance_code(None) == ""


def test_build_full_name_columns_raises_when_name_component_columns_missing():
    """Regression: full_name construction must fail fast when source columns are absent."""
    candidate_df = pd.DataFrame({"commune_insee": ["75056"], "gender": ["F"]})

    with pytest.raises(
        ValueError, match="Cannot build full_name without required columns"
    ):
        build_full_name_columns(candidate_df)


def test_build_full_name_columns_raises_when_name_components_are_blank():
    """Regression: blank family/given names must not silently collapse to empty full_name."""
    candidate_df = pd.DataFrame(
        {"family_name": ["DUPONT", ""], "given_name": ["Alice", "Bob"]}
    )

    with pytest.raises(
        ValueError,
        match="Cannot build full_name from blank family_name/given_name rows",
    ):
        build_full_name_columns(candidate_df)


def test_normalize_name_strips_hyphens():
    """Regression: hyphenated names must normalize to token-equivalent forms."""
    assert _normalize_name("JEAN-LUC") == "JEAN LUC"


def test_normalize_name_strips_apostrophe():
    """Regression: apostrophe variants must normalize consistently."""
    assert _normalize_name("O'BRIEN") == "O BRIEN"
    assert _normalize_name("O\u2019BRIEN") == "O BRIEN"


def test_normalize_name_handles_none():
    """Boundary: None input must return empty string, not raise."""
    assert _normalize_name(None) == ""  # type: ignore[arg-type]


def test_normalize_name_strips_accents():
    """Happy path: accented French characters are stripped to ASCII equivalents."""
    assert _normalize_name("Élise") == "ELISE"
    assert _normalize_name("François") == "FRANCOIS"


def test_match_incumbent_hyphenated_name_above_threshold():
    """Regression: Anne-Sophie vs Anne Sophie must clear the fuzzy threshold."""
    lookup_df = pd.DataFrame(
        {
            "commune_insee": ["01001"],
            "full_name_normalized": [_normalize_name("Anne-Sophie Martin")],
            "original_full_name": ["Anne-Sophie Martin"],
            "rne_mandate_role": ["Maire"],
        }
    )

    is_match, score = _match_incumbent(
        _normalize_name("Anne Sophie Martin"),
        "01001",
        lookup_df,
    )

    assert is_match is True
    assert score is not None and score >= 85


def test_apply_incumbent_matching_preserves_non_contiguous_index_alignment():
    """Regression: nullable-boolean assignment must align to the leader index."""
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


def test_apply_tour2_flag_matches_hyphenated_variant():
    """Regression: Tour 2 hyphen variants must match Tour 1 normalized names."""
    tour2_set = {("75056", _normalize_name("Anne-Sophie Martin"))}
    leader_df = pd.DataFrame(
        {
            "commune_insee": ["75056"],
            "full_name_normalized": [_normalize_name("Anne Sophie Martin")],
        }
    )

    result_df = _apply_tour2_flag(leader_df, tour2_set)

    assert result_df.loc[0, "advanced_to_tour2"] == True  # noqa: E712


def test_apply_tour2_flag_uses_nullable_boolean_and_preserves_missing_keys():
    """Boundary: missing commune or name keys must remain NULL, not false."""
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
    write_parquet_frame(tour2_df, bronze_path)

    tour2_set = _build_tour2_leader_set(bronze_path, candidates_column_map={})

    assert tour2_set == {("75056", _normalize_name("ANNE SOPHIE MARTIN"))}


def test_dim_candidate_output_columns_match_data_model():
    """Regression: Silver dim_candidate_leader must stay Kimball-clean."""
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
        assert forbidden not in _OUTPUT_COLUMNS

    for required in (
        "commune_insee",
        "same_name_candidate_count",
        "list_nuance",
        "nuance_group",
        "is_incumbent",
        "incumbent_match_score",
        "advanced_to_tour2",
    ):
        assert required in _OUTPUT_COLUMNS


def test_same_name_candidate_count_uses_normalized_full_name():
    """Regression: hyphen and accent variants must collapse to one collision count."""
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
