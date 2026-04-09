"""Tests for the Gold sampling slice and its audit artifacts."""

import json

import pandas as pd
import pytest

from src.config.settings import (
    CANDIDATE_SAMPLE_SIZE,
    SAMPLE_MAX_GENDER_BLOC_SHARE_GAP,
    SAMPLE_MAX_GENDER_WIN_RATE_GAP,
    SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER,
    SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION,
    SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1,
)
from src.transform._exceptions import SamplingError
from src.transform.sampling import (
    _SAMPLING_HARD_CONSTRAINTS,
    _SAMPLING_RULE_VERSION,
    _SAMPLING_SELECTION_PARAMETERS,
    _SAMPLING_SELECTION_PRIORITY,
    _SAMPLING_WARNING_THRESHOLDS,
    _cleanup_temp_artifacts,
    _select_unique_commune_candidates,
    _validate_sample,
    build_sample,
)
from tests.sampling_builders import (
    LARGE_PER_GENDER,
    MEDIUM_PER_GENDER,
    SMALL_PER_GENDER,
    TARGET_PER_GENDER,
    build_candidate_and_commune_frames,
    build_candidate_universe_frame,
    write_parquet_frame,
)


def write_sampling_inputs(
    tmp_path,
    leader_df: pd.DataFrame,
    commune_df: pd.DataFrame | None,
) -> tuple:
    """Write candidate_universe + manifest-join inputs to temp locations."""
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    duckdb_path = tmp_path / "warehouse.duckdb"

    if commune_df is not None:
        write_parquet_frame(commune_df, silver_dir / "dim_commune.parquet")
        write_parquet_frame(
            build_candidate_universe_frame(leader_df, commune_df),
            gold_dir / "candidate_universe.parquet",
        )
    else:
        write_parquet_frame(leader_df, gold_dir / "candidate_universe.parquet")

    return silver_dir, gold_dir, duckdb_path


def run_build_sample(tmp_path, leader_df, commune_df, **kwargs):
    """Convenience wrapper: write inputs, run build_sample, return the DataFrame."""
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    return build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
        **kwargs,
    )


def test_sampling_configuration_fails_fast_when_city_size_totals_drift(monkeypatch):
    """Regression: cohort config must fail before sampling if totals diverge.

    ``CANDIDATE_SAMPLE_SIZE`` is the top-level cohort contract, while
    ``_STRATUM_CONFIG`` allocates that total across city-size strata. Changing
    only one side creates silent config drift unless the sampler validates both
    before reading any data.
    """
    import src.transform.sampling as sampling_module

    monkeypatch.setattr(
        sampling_module,
        "_STRATUM_CONFIG",
        {
            "large": {"total": 6, "per_gender": 3},
            "medium": {"total": 12, "per_gender": 6},
            "small": {"total": 20, "per_gender": 10},
        },
    )

    with pytest.raises(
        SamplingError,
        match="city-size totals sum to 38, but CANDIDATE_SAMPLE_SIZE is 36",
    ):
        sampling_module._validate_sampling_configuration()


# â”€â”€ Happy-path tests â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_sample_size_and_unique_leader_ids(tmp_path):
    """Happy path: final sample must contain exactly CANDIDATE_SAMPLE_SIZE unique leaders."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    assert len(sample_df) == CANDIDATE_SAMPLE_SIZE
    assert sample_df["leader_id"].is_unique


def test_gender_balance_and_stratum_quotas(tmp_path):
    """Happy path: gender balance and per-stratum quotas must be met exactly."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    assert (sample_df["gender"] == "F").sum() == TARGET_PER_GENDER
    assert (sample_df["gender"] == "M").sum() == TARGET_PER_GENDER

    quota_series = sample_df.groupby(["city_size_bucket", "gender"]).size().sort_index()
    assert quota_series[("large", "F")] == LARGE_PER_GENDER
    assert quota_series[("large", "M")] == LARGE_PER_GENDER
    assert quota_series[("medium", "F")] == MEDIUM_PER_GENDER
    assert quota_series[("medium", "M")] == MEDIUM_PER_GENDER
    assert quota_series[("small", "F")] == SMALL_PER_GENDER
    assert quota_series[("small", "M")] == SMALL_PER_GENDER


def test_manifest_written_with_correct_candidate_count(tmp_path):
    """Happy path: sample_manifest.json must list all sampled candidates."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    manifest_path = gold_dir / "sample_manifest.json"
    with open(manifest_path, encoding="utf-8") as f:
        manifest = json.load(f)

    assert manifest_path.exists()
    assert "candidates" in manifest
    assert len(manifest["candidates"]) == CANDIDATE_SAMPLE_SIZE


def test_manifest_commune_audit_fields_not_empty(tmp_path):
    """Regression: commune audit fields must be populated via dim_commune join."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    with open(gold_dir / "sample_manifest.json", encoding="utf-8") as f:
        manifest = json.load(f)

    for candidate in manifest["candidates"]:
        assert candidate[
            "commune_name"
        ], f"commune_name empty for {candidate['leader_id']}"
        assert candidate["dep_code"], f"dep_code empty for {candidate['leader_id']}"
        assert candidate["population"] is not None


def test_manifest_serializes_nullable_boolean_flags_without_type_error(tmp_path):
    """Regression: pd.NA boolean fields must serialize to JSON null, not crash.

    build_sample materializes manifest candidates from pandas rows. When the
    underlying Silver columns use pandas' nullable boolean dtype, ``bool(pd.NA)``
    raises ``TypeError`` unless serialization checks ``pd.isna`` first.
    """
    leader_df, commune_df = build_candidate_and_commune_frames()
    leader_df["is_incumbent"] = pd.Series(pd.NA, index=leader_df.index, dtype="boolean")
    leader_df["won_final_round"] = pd.Series(
        pd.NA,
        index=leader_df.index,
        dtype="boolean",
    )
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    with open(gold_dir / "sample_manifest.json", encoding="utf-8") as f:
        manifest = json.load(f)

    assert all(
        candidate["is_incumbent"] is None for candidate in manifest["candidates"]
    )
    assert all(
        candidate["won_final_round"] is None for candidate in manifest["candidates"]
    )


def test_pipeline_run_id_preserved_in_manifest(tmp_path):
    """Happy path: orchestration can inject a shared run_id into the manifest."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
        pipeline_run_id="run-123",
    )

    with open(gold_dir / "sample_manifest.json", encoding="utf-8") as f:
        manifest = json.load(f)

    assert manifest["run_id"] == "run-123"


def test_manifest_records_current_sampling_rule_metadata(tmp_path):
    """Happy path: manifest must reflect the active cohort contract."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    with open(gold_dir / "sample_manifest.json", encoding="utf-8") as f:
        manifest = json.load(f)

    assert manifest["sampling_rule_version"] == _SAMPLING_RULE_VERSION
    assert manifest["hard_constraints"] == _SAMPLING_HARD_CONSTRAINTS
    assert manifest["selection_priority"] == _SAMPLING_SELECTION_PRIORITY
    assert manifest["selection_parameters"] == _SAMPLING_SELECTION_PARAMETERS
    assert manifest["warning_thresholds"] == _SAMPLING_WARNING_THRESHOLDS
    assert "triggered_warnings" in manifest
    assert "diagnostics" in manifest
    assert set(manifest["diagnostics"]) == {
        "political_bloc",
        "geography",
        "gender_bloc_balance",
        "gender_win_rate",
        "subgroup_size",
        "rare_nuance_group",
        "control_missingness",
        "region_singleton",
    }


def test_build_sample_gold_schema_includes_commune_name_and_dep_code(tmp_path):
    """Happy path: the Gold cohort must stay self-contained for downstream use."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    sample_df = run_build_sample(tmp_path, leader_df, commune_df, random_seed=42)

    assert "commune_name" in sample_df.columns
    assert "dep_code" in sample_df.columns


def test_build_sample_commune_fields_are_non_null(tmp_path):
    """Boundary: sampled leaders must keep non-null commune search attributes."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    sample_df = run_build_sample(tmp_path, leader_df, commune_df, random_seed=42)

    assert sample_df["commune_name"].notna().all()
    assert sample_df["dep_code"].notna().all()


def test_sampling_inputs_can_fill_region_and_bucket_from_commune_join(tmp_path):
    """Regression: sampling fixtures should cover commune-sourced geography joins.

    Production sampling reads ``gold.candidate_universe``, where ``reg_code``
    and ``city_size_bucket`` come from the commune join rather than the narrow
    candidate dimension. The fixture builder must therefore be able to fill
    those columns from ``commune_df`` when the candidate frame omits them.
    """
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    leader_df = leader_df.drop(columns=["reg_code", "city_size_bucket"])

    sample_df = run_build_sample(tmp_path, leader_df, commune_df, random_seed=42)

    assert sample_df["reg_code"].notna().all()
    assert set(sample_df["city_size_bucket"]) == {"large", "medium", "small"}


# â”€â”€ Boundary-condition tests â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_prefers_lower_same_name_candidate_count(tmp_path):
    """Boundary: high-ambiguity candidate should lose tie-break in a larger pool."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        include_extra_large_female=True
    )
    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    assert "leader-extra-large-f" not in sample_df["leader_id"].tolist()


def test_commune_uniqueness_enforced_by_greedy_selection(tmp_path):
    """Boundary: when a commune is shared, only one candidate from it is selected."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        include_extra_large_female=True
    )
    large_female_mask = (leader_df["city_size_bucket"] == "large") & (
        leader_df["gender"] == "F"
    )
    large_female_indexes = leader_df.index[large_female_mask].tolist()

    # Force two large-F candidates to share a commune.
    duplicate_commune = leader_df.loc[large_female_indexes[0], "commune_insee"]
    leader_df.loc[large_female_indexes[1], "commune_insee"] = duplicate_commune
    leader_df.loc[large_female_indexes[:2], "same_name_candidate_count"] = [1, 2]
    leader_df.loc[
        leader_df["leader_id"] == "leader-extra-large-f", "same_name_candidate_count"
    ] = 3

    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    assert sample_df["commune_insee"].is_unique
    # The duplicate commune must still appear exactly once.
    sampled_large_f_communes = sample_df.loc[
        (sample_df["city_size_bucket"] == "large") & (sample_df["gender"] == "F"),
        "commune_insee",
    ].tolist()
    assert sampled_large_f_communes.count(duplicate_commune) == 1


@pytest.mark.parametrize("random_seed", [0, 7, 42])
def test_region_diversity_priority_recomputed_after_each_selection(random_seed):
    """Regression: one remaining unseen region must not consume the whole quota.

    The region-diversity signal depends on the evolving sample state. If it is
    computed only once before the greedy loop starts, every candidate from the
    one remaining unseen region stays permanently ahead of the rest of the
    pool. The correct implementation recomputes that priority after each pick.
    """
    candidate_pool_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-new-1",
                "full_name": "Leader New 1",
                "gender": "F",
                "commune_insee": "10001",
                "dep_code": "10",
                "reg_code": "R_NEW",
                "city_size_bucket": "small",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-new-2",
                "full_name": "Leader New 2",
                "gender": "F",
                "commune_insee": "10002",
                "dep_code": "11",
                "reg_code": "R_NEW",
                "city_size_bucket": "small",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-old-1",
                "full_name": "Leader Old 1",
                "gender": "F",
                "commune_insee": "10003",
                "dep_code": "12",
                "reg_code": "R_OLD_1",
                "city_size_bucket": "small",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-old-2",
                "full_name": "Leader Old 2",
                "gender": "F",
                "commune_insee": "10004",
                "dep_code": "13",
                "reg_code": "R_OLD_2",
                "city_size_bucket": "small",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
        ]
    )

    sampled_df, _, _, _, _ = _select_unique_commune_candidates(
        candidate_pool_df=candidate_pool_df,
        bucket="small",
        gender="F",
        per_gender=2,
        random_seed=random_seed,
        used_communes=set(),
        used_regions={"R_OLD_1", "R_OLD_2"},
        used_region_counts={},
        used_departments_by_region={},
    )

    sampled_regions = sampled_df["reg_code"].tolist()
    assert sampled_regions.count("R_NEW") == 1


def test_bucket_gender_nuance_priority_recomputed_after_each_selection():
    """Regression: one bloc should not consume a cell when alternatives exist.

    The nuance-balancing signal must be recomputed after every pick inside the
    active city-size x gender cell. Otherwise the greedy sampler can keep
    selecting the same bloc even though viable alternatives remain available.
    """
    candidate_pool_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-gauche-1",
                "full_name": "Leader Gauche 1",
                "gender": "F",
                "commune_insee": "20001",
                "dep_code": "20",
                "reg_code": "R1",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-gauche-2",
                "full_name": "Leader Gauche 2",
                "gender": "F",
                "commune_insee": "20002",
                "dep_code": "21",
                "reg_code": "R2",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-droite-1",
                "full_name": "Leader Droite 1",
                "gender": "F",
                "commune_insee": "20003",
                "dep_code": "22",
                "reg_code": "R3",
                "city_size_bucket": "large",
                "nuance_group": "droite",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-divers-1",
                "full_name": "Leader Divers 1",
                "gender": "F",
                "commune_insee": "20004",
                "dep_code": "23",
                "reg_code": "R4",
                "city_size_bucket": "large",
                "nuance_group": "divers",
                "same_name_candidate_count": 1,
            },
        ]
    )

    sampled_df, _, _, _, _ = _select_unique_commune_candidates(
        candidate_pool_df=candidate_pool_df,
        bucket="large",
        gender="F",
        per_gender=3,
        random_seed=42,
        used_communes=set(),
        used_regions=set(),
        used_region_counts={},
        used_departments_by_region={},
    )

    sampled_nuance_counts = sampled_df["nuance_group"].value_counts().to_dict()
    assert max(sampled_nuance_counts.values()) == 1


def test_bucket_gender_nuance_priority_does_not_overcorrect_into_rare_blocs():
    """Regression: concentration control must still respect pool composition.

    When one bloc dominates the viable pool, the sampler should reduce
    over-concentration without forcing a near-even split that badly
    over-represents rare blocs.
    """
    candidate_pool_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-gauche-1",
                "full_name": "Leader Gauche 1",
                "gender": "F",
                "commune_insee": "30001",
                "dep_code": "30",
                "reg_code": "R1",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-gauche-2",
                "full_name": "Leader Gauche 2",
                "gender": "F",
                "commune_insee": "30002",
                "dep_code": "31",
                "reg_code": "R2",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-gauche-3",
                "full_name": "Leader Gauche 3",
                "gender": "F",
                "commune_insee": "30003",
                "dep_code": "32",
                "reg_code": "R3",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-droite-1",
                "full_name": "Leader Droite 1",
                "gender": "F",
                "commune_insee": "30004",
                "dep_code": "33",
                "reg_code": "R4",
                "city_size_bucket": "large",
                "nuance_group": "droite",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-centre-1",
                "full_name": "Leader Centre 1",
                "gender": "F",
                "commune_insee": "30005",
                "dep_code": "34",
                "reg_code": "R5",
                "city_size_bucket": "large",
                "nuance_group": "centre",
                "same_name_candidate_count": 1,
            },
        ]
    )

    sampled_df, _, _, _, _ = _select_unique_commune_candidates(
        candidate_pool_df=candidate_pool_df,
        bucket="large",
        gender="F",
        per_gender=3,
        random_seed=42,
        used_communes=set(),
        used_regions=set(),
        used_region_counts={},
        used_departments_by_region={},
    )

    sampled_nuance_counts = sampled_df["nuance_group"].value_counts().to_dict()
    assert sampled_nuance_counts["gauche"] == 2
    assert (
        sum(
            count
            for nuance, count in sampled_nuance_counts.items()
            if nuance != "gauche"
        )
        == 1
    )


def test_department_diversity_does_not_outrank_bucket_gender_bloc_balance():
    """Regression: bloc-balance tie-breaks must outrank intra-region department spread.

    This keeps the cohort closer to the viable pool's political composition
    inside each city-size x gender cell instead of preferring a new department
    when that would amplify an already-selected bloc.
    """
    candidate_pool_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-anchor-gauche",
                "full_name": "Leader Anchor Gauche",
                "gender": "F",
                "commune_insee": "31001",
                "dep_code": "31",
                "reg_code": "R1",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 1,
            },
            {
                "leader_id": "leader-gauche-new-department",
                "full_name": "Leader Gauche New Department",
                "gender": "F",
                "commune_insee": "31002",
                "dep_code": "32",
                "reg_code": "R1",
                "city_size_bucket": "large",
                "nuance_group": "gauche",
                "same_name_candidate_count": 2,
            },
            {
                "leader_id": "leader-droite-same-department",
                "full_name": "Leader Droite Same Department",
                "gender": "F",
                "commune_insee": "31003",
                "dep_code": "31",
                "reg_code": "R1",
                "city_size_bucket": "large",
                "nuance_group": "droite",
                "same_name_candidate_count": 2,
            },
        ]
    )

    sampled_df, _, _, _, _ = _select_unique_commune_candidates(
        candidate_pool_df=candidate_pool_df,
        bucket="large",
        gender="F",
        per_gender=2,
        random_seed=42,
        used_communes=set(),
        used_regions=set(),
        used_region_counts={},
        used_departments_by_region={},
    )

    assert sampled_df["leader_id"].tolist() == [
        "leader-anchor-gauche",
        "leader-droite-same-department",
    ]


def test_fails_when_commune_uniqueness_makes_quota_infeasible(tmp_path):
    """Boundary: if unique communes < per_gender quota, SamplingError is raised.

    Build a large-F pool that has exactly the quota count but with one shared
    commune, leaving fewer unique communes than needed.
    """
    # Use extra_candidates_per_slot=0 so the large bucket has EXACTLY per_gender
    # candidates per gender â€” no spares. Then force a duplicate commune.
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    large_female_mask = (leader_df["city_size_bucket"] == "large") & (
        leader_df["gender"] == "F"
    )
    large_female_indexes = leader_df.index[large_female_mask].tolist()
    duplicate_commune = leader_df.loc[large_female_indexes[0], "commune_insee"]
    leader_df.loc[large_female_indexes[1], "commune_insee"] = duplicate_commune

    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    with pytest.raises(
        SamplingError,
        match="Commune uniqueness \\+ region cap made the stratum/gender quota infeasible",
    ):
        build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)


# â”€â”€ Error-path tests â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_raises_when_insufficient_female_pool(tmp_path):
    """Error: a stratum with too few female candidates must raise SamplingError."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        insufficient_large_female=True
    )
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    with pytest.raises(SamplingError, match="Insufficient female candidates"):
        build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)


def test_fails_fast_when_dim_commune_missing(tmp_path):
    """Error: dim_commune is a required input â€” absence must fail immediately."""
    leader_df, _ = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df=None
    )

    with pytest.raises(FileNotFoundError, match="dim_commune silver file not found"):
        build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)


# â”€â”€ Regression tests for new hard constraints â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def test_validate_sample_raises_on_wrong_stratum_count(tmp_path):
    """Regression: _validate_sample must enforce per-stratum quotas as hard constraints.

    Prior to this check, the manifest documented city_size_totals as hard
    constraints but the validator only checked total size and gender balance.
    A bug in _sample_stratum could produce valid totals with wrong strata.
    """
    leader_df, commune_df = build_candidate_and_commune_frames()
    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    # Corrupt the stratum label of one row to simulate a mis-stratified sample.
    corrupted_df = sample_df.copy()
    corrupted_df.loc[
        corrupted_df["city_size_bucket"] == "large", "city_size_bucket"
    ] = "small"

    with pytest.raises(SamplingError, match="Stratum quota violated: bucket=large"):
        _validate_sample(corrupted_df)


def test_dom_tom_candidates_excluded_from_sample(tmp_path, monkeypatch):
    """Regression: DOM-TOM candidates must not enter the sample when exclusion is enabled.

    Uses monkeypatch to isolate the test from the environment EXCLUDE_DOM_TOM
    setting, ensuring this test always exercises the exclusion branch.
    """
    import src.transform.sampling as sampling_module

    monkeypatch.setattr(sampling_module, "EXCLUDE_DOM_TOM", True)
    monkeypatch.setattr(sampling_module, "DOM_TOM_REG_CODES", frozenset({"DOMTOM"}))

    leader_df, commune_df = build_candidate_and_commune_frames()

    # Inject DOM-TOM candidates into every stratum â€” they should all be excluded.
    dom_tom_rows: list[dict[str, object]] = []
    dom_tom_commune_rows: list[dict[str, object]] = []
    for i, bucket in enumerate(["large", "medium", "small"]):
        for gender in ("F", "M"):
            commune_insee = f"9{i}{gender}00"
            dom_tom_rows.append(
                {
                    "leader_id": f"domtom-{bucket}-{gender}",
                    "full_name": f"Overseas {bucket} {gender}",
                    "gender": gender,
                    "commune_insee": commune_insee,
                    "reg_code": "DOMTOM",
                    "city_size_bucket": bucket,
                    "same_name_candidate_count": 1,
                    "list_nuance": "DVG",
                    "nuance_group": "divers",
                    "is_incumbent": False,
                    "incumbent_match_score": None,
                    "incumbent_match_auditable": False,
                    "advanced_to_tour2": None,
                    "score_tour1_votes": 88,
                    "score_tour1_pct_expressed": 25.0,
                    "score_tour1_rank": 1,
                }
            )
            pop = {"large": 150_000, "medium": 50_000, "small": 10_000}[bucket]
            dom_tom_commune_rows.append(
                {
                    "commune_insee": commune_insee,
                    "commune_name": f"Overseas Commune {i}{gender}",
                    "dep_code": "DT",
                    "population": pop,
                }
            )

    combined_leader_df = pd.concat(
        [leader_df, pd.DataFrame(dom_tom_rows)], ignore_index=True
    )
    combined_commune_df = pd.concat(
        [commune_df, pd.DataFrame(dom_tom_commune_rows)], ignore_index=True
    )

    sample_df = run_build_sample(tmp_path, combined_leader_df, combined_commune_df)

    assert not sample_df["reg_code"].isin({"DOMTOM"}).any(), (
        "DOM-TOM candidates (reg_code=DOMTOM) must not appear in the sample "
        "when EXCLUDE_DOM_TOM is True."
    )


def test_political_bloc_warning_emitted_above_threshold(tmp_path, caplog):
    """Regression: a dominant political bloc must trigger a WARNING for reviewers.

    Build a pool where all candidates share one nuance_group so the
    over-representation check fires. This exercises the soft-constraint log
    path that signals reviewers to add nuance_group as a regression control.
    """
    import logging

    leader_df, commune_df = build_candidate_and_commune_frames()
    # All candidates already have nuance_group="gauche" from the fixture.
    # A 100% single-bloc sample must always exceed the overall threshold.

    with caplog.at_level(logging.WARNING, logger="src.transform.sampling"):
        run_build_sample(tmp_path, leader_df, commune_df)

    bloc_warnings = [
        r
        for r in caplog.records
        if "Political bloc over-represented" in r.message and r.levelname == "WARNING"
    ]
    assert bloc_warnings, (
        "Expected a WARNING about political bloc over-representation when "
        "all candidates share the same nuance_group."
    )
    assert "scope=overall" in bloc_warnings[0].message
    assert "gauche" in bloc_warnings[0].message


def test_political_bloc_warning_emitted_for_stratum_concentration(tmp_path, caplog):
    """Regression: a stratum-level political concentration should emit a warning.

    The matched cohort is interpreted within city-size strata, so bloc
    concentration must be checked both overall and inside each stratum. This
    test keeps the overall sample diversified while forcing the large stratum
    to be 100% gauche.
    """
    import logging

    leader_df, commune_df = build_candidate_and_commune_frames()
    large_mask = leader_df["city_size_bucket"] == "large"
    leader_df.loc[large_mask, "nuance_group"] = "gauche"

    diversified_blocs = ["divers", "droite", "extreme_droite"]
    non_large_indexes = leader_df.index[~large_mask].tolist()
    for position, index in enumerate(non_large_indexes):
        leader_df.loc[index, "nuance_group"] = diversified_blocs[
            position % len(diversified_blocs)
        ]

    with caplog.at_level(logging.WARNING, logger="src.transform.sampling"):
        run_build_sample(tmp_path, leader_df, commune_df)

    stratum_warnings = [
        record
        for record in caplog.records
        if "Political bloc over-represented" in record.message
        and "scope=city_size_bucket:large" in record.message
        and record.levelname == "WARNING"
    ]
    assert stratum_warnings, (
        "Expected a WARNING about bloc concentration inside the large stratum "
        "even when the overall sample remains diversified."
    )


def test_manifest_records_bucket_gender_political_warning(tmp_path):
    """Regression: manifest must persist bucket x gender soft-constraint breaches."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )

    medium_female_mask = (leader_df["city_size_bucket"] == "medium") & (
        leader_df["gender"] == "F"
    )
    leader_df.loc[medium_female_mask, "nuance_group"] = "gauche"
    leader_df.loc[medium_female_mask, "list_nuance"] = "DVG"

    diversified_blocs = [
        ("divers", "DVC"),
        ("droite", "DVD"),
        ("centre", "DVC"),
        ("extreme_droite", "RN"),
    ]
    other_indexes = leader_df.index[~medium_female_mask].tolist()
    for position, index in enumerate(other_indexes):
        nuance_group, list_nuance = diversified_blocs[position % len(diversified_blocs)]
        leader_df.loc[index, "nuance_group"] = nuance_group
        leader_df.loc[index, "list_nuance"] = list_nuance

    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    manifest = json.loads(
        (gold_dir / "sample_manifest.json").read_text(encoding="utf-8")
    )
    warning_scopes = {
        warning["scope"] for warning in manifest["triggered_warnings"] if warning
    }

    assert "city_size_bucket_gender:medium:F" in warning_scopes
    political_diagnostics = manifest["diagnostics"]["political_bloc"]
    subgroup_rows = political_diagnostics["by_city_size_gender"]
    matching_rows = [
        row
        for row in subgroup_rows
        if row["scope"] == "city_size_bucket_gender:medium:F"
    ]
    assert matching_rows
    assert (
        matching_rows[0]["top_share"] >= SAMPLE_MAX_SINGLE_BLOC_RATIO_PER_BUCKET_GENDER
    )


def test_manifest_records_department_concentration_warning(tmp_path):
    """Regression: manifest must surface department clustering inside one region."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    leaders_in_order = leader_df.sort_values("leader_id").reset_index(drop=True)
    target_leader_ids = leaders_in_order.loc[:3, "leader_id"].tolist()
    leader_df.loc[leader_df["leader_id"].isin(target_leader_ids), "reg_code"] = "32"

    target_communes = leaders_in_order.loc[:3, "commune_insee"].tolist()
    commune_df = commune_df.copy()
    dep_by_commune = {
        target_communes[0]: "62",
        target_communes[1]: "62",
        target_communes[2]: "62",
        target_communes[3]: "59",
    }
    commune_df["dep_code"] = (
        commune_df["commune_insee"].map(dep_by_commune).fillna(commune_df["dep_code"])
    )

    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    manifest = json.loads(
        (gold_dir / "sample_manifest.json").read_text(encoding="utf-8")
    )
    warning_scopes = {
        (warning["warning_code"], warning["scope"], warning.get("dep_code"))
        for warning in manifest["triggered_warnings"]
    }

    assert (
        "department_concentration_within_region",
        "region:32",
        "62",
    ) in warning_scopes
    geography = manifest["diagnostics"]["geography"]
    matching_rows = [
        row for row in geography["departments_by_region"] if row["scope"] == "region:32"
    ]
    assert matching_rows
    assert (
        matching_rows[0]["top_share"] >= SAMPLE_MAX_SINGLE_DEPARTMENT_RATIO_PER_REGION
    )


def test_manifest_records_gender_bloc_gap_and_small_subgroup_risks(tmp_path):
    """Regression: manifest must expose gender/bloc confounding and tiny bucket cells."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    leader_df.loc[leader_df["gender"] == "F", "nuance_group"] = "gauche"
    leader_df.loc[leader_df["gender"] == "F", "list_nuance"] = "DVG"
    leader_df.loc[leader_df["gender"] == "M", "nuance_group"] = "droite"
    leader_df.loc[leader_df["gender"] == "M", "list_nuance"] = "DVD"

    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    manifest = json.loads(
        (gold_dir / "sample_manifest.json").read_text(encoding="utf-8")
    )
    warning_codes = {
        (warning["warning_code"], warning["scope"], warning.get("value"))
        for warning in manifest["triggered_warnings"]
    }

    assert ("gender_bloc_share_gap", "overall", "gauche") in warning_codes
    assert ("subgroup_small_n", "city_size_bucket_gender:large:F", "n") in warning_codes
    assert ("subgroup_small_n", "city_size_bucket_gender:large:M", "n") in warning_codes

    overall_rows = manifest["diagnostics"]["gender_bloc_balance"]["overall"]
    gauche_row = next(row for row in overall_rows if row["nuance_group"] == "gauche")
    assert gauche_row["absolute_gap"] >= SAMPLE_MAX_GENDER_BLOC_SHARE_GAP


def test_manifest_records_gender_win_rate_missingness_and_singleton_region(tmp_path):
    """Regression: modeling-risk warnings must surface in the manifest."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    leader_df.loc[leader_df["gender"] == "F", "won_final_round"] = False
    leader_df.loc[leader_df["gender"] == "M", "won_final_round"] = True
    leader_df["is_incumbent"] = leader_df["is_incumbent"].astype("boolean")
    leader_df.loc[leader_df.index[0], "is_incumbent"] = pd.NA
    leader_df.loc[leader_df["leader_id"].isin(["leader-001"]), "reg_code"] = "94"
    leader_df.loc[
        leader_df["leader_id"].isin(["leader-002", "leader-003"]), "reg_code"
    ] = "11"

    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )
    build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    manifest = json.loads(
        (gold_dir / "sample_manifest.json").read_text(encoding="utf-8")
    )
    warning_codes = {
        (warning["warning_code"], warning["scope"], warning.get("dimension"))
        for warning in manifest["triggered_warnings"]
    }

    assert ("gender_win_rate_gap", "overall", "won_final_round") in warning_codes
    assert ("control_missingness", "overall", "is_incumbent") in warning_codes
    assert ("singleton_region_representation", "region:94", "reg_code") in warning_codes

    overall_win = manifest["diagnostics"]["gender_win_rate"]["overall"]
    assert overall_win["absolute_gap"] >= SAMPLE_MAX_GENDER_WIN_RATE_GAP


def test_build_sample_does_not_publish_partial_artifacts_when_duckdb_write_fails(
    tmp_path, monkeypatch
):
    """Regression: staged gold files must stay invisible if DuckDB persistence fails."""
    import src.transform.sampling as sampling_module

    leader_df, commune_df = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    def _raise_duckdb_failure(sample_df, duckdb_path, pipeline_run_id):  # noqa: ARG001
        raise RuntimeError("duckdb write failed")

    monkeypatch.setattr(
        sampling_module, "_write_sample_to_duckdb", _raise_duckdb_failure
    )

    with pytest.raises(RuntimeError, match="duckdb write failed"):
        build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)

    assert not (gold_dir / "sample_leaders.parquet").exists()
    assert not (gold_dir / "sample_manifest.json").exists()
    assert not list(gold_dir.glob("*.tmp.*"))


def test_cleanup_temp_artifacts_logs_oserror_without_masking_failure(
    tmp_path, monkeypatch, caplog
):
    """Regression: temp-file cleanup failures must be logged, not silently hidden."""
    import logging

    staged_path = tmp_path / "sample.tmp"
    staged_path.write_text("staged", encoding="utf-8")

    def _raise_unlink_error(self, missing_ok=False):  # noqa: ARG001
        raise OSError("device busy")

    monkeypatch.setattr(type(staged_path), "unlink", _raise_unlink_error)

    with caplog.at_level(logging.WARNING, logger="src.transform.sampling"):
        _cleanup_temp_artifacts([staged_path], "run-cleanup")

    assert staged_path.exists()
    assert any(
        "Failed to clean staged artifact run_id=run-cleanup" in record.message
        for record in caplog.records
    )


def test_build_sample_logs_run_id_on_artifact_lifecycle(tmp_path, caplog):
    """Regression: artifact logs must carry run_id for concurrent-run traceability."""
    import logging

    leader_df, commune_df = build_candidate_and_commune_frames()
    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    with caplog.at_level(logging.INFO, logger="src.transform.sampling"):
        build_sample(
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            duckdb_path=duckdb_path,
            pipeline_run_id="run-123",
        )

    messages = [record.message for record in caplog.records]
    assert any(
        "Staged sample Parquet run_id=run-123" in message for message in messages
    )
    assert any(
        "DuckDB gold.sample_leaders written run_id=run-123" in message
        for message in messages
    )
    assert any(
        "Gold Parquet published run_id=run-123" in message for message in messages
    )
    assert any(
        "Sample manifest published run_id=run-123" in message for message in messages
    )


def test_non_viable_candidates_are_excluded_from_primary_cohort(tmp_path):
    """Regression: candidates below the viability threshold must stay out.

    The primary cohort now targets electorally viable candidates only. A
    candidate with both low vote share and a rank worse than 2 should be
    excluded even if they otherwise fit the stratum and commune constraints.
    """
    leader_df, commune_df = build_candidate_and_commune_frames(
        include_extra_large_female=True
    )
    leader_df.loc[
        leader_df["leader_id"].isin({"leader-001", "leader-extra-large-f"}),
        ["score_tour1_pct_expressed", "score_tour1_rank"],
    ] = [4.0, 3]

    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    assert "leader-001" not in sample_df["leader_id"].tolist()
    assert "leader-extra-large-f" not in sample_df["leader_id"].tolist()
    assert (
        sample_df["score_tour1_pct_expressed"] < SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1
    ).sum() == 0


def test_rank_based_viability_keeps_top_two_candidate_below_10_percent(tmp_path):
    """Boundary: top-two finish should preserve viability below the vote-share cutoff."""
    leader_df, commune_df = build_candidate_and_commune_frames(
        extra_candidates_per_slot=0
    )
    leader_df.loc[
        leader_df["leader_id"] == "leader-001",
        ["score_tour1_pct_expressed", "score_tour1_rank"],
    ] = [4.0, 2]

    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    selected_row = sample_df.loc[sample_df["leader_id"] == "leader-001"].iloc[0]
    assert selected_row["score_tour1_pct_expressed"] == 4.0
    assert selected_row["score_tour1_rank"] == 2


def test_viability_filter_failure_raises_when_bucket_gender_pool_too_small(tmp_path):
    """Error: viability filtering must fail fast when a quota becomes infeasible."""
    leader_df, commune_df = build_candidate_and_commune_frames()
    large_female_indexes = leader_df.index[
        (leader_df["city_size_bucket"] == "large") & (leader_df["gender"] == "F")
    ].tolist()

    # Leave only two viable large-F candidates although the quota requires three.
    for index in large_female_indexes[:2]:
        leader_df.loc[index, ["score_tour1_pct_expressed", "score_tour1_rank"]] = [
            25.0,
            1,
        ]
    for index in large_female_indexes[2:]:
        leader_df.loc[index, ["score_tour1_pct_expressed", "score_tour1_rank"]] = [
            4.0,
            3,
        ]

    silver_dir, gold_dir, duckdb_path = write_sampling_inputs(
        tmp_path, leader_df, commune_df
    )

    with pytest.raises(
        SamplingError, match="Insufficient female candidates in 'large' bucket"
    ):
        build_sample(silver_dir=silver_dir, gold_dir=gold_dir, duckdb_path=duckdb_path)


def test_region_cap_enforced_during_selection_and_validation(tmp_path):
    """Regression: no region may exceed _MAX_CANDIDATES_PER_REGION in the final sample.

    The greedy selection must skip candidates from regions already at capacity,
    and _validate_sample must raise SamplingError if the cap is violated.
    Both the algorithm and the validator are tested here.
    """
    from src.transform.sampling import _MAX_CANDIDATES_PER_REGION, _validate_sample

    # Build a normal feasible sample â€” region codes spread across 20 regions
    # so the cap is never the binding constraint.
    leader_df, commune_df = build_candidate_and_commune_frames()
    sample_df = run_build_sample(tmp_path, leader_df, commune_df)

    # Algorithm invariant: no region exceeds the cap in the produced sample.
    region_counts = sample_df["reg_code"].value_counts()
    over_cap = region_counts[region_counts > _MAX_CANDIDATES_PER_REGION]
    assert over_cap.empty, (
        f"Region cap violated in sample: {over_cap.to_dict()}. "
        f"Max allowed per region: {_MAX_CANDIDATES_PER_REGION}"
    )

    # Validator invariant: injecting a violation raises SamplingError.
    violated_sample_df = sample_df.copy()
    violated_sample_df.loc[
        violated_sample_df.index[: _MAX_CANDIDATES_PER_REGION + 1], "reg_code"
    ] = "R_OVERFLOW"
    with pytest.raises(SamplingError, match="max.*candidates per region"):
        _validate_sample(violated_sample_df)
