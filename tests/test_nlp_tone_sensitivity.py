"""Tests for Phase 3 tone threshold sensitivity analysis."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd
import pytest

from src.nlp.tone_sensitivity import (
    DEFAULT_TONE_SENSITIVITY_THRESHOLDS,
    build_tone_sensitivity_analysis,
    materialize_tone_sensitivity_analysis,
)
from src.transform._exceptions import DataQualityError


def _summary_dataframe() -> pd.DataFrame:
    """Return a minimal tone-enriched summary fixture."""
    return pd.DataFrame(
        [
            _summary_row(
                mention_id="mention-001",
                leader_id="leader-f",
                label="favorable",
                probability=0.82,
            ),
            _summary_row(
                mention_id="mention-002",
                leader_id="leader-f",
                label="unclassified",
                probability=0.58,
            ),
            _summary_row(
                mention_id="mention-003",
                leader_id="leader-m",
                label="neutral",
                probability=0.62,
            ),
            _summary_row(
                mention_id="mention-004",
                leader_id="leader-m",
                label="unclassified",
                probability=0.39,
            ),
            _summary_row(
                mention_id="mention-005",
                leader_id="leader-m",
                label="unclassified",
                probability=None,
                status="skipped",
            ),
        ]
    )


def _summary_row(
    *,
    mention_id: str,
    leader_id: str,
    label: str,
    probability: float | None,
    status: str = "scored",
) -> dict[str, object]:
    """Return one tone summary row."""
    return {
        "mention_id": mention_id,
        "leader_id": leader_id,
        "target_tone_label": label,
        "target_tone_probability": probability,
        "nlp_enrichment_status": status,
        "nlp_model_bundle_version": "bundle-001",
    }


def _sample_leaders_dataframe() -> pd.DataFrame:
    """Return sampled leaders with gender metadata."""
    return pd.DataFrame(
        [
            {"leader_id": "leader-f", "gender": "F"},
            {"leader_id": "leader-m", "gender": "M"},
        ]
    )


def test_build_tone_sensitivity_analysis_reports_threshold_coverage():
    """Happy path: threshold rows quantify coverage without model inference."""
    analysis = build_tone_sensitivity_analysis(
        _summary_dataframe(),
        _sample_leaders_dataframe(),
        thresholds=[0.40, 0.60, 0.80],
        generated_at=datetime(2026, 5, 6, 12, 0, tzinfo=UTC),
        configured_tone_threshold=0.60,
    )

    sensitivity_table = analysis.sensitivity_table
    overall_at_060 = sensitivity_table.loc[
        sensitivity_table["threshold"].eq(0.60)
        & sensitivity_table["segment_type"].eq("overall")
    ].iloc[0]
    female_at_040 = sensitivity_table.loc[
        sensitivity_table["threshold"].eq(0.40)
        & sensitivity_table["segment_value"].eq("F")
    ].iloc[0]
    male_at_040 = sensitivity_table.loc[
        sensitivity_table["threshold"].eq(0.40)
        & sensitivity_table["segment_value"].eq("M")
    ].iloc[0]

    assert len(sensitivity_table) == 9
    assert overall_at_060["scoreable_mentions"] == 4
    assert overall_at_060["not_scoreable_mentions"] == 1
    assert overall_at_060["classified_mentions_at_threshold"] == 2
    assert overall_at_060["classified_share_of_scoreable"] == pytest.approx(0.5)
    assert female_at_040["classified_share_of_scoreable"] == pytest.approx(1.0)
    assert male_at_040["classified_share_of_scoreable"] == pytest.approx(0.5)

    report = analysis.report
    assert report["configured_tone_threshold"] == 0.60
    assert report["current_summary"] == {
        "total_mentions": 5,
        "scoreable_mentions": 4,
        "not_scoreable_mentions": 1,
        "persisted_classified_mentions": 2,
        "persisted_unclassified_mentions": 3,
        "persisted_classified_share_of_scoreable": 0.5,
    }
    assert (
        "does not reconstruct alternate label distributions"
        in report["analysis_scope"]["limitation"]
    )
    label_bin_record = next(
        record
        for record in report["probability_bins_by_current_label"]
        if record["segment_type"] == "overall"
        and record["target_tone_label"] == "unclassified"
        and record["probability_bin"] == "0.50-0.60"
    )
    assert label_bin_record["mentions"] == 1


def test_build_tone_sensitivity_analysis_computes_gender_gap():
    """Boundary: female-minus-male coverage gap is explicit per threshold."""
    analysis = build_tone_sensitivity_analysis(
        _summary_dataframe(),
        _sample_leaders_dataframe(),
        thresholds=[0.40],
        generated_at=datetime(2026, 5, 6, 12, 0, tzinfo=UTC),
    )

    assert analysis.report["gender_gap"] == [
        {
            "threshold": 0.40,
            "female_classified_share_of_scoreable": 1.0,
            "male_classified_share_of_scoreable": 0.5,
            "female_minus_male_classified_share": 0.5,
        }
    ]


def test_build_tone_sensitivity_analysis_handles_zero_and_one_thresholds():
    """Boundary: exact 0 and 1 thresholds remain valid probability cutoffs."""
    analysis = build_tone_sensitivity_analysis(
        _summary_dataframe(),
        _sample_leaders_dataframe(),
        thresholds=[0.0, 1.0],
        generated_at=datetime(2026, 5, 6, 12, 0, tzinfo=UTC),
    )

    threshold_lookup = {
        row.threshold: row.classified_mentions_at_threshold
        for row in analysis.sensitivity_table.loc[
            analysis.sensitivity_table["segment_type"].eq("overall")
        ].itertuples(index=False)
    }

    assert threshold_lookup[0.0] == 4
    assert threshold_lookup[1.0] == 0


def test_build_tone_sensitivity_analysis_normalizes_labels_without_mutation():
    """Regression: validation does not mutate the caller's summary DataFrame."""
    summary_dataframe = _summary_dataframe()
    summary_dataframe.loc[0, "target_tone_label"] = " favorable "

    analysis = build_tone_sensitivity_analysis(
        summary_dataframe,
        _sample_leaders_dataframe(),
        thresholds=[0.60],
    )
    favorable_record = next(
        record
        for record in analysis.report["observed_current_label_distribution"]
        if record["segment_type"] == "overall"
        and record["target_tone_label"] == "favorable"
    )

    assert favorable_record["mentions"] == 1
    assert summary_dataframe.loc[0, "target_tone_label"] == " favorable "


@pytest.mark.parametrize("thresholds", [[], [-0.1], [1.1], [0.6, 0.6]])
def test_build_tone_sensitivity_analysis_rejects_invalid_thresholds(thresholds):
    """Error path: invalid threshold grids fail before report generation."""
    with pytest.raises(ValueError):
        build_tone_sensitivity_analysis(
            _summary_dataframe(),
            _sample_leaders_dataframe(),
            thresholds=thresholds,
        )


def test_build_tone_sensitivity_analysis_rejects_duplicate_mentions():
    """Error path: one mention must contribute once to the audit."""
    summary_dataframe = _summary_dataframe()
    summary_dataframe.loc[1, "mention_id"] = "mention-001"

    with pytest.raises(DataQualityError, match="duplicate key"):
        build_tone_sensitivity_analysis(
            summary_dataframe,
            _sample_leaders_dataframe(),
        )


def test_build_tone_sensitivity_analysis_rejects_missing_sample_leader():
    """Error path: gender segmentation requires a complete leader join."""
    sample_leaders_dataframe = pd.DataFrame([{"leader_id": "leader-f", "gender": "F"}])

    with pytest.raises(DataQualityError, match="missing from sample_leaders"):
        build_tone_sensitivity_analysis(
            _summary_dataframe(),
            sample_leaders_dataframe,
        )


def test_build_tone_sensitivity_analysis_rejects_invalid_probabilities():
    """Error path: top probabilities must remain valid probability values."""
    summary_dataframe = _summary_dataframe()
    summary_dataframe.loc[0, "target_tone_probability"] = 1.2

    with pytest.raises(DataQualityError, match="between 0 and 1"):
        build_tone_sensitivity_analysis(
            summary_dataframe,
            _sample_leaders_dataframe(),
        )


def test_build_tone_sensitivity_analysis_rejects_non_scored_probability():
    """Regression: skipped rows cannot inflate threshold coverage."""
    summary_dataframe = _summary_dataframe()
    summary_dataframe.loc[4, "target_tone_probability"] = 0.90

    with pytest.raises(DataQualityError, match="non-scored rows"):
        build_tone_sensitivity_analysis(
            summary_dataframe,
            _sample_leaders_dataframe(),
        )


def test_materialize_tone_sensitivity_analysis_writes_artifacts(tmp_path):
    """Integration: report, Parquet, and DuckDB outputs are queryable."""
    duckdb = pytest.importorskip("duckdb")
    report_path = tmp_path / "nlp_tone_sensitivity_report.json"
    parquet_path = tmp_path / "nlp_tone_threshold_sensitivity.parquet"
    duckdb_path = tmp_path / "warehouse.duckdb"

    analysis = materialize_tone_sensitivity_analysis(
        _summary_dataframe(),
        _sample_leaders_dataframe(),
        thresholds=[0.40, 0.60],
        report_path=report_path,
        parquet_path=parquet_path,
        duckdb_path=duckdb_path,
        configured_tone_threshold=0.60,
    )

    assert report_path.exists()
    assert parquet_path.exists()
    assert len(pd.read_parquet(parquet_path)) == len(analysis.sensitivity_table)
    with report_path.open(encoding="utf-8") as file_handle:
        report = json.load(file_handle)
    assert report["thresholds"] == [0.40, 0.60]

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM gold.nlp_tone_threshold_sensitivity"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == 6


def test_default_threshold_grid_includes_current_phase3_threshold():
    """Regression: the default audit grid includes the current 0.60 baseline."""
    assert 0.60 in DEFAULT_TONE_SENSITIVITY_THRESHOLDS
