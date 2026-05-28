"""Tests for the Streamlit dashboard helpers."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import pandas as pd
import pytest

from src.dashboard.app import (
    _dataframe_to_csv_bytes,
    build_artifact_health_warnings,
    build_documentation_links,
    build_frame_distribution,
    build_frame_gender_distribution,
    build_generic_sentiment_table,
    build_hypothesis_examples_table,
    build_nlp_audit_metrics,
    build_nlp_bias_table,
    build_overview_metrics,
    build_population_adjusted_exposure_table,
    build_primary_frame_gender_distribution,
    build_regression_governance_summary,
    build_regression_model_priority_table,
    build_run_metadata,
    build_sampling_warning_callout,
    build_sampling_warnings_table,
    build_scandal_aggregation_comparison,
    build_tone_probability_distribution_table,
    build_trait_candidate_table,
    build_trait_outlier_sensitivity_table,
    build_trait_overview_table,
    build_trait_qa_samples_table,
    build_trait_top_terms_table,
    has_blocking_nlp_health_issue,
    load_dashboard_payload,
    resolve_dashboard_gold_dir,
)


def test_load_dashboard_payload_reports_missing_artifacts(tmp_path):
    """Boundary: dashboard loader should stay informative when artifacts are absent."""
    payload = load_dashboard_payload(tmp_path)

    assert payload["sample_df"].empty
    assert payload["gold_dir"] == str(tmp_path)
    assert payload["exposure_df"].empty
    assert payload["regression_df"].empty
    assert payload["framing_df"].empty
    assert payload["bias_df"].empty
    assert payload["nlp_qa_report"] == {}
    assert set(payload["missing_artifacts"]) == {
        "sample_leaders",
        "mart_exposure_metrics",
        "mart_regression_results",
        "mart_bootstrap_ci",
        "mart_analysis_summary",
        "sample_manifest",
        "news_corpus_qa_report",
    }
    assert set(payload["missing_optional_artifacts"]) == {
        "mart_framing_metrics",
        "mart_primary_frame_metrics",
        "mart_bias_indicators",
        "mart_trait_metrics",
        "mart_trait_top_terms",
        "mart_trait_candidate_metrics",
        "mart_trait_qa_samples",
        "mart_frame_article_drilldown",
        "nlp_backup_summary_sample",
        "nlp_tone_sensitivity_report",
        "nlp_tone_threshold_sensitivity",
        "nlp_qa_report",
    }


def test_resolve_dashboard_gold_dir_uses_environment_override(monkeypatch, tmp_path):
    """Happy path: deployment can pin a blessed artifact bundle by env var."""
    monkeypatch.setenv("DASHBOARD_GOLD_URI", str(tmp_path))

    assert resolve_dashboard_gold_dir() == tmp_path


def test_build_artifact_health_warnings_surfaces_cache_only_warning(tmp_path):
    """Regression: cache-only web enrichment must be visible in the dashboard."""
    payload = {
        "gold_dir": str(tmp_path),
        "qa_report": {
            "qa": {
                "warnings": [
                    "Web enrichment ran in cache-only mode: queued URL rows were handled."
                ]
            }
        },
        "nlp_qa_report": {},
    }

    warnings = build_artifact_health_warnings(payload)

    assert warnings[0]["area"] == "News corpus"
    assert "cache-only" in warnings[0]["message"]


def test_build_artifact_health_warnings_blocks_nlp_lineage_mismatch(tmp_path):
    """Regression: Q1/Q7 mention-count mismatch must block NLP interpretation."""
    payload = {
        "gold_dir": str(tmp_path),
        "qa_report": {"qa": {"mention_count": 3392}},
        "nlp_qa_report": {
            "input_coverage": {"total_mentions": 3394},
            "model_bundle": {"matches_current_config": True},
        },
    }

    warnings = build_artifact_health_warnings(payload)

    assert warnings[0]["severity"] == "error"
    assert warnings[0]["area"] == "NLP lineage"
    assert "3,394 NLP input rows vs 3,392 fact_mention rows" in warnings[0]["message"]
    assert has_blocking_nlp_health_issue(payload) is True


def test_build_artifact_health_warnings_blocks_nlp_bundle_mismatch(tmp_path):
    """Regression: stale model bundles must be red governance blockers."""
    payload = {
        "gold_dir": str(tmp_path),
        "qa_report": {"qa": {"mention_count": 10}},
        "nlp_qa_report": {
            "input_coverage": {"total_mentions": 10},
            "model_bundle": {
                "matches_current_config": False,
                "observed_nlp_model_bundle_version": "old-bundle",
                "current_config_nlp_model_bundle_version": "new-bundle",
            },
        },
    }

    warnings = build_artifact_health_warnings(payload)

    assert warnings[0]["severity"] == "error"
    assert warnings[0]["area"] == "NLP bundle"
    assert "old-bundle -> new-bundle" in warnings[0]["message"]
    assert has_blocking_nlp_health_issue(payload) is True


def test_build_hypothesis_examples_table_returns_readable_rows():
    """Happy path: dashboard can show exact NLI hypothesis strings."""
    report = {
        "hypothesis_examples": {
            "frame_hypotheses": {"scandale": "Le texte discute un scandale."},
            "tone_example_hypotheses": {
                "favorable": "Le texte presente Candidate Example de maniere favorable."
            },
        }
    }

    table = build_hypothesis_examples_table(report)

    assert table["label"].tolist() == ["scandale", "favorable"]
    assert "hypothesis" in table.columns


@pytest.mark.smoke
def test_build_overview_metrics_aggregates_materialized_artifacts(tmp_path):
    """Happy path: dashboard metrics should summarize the persisted gold artifacts."""
    sample_df = pd.DataFrame(
        [
            {"leader_id": "leader-001"},
            {"leader_id": "leader-002"},
            {"leader_id": "leader-003"},
        ]
    )
    exposure_df = pd.DataFrame(
        [
            {"leader_id": "leader-001", "article_count": 2},
            {"leader_id": "leader-002", "article_count": 0},
            {"leader_id": "leader-003", "article_count": 1},
        ]
    )
    regression_df = pd.DataFrame([{"status": "fitted"}])
    bootstrap_df = pd.DataFrame(columns=["variable_name"])
    analysis_df = pd.DataFrame(columns=["analysis_id"])
    manifest = {"triggered_warnings": [{"warning_code": "bloc"}]}
    qa_report = {
        "qa": {
            "zero_coverage_leader_count": 1,
            "canonical_article_count": 7,
        }
    }

    sample_df.to_parquet(tmp_path / "sample_leaders.parquet", index=False)
    exposure_df.to_parquet(tmp_path / "mart_exposure_metrics.parquet", index=False)
    regression_df.to_parquet(tmp_path / "mart_regression_results.parquet", index=False)
    bootstrap_df.to_parquet(tmp_path / "mart_bootstrap_ci.parquet", index=False)
    analysis_df.to_parquet(tmp_path / "mart_analysis_summary.parquet", index=False)
    (tmp_path / "sample_manifest.json").write_text(
        json.dumps(manifest),
        encoding="utf-8",
    )
    (tmp_path / "news_corpus_qa_report.json").write_text(
        json.dumps(qa_report),
        encoding="utf-8",
    )

    payload = load_dashboard_payload(tmp_path)
    assert payload["missing_artifacts"] == []

    metrics = {metric["label"]: metric for metric in build_overview_metrics(payload)}

    assert metrics["Cohort Coverage"]["value"] == "2/3"
    assert metrics["Canonical Articles"]["value"] == "7"
    assert metrics["Sampling Warnings"]["value"] == "1"
    assert metrics["Sampling Warnings"]["tone"] == "lavender"


def test_build_overview_metrics_marks_many_sampling_warnings_yellow():
    """Regression: high warning counts must not look visually neutral."""
    payload = {
        "sample_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "exposure_df": pd.DataFrame([{"article_count": 1}]),
        "regression_df": pd.DataFrame([{"status": "fitted"}]),
        "manifest": {
            "triggered_warnings": [{"warning_code": str(i)} for i in range(11)]
        },
        "qa_report": {"qa": {}},
    }

    metrics = {metric["label"]: metric for metric in build_overview_metrics(payload)}

    assert metrics["Cohort Coverage"]["value"] == "1/1"
    assert metrics["Sampling Warnings"]["value"] == "11"
    assert metrics["Sampling Warnings"]["tone"] == "yellow"


def test_build_overview_metrics_raises_on_exposure_schema_drift():
    """Regression: present-but-invalid exposure artifacts must fail fast."""
    payload = {
        "sample_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "exposure_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "regression_df": pd.DataFrame([{"status": "fitted"}]),
        "manifest": {},
        "qa_report": {"qa": {}},
    }

    with pytest.raises(KeyError, match="article_count"):
        build_overview_metrics(payload)


def test_load_dashboard_payload_reads_optional_nlp_artifacts(tmp_path):
    """Happy path: optional NLP artifacts should load without becoming required."""
    sample_df = pd.DataFrame([{"leader_id": "leader-001", "gender": "F"}])
    exposure_df = pd.DataFrame([{"leader_id": "leader-001", "article_count": 1}])
    regression_df = pd.DataFrame([{"status": "fitted"}])
    bootstrap_df = pd.DataFrame(columns=["variable_name"])
    analysis_df = pd.DataFrame(columns=["analysis_id"])
    framing_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "frame_label": "politique",
                "mention_count": 2,
                "mean_frame_score": 0.7,
            }
        ]
    )
    primary_frame_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "frame_label": "politique",
                "mention_count": 1,
                "mean_primary_frame_score": 0.7,
            }
        ]
    )
    bias_df = pd.DataFrame(
        [
            {
                "gender": "F",
                "metric_name": "nlp_inference_coverage_rate",
                "metric_value": 0.5,
            }
        ]
    )
    nlp_qa_report = {
        "input_coverage": {
            "total_mentions": 2,
            "eligible_for_inference_mentions": 1,
        },
        "output_coverage": {
            "tone": {
                "scoreable_mentions": 1,
                "classified_mentions": 1,
                "classified_share_of_scoreable": 1.0,
            },
            "framing": {
                "frame_scored_mentions": 2,
                "mentions_with_primary_frame": 1,
                "primary_frame_share_of_frame_scored": 0.5,
            },
        },
        "model_bundle": {"observed_nlp_model_bundle_version": "bundle-v1"},
        "warnings": ["Generic sentiment is a baseline diagnostic."],
    }
    trait_metrics_df = pd.DataFrame(
        [
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "gender": "F",
                "trait_category": "political_work",
                "hit_mentions": 1,
                "term_hits": 2,
                "hits_per_1k_context_words": 10.0,
                "coverage_rate": 1.0,
                "evidence_level": "table_only",
            }
        ]
    )
    trait_top_terms_df = pd.DataFrame(
        [
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "gender": "F",
                "trait_category": "political_work",
                "term": "programme",
                "term_hits": 2,
                "hit_mentions": 1,
                "rank": 1,
            }
        ]
    )
    trait_candidate_df = pd.DataFrame(
        [
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "leader_id": "leader-001",
                "full_name": "Candidate One",
                "gender": "F",
                "commune_name": "Commune One",
                "trait_category": "political_work",
                "article_count": 1,
                "mention_count": 1,
                "term_hits": 2,
                "hits_per_1k_context_words": 10.0,
                "coverage_rate": 1.0,
            }
        ]
    )
    trait_qa_df = pd.DataFrame(
        [
            {
                "trait_tier": "core",
                "trait_category": "political_work",
                "term": "programme",
                "gender": "F",
                "full_name": "Candidate One",
                "mention_id": "abcdef123456",
                "context_excerpt": "Le programme est presente.",
                "rationale": "Program reference.",
            }
        ]
    )
    tone_sensitivity_df = pd.DataFrame(
        [
            {
                "threshold": 0.6,
                "segment_type": "overall",
                "segment_value": "all",
                "classified_share_of_scoreable": 1.0,
            }
        ]
    )
    frame_drilldown_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-1",
                "gender": "F",
                "commune_name": "Paris",
                "frame_label": "scandale",
                "canonical_article_id": "article-1",
                "publication_date": "2026-03-01",
                "outlet_name": "Le Test",
                "title": "Election coverage",
                "primary_frame_probability": 0.9,
            }
        ]
    )
    backup_summary_df = pd.DataFrame(
        [
            {
                "mention_id": "mention-1",
                "target_tone_label": "neutral",
                "primary_frame_label": "politique",
            }
        ]
    )

    sample_df.to_parquet(tmp_path / "sample_leaders.parquet", index=False)
    exposure_df.to_parquet(tmp_path / "mart_exposure_metrics.parquet", index=False)
    regression_df.to_parquet(tmp_path / "mart_regression_results.parquet", index=False)
    bootstrap_df.to_parquet(tmp_path / "mart_bootstrap_ci.parquet", index=False)
    analysis_df.to_parquet(tmp_path / "mart_analysis_summary.parquet", index=False)
    framing_df.to_parquet(tmp_path / "mart_framing_metrics.parquet", index=False)
    primary_frame_df.to_parquet(
        tmp_path / "mart_primary_frame_metrics.parquet",
        index=False,
    )
    bias_df.to_parquet(tmp_path / "mart_bias_indicators.parquet", index=False)
    trait_metrics_df.to_parquet(tmp_path / "mart_trait_metrics.parquet", index=False)
    trait_top_terms_df.to_parquet(
        tmp_path / "mart_trait_top_terms.parquet",
        index=False,
    )
    trait_candidate_df.to_parquet(
        tmp_path / "mart_trait_candidate_metrics.parquet",
        index=False,
    )
    trait_qa_df.to_parquet(tmp_path / "mart_trait_qa_samples.parquet", index=False)
    tone_sensitivity_df.to_parquet(
        tmp_path / "nlp_tone_threshold_sensitivity.parquet",
        index=False,
    )
    frame_drilldown_df.to_parquet(
        tmp_path / "mart_frame_article_drilldown.parquet",
        index=False,
    )
    backup_summary_df.to_parquet(
        tmp_path / "nlp_backup_summary_sample.parquet",
        index=False,
    )
    (tmp_path / "sample_manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "news_corpus_qa_report.json").write_text(
        json.dumps({"qa": {}}),
        encoding="utf-8",
    )
    (tmp_path / "nlp_qa_report.json").write_text(
        json.dumps(nlp_qa_report),
        encoding="utf-8",
    )
    (tmp_path / "nlp_tone_sensitivity_report.json").write_text(
        json.dumps({"probability_bins_by_current_label": []}),
        encoding="utf-8",
    )

    payload = load_dashboard_payload(tmp_path)

    assert payload["missing_artifacts"] == []
    assert payload["missing_optional_artifacts"] == []
    assert len(payload["framing_df"]) == 1
    assert len(payload["primary_frame_df"]) == 1
    assert len(payload["bias_df"]) == 1
    assert len(payload["trait_metrics_df"]) == 1
    assert len(payload["tone_sensitivity_df"]) == 1
    metrics = {
        metric["label"]: metric["value"] for metric in build_nlp_audit_metrics(payload)
    }
    assert metrics["NLP Mentions"] == "2"
    assert metrics["Inference Eligible"] == "1 / 2 (50%)"
    assert metrics["Tone Classified"] == "100% at theta=0.60"
    assert metrics["Frame Classified"] == "50% at theta=0.60"


def test_build_nlp_audit_metrics_marks_low_tone_coverage_yellow_with_anchors():
    """Regression: low tone classification is a threshold diagnostic, not LOW text."""
    payload = {
        "nlp_qa_report": {
            "input_coverage": {
                "total_mentions": 100,
                "eligible_for_inference_mentions": 96,
            },
            "output_coverage": {
                "tone": {
                    "scoreable_mentions": 100,
                    "classified_mentions": 24,
                    "classified_share_of_scoreable": 0.24,
                },
                "framing": {
                    "frame_scored_mentions": 100,
                    "mentions_with_primary_frame": 92,
                    "primary_frame_share_of_frame_scored": 0.92,
                },
            },
            "model_bundle": {"observed_nlp_model_bundle_version": "bundle-v1"},
            "warnings": [],
        },
        "tone_sensitivity_df": pd.DataFrame(
            [
                {
                    "threshold": 0.5,
                    "segment_type": "overall",
                    "segment_value": "all",
                    "classified_share_of_scoreable": 0.51,
                },
                {
                    "threshold": 0.4,
                    "segment_type": "overall",
                    "segment_value": "all",
                    "classified_share_of_scoreable": 0.84,
                },
            ]
        ),
    }

    metrics = {metric["label"]: metric for metric in build_nlp_audit_metrics(payload)}

    assert metrics["Tone Classified"]["value"] == "24% at theta=0.60"
    assert metrics["Tone Classified"]["tone"] == "yellow"
    assert metrics["Tone Classified"]["status"] == "Low coverage"
    assert "at 0.5: 51%" in metrics["Tone Classified"]["help"]
    assert "LOW" not in metrics["Tone Classified"]["value"]


def test_build_tone_probability_distribution_table_uses_current_label_bins():
    """Regression: Q7 probability histogram must be driven by governed report data."""
    tone_probability_df = build_tone_probability_distribution_table(
        {
            "probability_bins_by_current_label": [
                {
                    "segment_type": "overall",
                    "segment_value": "all",
                    "target_tone_label": "unclassified",
                    "probability_bin": "0.50-0.60",
                    "mentions": 76,
                },
                {
                    "segment_type": "gender",
                    "segment_value": "F",
                    "target_tone_label": "unclassified",
                    "probability_bin": "0.50-0.60",
                    "mentions": 22,
                },
                {
                    "segment_type": "overall",
                    "segment_value": "all",
                    "target_tone_label": "favorable",
                    "probability_bin": "0.60-0.70",
                    "mentions": 12,
                },
            ]
        }
    )

    assert tone_probability_df["segment_type"].eq("overall").all()
    lookup = {
        (row.target_tone_label, str(row.probability_bin)): row.mentions
        for row in tone_probability_df.itertuples(index=False)
    }
    assert lookup[("unclassified", "0.50-0.60")] == 76
    assert lookup[("favorable", "0.60-0.70")] == 12


def test_build_run_metadata_uses_latest_artifact_timestamp():
    """Happy path: run metadata should expose IDs, cohort, window, and snapshot."""
    payload = {
        "manifest": {
            "run_id": "manifest-run",
            "created_at": "2026-04-09T23:08:01+00:00",
            "sampling_rule_version": "v11_metropolitan_36",
            "total_sampled": 36,
        },
        "qa_report": {
            "run_id": "news-run-123456789",
            "batch_id": "europresse_batch",
        },
        "nlp_qa_report": {"generated_at": "2026-05-16T10:58:28+00:00"},
    }

    metadata = build_run_metadata(
        payload,
        as_of=datetime(2026, 5, 26, tzinfo=UTC),
    )

    assert metadata["run_id"] == "news-run-123456789"
    assert metadata["batch_id"] == "europresse_batch"
    assert metadata["cohort"] == "36-leader stratified quota cohort (rule v11)"
    assert metadata["cohort_rule"] == "v11_metropolitan_36"
    assert metadata["analysis_window"] == "2025-11-01 -> 2026-04-30"
    assert metadata["snapshot_label"] == "2026-05-16 10:58 UTC"
    assert metadata["data_age_label"] == "10 days"
    assert metadata["data_age_tone"] == "neutral"
    assert "freshness_status" not in metadata


def test_build_run_metadata_marks_old_snapshot_yellow():
    """Boundary: snapshot age should be visible without treating frozen data as live."""
    payload = {
        "manifest": {"created_at": "2026-04-01T00:00:00+00:00"},
        "qa_report": {},
        "nlp_qa_report": {},
    }

    metadata = build_run_metadata(
        payload,
        as_of=datetime(2026, 5, 27, tzinfo=UTC),
    )

    assert metadata["data_age_label"] == "56 days"
    assert metadata["data_age_tone"] == "yellow"


def test_build_run_metadata_uses_oldest_required_artifact_mtime(tmp_path):
    """Regression: data age should reflect the oldest required artifact."""
    for file_name in [
        "sample_leaders.parquet",
        "mart_exposure_metrics.parquet",
        "mart_regression_results.parquet",
        "mart_bootstrap_ci.parquet",
        "mart_analysis_summary.parquet",
    ]:
        pd.DataFrame([{"value": 1}]).to_parquet(tmp_path / file_name, index=False)
    for file_name in ["sample_manifest.json", "news_corpus_qa_report.json"]:
        (tmp_path / file_name).write_text("{}", encoding="utf-8")
    oldest_timestamp = datetime(2026, 4, 10, tzinfo=UTC).timestamp()
    newest_timestamp = datetime(2026, 5, 28, tzinfo=UTC).timestamp()
    for artifact_path in tmp_path.iterdir():
        os.utime(artifact_path, (newest_timestamp, newest_timestamp))
    os.utime(
        tmp_path / "sample_leaders.parquet",
        (oldest_timestamp, oldest_timestamp),
    )

    metadata = build_run_metadata(
        {
            "gold_dir": str(tmp_path),
            "manifest": {},
            "qa_report": {},
            "nlp_qa_report": {"generated_at": "2026-05-28T00:00:00+00:00"},
        },
        as_of=datetime(2026, 5, 29, tzinfo=UTC),
    )

    assert metadata["data_age_label"] == "49 days"
    assert metadata["data_age_source"] == "oldest required artifact"
    assert metadata["data_age_tone"] == "yellow"


def test_build_sampling_warnings_table_flattens_manifest_warnings():
    """Happy path: sampling warnings should be visible rather than black-box counts."""
    warnings_df = build_sampling_warnings_table(
        {
            "triggered_warnings": [
                {
                    "warning_code": "political_bloc_concentration",
                    "scope": "city_size_bucket_gender:large:F",
                    "dimension": "nuance_group",
                    "value": "gauche",
                    "count": 2,
                    "denominator": 3,
                    "share": 0.667,
                    "threshold": 0.5,
                    "recommended_action": "Include nuance_group as a regression control variable.",
                },
                {
                    "warning_code": "region_cap",
                    "scope": "sample",
                    "dimension": "reg_code",
                    "value": "76",
                    "share": 0.25,
                    "threshold": 0.2,
                    "recommended_action": "Review region balance.",
                },
            ]
        }
    )

    assert warnings_df.loc[0, "warning_code"] == "political_bloc_concentration"
    assert warnings_df.loc[0, "over_threshold"] == pytest.approx(0.167)
    assert "political bloc concentration" in build_sampling_warning_callout(warnings_df)


def test_build_documentation_links_use_public_urls():
    """Regression: Streamlit-hosted relative doc links return 404."""
    doc_links = build_documentation_links()

    assert set(doc_links) == {
        "Architecture",
        "Metric dictionary",
        "Limitations",
        "Deployment",
    }
    assert all(link.startswith("https://github.com/") for link in doc_links.values())


def test_build_nlp_bias_table_filters_gold_nlp_metrics():
    """Happy path: dashboard should display only NLP rows from bias indicators."""
    bias_df = pd.DataFrame(
        [
            {
                "gender": "F",
                "metric_name": "mean_article_count",
                "metric_value": 10.0,
            },
            {
                "gender": "F",
                "metric_name": "mean_unfavorable_tone_share",
                "metric_value": 0.25,
            },
            {
                "gender": "M",
                "metric_name": "mean_unfavorable_tone_share",
                "metric_value": 0.10,
            },
        ]
    )

    nlp_bias_df = build_nlp_bias_table(bias_df)

    assert set(nlp_bias_df["metric_name"]) == {"mean_unfavorable_tone_share"}
    assert len(nlp_bias_df) == 2


def test_build_generic_sentiment_table_filters_baseline_metrics():
    """Happy path: generic sentiment should be isolated as baseline diagnostics."""
    bias_df = pd.DataFrame(
        [
            {
                "gender": "F",
                "metric_name": "generic_sentiment_coverage_rate",
                "metric_value": 0.9,
            },
            {
                "gender": "M",
                "metric_name": "mean_generic_sentiment_score",
                "metric_value": -0.1,
            },
            {
                "gender": "F",
                "metric_name": "mean_policy_frame_share",
                "metric_value": 0.4,
            },
        ]
    )

    generic_df = build_generic_sentiment_table(bias_df)

    assert set(generic_df["metric_name"]) == {
        "generic_sentiment_coverage_rate",
        "mean_generic_sentiment_score",
    }
    assert len(generic_df) == 2


def test_build_population_adjusted_exposure_table_reproduces_key_segments():
    """Happy path: per-capita exposure summaries should be dashboard-verifiable."""
    exposure_df = pd.DataFrame(
        [
            {
                "gender": "F",
                "city_size_bucket": "medium",
                "exposure_per_10k_population": 22.0,
            },
            {
                "gender": "F",
                "city_size_bucket": "large",
                "exposure_per_10k_population": 43.4,
            },
            {
                "gender": "M",
                "city_size_bucket": "medium",
                "exposure_per_10k_population": 9.6,
            },
            {
                "gender": "M",
                "city_size_bucket": "large",
                "exposure_per_10k_population": 45.4,
            },
        ]
    )

    population_df = build_population_adjusted_exposure_table(exposure_df)
    lookup = {
        (row.segment, row.gender): row.exposure_per_10k_population
        for row in population_df.itertuples(index=False)
    }

    assert lookup[("overall", "F")] == pytest.approx(32.7)
    assert lookup[("overall", "M")] == pytest.approx(27.5)
    assert lookup[("medium", "F")] == pytest.approx(22.0)
    assert lookup[("medium", "M")] == pytest.approx(9.6)


def test_build_frame_distribution_aggregates_gold_frame_rows():
    """Happy path: frame distribution should aggregate leader-level Gold rows."""
    framing_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "frame_label": "politique",
                "mention_count": 2,
                "mean_frame_score": 0.8,
            },
            {
                "leader_id": "leader-002",
                "frame_label": "politique",
                "mention_count": 1,
                "mean_frame_score": 0.6,
            },
        ]
    )

    frame_distribution_df = build_frame_distribution(framing_df)

    assert frame_distribution_df.loc[0, "frame_label"] == "politique"
    assert frame_distribution_df.loc[0, "mention_count"] == 3
    assert frame_distribution_df.loc[0, "mean_frame_score"] == pytest.approx(0.7)


def test_build_frame_gender_distribution_splits_frames_by_gender():
    """Happy path: frame chart rows should preserve gender comparison."""
    framing_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "frame_label": "politique",
                "mention_count": 2,
            },
            {
                "leader_id": "leader-002",
                "frame_label": "politique",
                "mention_count": 3,
            },
            {
                "leader_id": "leader-001",
                "frame_label": "unclassified",
                "mention_count": 1,
            },
        ]
    )
    sample_df = pd.DataFrame(
        [
            {"leader_id": "leader-001", "gender": "F"},
            {"leader_id": "leader-002", "gender": "M"},
        ]
    )

    frame_gender_df = build_frame_gender_distribution(framing_df, sample_df)

    lookup = {
        (str(row.frame_label), row.gender): row.mention_count
        for row in frame_gender_df.itertuples(index=False)
    }
    assert lookup[("politique", "F")] == 2
    assert lookup[("politique", "M")] == 3
    assert lookup[("unclassified", "F")] == 1


def test_build_frame_gender_distribution_rejects_null_sample_gender():
    """Regression: null gender is a cohort contract failure, not an Unknown bucket."""
    framing_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "frame_label": "politique",
                "mention_count": 2,
            }
        ]
    )
    sample_df = pd.DataFrame([{"leader_id": "leader-001", "gender": None}])

    with pytest.raises(KeyError, match="null gender"):
        build_frame_gender_distribution(framing_df, sample_df)


def test_build_primary_frame_gender_distribution_preserves_mention_denominator():
    """Regression: primary-frame counts should not exceed the mention denominator."""
    primary_frame_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "frame_label": "politique",
                "mention_count": 2,
            },
            {
                "leader_id": "leader-001",
                "frame_label": "unclassified",
                "mention_count": 1,
            },
            {
                "leader_id": "leader-002",
                "frame_label": "scandale",
                "mention_count": 3,
            },
        ]
    )
    sample_df = pd.DataFrame(
        [
            {"leader_id": "leader-001", "gender": "F"},
            {"leader_id": "leader-002", "gender": "M"},
        ]
    )

    primary_frame_gender_df = build_primary_frame_gender_distribution(
        primary_frame_df,
        sample_df,
    )

    assert int(primary_frame_gender_df["mention_count"].sum()) == 6
    lookup = {
        (str(row.frame_label), row.gender): row.mention_count
        for row in primary_frame_gender_df.itertuples(index=False)
    }
    assert lookup[("politique", "F")] == 2
    assert lookup[("scandale", "M")] == 3


def test_build_regression_model_priority_table_marks_poisson_diagnostic():
    """Regression: high Poisson dispersion should make NB the primary model."""
    regression_df = pd.DataFrame(
        [
            {
                "model_name": "poisson_exposure",
                "variable_name": "gender_female",
                "coefficient": 0.1348,
                "std_error": 0.0755,
                "p_value": 0.00000084,
                "q_value": 0.0000016,
                "status": "fitted",
            },
            {
                "model_name": "negbinom_exposure",
                "variable_name": "gender_female",
                "coefficient": -0.0278,
                "std_error": 0.432,
                "p_value": 0.9487,
                "q_value": 0.9487,
                "status": "fitted",
            },
            {
                "model_name": "poisson_exposure",
                "variable_name": "_dispersion_ratio",
                "coefficient": 33.82,
                "std_error": None,
                "p_value": None,
                "q_value": None,
                "status": "fitted",
            },
        ]
    )

    priority_df = build_regression_model_priority_table(regression_df)
    role_lookup = {
        row.model_name: row.model_role for row in priority_df.itertuples(index=False)
    }

    assert role_lookup["poisson_exposure"] == "Diagnostic only"
    assert role_lookup["negbinom_exposure"] == "Primary model"
    assert priority_df["model_name"].tolist()[0] == "negbinom_exposure"
    assert (
        "Overdispersed"
        in priority_df.loc[
            priority_df["model_name"].eq("poisson_exposure"),
            "interpretation",
        ].iloc[0]
    )
    poisson_row = priority_df.loc[
        priority_df["model_name"].eq("poisson_exposure")
    ].iloc[0]
    assert poisson_row["p_value_display"] == "8.40e-07"
    assert poisson_row["q_value_display"] == "1.60e-06"


def test_build_regression_governance_summary_surfaces_nonsignificant_primary_model():
    """Regression: Q6 headline should not imply retained gender significance."""
    regression_df = pd.DataFrame(
        [
            {
                "model_name": "poisson_exposure",
                "model_role": "Diagnostic only",
                "variable_name": "gender_female",
                "coefficient": -0.1248,
                "std_error": 0.039,
                "p_value": 0.0015,
                "q_value": 0.0076,
                "status": "fitted",
            },
            {
                "model_name": "poisson_exposure",
                "model_role": "Diagnostic only",
                "variable_name": "_dispersion_ratio",
                "coefficient": 190.23,
                "std_error": None,
                "p_value": None,
                "q_value": None,
                "status": "fitted",
            },
            {
                "model_name": "negbinom_exposure",
                "model_role": "Primary model",
                "variable_name": "gender_female",
                "coefficient": 0.1351,
                "std_error": 0.343,
                "p_value": 0.6939,
                "q_value": 0.9447,
                "status": "fitted",
            },
            {
                "model_name": "negbinom_exposure_full_controls",
                "model_role": "Sensitivity model",
                "variable_name": "gender_female",
                "coefficient": -0.1243,
                "std_error": 0.447,
                "p_value": 0.7810,
                "q_value": 0.9447,
                "status": "fitted",
            },
        ]
    )
    bootstrap_df = pd.DataFrame(
        [
            {
                "variable_name": "gender_female",
                "ci_lower_95": -1.0,
                "ci_upper_95": 1.2,
            }
        ]
    )

    summary = build_regression_governance_summary(regression_df, bootstrap_df)

    assert "does not predict article count" in summary["headline"]
    assert "adjusted p=0.945" in summary["headline"]
    assert "[-1.00, +1.20] spans zero" in summary["headline"]
    assert "dispersion ratio is 190" in summary["caveat"]
    assert "expected = 1 under Poisson" in summary["caveat"]
    assert "changes sign" in summary["caveat"]
    assert "sampling noise" in summary["caveat"]


def test_build_scandal_aggregation_comparison_surfaces_weighting_difference():
    """Regression: Q8 should make volume vs leader-mean weighting explicit."""
    primary_frame_gender_df = pd.DataFrame(
        [
            {"gender": "F", "frame_label": "scandale", "mention_count": 39},
            {"gender": "F", "frame_label": "politique", "mention_count": 61},
            {"gender": "M", "frame_label": "scandale", "mention_count": 49},
            {"gender": "M", "frame_label": "politique", "mention_count": 51},
        ]
    )
    nlp_bias_df = pd.DataFrame(
        [
            {
                "gender": "F",
                "metric_name": "mean_scandal_frame_share",
                "metric_value": 0.35,
            },
            {
                "gender": "M",
                "metric_name": "mean_scandal_frame_share",
                "metric_value": 0.35,
            },
        ]
    )

    comparison_df = build_scandal_aggregation_comparison(
        primary_frame_gender_df,
        nlp_bias_df,
    )

    assert comparison_df["aggregation"].tolist() == [
        "Volume-weighted mentions",
        "Leader-mean rates",
    ]
    assert comparison_df["evidence_level"].tolist() == [
        "Volume-weighted",
        "Leader-mean",
    ]
    assert comparison_df.iloc[0]["gap"] == pytest.approx(0.10)
    assert comparison_df.iloc[1]["gap"] == pytest.approx(0.0)


def test_dataframe_to_csv_bytes_serializes_dashboard_export():
    """Happy path: CSV export helper should return portable bytes."""
    csv_bytes = _dataframe_to_csv_bytes(
        pd.DataFrame([{"metric": "coverage", "value": 1.0}])
    )

    assert csv_bytes.startswith(b"metric,value")
    assert b"coverage,1.0" in csv_bytes


def test_build_nlp_bias_table_raises_on_schema_drift():
    """Regression: present-but-invalid NLP bias artifacts must fail fast."""
    with pytest.raises(KeyError, match="metric_value"):
        build_nlp_bias_table(pd.DataFrame([{"gender": "F", "metric_name": "coverage"}]))


def test_build_frame_distribution_raises_on_schema_drift():
    """Regression: present-but-invalid frame artifacts must fail fast."""
    with pytest.raises(KeyError, match="mean_frame_score"):
        build_frame_distribution(
            pd.DataFrame(
                [
                    {
                        "leader_id": "leader-001",
                        "frame_label": "politique",
                        "mention_count": 1,
                    }
                ]
            )
        )


def test_build_trait_dashboard_tables_filter_expected_rows():
    """Happy path: trait dashboard helpers should filter scenario and tier."""
    trait_metrics_df = pd.DataFrame(
        [
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "gender": "F",
                "trait_category": "political_work",
                "hit_mentions": 12,
                "term_hits": 18,
                "hits_per_1k_context_words": 4.5,
                "coverage_rate": 0.2,
                "evidence_level": "sparse_evidence",
            },
            {
                "scenario_id": "drop_top_overall",
                "trait_tier": "core",
                "gender": "M",
                "trait_category": "political_work",
                "hit_mentions": 8,
                "term_hits": 9,
                "hits_per_1k_context_words": 2.0,
                "coverage_rate": 0.1,
                "evidence_level": "table_only",
            },
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "gender": "M",
                "trait_category": "political_work",
                "hit_mentions": 20,
                "term_hits": 30,
                "hits_per_1k_context_words": 6.0,
                "coverage_rate": 0.3,
                "evidence_level": "chart_ready",
            },
        ]
    )
    trait_top_terms_df = pd.DataFrame(
        [
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "gender": "F",
                "trait_category": "political_work",
                "term": "programme",
                "term_hits": 5,
                "hit_mentions": 4,
                "rank": 1,
            },
            {
                "scenario_id": "all",
                "trait_tier": "exploratory",
                "gender": "F",
                "trait_category": "personality",
                "term": "dynamique",
                "term_hits": 4,
                "hit_mentions": 3,
                "rank": 1,
            },
        ]
    )
    trait_candidate_df = pd.DataFrame(
        [
            {
                "scenario_id": "all",
                "trait_tier": "core",
                "leader_id": "leader-001",
                "full_name": "Candidate One",
                "gender": "F",
                "commune_name": "Commune One",
                "trait_category": "political_work",
                "article_count": 10,
                "mention_count": 10,
                "term_hits": 3,
                "hits_per_1k_context_words": 2.5,
                "coverage_rate": 0.2,
            }
        ]
    )
    trait_qa_df = pd.DataFrame(
        [
            {
                "trait_tier": "core",
                "trait_category": "political_work",
                "term": "programme",
                "gender": "F",
                "full_name": "Candidate One",
                "mention_id": "abcdef123456",
                "context_excerpt": "Programme local.",
                "rationale": "Program reference.",
            }
        ]
    )

    assert len(build_trait_overview_table(trait_metrics_df)) == 2
    assert build_trait_top_terms_table(trait_top_terms_df)["term"].tolist() == [
        "programme"
    ]
    assert len(build_trait_candidate_table(trait_candidate_df)) == 1
    qa_samples_df = build_trait_qa_samples_table(trait_qa_df)
    assert len(qa_samples_df) == 1
    assert qa_samples_df.loc[0, "mention_id_short"] == "abcdef12"
    outlier_df = build_trait_outlier_sensitivity_table(trait_metrics_df)
    assert outlier_df.loc[
        outlier_df["scenario_id"].eq("drop_top_overall"),
        "delta_vs_all",
    ].iloc[0] == pytest.approx(-4.0)


def test_build_trait_overview_table_raises_on_schema_drift():
    """Regression: present-but-invalid trait metrics must fail fast."""
    with pytest.raises(KeyError, match="hits_per_1k_context_words"):
        build_trait_overview_table(
            pd.DataFrame(
                [
                    {
                        "scenario_id": "all",
                        "trait_tier": "core",
                        "gender": "F",
                        "trait_category": "political_work",
                    }
                ]
            )
        )
