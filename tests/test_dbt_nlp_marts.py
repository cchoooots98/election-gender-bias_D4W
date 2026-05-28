"""Integration tests for dbt-owned Phase 6 NLP Gold marts."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import pytest

from src.metrics.news.dbt_runner import read_duckdb_table, run_dbt_news_marts
from src.storage.tables import write_duckdb_table


def _require_dbt_executable() -> None:
    """Skip dbt integration tests when the active environment lacks dbt."""
    executable_name = "dbt.exe" if os.name == "nt" else "dbt"
    if Path(sys.executable).with_name(executable_name).exists():
        return
    if shutil.which("dbt"):
        return
    pytest.skip("dbt executable is not available in the active test environment")


def _write_base_news_sources(duckdb_path: Path) -> None:
    """Write minimal source tables required by dbt news marts."""
    sample_leaders_dataframe = pd.DataFrame(
        [
            {
                "leader_id": "leader-f",
                "full_name": "Leader Female",
                "gender": "F",
                "commune_name": "Commune F",
                "commune_insee": "00001",
                "city_size_bucket": "small",
                "reg_code": "11",
                "nuance_group": "centre",
                "is_incumbent": False,
                "won_final_round": True,
            },
            {
                "leader_id": "leader-m",
                "full_name": "Leader Male",
                "gender": "M",
                "commune_name": "Commune M",
                "commune_insee": "00002",
                "city_size_bucket": "small",
                "reg_code": "11",
                "nuance_group": "centre",
                "is_incumbent": False,
                "won_final_round": False,
            },
        ]
    )
    dim_commune_dataframe = pd.DataFrame(
        [
            {"commune_insee": "00001", "population": 10_000},
            {"commune_insee": "00002", "population": 20_000},
        ]
    )
    fact_article_dataframe = pd.DataFrame(
        [
            {
                "canonical_article_id": "article-f-1",
                "published_date": "2026-03-01",
                "outlet_name_normalized": "local one",
                "title": "Female leader launches campaign",
                "rights_class": "restricted_local",
                "acquisition_methods": "europresse",
                "has_full_text": True,
            },
            {
                "canonical_article_id": "article-f-2",
                "published_date": "2026-03-02",
                "outlet_name_normalized": "local two",
                "title": "Female leader hosts meeting",
                "rights_class": "restricted_local",
                "acquisition_methods": "europresse",
                "has_full_text": True,
            },
            {
                "canonical_article_id": "article-m-1",
                "published_date": "2026-03-03",
                "outlet_name_normalized": "local one",
                "title": "Male leader faces scandal question",
                "rights_class": "restricted_local",
                "acquisition_methods": "europresse",
                "has_full_text": True,
            },
        ]
    )
    fact_mention_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-f-1",
                "canonical_article_id": "article-f-1",
                "leader_id": "leader-f",
                "headline_mention_flag": True,
            },
            {
                "mention_id": "mention-f-2",
                "canonical_article_id": "article-f-2",
                "leader_id": "leader-f",
                "headline_mention_flag": False,
            },
            {
                "mention_id": "mention-m-1",
                "canonical_article_id": "article-m-1",
                "leader_id": "leader-m",
                "headline_mention_flag": False,
            },
        ]
    )

    for dataframe, schema_name, table_name in [
        (sample_leaders_dataframe, "gold", "sample_leaders"),
        (dim_commune_dataframe, "silver", "dim_commune"),
        (fact_article_dataframe, "silver", "fact_article"),
        (fact_mention_dataframe, "silver", "fact_mention"),
    ]:
        write_duckdb_table(
            dataframe=dataframe,
            schema_name=schema_name,
            table_name=table_name,
            duckdb_path=duckdb_path,
        )


def _write_nlp_sources(duckdb_path: Path) -> None:
    """Write minimal Phase 0-4 NLP sources for Gold activation tests."""
    nlp_input_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-f-1",
                "leader_id": "leader-f",
                "eligible_for_lexicon": True,
            },
            {
                "mention_id": "mention-f-2",
                "leader_id": "leader-f",
                "eligible_for_lexicon": False,
            },
            {
                "mention_id": "mention-m-1",
                "leader_id": "leader-m",
                "eligible_for_lexicon": True,
            },
        ]
    )
    nlp_summary_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-f-1",
                "canonical_article_id": "article-f-1",
                "leader_id": "leader-f",
                "nlp_enrichment_status": "scored",
                "generic_sentiment_label": "positive",
                "generic_sentiment_score": 0.5,
                "target_tone_label": "unfavorable",
                "target_tone_probability": 0.91,
                "primary_frame_label": "politique",
            },
            {
                "mention_id": "mention-f-2",
                "canonical_article_id": "article-f-2",
                "leader_id": "leader-f",
                "nlp_enrichment_status": "scored",
                "generic_sentiment_label": "neutral",
                "generic_sentiment_score": 0.0,
                "target_tone_label": "unclassified",
                "target_tone_probability": None,
                "primary_frame_label": "unclassified",
            },
            {
                "mention_id": "mention-m-1",
                "canonical_article_id": "article-m-1",
                "leader_id": "leader-m",
                "nlp_enrichment_status": "scored",
                "generic_sentiment_label": "negative",
                "generic_sentiment_score": -0.5,
                "target_tone_label": "neutral",
                "target_tone_probability": 0.84,
                "primary_frame_label": "unclassified",
            },
        ]
    )
    frame_score_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-f-1",
                "frame_label": "politique",
                "frame_probability": 0.80,
                "passes_threshold": True,
            },
            {
                "mention_id": "mention-f-1",
                "frame_label": "apparence",
                "frame_probability": 0.20,
                "passes_threshold": False,
            },
            {
                "mention_id": "mention-m-1",
                "frame_label": "politique",
                "frame_probability": 0.30,
                "passes_threshold": False,
            },
            {
                "mention_id": "mention-m-1",
                "frame_label": "apparence",
                "frame_probability": 0.40,
                "passes_threshold": False,
            },
        ]
    )
    stereotype_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-f-1",
                "lexicon_category": "apparence",
                "term": "apparence",
                "count": 1,
                "count_per_1k_tokens": 2.0,
                "lexicon_version": "stereotype_terms_v1",
            }
        ]
    )

    for dataframe, table_name in [
        (nlp_input_dataframe, "fact_mention_nlp_input"),
        (nlp_summary_dataframe, "fact_mention_nlp_summary"),
        (frame_score_dataframe, "fact_mention_frame_score"),
        (stereotype_dataframe, "fact_stereotype_word_counts"),
    ]:
        write_duckdb_table(
            dataframe=dataframe,
            schema_name="silver",
            table_name=table_name,
            duckdb_path=duckdb_path,
        )


def _write_male_only_nlp_sources(duckdb_path: Path) -> None:
    """Write NLP rows where one gender has no scored mention contexts."""
    nlp_input_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-m-1",
                "leader_id": "leader-m",
                "eligible_for_lexicon": True,
            }
        ]
    )
    nlp_summary_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-m-1",
                "canonical_article_id": "article-m-1",
                "leader_id": "leader-m",
                "nlp_enrichment_status": "scored",
                "generic_sentiment_label": "negative",
                "generic_sentiment_score": -0.5,
                "target_tone_label": "unfavorable",
                "target_tone_probability": 0.91,
                "primary_frame_label": "scandale",
            }
        ]
    )
    frame_score_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-m-1",
                "frame_label": "scandale",
                "frame_probability": 0.90,
                "passes_threshold": True,
            }
        ]
    )
    stereotype_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-m-1",
                "lexicon_category": "apparence",
                "term": "apparence",
                "count": 1,
                "count_per_1k_tokens": 2.0,
                "lexicon_version": "stereotype_terms_v1",
            }
        ]
    )

    for dataframe, table_name in [
        (nlp_input_dataframe, "fact_mention_nlp_input"),
        (nlp_summary_dataframe, "fact_mention_nlp_summary"),
        (frame_score_dataframe, "fact_mention_frame_score"),
        (stereotype_dataframe, "fact_stereotype_word_counts"),
    ]:
        write_duckdb_table(
            dataframe=dataframe,
            schema_name="silver",
            table_name=table_name,
            duckdb_path=duckdb_path,
        )


def test_dbt_marts_keep_unclassified_fallback_without_nlp_sources(tmp_path):
    """Fallback: news marts must still build before NLP Silver tables exist."""
    _require_dbt_executable()
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_base_news_sources(duckdb_path)

    run_dbt_news_marts(duckdb_path=duckdb_path)

    framing_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_framing_metrics",
    )
    primary_frame_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_primary_frame_metrics",
    )
    bias_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_bias_indicators",
    )

    assert len(framing_dataframe) == 14
    assert len(primary_frame_dataframe) == 14
    assert (
        framing_dataframe.loc[
            (framing_dataframe["leader_id"] == "leader-f")
            & (framing_dataframe["frame_label"] == "unclassified"),
            "mention_count",
        ].iloc[0]
        == 2
    )
    assert (
        primary_frame_dataframe.loc[
            (primary_frame_dataframe["leader_id"] == "leader-f")
            & (primary_frame_dataframe["frame_label"] == "unclassified"),
            "mention_count",
        ].iloc[0]
        == 2
    )
    assert "nlp_inference_coverage_rate" not in set(bias_dataframe["metric_name"])


def test_mart_analysis_summary_always_contains_a1_to_a4_sections(tmp_path):
    """Regression: core analysis sections must exist before NLP activation."""
    _require_dbt_executable()
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_base_news_sources(duckdb_path)

    run_dbt_news_marts(duckdb_path=duckdb_path)

    analysis_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_analysis_summary",
    )

    assert {"A1", "A2", "A3", "A4"}.issubset(
        set(analysis_dataframe["analysis_section_id"])
    )


def test_dbt_marts_activate_nlp_metrics_when_silver_outputs_exist(tmp_path):
    """Activation: NLP Silver outputs should promote into Gold marts."""
    _require_dbt_executable()
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_base_news_sources(duckdb_path)
    _write_nlp_sources(duckdb_path)

    run_dbt_news_marts(duckdb_path=duckdb_path)

    framing_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_framing_metrics",
    )
    primary_frame_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_primary_frame_metrics",
    )
    bias_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_bias_indicators",
    )
    analysis_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_analysis_summary",
    )

    assert len(framing_dataframe) == 14
    assert len(primary_frame_dataframe) == 14
    assert (
        framing_dataframe.loc[
            (framing_dataframe["leader_id"] == "leader-f")
            & (framing_dataframe["frame_label"] == "politique"),
            "mention_count",
        ].iloc[0]
        == 1
    )
    assert (
        framing_dataframe.loc[
            (framing_dataframe["leader_id"] == "leader-f")
            & (framing_dataframe["frame_label"] == "unclassified"),
            "mention_count",
        ].iloc[0]
        == 1
    )
    assert (
        primary_frame_dataframe.loc[
            (primary_frame_dataframe["leader_id"] == "leader-f")
            & (primary_frame_dataframe["frame_label"] == "politique"),
            "mention_count",
        ].iloc[0]
        == 1
    )
    assert (
        primary_frame_dataframe.loc[
            (primary_frame_dataframe["leader_id"] == "leader-f")
            & (primary_frame_dataframe["frame_label"] == "unclassified"),
            "mention_count",
        ].iloc[0]
        == 1
    )
    assert int(primary_frame_dataframe["mention_count"].sum()) == 3
    metric_lookup = {
        (row.gender, row.metric_name): row.metric_value
        for row in bias_dataframe.itertuples(index=False)
    }
    assert metric_lookup[("F", "nlp_inference_coverage_rate")] == 1.0
    assert metric_lookup[("F", "mean_unfavorable_tone_share")] == 1.0
    assert metric_lookup[("M", "mean_unfavorable_tone_share")] == 0.0
    assert metric_lookup[("F", "mean_policy_frame_share")] == 1.0
    assert metric_lookup[("F", "mean_scandal_frame_share")] == 0.0
    assert metric_lookup[("M", "mean_scandal_frame_share")] == 0.0
    assert metric_lookup[("F", "generic_sentiment_coverage_rate")] == 1.0
    assert metric_lookup[("F", "mean_generic_sentiment_score")] == pytest.approx(0.25)
    assert metric_lookup[("M", "mean_generic_sentiment_score")] == pytest.approx(-0.5)
    assert metric_lookup[("F", "mean_stereotype_count_per_1k_tokens")] == 2.0
    assert "A5" in set(analysis_dataframe["analysis_section_id"])


def test_mart_bias_indicators_handles_zero_coverage_gender_group(tmp_path):
    """Regression: a gender with no NLP rows must emit zero metrics, not NaN."""
    _require_dbt_executable()
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_base_news_sources(duckdb_path)
    _write_male_only_nlp_sources(duckdb_path)

    run_dbt_news_marts(duckdb_path=duckdb_path)

    bias_dataframe = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_bias_indicators",
    )
    metric_lookup = {
        (row.gender, row.metric_name): row.metric_value
        for row in bias_dataframe.itertuples(index=False)
    }

    assert metric_lookup[("F", "nlp_inference_coverage_rate")] == 0.0
    assert metric_lookup[("F", "mean_unfavorable_tone_share")] == 0.0
    assert metric_lookup[("F", "mean_scandal_frame_share")] == 0.0
    assert metric_lookup[("M", "mean_unfavorable_tone_share")] == 1.0
