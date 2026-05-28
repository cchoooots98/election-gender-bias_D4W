"""Tests for two-tier deterministic trait lexicon metrics."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.nlp.trait_lexicon import (
    CONTROLLED_TRAIT_CATEGORIES,
    FACT_TRAIT_WORD_COUNTS_COLUMNS,
    MART_TRAIT_METRICS_COLUMNS,
    TraitLexiconConfig,
    TraitLexiconTerm,
    build_fact_trait_word_counts,
    build_trait_metric_artifacts,
    load_trait_lexicon,
    materialize_trait_metric_artifacts,
    validate_fact_trait_word_counts,
)
from src.transform._exceptions import DataQualityError


def _trait_config() -> TraitLexiconConfig:
    """Return a small two-tier lexicon for deterministic tests."""
    return TraitLexiconConfig(
        lexicon_version="trait_test_v1",
        terms=(
            TraitLexiconTerm(
                "political_work",
                "core",
                "programme",
                "Program reference.",
                ("programme",),
            ),
            TraitLexiconTerm(
                "political_work",
                "core",
                "conseil municipal",
                "Municipal council reference.",
                ("conseil", "municipal"),
            ),
            TraitLexiconTerm(
                "leadership_competence",
                "exploratory",
                "experience",
                "Experience reference.",
                ("experience",),
            ),
            TraitLexiconTerm(
                "family_private_life",
                "core",
                "famille",
                "Family reference.",
                ("famille",),
            ),
        ),
    )


def _nlp_input() -> pd.DataFrame:
    """Return valid NLP input rows for trait tests."""
    return pd.DataFrame(
        [
            {
                "mention_id": "mention-f1",
                "leader_id": "leader-f",
                "canonical_article_id": "article-1",
                "input_text": ("Le programme cite le conseil municipal et la famille."),
                "context_word_count": 8,
                "eligible_for_lexicon": True,
            },
            {
                "mention_id": "mention-m1",
                "leader_id": "leader-m",
                "canonical_article_id": "article-2",
                "input_text": "Son experience porte le projet local.",
                "context_word_count": 6,
                "eligible_for_lexicon": True,
            },
            {
                "mention_id": "mention-m2",
                "leader_id": "leader-m",
                "canonical_article_id": "article-3",
                "input_text": None,
                "context_word_count": 0,
                "eligible_for_lexicon": False,
            },
            {
                "mention_id": "mention-male-outlier",
                "leader_id": "leader-m-outlier",
                "canonical_article_id": "article-4",
                "input_text": "Le programme domine la campagne.",
                "context_word_count": 5,
                "eligible_for_lexicon": True,
            },
            {
                "mention_id": "mention-female-outlier",
                "leader_id": "leader-f-outlier",
                "canonical_article_id": "article-5",
                "input_text": "Aucune categorie stable.",
                "context_word_count": 3,
                "eligible_for_lexicon": True,
            },
        ]
    )


def _sample_leaders() -> pd.DataFrame:
    """Return sample leaders for trait aggregation tests."""
    return pd.DataFrame(
        [
            {
                "leader_id": "leader-f",
                "full_name": "Candidate Female",
                "gender": "F",
                "commune_name": "Commune F",
            },
            {
                "leader_id": "leader-m",
                "full_name": "Candidate Male",
                "gender": "M",
                "commune_name": "Commune M",
            },
            {
                "leader_id": "leader-m-outlier",
                "full_name": "Male Outlier",
                "gender": "M",
                "commune_name": "Big City",
            },
            {
                "leader_id": "leader-f-outlier",
                "full_name": "Female Outlier",
                "gender": "F",
                "commune_name": "Medium City",
            },
        ]
    )


def _exposure_metrics() -> pd.DataFrame:
    """Return exposure rows with one male and one female outlier."""
    return pd.DataFrame(
        [
            {"leader_id": "leader-f", "gender": "F", "article_count": 10},
            {"leader_id": "leader-m", "gender": "M", "article_count": 20},
            {"leader_id": "leader-m-outlier", "gender": "M", "article_count": 100},
            {"leader_id": "leader-f-outlier", "gender": "F", "article_count": 80},
        ]
    )


def test_build_fact_trait_word_counts_counts_core_and_exploratory_terms():
    """Happy path: single- and multi-token trait terms should be counted."""
    trait_counts = build_fact_trait_word_counts(_nlp_input(), _trait_config())

    assert tuple(trait_counts.columns) == FACT_TRAIT_WORD_COUNTS_COLUMNS
    output_counts = {
        (row.mention_id, row.trait_tier, row.trait_category, row.term): row.count
        for row in trait_counts.itertuples(index=False)
    }

    assert output_counts[("mention-f1", "core", "political_work", "programme")] == 1
    assert (
        output_counts[("mention-f1", "core", "political_work", "conseil municipal")]
        == 1
    )
    assert output_counts[("mention-f1", "core", "family_private_life", "famille")] == 1
    assert (
        output_counts[
            ("mention-m1", "exploratory", "leadership_competence", "experience")
        ]
        == 1
    )
    assert "mention-m2" not in set(trait_counts["mention_id"])


def test_build_fact_trait_word_counts_returns_empty_schema_when_no_terms_match():
    """Boundary: valid input with no matches should keep the output schema."""
    nlp_input = _nlp_input().assign(input_text="Aucun terme utile.")

    trait_counts = build_fact_trait_word_counts(nlp_input, _trait_config())

    assert trait_counts.empty
    assert tuple(trait_counts.columns) == FACT_TRAIT_WORD_COUNTS_COLUMNS


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "payload must be a JSON object"),
        ({"lexicon_version": "", "terms": []}, "lexicon_version"),
        ({"lexicon_version": "v1", "terms": {}}, "terms must be a list"),
        (
            {
                "lexicon_version": "v1",
                "terms": [
                    {
                        "term": "programme",
                        "category": "political_work",
                        "tier": "core",
                    }
                ],
            },
            "missing required fields",
        ),
        (
            {
                "lexicon_version": "v1",
                "terms": [
                    {
                        "term": "programme",
                        "category": "unsupported",
                        "tier": "core",
                        "rationale": "Bad category.",
                    }
                ],
            },
            "unsupported trait category",
        ),
        (
            {
                "lexicon_version": "v1",
                "terms": [
                    {
                        "term": "programme",
                        "category": "political_work",
                        "tier": "bad",
                        "rationale": "Bad tier.",
                    }
                ],
            },
            "unsupported trait tier",
        ),
        (
            {
                "lexicon_version": "v1",
                "terms": [
                    {
                        "term": "programme",
                        "category": "political_work",
                        "tier": "core",
                        "rationale": "Duplicate.",
                    },
                    {
                        "term": "programme",
                        "category": "political_work",
                        "tier": "core",
                        "rationale": "Duplicate.",
                    },
                ],
            },
            "duplicate trait lexicon term",
        ),
    ],
)
def test_load_trait_lexicon_rejects_malformed_config(tmp_path, payload, message):
    """Error path: trait lexicon JSON should fail fast on bad contracts."""
    lexicon_path = tmp_path / "trait_terms_bad.json"
    lexicon_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_trait_lexicon(lexicon_path)


def test_load_trait_lexicon_loads_packaged_default():
    """Happy path: packaged v1 trait lexicon should be loadable."""
    trait_config = load_trait_lexicon()

    assert trait_config.lexicon_version == "trait_terms_v1"
    assert {term.trait_category for term in trait_config.terms} == set(
        CONTROLLED_TRAIT_CATEGORIES
    )
    assert {term.trait_tier for term in trait_config.terms} == {
        "core",
        "exploratory",
    }


def test_build_trait_metric_artifacts_keeps_zero_rows_and_evidence_flags():
    """Happy path: Gold metrics should keep all categories and sparse flags."""
    trait_counts = build_fact_trait_word_counts(_nlp_input(), _trait_config())

    artifacts = build_trait_metric_artifacts(
        nlp_input_dataframe=_nlp_input(),
        sample_leaders_dataframe=_sample_leaders(),
        exposure_metrics_dataframe=_exposure_metrics(),
        trait_word_counts_dataframe=trait_counts,
        generated_at="2026-05-26T00:00:00+00:00",
    )

    assert tuple(artifacts.metrics.columns) == MART_TRAIT_METRICS_COLUMNS
    all_core_f = artifacts.metrics.loc[
        artifacts.metrics["scenario_id"].eq("all")
        & artifacts.metrics["trait_tier"].eq("core")
        & artifacts.metrics["gender"].eq("F")
        & artifacts.metrics["trait_category"].eq("political_work")
    ].iloc[0]
    assert all_core_f["mention_count"] == 2
    assert all_core_f["hit_mentions"] == 1
    assert all_core_f["term_hits"] == 2
    assert all_core_f["evidence_level"] == "table_only"

    zero_category = artifacts.metrics.loc[
        artifacts.metrics["scenario_id"].eq("all")
        & artifacts.metrics["trait_tier"].eq("core")
        & artifacts.metrics["gender"].eq("M")
        & artifacts.metrics["trait_category"].eq("romance_relationship")
    ].iloc[0]
    assert zero_category["term_hits"] == 0


def test_trait_outlier_scenarios_remove_expected_leaders():
    """Regression: symmetric outlier scenarios must remove expected leaders."""
    trait_counts = build_fact_trait_word_counts(_nlp_input(), _trait_config())

    artifacts = build_trait_metric_artifacts(
        nlp_input_dataframe=_nlp_input(),
        sample_leaders_dataframe=_sample_leaders(),
        exposure_metrics_dataframe=_exposure_metrics(),
        trait_word_counts_dataframe=trait_counts,
        generated_at="2026-05-26T00:00:00+00:00",
    )
    metrics = artifacts.metrics

    all_m = metrics.loc[
        metrics["scenario_id"].eq("all")
        & metrics["gender"].eq("M")
        & metrics["trait_tier"].eq("core")
        & metrics["trait_category"].eq("political_work")
    ].iloc[0]
    drop_overall = metrics.loc[
        metrics["scenario_id"].eq("drop_top_overall")
        & metrics["gender"].eq("M")
        & metrics["trait_tier"].eq("core")
        & metrics["trait_category"].eq("political_work")
    ].iloc[0]
    drop_each_f = metrics.loc[
        metrics["scenario_id"].eq("drop_top_each_gender")
        & metrics["gender"].eq("F")
        & metrics["trait_tier"].eq("core")
        & metrics["trait_category"].eq("political_work")
    ].iloc[0]

    assert all_m["mention_count"] == 3
    assert drop_overall["mention_count"] == 2
    assert drop_each_f["mention_count"] == 1


def test_trait_metric_artifacts_build_top_terms_candidate_rows_and_qa_samples():
    """Happy path: dashboard companion artifacts should be populated."""
    trait_counts = build_fact_trait_word_counts(_nlp_input(), _trait_config())

    artifacts = build_trait_metric_artifacts(
        nlp_input_dataframe=_nlp_input(),
        sample_leaders_dataframe=_sample_leaders(),
        exposure_metrics_dataframe=_exposure_metrics(),
        trait_word_counts_dataframe=trait_counts,
        generated_at="2026-05-26T00:00:00+00:00",
    )

    assert not artifacts.top_terms.empty
    assert artifacts.top_terms["rank"].min() == 1
    assert not artifacts.candidate_metrics.empty
    assert not artifacts.qa_samples.empty
    assert artifacts.qa_samples["context_excerpt"].str.len().max() <= 260


def test_validate_fact_trait_word_counts_rejects_contract_violations():
    """Error path: Silver trait count DQ gates should fail independently."""
    trait_counts = pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "trait_category": "political_work",
                "trait_tier": "core",
                "term": "programme",
                "count": -1,
                "count_per_1k_tokens": 1.0,
                "lexicon_version": "trait_test_v1",
                "rationale": "Program reference.",
            }
        ],
        columns=FACT_TRAIT_WORD_COUNTS_COLUMNS,
    )

    with pytest.raises(DataQualityError, match="count must be positive"):
        validate_fact_trait_word_counts(trait_counts)


def test_materialize_trait_metric_artifacts_writes_silver_and_gold(tmp_path):
    """Integration: trait artifacts should persist to Parquet and DuckDB."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    duckdb_path = tmp_path / "warehouse.duckdb"

    artifacts = materialize_trait_metric_artifacts(
        nlp_input_dataframe=_nlp_input(),
        sample_leaders_dataframe=_sample_leaders(),
        exposure_metrics_dataframe=_exposure_metrics(),
        trait_lexicon_config=_trait_config(),
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        duckdb_path=duckdb_path,
    )

    assert (silver_dir / "fact_trait_word_counts.parquet").exists()
    assert (gold_dir / "mart_trait_metrics.parquet").exists()
    assert len(pd.read_parquet(gold_dir / "mart_trait_metrics.parquet")) == len(
        artifacts.metrics
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM gold.mart_trait_metrics"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(artifacts.metrics)
