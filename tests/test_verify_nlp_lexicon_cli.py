"""Tests for the NLP lexicon verification CLI."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pytest

from src.cli.verify_nlp_lexicon import main, verify_nlp_lexicon_artifacts
from src.storage.tables import write_duckdb_table


def _write_verification_tables(
    duckdb_path: Path,
    *,
    include_trait_counts: bool = True,
) -> None:
    """Write minimal NLP lexicon artifacts for CLI verification tests."""
    tables = [
        (
            pd.DataFrame(
                [
                    {
                        "mention_id": "mention-1",
                        "lexicon_category": "apparence",
                        "count": 1,
                    },
                    {
                        "mention_id": "mention-2",
                        "lexicon_category": "securite",
                        "count": 2,
                    },
                ]
            ),
            "silver",
            "fact_stereotype_word_counts",
        ),
        (
            pd.DataFrame(
                [
                    {"scenario_id": "all", "trait_tier": "core"},
                    {"scenario_id": "all", "trait_tier": "exploratory"},
                    {"scenario_id": "drop_top_overall", "trait_tier": "core"},
                ]
            ),
            "gold",
            "mart_trait_metrics",
        ),
        (
            pd.DataFrame([{"term": "programme"}]),
            "gold",
            "mart_trait_top_terms",
        ),
        (
            pd.DataFrame([{"leader_id": "leader-1"}]),
            "gold",
            "mart_trait_candidate_metrics",
        ),
        (
            pd.DataFrame([{"mention_id": "mention-1"}, {"mention_id": "mention-2"}]),
            "gold",
            "mart_trait_qa_samples",
        ),
    ]
    if include_trait_counts:
        tables.append(
            (
                pd.DataFrame(
                    [
                        {
                            "mention_id": "mention-1",
                            "trait_category": "political_work",
                            "count": 1,
                        },
                        {
                            "mention_id": "mention-1",
                            "trait_category": "political_work",
                            "count": 2,
                        },
                        {
                            "mention_id": "mention-2",
                            "trait_category": "security_order",
                            "count": 1,
                        },
                    ]
                ),
                "silver",
                "fact_trait_word_counts",
            )
        )

    for dataframe, schema_name, table_name in tables:
        write_duckdb_table(
            dataframe=dataframe,
            schema_name=schema_name,
            table_name=table_name,
            duckdb_path=duckdb_path,
        )


def test_verify_nlp_lexicon_artifacts_returns_summary(tmp_path):
    """Happy path: verification should summarize persisted artifacts."""
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_verification_tables(duckdb_path)

    summary = verify_nlp_lexicon_artifacts(duckdb_path)

    assert summary.stereotype_rows == 2
    assert summary.stereotype_mentions == 2
    assert summary.stereotype_categories == 2
    assert summary.stereotype_total_terms == 3
    assert summary.trait_rows == 3
    assert summary.trait_mentions == 2
    assert summary.trait_categories == 2
    assert summary.trait_total_terms == 4
    assert summary.trait_metric_rows == 3
    assert summary.scenario_count == 2
    assert summary.tier_count == 2
    assert summary.qa_sample_rows == 2


def test_verify_nlp_lexicon_artifacts_raises_when_required_table_is_missing(tmp_path):
    """Error path: missing artifacts should fail fast with table context."""
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_verification_tables(duckdb_path, include_trait_counts=False)

    with pytest.raises(RuntimeError, match="silver.fact_trait_word_counts"):
        verify_nlp_lexicon_artifacts(duckdb_path)


def test_verify_nlp_lexicon_cli_returns_zero_on_success(tmp_path, caplog):
    """CLI path: a complete artifact set should return a success exit code."""
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_verification_tables(duckdb_path)
    caplog.set_level(logging.INFO)

    exit_code = main(["--duckdb-path", str(duckdb_path)])

    assert exit_code == 0
    assert "NLP lexicon verification passed" in caplog.text


def test_verify_nlp_lexicon_cli_returns_one_on_missing_table(tmp_path, caplog):
    """CLI path: missing artifacts should return a failure exit code."""
    duckdb_path = tmp_path / "warehouse.duckdb"
    _write_verification_tables(duckdb_path, include_trait_counts=False)
    caplog.set_level(logging.ERROR)

    exit_code = main(["--duckdb-path", str(duckdb_path)])

    assert exit_code == 1
    assert "NLP lexicon verification failed" in caplog.text
