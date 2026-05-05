"""Tests for Phase 1 deterministic NLP lexicon audit."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from src.nlp.lexicon import (
    FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    LexiconConfig,
    LexiconTerm,
    build_fact_stereotype_word_counts,
    load_stereotype_lexicon,
    materialize_fact_stereotype_word_counts,
    validate_fact_stereotype_word_counts,
)
from src.nlp.normalization import normalize_lexicon_text, tokenize_lexicon_text
from src.transform._exceptions import DataQualityError


def _lexicon_config() -> LexiconConfig:
    """Return a minimal lexicon for deterministic count tests."""
    return LexiconConfig(
        lexicon_version="test_lexicon_v1",
        terms=(
            LexiconTerm("politique", "programme", ("programme",)),
            LexiconTerm("politique", "conseil municipal", ("conseil", "municipal")),
            LexiconTerm("vie_privee", "famille", ("famille",)),
            LexiconTerm("apparence", "robe", ("robe",)),
            LexiconTerm("scandale", "polemique", ("polemique",)),
            LexiconTerm("securite", "securite", ("securite",)),
        ),
    )


def _eligible_nlp_input_dataframe(input_text: str) -> pd.DataFrame:
    """Return one eligible NLP input row for lexicon tests."""
    return pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "input_text": input_text,
                "eligible_for_lexicon": True,
            }
        ]
    )


def test_build_fact_stereotype_word_counts_counts_expected_categories():
    """Happy path: known terms produce positive category-term rows."""
    input_text = (
        "Le programme local mentionne le conseil municipal. Sa famille parle "
        "de sa robe pendant une polemique sur la securite."
    )

    stereotype_word_counts_dataframe = build_fact_stereotype_word_counts(
        _eligible_nlp_input_dataframe(input_text),
        _lexicon_config(),
    )

    assert tuple(stereotype_word_counts_dataframe.columns) == (
        FACT_STEREOTYPE_WORD_COUNTS_COLUMNS
    )
    output_counts = {
        (row.lexicon_category, row.term): row.count
        for row in stereotype_word_counts_dataframe.itertuples(index=False)
    }
    assert output_counts == {
        ("politique", "programme"): 1,
        ("politique", "conseil municipal"): 1,
        ("vie_privee", "famille"): 1,
        ("apparence", "robe"): 1,
        ("scandale", "polemique"): 1,
        ("securite", "securite"): 1,
    }
    expected_rate = 1 / len(tokenize_lexicon_text(input_text)) * 1000
    assert stereotype_word_counts_dataframe["count_per_1k_tokens"].tolist() == (
        pytest.approx([expected_rate] * 6)
    )
    assert stereotype_word_counts_dataframe["lexicon_version"].unique().tolist() == [
        "test_lexicon_v1"
    ]


def test_build_fact_stereotype_word_counts_skips_non_eligible_rows():
    """Boundary: skipped NLP input rows do not produce zero-count output rows."""
    nlp_input_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "input_text": None,
                "eligible_for_lexicon": False,
            }
        ]
    )

    stereotype_word_counts_dataframe = build_fact_stereotype_word_counts(
        nlp_input_dataframe,
        _lexicon_config(),
    )

    assert stereotype_word_counts_dataframe.empty
    assert tuple(stereotype_word_counts_dataframe.columns) == (
        FACT_STEREOTYPE_WORD_COUNTS_COLUMNS
    )


def test_build_fact_stereotype_word_counts_returns_empty_when_no_terms_match():
    """Boundary: valid eligible text with no matches keeps the output schema."""
    stereotype_word_counts_dataframe = build_fact_stereotype_word_counts(
        _eligible_nlp_input_dataframe("Alice Martin visite le marche local."),
        _lexicon_config(),
    )

    assert stereotype_word_counts_dataframe.empty
    assert tuple(stereotype_word_counts_dataframe.columns) == (
        FACT_STEREOTYPE_WORD_COUNTS_COLUMNS
    )


def test_build_fact_stereotype_word_counts_rejects_missing_input_columns():
    """Error path: Phase 1 must fail fast on a broken Phase 0 contract."""
    nlp_input_dataframe = _eligible_nlp_input_dataframe("programme").drop(
        columns=["mention_id"]
    )

    with pytest.raises(DataQualityError, match="missing required columns"):
        build_fact_stereotype_word_counts(nlp_input_dataframe, _lexicon_config())


def test_build_fact_stereotype_word_counts_rejects_duplicate_mention_id():
    """Regression: Phase 1 must fail fast on duplicate Phase 0 mention IDs."""
    nlp_input_dataframe = pd.concat(
        [
            _eligible_nlp_input_dataframe("programme"),
            _eligible_nlp_input_dataframe("famille"),
        ],
        ignore_index=True,
    )

    with pytest.raises(DataQualityError, match="duplicate key rows"):
        build_fact_stereotype_word_counts(nlp_input_dataframe, _lexicon_config())


def test_build_fact_stereotype_word_counts_rejects_blank_eligible_text():
    """Error path: eligible lexicon rows must have usable input text."""
    nlp_input_dataframe = _eligible_nlp_input_dataframe(" ")

    with pytest.raises(DataQualityError, match="non-empty input_text"):
        build_fact_stereotype_word_counts(nlp_input_dataframe, _lexicon_config())


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ([], "lexicon payload must be a JSON object"),
        ({"lexicon_version": "", "categories": []}, "lexicon_version"),
        ({"lexicon_version": "v1", "categories": {}}, "categories must be a list"),
        (
            {"lexicon_version": "v1", "categories": [{"category": "", "terms": []}]},
            "category must be non-blank",
        ),
        (
            {
                "lexicon_version": "v1",
                "categories": [{"category": "politique", "terms": []}],
            },
            "must contain terms",
        ),
        (
            {
                "lexicon_version": "v1",
                "categories": [{"category": "politique", "terms": [" ", "a"]}],
            },
            "blank term",
        ),
        (
            {
                "lexicon_version": "v1",
                "categories": [
                    {"category": "politique", "terms": ["sécurité", "securite"]}
                ],
            },
            "duplicate lexicon term",
        ),
        (
            {
                "lexicon_version": "v1",
                "categories": [{"category": "unsupported", "terms": ["test"]}],
            },
            "unsupported lexicon category",
        ),
    ],
)
def test_load_stereotype_lexicon_rejects_malformed_config(
    tmp_path,
    payload,
    message,
):
    """Error path: malformed lexicon JSON fails with actionable messages."""
    lexicon_path = tmp_path / "bad_lexicon.json"
    lexicon_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_stereotype_lexicon(lexicon_path)


def test_load_stereotype_lexicon_loads_packaged_default():
    """Happy path: the packaged minimal seed lexicon is loadable."""
    lexicon_config = load_stereotype_lexicon()

    assert lexicon_config.lexicon_version == "stereotype_terms_v1"
    assert {term.lexicon_category for term in lexicon_config.terms} == {
        "politique",
        "vie_privee",
        "apparence",
        "scandale",
        "personnalite",
        "securite",
    }


def test_fact_stereotype_word_counts_columns_are_immutable():
    """Regression: the output schema constant must not be mutable."""
    with pytest.raises(AttributeError):
        FACT_STEREOTYPE_WORD_COUNTS_COLUMNS.append("unexpected_column")


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda dataframe: pd.concat([dataframe, dataframe], ignore_index=True),
            "duplicate key rows",
        ),
        (
            lambda dataframe: dataframe.assign(count=-1),
            "count must be positive",
        ),
        (
            lambda dataframe: dataframe.assign(count_per_1k_tokens=-0.1),
            "count_per_1k_tokens is negative",
        ),
        (
            lambda dataframe: dataframe.assign(lexicon_version=" "),
            "lexicon_version has blanks",
        ),
    ],
    ids=[
        "duplicate_key",
        "negative_count",
        "negative_rate",
        "blank_version",
    ],
)
def test_validate_fact_stereotype_word_counts_rejects_contract_violations(
    mutator,
    message,
):
    """Error path: output DQ gates fail independently."""
    stereotype_word_counts_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "lexicon_category": "politique",
                "term": "programme",
                "count": 1,
                "count_per_1k_tokens": 100.0,
                "lexicon_version": "test_lexicon_v1",
            }
        ],
        columns=FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    )

    with pytest.raises(DataQualityError, match=message):
        validate_fact_stereotype_word_counts(mutator(stereotype_word_counts_dataframe))


def test_normalize_lexicon_text_handles_french_text_variants():
    """Regression: accents, apostrophes, hyphens, and whitespace normalize once."""
    raw_text = "  Sécurité, l’ordre-public d'Alice--Martin! \n Élégance "

    assert normalize_lexicon_text(raw_text) == (
        "securite l ordre public d alice martin elegance"
    )
    assert tokenize_lexicon_text(raw_text) == [
        "securite",
        "l",
        "ordre",
        "public",
        "d",
        "alice",
        "martin",
        "elegance",
    ]


def test_build_fact_stereotype_word_counts_matches_normalized_phrases():
    """Regression: normalized phrase matching handles accents and hyphens."""
    lexicon_config = LexiconConfig(
        lexicon_version="phrase_v1",
        terms=(
            LexiconTerm("securite", "ordre public", ("ordre", "public")),
            LexiconTerm("securite", "securite", ("securite",)),
        ),
    )

    stereotype_word_counts_dataframe = build_fact_stereotype_word_counts(
        _eligible_nlp_input_dataframe("La Sécurité et l'ordre-public dominent."),
        lexicon_config,
    )

    assert stereotype_word_counts_dataframe["term"].tolist() == [
        "ordre public",
        "securite",
    ]


def test_materialize_fact_stereotype_word_counts_writes_parquet_and_duckdb(tmp_path):
    """Integration: Phase 1 can materialize its Silver artifact and table."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    materialized_dataframe = materialize_fact_stereotype_word_counts(
        _eligible_nlp_input_dataframe("Le programme parle de securite."),
        lexicon_config=_lexicon_config(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )

    parquet_path = silver_dir / "fact_stereotype_word_counts.parquet"
    assert parquet_path.exists()
    persisted_dataframe = pd.read_parquet(parquet_path)
    assert len(persisted_dataframe) == len(materialized_dataframe)

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_stereotype_word_counts"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(materialized_dataframe)


def test_materialize_fact_stereotype_word_counts_is_idempotent(tmp_path):
    """Regression: repeated materialization replaces rows instead of appending."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    materialize_fact_stereotype_word_counts(
        _eligible_nlp_input_dataframe("Le programme parle de securite."),
        lexicon_config=_lexicon_config(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )
    materialized_dataframe = materialize_fact_stereotype_word_counts(
        _eligible_nlp_input_dataframe("Le programme parle de securite."),
        lexicon_config=_lexicon_config(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_stereotype_word_counts"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(materialized_dataframe) == 2
