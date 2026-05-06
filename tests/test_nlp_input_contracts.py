"""Tests for Phase 0 NLP input contract preparation."""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from src.nlp.input_contracts import (
    FACT_MENTION_NLP_INPUT_COLUMNS,
    build_fact_mention_nlp_input,
    compute_input_hash,
    materialize_fact_mention_nlp_input,
    validate_fact_mention_nlp_input,
)
from src.transform._exceptions import DataQualityError


def _base_fact_mention_dataframe() -> pd.DataFrame:
    """Return minimal valid mention facts for NLP input tests."""
    return pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "canonical_article_id": "article-001",
                "leader_id": "leader-001",
                "context_sentences": (
                    "Alice Martin presente   son programme local.\n"
                    "Elle defend le logement public devant les habitants."
                ),
            }
        ]
    )


def _base_fact_article_dataframe(language: str | None = "fr") -> pd.DataFrame:
    """Return minimal valid article facts for NLP input tests."""
    return pd.DataFrame(
        [
            {
                "canonical_article_id": "article-001",
                "language": language,
            }
        ]
    )


def _empty_fact_mention_dataframe() -> pd.DataFrame:
    """Return an empty mention fact table with the required source schema."""
    return pd.DataFrame(
        columns=[
            "mention_id",
            "canonical_article_id",
            "leader_id",
            "context_sentences",
        ]
    )


def test_build_fact_mention_nlp_input_creates_inference_eligible_row():
    """Happy path: valid French mention context becomes a model-ready row."""
    prepared_at = pd.Timestamp("2026-04-01T12:00:00Z")

    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        prepared_at=prepared_at,
    )

    expected_text = (
        "Alice Martin presente son programme local. Elle defend le logement "
        "public devant les habitants."
    )
    output_row = nlp_input_dataframe.iloc[0]
    assert tuple(nlp_input_dataframe.columns) == FACT_MENTION_NLP_INPUT_COLUMNS
    assert output_row["article_language"] == "fr"
    assert output_row["input_text"] == expected_text
    assert output_row["input_hash"] == compute_input_hash(expected_text)
    assert output_row["context_word_count"] == 14
    assert bool(output_row["eligible_for_lexicon"]) is True
    assert bool(output_row["eligible_for_inference"]) is True
    assert output_row["skip_reason"] is None
    assert output_row["prepared_at"] == prepared_at
    assert output_row["input_contract_version"] == "mention_context_v2"


def test_build_fact_mention_nlp_input_marks_empty_context_as_skipped():
    """Boundary: empty mention context is retained as skipped, not scored."""
    fact_mention_dataframe = _base_fact_mention_dataframe()
    fact_mention_dataframe.loc[0, "context_sentences"] = " \n\t "

    nlp_input_dataframe = build_fact_mention_nlp_input(
        fact_mention_dataframe,
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )

    output_row = nlp_input_dataframe.iloc[0]
    assert output_row["input_text"] is None
    assert output_row["input_hash"] is None
    assert output_row["context_word_count"] == 0
    assert bool(output_row["eligible_for_lexicon"]) is False
    assert bool(output_row["eligible_for_inference"]) is False
    assert output_row["skip_reason"] == "empty_context"


def test_build_fact_mention_nlp_input_marks_too_short_context_for_lexicon():
    """Boundary: very short contexts are audit rows but not lexicon inputs."""
    fact_mention_dataframe = _base_fact_mention_dataframe()
    fact_mention_dataframe.loc[0, "context_sentences"] = "Alice gagne"

    nlp_input_dataframe = build_fact_mention_nlp_input(
        fact_mention_dataframe,
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )

    output_row = nlp_input_dataframe.iloc[0]
    assert output_row["input_text"] == "Alice gagne"
    assert output_row["input_hash"] == compute_input_hash("Alice gagne")
    assert output_row["context_word_count"] == 2
    assert bool(output_row["eligible_for_lexicon"]) is False
    assert bool(output_row["eligible_for_inference"]) is False
    assert output_row["skip_reason"] == "too_short_for_lexicon"


def test_build_fact_mention_nlp_input_marks_too_short_context_for_inference():
    """Boundary: mid-length contexts can feed lexicon audit but not NLI."""
    fact_mention_dataframe = _base_fact_mention_dataframe()
    fact_mention_dataframe.loc[0, "context_sentences"] = (
        "Alice Martin parle du logement"
    )

    nlp_input_dataframe = build_fact_mention_nlp_input(
        fact_mention_dataframe,
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )

    output_row = nlp_input_dataframe.iloc[0]
    assert output_row["context_word_count"] == 5
    assert bool(output_row["eligible_for_lexicon"]) is True
    assert bool(output_row["eligible_for_inference"]) is False
    assert output_row["skip_reason"] == "too_short_for_inference"


@pytest.mark.parametrize("language", ["fr", "FR", "fr-FR", "fr_CA", "FR-fr"])
def test_build_fact_mention_nlp_input_normalizes_french_language_subtags(language):
    """Boundary: regional French language tags stay eligible as French."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(language=language),
        prepared_at="2026-04-01T12:00:00Z",
    )

    output_row = nlp_input_dataframe.iloc[0]
    assert output_row["article_language"] == "fr"
    assert bool(output_row["eligible_for_lexicon"]) is True
    assert bool(output_row["eligible_for_inference"]) is True
    assert output_row["skip_reason"] is None


@pytest.mark.parametrize(
    ("language", "expected_language"),
    [("en-US", "en-us"), ("", "unknown")],
)
def test_build_fact_mention_nlp_input_applies_language_gate(
    language,
    expected_language,
):
    """DQ gate: non-French or unknown language rows are not model inputs."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(language=language),
        prepared_at="2026-04-01T12:00:00Z",
    )

    output_row = nlp_input_dataframe.iloc[0]
    assert output_row["article_language"] == expected_language
    assert bool(output_row["eligible_for_lexicon"]) is False
    assert bool(output_row["eligible_for_inference"]) is False
    assert output_row["skip_reason"] == "language_not_french"


@pytest.mark.parametrize(
    ("lexicon_words", "inference_words", "message"),
    [
        (-1, 12, "min_lexicon_words must be non-negative"),
        (3, -1, "min_inference_words must be non-negative"),
        (10, 5, "min_inference_words must be >= min_lexicon_words"),
    ],
)
def test_build_fact_mention_nlp_input_rejects_invalid_word_thresholds(
    lexicon_words,
    inference_words,
    message,
):
    """Error path: threshold contracts fail before deriving eligibility."""
    with pytest.raises(ValueError, match=message):
        build_fact_mention_nlp_input(
            _base_fact_mention_dataframe(),
            _base_fact_article_dataframe(),
            min_lexicon_words=lexicon_words,
            min_inference_words=inference_words,
        )


def test_build_fact_mention_nlp_input_rejects_missing_article_language_join():
    """Error path: every mention must resolve to an article language row."""
    fact_article_dataframe = pd.DataFrame(
        [{"canonical_article_id": "other-article", "language": "fr"}]
    )

    with pytest.raises(DataQualityError, match="without matching fact_article"):
        build_fact_mention_nlp_input(
            _base_fact_mention_dataframe(),
            fact_article_dataframe,
        )


def test_build_fact_mention_nlp_input_raises_on_missing_mention_id_column():
    """Error path: missing core source columns must fail fast."""
    fact_mention_dataframe = _base_fact_mention_dataframe().drop(columns=["mention_id"])

    with pytest.raises(DataQualityError, match="missing required columns"):
        build_fact_mention_nlp_input(
            fact_mention_dataframe,
            _base_fact_article_dataframe(),
        )


@pytest.mark.parametrize(
    ("column_name", "bad_value"),
    [
        ("mention_id", ""),
        ("canonical_article_id", None),
        ("leader_id", "  "),
    ],
)
def test_build_fact_mention_nlp_input_rejects_blank_identifier_values(
    column_name,
    bad_value,
):
    """Error path: identifier values must be present, not only the columns."""
    fact_mention_dataframe = _base_fact_mention_dataframe()
    fact_mention_dataframe.loc[0, column_name] = bad_value

    with pytest.raises(DataQualityError, match="null or blank"):
        build_fact_mention_nlp_input(
            fact_mention_dataframe,
            _base_fact_article_dataframe(),
        )


def test_build_fact_mention_nlp_input_rejects_duplicate_mention_id():
    """Regression: duplicate mention IDs must fail before table persistence."""
    fact_mention_dataframe = pd.concat(
        [_base_fact_mention_dataframe(), _base_fact_mention_dataframe()],
        ignore_index=True,
    )

    with pytest.raises(DataQualityError, match="duplicate key rows"):
        build_fact_mention_nlp_input(
            fact_mention_dataframe,
            _base_fact_article_dataframe(),
        )


def test_compute_input_hash_is_invariant_to_repeated_whitespace():
    """Unit contract: canonical hashing ignores whitespace-only formatting."""
    assert compute_input_hash("Alice  Martin\nparle") == compute_input_hash(
        "Alice Martin parle"
    )


@pytest.mark.parametrize("value", [None, "", "   \n\t  "])
def test_compute_input_hash_returns_none_for_empty_inputs(value):
    """Boundary: empty or null-like text has no hashable semantic payload."""
    assert compute_input_hash(value) is None


def test_build_fact_mention_nlp_input_hash_is_invariant_to_repeated_whitespace():
    """Regression: builder hashing uses the shared canonical hash function."""
    fact_mention_a = _base_fact_mention_dataframe()
    fact_mention_b = _base_fact_mention_dataframe()
    fact_mention_a.loc[0, "context_sentences"] = "Alice  Martin\nparle"
    fact_mention_b.loc[0, "context_sentences"] = "Alice Martin parle"

    nlp_input_a = build_fact_mention_nlp_input(
        fact_mention_a,
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )
    nlp_input_b = build_fact_mention_nlp_input(
        fact_mention_b,
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )

    assert nlp_input_a.loc[0, "input_hash"] == nlp_input_b.loc[0, "input_hash"]


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda dataframe: dataframe.assign(input_hash=None),
            "input_hash does not match",
        ),
        (
            lambda dataframe: dataframe.assign(context_word_count=-1),
            "context_word_count is negative",
        ),
        (
            lambda dataframe: dataframe.assign(context_word_count=pd.NA),
            "context_word_count has nulls",
        ),
        (
            lambda dataframe: dataframe.assign(input_contract_version=" "),
            "input_contract_version has null or blank values",
        ),
        (
            lambda dataframe: dataframe.assign(prepared_at=pd.NaT),
            "prepared_at has nulls",
        ),
        (
            lambda dataframe: dataframe.assign(skip_reason="too_short_for_inference"),
            "skip_reason must be empty",
        ),
        (
            lambda dataframe: dataframe.assign(
                eligible_for_inference=False,
                skip_reason="pdf_noise",
            ),
            "unsupported values",
        ),
        (
            lambda dataframe: dataframe.assign(eligible_for_lexicon=False),
            "inference eligibility requires lexicon eligibility",
        ),
    ],
    ids=[
        "input_hash_mismatch",
        "negative_word_count",
        "null_word_count",
        "blank_version",
        "null_prepared_at",
        "skip_reason_for_eligible_row",
        "unsupported_skip_reason",
        "inference_without_lexicon",
    ],
)
def test_validate_fact_mention_nlp_input_rejects_each_contract_gate(
    mutator,
    message,
):
    """Error path: validator gates fail independently with clear messages."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )

    with pytest.raises(DataQualityError, match=message):
        validate_fact_mention_nlp_input(mutator(nlp_input_dataframe))


def test_validate_fact_mention_nlp_input_requires_skip_reason_for_skipped_rows():
    """Error path: skipped rows need an explicit reason for auditability."""
    fact_mention_dataframe = _base_fact_mention_dataframe()
    fact_mention_dataframe.loc[0, "context_sentences"] = "Alice Martin parle"
    nlp_input_dataframe = build_fact_mention_nlp_input(
        fact_mention_dataframe,
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
    )
    nlp_input_dataframe.loc[0, "skip_reason"] = None

    with pytest.raises(DataQualityError, match="skip_reason is required"):
        validate_fact_mention_nlp_input(nlp_input_dataframe)


def test_validate_fact_mention_nlp_input_warns_on_oversized_input_text(caplog):
    """DQ warning: very large inputs signal possible full-text leakage."""
    fact_mention_dataframe = _base_fact_mention_dataframe()
    fact_mention_dataframe.loc[0, "context_sentences"] = "mot " * 1300

    with caplog.at_level(logging.WARNING):
        build_fact_mention_nlp_input(
            fact_mention_dataframe,
            _base_fact_article_dataframe(),
            prepared_at="2026-04-01T12:00:00Z",
        )

    assert "input_text exceeds" in caplog.text


def test_build_fact_mention_nlp_input_accepts_custom_contract_version():
    """Contract version: custom values flow through exactly for audits."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T12:00:00Z",
        input_contract_version="custom_v2",
    )

    assert nlp_input_dataframe.loc[0, "input_contract_version"] == "custom_v2"


def test_build_fact_mention_nlp_input_coerces_none_timestamp_to_utc_now():
    """Timestamp branch: missing prepared_at gets a timezone-aware UTC value."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        prepared_at=None,
    )

    prepared_at = nlp_input_dataframe.loc[0, "prepared_at"]
    assert prepared_at.tzinfo is not None
    assert str(prepared_at.tzinfo) == "UTC"


def test_build_fact_mention_nlp_input_localizes_naive_timestamp_to_utc():
    """Timestamp branch: naive timestamps are treated as UTC."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01 12:00:00",
    )

    assert nlp_input_dataframe.loc[0, "prepared_at"] == pd.Timestamp(
        "2026-04-01T12:00:00Z"
    )


def test_build_fact_mention_nlp_input_converts_aware_timestamp_to_utc():
    """Timestamp branch: aware non-UTC timestamps are converted to UTC."""
    nlp_input_dataframe = build_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        prepared_at="2026-04-01T14:00:00+02:00",
    )

    assert nlp_input_dataframe.loc[0, "prepared_at"] == pd.Timestamp(
        "2026-04-01T12:00:00Z"
    )


def test_materialize_fact_mention_nlp_input_writes_parquet_and_duckdb(tmp_path):
    """Integration: Phase 0 can materialize its Silver artifact and table."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    materialized_dataframe = materialize_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        prepared_at="2026-04-01T12:00:00Z",
    )

    parquet_path = silver_dir / "fact_mention_nlp_input.parquet"
    assert parquet_path.exists()
    persisted_dataframe = pd.read_parquet(parquet_path)
    assert len(persisted_dataframe) == len(materialized_dataframe)

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_input"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(materialized_dataframe)


def test_materialize_fact_mention_nlp_input_is_idempotent(tmp_path):
    """Regression: repeated materialization replaces rows instead of appending."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    materialize_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        prepared_at="2026-04-01T12:00:00Z",
    )
    materialized_dataframe = materialize_fact_mention_nlp_input(
        _base_fact_mention_dataframe(),
        _base_fact_article_dataframe(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        prepared_at="2026-04-01T12:00:00Z",
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_input"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(materialized_dataframe) == 1


def test_materialize_fact_mention_nlp_input_accepts_empty_input(tmp_path, caplog):
    """Boundary: empty mention inputs write empty Parquet and DuckDB artifacts."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"

    with caplog.at_level(logging.WARNING):
        materialized_dataframe = materialize_fact_mention_nlp_input(
            _empty_fact_mention_dataframe(),
            _base_fact_article_dataframe(),
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
            prepared_at="2026-04-01T12:00:00Z",
        )

    assert materialized_dataframe.empty
    assert "Empty fact_mention input" in caplog.text
    assert (silver_dir / "fact_mention_nlp_input.parquet").exists()

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_input"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == 0
