"""Phase 0 NLP input contract builders.

The NLP enrichment layer must not read or persist full article body text. This
module derives a model-ready mention context table from ``silver.fact_mention``
and joins only the article language from ``silver.fact_article``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from pathlib import Path

import pandas as pd

from src.config.settings import (
    NLP_INPUT_CONTRACT_VERSION,
    NLP_MAX_INPUT_TEXT_CHARS,
    NLP_MIN_INFERENCE_WORD_COUNT,
    NLP_MIN_LEXICON_WORD_COUNT,
    SILVER_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.corpus_storage import write_duckdb_table, write_parquet_table
from src.ingest.news.normalize import stable_md5
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

_REPEATED_WHITESPACE_PATTERN = re.compile(r"\s+")

SKIP_REASON_EMPTY_CONTEXT = "empty_context"
SKIP_REASON_TOO_SHORT_FOR_LEXICON = "too_short_for_lexicon"
SKIP_REASON_TOO_SHORT_FOR_INFERENCE = "too_short_for_inference"
SKIP_REASON_LANGUAGE_NOT_FRENCH = "language_not_french"
LANGUAGE_FRENCH_PREFIX = "fr"

CONTROLLED_SKIP_REASONS: frozenset[str] = frozenset(
    {
        SKIP_REASON_EMPTY_CONTEXT,
        SKIP_REASON_TOO_SHORT_FOR_LEXICON,
        SKIP_REASON_TOO_SHORT_FOR_INFERENCE,
        SKIP_REASON_LANGUAGE_NOT_FRENCH,
    }
)

FACT_MENTION_NLP_INPUT_COLUMNS: list[str] = [
    "mention_id",
    "canonical_article_id",
    "leader_id",
    "article_language",
    "input_text",
    "input_hash",
    "context_word_count",
    "eligible_for_lexicon",
    "eligible_for_inference",
    "skip_reason",
    "prepared_at",
    "input_contract_version",
]

_FACT_MENTION_SOURCE_COLUMNS = (
    "mention_id",
    "canonical_article_id",
    "leader_id",
    "context_sentences",
)
_FACT_ARTICLE_LANGUAGE_COLUMNS = ("canonical_article_id", "language")
_REQUIRED_FACT_MENTION_COLUMNS = frozenset(_FACT_MENTION_SOURCE_COLUMNS)
_REQUIRED_FACT_ARTICLE_COLUMNS = frozenset(_FACT_ARTICLE_LANGUAGE_COLUMNS)
_REQUIRED_NLP_INPUT_COLUMNS = frozenset(FACT_MENTION_NLP_INPUT_COLUMNS)
_CORE_IDENTIFIER_COLUMNS = (
    "mention_id",
    "canonical_article_id",
    "leader_id",
)


def compute_input_hash(input_text: str | None) -> str | None:
    """Return the canonical hash for one normalized mention context.

    Args:
        input_text: Mention-context text. Repeated whitespace is collapsed before
            hashing so semantically identical context windows produce the same
            audit key.

    Returns:
        MD5 hash of the normalized text, or ``None`` when the text is empty.
    """
    normalized_text = _normalize_context_text(input_text)
    if not normalized_text:
        return None
    return stable_md5(normalized_text)


def build_fact_mention_nlp_input(
    fact_mention_dataframe: pd.DataFrame,
    fact_article_dataframe: pd.DataFrame,
    *,
    prepared_at: pd.Timestamp | str | None = None,
    input_contract_version: str = NLP_INPUT_CONTRACT_VERSION,
    min_lexicon_words: int = NLP_MIN_LEXICON_WORD_COUNT,
    min_inference_words: int = NLP_MIN_INFERENCE_WORD_COUNT,
) -> pd.DataFrame:
    """Build the Phase 0 NLP input table from candidate mention facts.

    Args:
        fact_mention_dataframe: ``silver.fact_mention`` rows. The builder uses
            only ``context_sentences`` as the text source.
        fact_article_dataframe: ``silver.fact_article`` rows. Only
            ``canonical_article_id`` and ``language`` are read for the language
            gate; article body text is not read or persisted.
        prepared_at: UTC timestamp to stamp on every output row. Defaults to
            the current UTC time.
        input_contract_version: Version identifier for the input preparation
            rules.
        min_lexicon_words: Minimum whitespace-delimited word count required for
            deterministic lexicon counting. This is not a tokenizer or BPE
            token count.
        min_inference_words: Minimum whitespace-delimited word count required
            for Transformer inference. This is not a tokenizer or BPE token
            count.

    Returns:
        DataFrame matching the ``silver.fact_mention_nlp_input`` contract.

    Raises:
        DataQualityError: If required columns, core identifiers, primary-key
            uniqueness checks, or article-language joins fail.
        ValueError: If word-count thresholds are negative or inconsistent.
    """
    _validate_word_thresholds(
        min_lexicon_words=min_lexicon_words,
        min_inference_words=min_inference_words,
    )
    _validate_fact_mention_source(fact_mention_dataframe)
    _validate_fact_article_source(fact_article_dataframe)

    preparation_timestamp = _coerce_utc_timestamp(prepared_at)
    mention_dataframe = fact_mention_dataframe.loc[
        :, list(_FACT_MENTION_SOURCE_COLUMNS)
    ].copy()
    for column_name in _CORE_IDENTIFIER_COLUMNS:
        mention_dataframe[column_name] = mention_dataframe[column_name].map(
            lambda value, column_name=column_name: _coerce_required_identifier(
                value,
                column_name,
            )
        )

    article_language_dataframe = fact_article_dataframe.loc[
        :, list(_FACT_ARTICLE_LANGUAGE_COLUMNS)
    ].copy()
    article_language_dataframe["canonical_article_id"] = article_language_dataframe[
        "canonical_article_id"
    ].map(lambda value: _coerce_required_identifier(value, "canonical_article_id"))
    article_language_dataframe["article_language"] = article_language_dataframe[
        "language"
    ].map(_normalize_language_code)
    article_language_dataframe = article_language_dataframe[
        ["canonical_article_id", "article_language"]
    ]

    nlp_input_dataframe = mention_dataframe.merge(
        article_language_dataframe,
        on="canonical_article_id",
        how="left",
        validate="many_to_one",
    )
    missing_article_language = nlp_input_dataframe["article_language"].isna()
    if missing_article_language.any():
        examples = (
            nlp_input_dataframe.loc[missing_article_language, "canonical_article_id"]
            .head(5)
            .tolist()
        )
        raise DataQualityError(
            "fact_mention_nlp_input has mentions without matching fact_article "
            f"language: {examples}"
        )

    normalized_text_series = nlp_input_dataframe["context_sentences"].map(
        _normalize_context_text
    )
    context_word_counts = normalized_text_series.map(_count_context_words)
    non_empty_context = normalized_text_series.ne("")
    french_language = nlp_input_dataframe["article_language"].eq("fr")
    eligible_for_lexicon = (
        non_empty_context & french_language & context_word_counts.ge(min_lexicon_words)
    )
    eligible_for_inference = eligible_for_lexicon & context_word_counts.ge(
        min_inference_words
    )
    skip_reason_series = _build_skip_reason_series(
        normalized_text_series=normalized_text_series,
        context_word_counts=context_word_counts,
        french_language=french_language,
        eligible_for_lexicon=eligible_for_lexicon,
        eligible_for_inference=eligible_for_inference,
        min_lexicon_words=min_lexicon_words,
    )

    nlp_input_dataframe = pd.DataFrame(
        {
            "mention_id": nlp_input_dataframe["mention_id"],
            "canonical_article_id": nlp_input_dataframe["canonical_article_id"],
            "leader_id": nlp_input_dataframe["leader_id"],
            "article_language": nlp_input_dataframe["article_language"],
            "input_text": normalized_text_series.mask(~non_empty_context, None),
            "input_hash": normalized_text_series.map(compute_input_hash),
            "context_word_count": context_word_counts,
            "eligible_for_lexicon": eligible_for_lexicon,
            "eligible_for_inference": eligible_for_inference,
            "skip_reason": skip_reason_series,
            "prepared_at": preparation_timestamp,
            "input_contract_version": input_contract_version,
        },
        columns=FACT_MENTION_NLP_INPUT_COLUMNS,
    )
    validate_fact_mention_nlp_input(nlp_input_dataframe)
    logger.info(
        "Built NLP input contract rows=%d lexicon_eligible=%d inference_eligible=%d "
        "skipped=%d version=%s",
        len(nlp_input_dataframe),
        int(nlp_input_dataframe["eligible_for_lexicon"].sum()),
        int(nlp_input_dataframe["eligible_for_inference"].sum()),
        int((~nlp_input_dataframe["eligible_for_inference"]).sum()),
        input_contract_version,
    )
    return nlp_input_dataframe


def materialize_fact_mention_nlp_input(
    fact_mention_dataframe: pd.DataFrame,
    fact_article_dataframe: pd.DataFrame,
    *,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    prepared_at: pd.Timestamp | str | None = None,
    input_contract_version: str = NLP_INPUT_CONTRACT_VERSION,
    min_lexicon_words: int = NLP_MIN_LEXICON_WORD_COUNT,
    min_inference_words: int = NLP_MIN_INFERENCE_WORD_COUNT,
) -> pd.DataFrame:
    """Build and persist ``silver.fact_mention_nlp_input``.

    Args:
        fact_mention_dataframe: ``silver.fact_mention`` rows.
        fact_article_dataframe: ``silver.fact_article`` rows.
        silver_dir: Directory where the Silver Parquet artifact is written.
        duckdb_path: DuckDB warehouse path for the Silver table write.
        prepared_at: UTC timestamp to stamp on every output row. Defaults to
            the current UTC time.
        input_contract_version: Version identifier for the input preparation
            rules.
        min_lexicon_words: Minimum whitespace-delimited word count required for
            deterministic lexicon counting.
        min_inference_words: Minimum whitespace-delimited word count required
            for Transformer inference.

    Returns:
        The DataFrame that was written to Parquet and DuckDB.

    Raises:
        DataQualityError: If source or output contract validation fails.
        ValueError: If word-count thresholds are invalid.
        RuntimeError: If DuckDB is unavailable while persisting the table.
    """
    if fact_mention_dataframe.empty:
        logger.warning("Empty fact_mention input - writing empty NLP input table")

    nlp_input_dataframe = build_fact_mention_nlp_input(
        fact_mention_dataframe,
        fact_article_dataframe,
        prepared_at=prepared_at,
        input_contract_version=input_contract_version,
        min_lexicon_words=min_lexicon_words,
        min_inference_words=min_inference_words,
    )
    parquet_path = silver_dir / "fact_mention_nlp_input.parquet"
    write_parquet_table(nlp_input_dataframe, parquet_path)
    write_duckdb_table(
        dataframe=nlp_input_dataframe,
        schema_name="silver",
        table_name="fact_mention_nlp_input",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized NLP input contract parquet_path=%s duckdb_path=%s rows=%d",
        parquet_path,
        duckdb_path,
        len(nlp_input_dataframe),
    )
    return nlp_input_dataframe


def validate_fact_mention_nlp_input(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate the Phase 0 NLP input table before persistence.

    Args:
        nlp_input_dataframe: Candidate NLP input contract table.

    Raises:
        DataQualityError: If required columns, primary key uniqueness, hashes,
            skip reasons, word counts, eligibility flags, or version checks fail.
    """
    _require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    _validate_required_identifier_values(
        dataframe=nlp_input_dataframe,
        dataframe_name="fact_mention_nlp_input",
        identifier_columns=_CORE_IDENTIFIER_COLUMNS,
    )
    _validate_unique_key(
        dataframe=nlp_input_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention_nlp_input",
    )
    _validate_context_word_count(nlp_input_dataframe)
    _validate_boolean_column(nlp_input_dataframe, "eligible_for_lexicon")
    _validate_boolean_column(nlp_input_dataframe, "eligible_for_inference")
    _validate_language_values(nlp_input_dataframe)
    _validate_hash_contract(nlp_input_dataframe)
    _validate_skip_reason_contract(nlp_input_dataframe)
    _validate_version_and_timestamp(nlp_input_dataframe)
    _warn_on_oversized_input_text(nlp_input_dataframe)


def _validate_word_thresholds(
    *,
    min_lexicon_words: int,
    min_inference_words: int,
) -> None:
    """Validate threshold relationships before deriving eligibility flags."""
    if min_lexicon_words < 0:
        raise ValueError("min_lexicon_words must be non-negative")
    if min_inference_words < 0:
        raise ValueError("min_inference_words must be non-negative")
    if min_inference_words < min_lexicon_words:
        raise ValueError("min_inference_words must be >= min_lexicon_words")


def _validate_fact_mention_source(fact_mention_dataframe: pd.DataFrame) -> None:
    """Validate source mention facts before deriving model inputs."""
    _require_columns(
        dataframe=fact_mention_dataframe,
        required_columns=_REQUIRED_FACT_MENTION_COLUMNS,
        dataframe_name="fact_mention",
    )
    _validate_required_identifier_values(
        dataframe=fact_mention_dataframe,
        dataframe_name="fact_mention",
        identifier_columns=_CORE_IDENTIFIER_COLUMNS,
    )
    _validate_unique_key(
        dataframe=fact_mention_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention",
    )


def _validate_fact_article_source(fact_article_dataframe: pd.DataFrame) -> None:
    """Validate article language source before joining to mention rows."""
    _require_columns(
        dataframe=fact_article_dataframe,
        required_columns=_REQUIRED_FACT_ARTICLE_COLUMNS,
        dataframe_name="fact_article",
    )
    _validate_required_identifier_values(
        dataframe=fact_article_dataframe,
        dataframe_name="fact_article",
        identifier_columns=("canonical_article_id",),
    )
    _validate_unique_key(
        dataframe=fact_article_dataframe,
        key_columns=("canonical_article_id",),
        dataframe_name="fact_article",
    )


def _require_columns(
    *,
    dataframe: pd.DataFrame,
    required_columns: frozenset[str],
    dataframe_name: str,
) -> None:
    """Raise when a DataFrame is missing contract columns."""
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise DataQualityError(
            f"{dataframe_name} missing required columns: {missing_columns}"
        )


def _validate_required_identifier_values(
    *,
    dataframe: pd.DataFrame,
    dataframe_name: str,
    identifier_columns: Iterable[str],
) -> None:
    """Raise when core identifiers contain null or blank values."""
    for column_name in identifier_columns:
        invalid_identifier_mask = dataframe[column_name].map(_is_null_or_blank)
        if invalid_identifier_mask.any():
            invalid_count = int(invalid_identifier_mask.sum())
            raise DataQualityError(
                f"{dataframe_name} has {invalid_count} null or blank "
                f"{column_name} values"
            )


def _validate_unique_key(
    *,
    dataframe: pd.DataFrame,
    key_columns: tuple[str, ...],
    dataframe_name: str,
) -> None:
    """Raise when a declared primary key is duplicated."""
    duplicate_mask = dataframe.duplicated(subset=list(key_columns), keep=False)
    if duplicate_mask.any():
        duplicate_examples = (
            dataframe.loc[duplicate_mask, list(key_columns)]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise DataQualityError(
            f"{dataframe_name} has duplicate key rows for {list(key_columns)}: "
            f"{duplicate_examples}"
        )


def _validate_context_word_count(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate non-null, non-negative whitespace word counts."""
    word_counts = pd.to_numeric(
        nlp_input_dataframe["context_word_count"],
        errors="coerce",
    )
    if word_counts.isna().any():
        raise DataQualityError("fact_mention_nlp_input context_word_count has nulls")
    negative_word_count = word_counts < 0
    if negative_word_count.any():
        raise DataQualityError("fact_mention_nlp_input context_word_count is negative")


def _validate_boolean_column(
    nlp_input_dataframe: pd.DataFrame,
    column_name: str,
) -> None:
    """Validate strict boolean flags without coercing integer 0/1 values."""
    if nlp_input_dataframe[column_name].isna().any():
        raise DataQualityError(f"fact_mention_nlp_input {column_name} has nulls")
    if pd.api.types.is_bool_dtype(nlp_input_dataframe[column_name]):
        return
    invalid_values = ~nlp_input_dataframe[column_name].map(
        lambda value: isinstance(value, bool)
    )
    if invalid_values.any():
        raise DataQualityError(
            f"fact_mention_nlp_input {column_name} must contain booleans"
        )


def _validate_language_values(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate that article language is present for auditability."""
    missing_language = nlp_input_dataframe["article_language"].map(_is_null_or_blank)
    if missing_language.any():
        raise DataQualityError("fact_mention_nlp_input article_language has blanks")


def _validate_hash_contract(nlp_input_dataframe: pd.DataFrame) -> None:
    """Recompute hashes deliberately as an audit check on persisted values."""
    expected_hashes = nlp_input_dataframe["input_text"].map(compute_input_hash)
    actual_hashes = nlp_input_dataframe["input_hash"]
    mismatch = expected_hashes.fillna("") != actual_hashes.fillna("")
    if mismatch.any():
        raise DataQualityError(
            "fact_mention_nlp_input input_hash does not match compute_input_hash"
        )


def _validate_skip_reason_contract(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate controlled skip reasons and eligibility consistency."""
    eligible_for_lexicon = nlp_input_dataframe["eligible_for_lexicon"].astype(bool)
    eligible_for_inference = nlp_input_dataframe["eligible_for_inference"].astype(bool)
    inference_without_lexicon = eligible_for_inference & ~eligible_for_lexicon
    if inference_without_lexicon.any():
        raise DataQualityError(
            "fact_mention_nlp_input inference eligibility requires lexicon eligibility"
        )

    skipped_reason_missing = nlp_input_dataframe.loc[
        ~eligible_for_inference, "skip_reason"
    ].isna() | nlp_input_dataframe.loc[~eligible_for_inference, "skip_reason"].astype(
        str
    ).str.strip().eq(
        ""
    )
    if skipped_reason_missing.any():
        raise DataQualityError(
            "fact_mention_nlp_input skip_reason is required for skipped rows"
        )

    scored_reason_present = (
        nlp_input_dataframe.loc[eligible_for_inference, "skip_reason"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
    )
    if scored_reason_present.any():
        raise DataQualityError(
            "fact_mention_nlp_input skip_reason must be empty for inference rows"
        )

    present_reasons = (
        nlp_input_dataframe["skip_reason"].dropna().astype(str).str.strip()
    )
    unsupported_reasons = ~present_reasons.isin(CONTROLLED_SKIP_REASONS)
    if unsupported_reasons.any():
        examples = present_reasons.loc[unsupported_reasons].drop_duplicates().tolist()
        raise DataQualityError(
            "fact_mention_nlp_input skip_reason has unsupported values: " f"{examples}"
        )

    too_short_for_inference = (
        nlp_input_dataframe["skip_reason"] == SKIP_REASON_TOO_SHORT_FOR_INFERENCE
    )
    if (too_short_for_inference & ~eligible_for_lexicon).any():
        raise DataQualityError(
            "fact_mention_nlp_input too_short_for_inference rows must be "
            "eligible_for_lexicon"
        )


def _validate_version_and_timestamp(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate version and preparation timestamp fields."""
    missing_versions = nlp_input_dataframe[
        "input_contract_version"
    ].isna() | nlp_input_dataframe["input_contract_version"].astype(str).str.strip().eq(
        ""
    )
    if missing_versions.any():
        raise DataQualityError(
            "fact_mention_nlp_input input_contract_version has null or blank values"
        )

    if nlp_input_dataframe["prepared_at"].isna().any():
        raise DataQualityError("fact_mention_nlp_input prepared_at has nulls")


def _warn_on_oversized_input_text(nlp_input_dataframe: pd.DataFrame) -> None:
    """Warn when a context looks like full article text leaked into the table."""
    input_text_lengths = (
        nlp_input_dataframe["input_text"].fillna("").astype(str).str.len()
    )
    oversized_input = input_text_lengths > NLP_MAX_INPUT_TEXT_CHARS
    if oversized_input.any():
        logger.warning(
            "input_text exceeds %d chars on %d rows",
            NLP_MAX_INPUT_TEXT_CHARS,
            int(oversized_input.sum()),
        )


def _build_skip_reason_series(
    *,
    normalized_text_series: pd.Series,
    context_word_counts: pd.Series,
    french_language: pd.Series,
    eligible_for_lexicon: pd.Series,
    eligible_for_inference: pd.Series,
    min_lexicon_words: int,
) -> pd.Series:
    """Build controlled skip reasons in deterministic precedence order."""
    skip_reason_series = pd.Series(
        [None] * len(normalized_text_series),
        index=normalized_text_series.index,
        dtype="object",
    )
    non_empty_context = normalized_text_series.ne("")
    skip_reason_series.loc[~non_empty_context] = SKIP_REASON_EMPTY_CONTEXT
    skip_reason_series.loc[non_empty_context & ~french_language] = (
        SKIP_REASON_LANGUAGE_NOT_FRENCH
    )
    skip_reason_series.loc[
        non_empty_context & french_language & context_word_counts.lt(min_lexicon_words)
    ] = SKIP_REASON_TOO_SHORT_FOR_LEXICON
    skip_reason_series.loc[eligible_for_lexicon & ~eligible_for_inference] = (
        SKIP_REASON_TOO_SHORT_FOR_INFERENCE
    )
    return skip_reason_series


def _normalize_context_text(value: object) -> str:
    """Collapse repeated whitespace while preserving scoring-relevant text."""
    if _is_missing(value):
        return ""
    return _REPEATED_WHITESPACE_PATTERN.sub(" ", str(value)).strip()


def _count_context_words(input_text: str) -> int:
    """Count whitespace-delimited words after context normalization."""
    if not input_text:
        return 0
    return len(input_text.split())


def _normalize_language_code(value: object) -> str:
    """Normalize nullable article language values for the language gate."""
    if _is_null_or_blank(value):
        return "unknown"
    normalized_language = str(value).strip().lower().replace("_", "-")
    primary_subtag = normalized_language.split("-", 1)[0]
    if primary_subtag == LANGUAGE_FRENCH_PREFIX:
        return LANGUAGE_FRENCH_PREFIX
    return normalized_language


def _coerce_required_identifier(value: object, column_name: str) -> str:
    """Return a stripped string identifier after null/blank validation."""
    if _is_null_or_blank(value):
        raise DataQualityError(f"{column_name} is required")
    return str(value).strip()


def _coerce_utc_timestamp(value: pd.Timestamp | str | None) -> pd.Timestamp:
    """Return a timezone-aware UTC timestamp."""
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _is_missing(value: object) -> bool:
    """Return whether a scalar value should be treated as missing."""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _is_null_or_blank(value: object) -> bool:
    """Return whether a required text identifier is null or blank."""
    if _is_missing(value):
        return True
    return str(value).strip() == ""
