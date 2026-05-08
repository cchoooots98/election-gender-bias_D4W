"""Phase 1 deterministic lexicon audit for mention-level NLP inputs.

This module consumes ``silver.fact_mention_nlp_input`` and writes
``silver.fact_stereotype_word_counts``. It deliberately does not import or run
Transformer models; Phase 1 is a reproducible audit feature layer.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.settings import SILVER_DIR, WAREHOUSE_PATH
from src.nlp._validation import (
    require_columns,
    validate_unique_key,
)
from src.nlp.normalization import (
    is_null_or_blank,
    normalize_lexicon_text,
    tokenize_lexicon_text,
)
from src.storage.tables import write_duckdb_table, write_parquet_table
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

DEFAULT_LEXICON_RESOURCE_PACKAGE = "src.nlp.lexicons"
DEFAULT_LEXICON_RESOURCE_NAME = "stereotype_terms_v1.json"

CONTROLLED_LEXICON_CATEGORIES: tuple[str, ...] = (
    "politique",
    "vie_privee",
    "apparence",
    "scandale",
    "personnalite",
    "securite",
)

FACT_STEREOTYPE_WORD_COUNTS_COLUMNS: tuple[str, ...] = (
    "mention_id",
    "lexicon_category",
    "term",
    "count",
    "count_per_1k_tokens",
    "lexicon_version",
)

_REQUIRED_NLP_INPUT_COLUMNS = frozenset(
    {
        "mention_id",
        "input_text",
        "eligible_for_lexicon",
    }
)
_REQUIRED_LEXICON_OUTPUT_COLUMNS = frozenset(FACT_STEREOTYPE_WORD_COUNTS_COLUMNS)


@dataclass(frozen=True)
class LexiconTerm:
    """One normalized term owned by one lexicon category.

    Args:
        lexicon_category: Controlled category name used by downstream marts.
        term: Normalized term text persisted in the output table.
        tokens: Normalized token sequence used for exact phrase matching.
    """

    lexicon_category: str
    term: str
    tokens: tuple[str, ...]

    def __post_init__(self) -> None:
        """Validate one lexicon term contract."""
        if not self.lexicon_category.strip():
            raise ValueError("lexicon_category must be non-blank")
        if not self.term.strip():
            raise ValueError("term must be non-blank")
        if not self.tokens:
            raise ValueError("tokens must be non-empty")


@dataclass(frozen=True)
class LexiconConfig:
    """Versioned lexicon configuration for deterministic audit counts.

    Args:
        lexicon_version: Version identifier persisted on every output row.
        terms: Normalized category-term definitions.
    """

    lexicon_version: str
    terms: tuple[LexiconTerm, ...]

    def __post_init__(self) -> None:
        """Validate lexicon-level metadata."""
        if not self.lexicon_version.strip():
            raise ValueError("lexicon_version must be non-blank")
        if not self.terms:
            raise ValueError("lexicon must contain at least one term")


def load_stereotype_lexicon(lexicon_path: Path | None = None) -> LexiconConfig:
    """Load the versioned stereotype lexicon.

    Args:
        lexicon_path: Optional filesystem path to a lexicon JSON file. When
            omitted, the packaged ``stereotype_terms_v1.json`` resource is used.

    Returns:
        Parsed and validated lexicon configuration.

    Raises:
        ValueError: If the JSON schema, version, category, or term contract is
            invalid.
    """
    if lexicon_path is None:
        resource = resources.files(DEFAULT_LEXICON_RESOURCE_PACKAGE).joinpath(
            DEFAULT_LEXICON_RESOURCE_NAME
        )
        with resource.open("r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)
    else:
        with lexicon_path.open("r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)

    return _parse_lexicon_payload(payload)


def build_fact_stereotype_word_counts(
    nlp_input_dataframe: pd.DataFrame,
    lexicon_config: LexiconConfig,
) -> pd.DataFrame:
    """Build deterministic stereotype word-count rows from NLP input rows.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        lexicon_config: Versioned lexicon definitions.

    Returns:
        DataFrame matching the ``silver.fact_stereotype_word_counts`` contract.

    Raises:
        DataQualityError: If required input columns, eligible input text, or
            output validation gates fail.
    """
    _validate_nlp_input_for_lexicon(nlp_input_dataframe)
    output_rows: list[dict[str, object]] = []
    eligible_input_dataframe = nlp_input_dataframe.loc[
        nlp_input_dataframe["eligible_for_lexicon"].astype(bool),
        ["mention_id", "input_text"],
    ]

    for input_row in eligible_input_dataframe.itertuples(index=False):
        mention_id = str(input_row.mention_id).strip()
        input_tokens = tokenize_lexicon_text(input_row.input_text)
        if not input_tokens:
            raise DataQualityError(
                "fact_mention_nlp_input eligible rows must have non-empty input_text"
            )

        token_count = len(input_tokens)
        term_counts = _count_lexicon_terms(input_tokens, lexicon_config.terms)
        for lexicon_term in lexicon_config.terms:
            count = term_counts[lexicon_term]
            if count <= 0:
                continue
            output_rows.append(
                {
                    "mention_id": mention_id,
                    "lexicon_category": lexicon_term.lexicon_category,
                    "term": lexicon_term.term,
                    "count": int(count),
                    "count_per_1k_tokens": (count / token_count) * 1000,
                    "lexicon_version": lexicon_config.lexicon_version,
                }
            )

    stereotype_word_counts_dataframe = pd.DataFrame(
        output_rows,
        columns=FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    )
    validate_fact_stereotype_word_counts(stereotype_word_counts_dataframe)
    logger.info(
        "Built stereotype word counts rows=%d eligible_inputs=%d version=%s",
        len(stereotype_word_counts_dataframe),
        len(eligible_input_dataframe),
        lexicon_config.lexicon_version,
    )
    return stereotype_word_counts_dataframe


def materialize_fact_stereotype_word_counts(
    nlp_input_dataframe: pd.DataFrame,
    *,
    lexicon_config: LexiconConfig | None = None,
    lexicon_path: Path | None = None,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> pd.DataFrame:
    """Build and persist ``silver.fact_stereotype_word_counts``.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        lexicon_config: Optional pre-loaded lexicon configuration for tests.
        lexicon_path: Optional JSON lexicon path. Ignored when
            ``lexicon_config`` is provided.
        silver_dir: Directory where the Silver Parquet artifact is written.
        duckdb_path: DuckDB warehouse path for the Silver table write.

    Returns:
        The DataFrame that was written to Parquet and DuckDB.

    Raises:
        DataQualityError: If source or output contract validation fails.
        ValueError: If the lexicon configuration is invalid.
        RuntimeError: If DuckDB is unavailable while persisting the table.
    """
    effective_lexicon_config = lexicon_config or load_stereotype_lexicon(lexicon_path)
    stereotype_word_counts_dataframe = build_fact_stereotype_word_counts(
        nlp_input_dataframe,
        effective_lexicon_config,
    )
    parquet_path = silver_dir / "fact_stereotype_word_counts.parquet"
    write_parquet_table(stereotype_word_counts_dataframe, parquet_path)
    write_duckdb_table(
        dataframe=stereotype_word_counts_dataframe,
        schema_name="silver",
        table_name="fact_stereotype_word_counts",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized stereotype word counts parquet_path=%s duckdb_path=%s rows=%d",
        parquet_path,
        duckdb_path,
        len(stereotype_word_counts_dataframe),
    )
    return stereotype_word_counts_dataframe


def validate_fact_stereotype_word_counts(
    stereotype_word_counts_dataframe: pd.DataFrame,
) -> None:
    """Validate the Phase 1 stereotype word-count table.

    Args:
        stereotype_word_counts_dataframe: Candidate output table.

    Raises:
        DataQualityError: If required columns, primary-key uniqueness, counts,
            rates, terms, categories, or version metadata are invalid.
    """
    require_columns(
        dataframe=stereotype_word_counts_dataframe,
        required_columns=_REQUIRED_LEXICON_OUTPUT_COLUMNS,
        dataframe_name="fact_stereotype_word_counts",
    )
    validate_unique_key(
        dataframe=stereotype_word_counts_dataframe,
        key_columns=("mention_id", "lexicon_category", "term"),
        dataframe_name="fact_stereotype_word_counts",
    )
    _validate_output_text_columns(stereotype_word_counts_dataframe)
    _validate_count_columns(stereotype_word_counts_dataframe)


def _parse_lexicon_payload(payload: Any) -> LexiconConfig:
    """Parse and validate one lexicon JSON payload."""
    if not isinstance(payload, dict):
        raise ValueError("lexicon payload must be a JSON object")

    lexicon_version = str(payload.get("lexicon_version", "")).strip()
    categories = payload.get("categories")
    if not lexicon_version:
        raise ValueError("lexicon_version must be non-blank")
    if not isinstance(categories, list):
        raise ValueError("lexicon categories must be a list")

    lexicon_terms: list[LexiconTerm] = []
    seen_categories: set[str] = set()
    for category_payload in categories:
        if not isinstance(category_payload, dict):
            raise ValueError("each lexicon category must be an object")
        raw_category = str(category_payload.get("category", "")).strip()
        lexicon_category = normalize_lexicon_text(raw_category)
        if not lexicon_category:
            raise ValueError("lexicon category must be non-blank")
        if lexicon_category not in CONTROLLED_LEXICON_CATEGORIES:
            raise ValueError(f"unsupported lexicon category: {lexicon_category}")
        if lexicon_category in seen_categories:
            raise ValueError(f"duplicate lexicon category: {lexicon_category}")
        seen_categories.add(lexicon_category)

        raw_terms = category_payload.get("terms")
        if not isinstance(raw_terms, list) or not raw_terms:
            raise ValueError(f"lexicon category {lexicon_category} must contain terms")
        seen_terms_in_category: set[str] = set()
        for raw_term in raw_terms:
            normalized_term = normalize_lexicon_text(raw_term)
            term_tokens = tuple(tokenize_lexicon_text(raw_term))
            if not normalized_term or not term_tokens:
                raise ValueError(f"lexicon category {lexicon_category} has blank term")
            if normalized_term in seen_terms_in_category:
                raise ValueError(
                    "duplicate lexicon term within category "
                    f"{lexicon_category}: {normalized_term}"
                )
            seen_terms_in_category.add(normalized_term)
            lexicon_terms.append(
                LexiconTerm(
                    lexicon_category=lexicon_category,
                    term=normalized_term,
                    tokens=term_tokens,
                )
            )

    return LexiconConfig(
        lexicon_version=lexicon_version,
        terms=tuple(lexicon_terms),
    )


def _validate_nlp_input_for_lexicon(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate the Phase 0 source rows needed by Phase 1."""
    require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    missing_mention_id = nlp_input_dataframe["mention_id"].map(is_null_or_blank)
    if missing_mention_id.any():
        raise DataQualityError("fact_mention_nlp_input mention_id has blanks")
    validate_unique_key(
        dataframe=nlp_input_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention_nlp_input",
    )
    if nlp_input_dataframe["eligible_for_lexicon"].isna().any():
        raise DataQualityError("fact_mention_nlp_input eligible_for_lexicon has nulls")
    if not pd.api.types.is_bool_dtype(nlp_input_dataframe["eligible_for_lexicon"]):
        invalid_values = ~nlp_input_dataframe["eligible_for_lexicon"].map(
            lambda value: isinstance(value, bool)
        )
        if invalid_values.any():
            raise DataQualityError(
                "fact_mention_nlp_input eligible_for_lexicon must contain booleans"
            )

    eligible_rows = nlp_input_dataframe["eligible_for_lexicon"].astype(bool)
    blank_eligible_text = nlp_input_dataframe.loc[eligible_rows, "input_text"].map(
        is_null_or_blank
    )
    if blank_eligible_text.any():
        raise DataQualityError(
            "fact_mention_nlp_input eligible rows must have non-empty input_text"
        )


def _count_lexicon_terms(
    input_tokens: list[str],
    lexicon_terms: tuple[LexiconTerm, ...],
) -> Counter[LexiconTerm]:
    """Count exact token or phrase matches for each lexicon term."""
    term_counts: Counter[LexiconTerm] = Counter()
    for lexicon_term in lexicon_terms:
        term_length = len(lexicon_term.tokens)
        if term_length > len(input_tokens):
            continue
        for start_index in range(0, len(input_tokens) - term_length + 1):
            candidate_tokens = tuple(
                input_tokens[start_index : start_index + term_length]
            )
            if candidate_tokens == lexicon_term.tokens:
                term_counts[lexicon_term] += 1
    return term_counts


def _validate_output_text_columns(
    stereotype_word_counts_dataframe: pd.DataFrame,
) -> None:
    """Validate non-blank text metadata in output rows."""
    for column_name in (
        "mention_id",
        "lexicon_category",
        "term",
        "lexicon_version",
    ):
        blank_values = stereotype_word_counts_dataframe[column_name].map(
            is_null_or_blank
        )
        if blank_values.any():
            raise DataQualityError(
                f"fact_stereotype_word_counts {column_name} has blanks"
            )


def _validate_count_columns(
    stereotype_word_counts_dataframe: pd.DataFrame,
) -> None:
    """Validate positive counts and non-negative normalized rates."""
    counts = pd.to_numeric(stereotype_word_counts_dataframe["count"], errors="coerce")
    if counts.isna().any():
        raise DataQualityError("fact_stereotype_word_counts count has nulls")
    if (counts <= 0).any():
        raise DataQualityError("fact_stereotype_word_counts count must be positive")

    rates = pd.to_numeric(
        stereotype_word_counts_dataframe["count_per_1k_tokens"],
        errors="coerce",
    )
    if rates.isna().any():
        raise DataQualityError(
            "fact_stereotype_word_counts count_per_1k_tokens has nulls"
        )
    if (rates < 0).any():
        raise DataQualityError(
            "fact_stereotype_word_counts count_per_1k_tokens is negative"
        )
