"""Two-tier deterministic trait lexicon metrics for NLP mention contexts."""

from __future__ import annotations

import itertools
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib import resources
from pathlib import Path
from typing import Any

import pandas as pd

from src.config.settings import GOLD_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp._validation import require_columns, validate_unique_key
from src.nlp.normalization import (
    is_null_or_blank,
    normalize_lexicon_text,
    tokenize_lexicon_text,
)
from src.storage.tables import write_duckdb_table, write_parquet_table
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

DEFAULT_TRAIT_LEXICON_RESOURCE_PACKAGE = "src.nlp.lexicons"
DEFAULT_TRAIT_LEXICON_RESOURCE_NAME = "trait_terms_v1.json"

CONTROLLED_TRAIT_CATEGORIES: tuple[str, ...] = (
    "political_work",
    "leadership_competence",
    "personality",
    "family_private_life",
    "appearance_body",
    "romance_relationship",
    "scandal_conflict",
    "security_order",
)
CONTROLLED_TRAIT_TIERS: tuple[str, ...] = ("core", "exploratory")
CONTROLLED_TRAIT_SCENARIOS: tuple[str, ...] = (
    "all",
    "drop_top_overall",
    "drop_top_each_gender",
)

FACT_TRAIT_WORD_COUNTS_COLUMNS: tuple[str, ...] = (
    "mention_id",
    "leader_id",
    "canonical_article_id",
    "trait_category",
    "trait_tier",
    "term",
    "count",
    "count_per_1k_tokens",
    "lexicon_version",
    "rationale",
)
MART_TRAIT_METRICS_COLUMNS: tuple[str, ...] = (
    "scenario_id",
    "trait_tier",
    "gender",
    "trait_category",
    "mention_count",
    "hit_mentions",
    "term_hits",
    "context_word_count",
    "hits_per_1k_context_words",
    "coverage_rate",
    "evidence_level",
    "generated_at",
)
MART_TRAIT_TOP_TERMS_COLUMNS: tuple[str, ...] = (
    "scenario_id",
    "trait_tier",
    "gender",
    "trait_category",
    "term",
    "term_hits",
    "hit_mentions",
    "rank",
    "generated_at",
)
MART_TRAIT_CANDIDATE_COLUMNS: tuple[str, ...] = (
    "scenario_id",
    "trait_tier",
    "leader_id",
    "full_name",
    "gender",
    "commune_name",
    "trait_category",
    "article_count",
    "mention_count",
    "hit_mentions",
    "term_hits",
    "context_word_count",
    "hits_per_1k_context_words",
    "coverage_rate",
    "generated_at",
)
MART_TRAIT_QA_SAMPLE_COLUMNS: tuple[str, ...] = (
    "trait_tier",
    "trait_category",
    "term",
    "gender",
    "leader_id",
    "full_name",
    "mention_id",
    "context_excerpt",
    "rationale",
    "lexicon_version",
    "generated_at",
)

_REQUIRED_NLP_INPUT_COLUMNS = frozenset(
    {
        "mention_id",
        "leader_id",
        "canonical_article_id",
        "input_text",
        "context_word_count",
        "eligible_for_lexicon",
    }
)
_REQUIRED_SAMPLE_COLUMNS = frozenset(
    {"leader_id", "full_name", "gender", "commune_name"}
)
_REQUIRED_EXPOSURE_COLUMNS = frozenset({"leader_id", "gender", "article_count"})
_SPARSE_EVIDENCE_MIN_HIT_MENTIONS = 30
_TABLE_ONLY_MIN_HIT_MENTIONS = 10
_QA_SAMPLES_PER_CATEGORY_TIER = 5
_QA_CONTEXT_EXCERPT_CHARS = 260


@dataclass(frozen=True)
class TraitLexiconTerm:
    """One normalized French trait lexicon term.

    Args:
        trait_category: Controlled trait category.
        trait_tier: ``core`` or ``exploratory``.
        term: Normalized persisted French term.
        rationale: English explanation for the term placement.
        tokens: Normalized token sequence used for exact phrase matching.
    """

    trait_category: str
    trait_tier: str
    term: str
    rationale: str
    tokens: tuple[str, ...]

    def __post_init__(self) -> None:
        """Validate one term after dataclass construction."""
        if self.trait_category not in CONTROLLED_TRAIT_CATEGORIES:
            raise ValueError(f"unsupported trait category: {self.trait_category}")
        if self.trait_tier not in CONTROLLED_TRAIT_TIERS:
            raise ValueError(f"unsupported trait tier: {self.trait_tier}")
        if not self.term.strip():
            raise ValueError("trait term must be non-blank")
        if not self.rationale.strip():
            raise ValueError("trait term rationale must be non-blank")
        if not self.tokens:
            raise ValueError("trait term tokens must be non-empty")


@dataclass(frozen=True)
class TraitLexiconConfig:
    """Versioned two-tier trait lexicon configuration.

    Args:
        lexicon_version: Version identifier persisted on output rows.
        terms: Parsed, normalized terms.
    """

    lexicon_version: str
    terms: tuple[TraitLexiconTerm, ...]

    def __post_init__(self) -> None:
        """Validate lexicon-level metadata."""
        if not self.lexicon_version.strip():
            raise ValueError("lexicon_version must be non-blank")
        if not self.terms:
            raise ValueError("trait lexicon must contain at least one term")


@dataclass(frozen=True)
class TraitMetricArtifacts:
    """All deterministic trait artifacts produced from one NLP input snapshot.

    Args:
        word_counts: Silver trait word-count rows.
        metrics: Gold gender/category/tier summary rows.
        top_terms: Gold top-term rows.
        candidate_metrics: Gold candidate/category/tier rows.
        qa_samples: Gold representative context rows.
    """

    word_counts: pd.DataFrame
    metrics: pd.DataFrame
    top_terms: pd.DataFrame
    candidate_metrics: pd.DataFrame
    qa_samples: pd.DataFrame


def load_trait_lexicon(lexicon_path: Path | None = None) -> TraitLexiconConfig:
    """Load the versioned trait lexicon.

    Args:
        lexicon_path: Optional filesystem path to a trait lexicon JSON file.
            When omitted, the packaged ``trait_terms_v1.json`` resource is used.

    Returns:
        Parsed and validated trait lexicon configuration.

    Raises:
        ValueError: If the JSON schema, terms, category, tier, or rationale
            contract is invalid.
    """
    if lexicon_path is None:
        resource = resources.files(DEFAULT_TRAIT_LEXICON_RESOURCE_PACKAGE).joinpath(
            DEFAULT_TRAIT_LEXICON_RESOURCE_NAME
        )
        with resource.open("r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)
    else:
        with lexicon_path.open("r", encoding="utf-8") as file_handle:
            payload = json.load(file_handle)

    return _parse_trait_lexicon_payload(payload)


def build_fact_trait_word_counts(
    nlp_input_dataframe: pd.DataFrame,
    trait_lexicon_config: TraitLexiconConfig,
) -> pd.DataFrame:
    """Build deterministic trait word-count rows from NLP input rows.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        trait_lexicon_config: Versioned two-tier trait lexicon.

    Returns:
        DataFrame matching the ``silver.fact_trait_word_counts`` contract.

    Raises:
        DataQualityError: If source rows or output rows violate the metric
            contract.
    """
    _validate_nlp_input_for_traits(nlp_input_dataframe)
    eligible_input_dataframe = nlp_input_dataframe.loc[
        nlp_input_dataframe["eligible_for_lexicon"].astype(bool),
        [
            "mention_id",
            "leader_id",
            "canonical_article_id",
            "input_text",
            "context_word_count",
        ],
    ]
    output_rows: list[dict[str, object]] = []

    for input_row in eligible_input_dataframe.itertuples(index=False):
        mention_id = str(input_row.mention_id).strip()
        leader_id = str(input_row.leader_id).strip()
        canonical_article_id = str(input_row.canonical_article_id).strip()
        input_tokens = tokenize_lexicon_text(input_row.input_text)
        if not input_tokens:
            raise DataQualityError(
                "fact_mention_nlp_input eligible rows must have non-empty input_text"
            )
        token_count = len(input_tokens)
        term_counts = _count_trait_terms(input_tokens, trait_lexicon_config.terms)
        for trait_term in trait_lexicon_config.terms:
            count = term_counts[trait_term]
            if count <= 0:
                continue
            output_rows.append(
                {
                    "mention_id": mention_id,
                    "leader_id": leader_id,
                    "canonical_article_id": canonical_article_id,
                    "trait_category": trait_term.trait_category,
                    "trait_tier": trait_term.trait_tier,
                    "term": trait_term.term,
                    "count": int(count),
                    "count_per_1k_tokens": (count / token_count) * 1000,
                    "lexicon_version": trait_lexicon_config.lexicon_version,
                    "rationale": trait_term.rationale,
                }
            )

    trait_word_counts_dataframe = pd.DataFrame(
        output_rows,
        columns=FACT_TRAIT_WORD_COUNTS_COLUMNS,
    )
    validate_fact_trait_word_counts(trait_word_counts_dataframe)
    logger.info(
        "Built trait word counts rows=%d eligible_inputs=%d version=%s",
        len(trait_word_counts_dataframe),
        len(eligible_input_dataframe),
        trait_lexicon_config.lexicon_version,
    )
    return trait_word_counts_dataframe


def build_trait_metric_artifacts(
    *,
    nlp_input_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    exposure_metrics_dataframe: pd.DataFrame,
    trait_word_counts_dataframe: pd.DataFrame,
    generated_at: str | None = None,
) -> TraitMetricArtifacts:
    """Build Gold dashboard artifacts from Silver trait word counts.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        sample_leaders_dataframe: ``gold.sample_leaders`` rows.
        exposure_metrics_dataframe: ``gold.mart_exposure_metrics`` rows.
        trait_word_counts_dataframe: ``silver.fact_trait_word_counts`` rows.
        generated_at: Optional timestamp for reproducible tests.

    Returns:
        All Gold trait artifacts needed by the dashboard.

    Raises:
        DataQualityError: If any source table violates the required contracts.
    """
    _validate_nlp_input_for_traits(nlp_input_dataframe)
    _validate_sample_leaders(sample_leaders_dataframe)
    _validate_exposure_metrics(exposure_metrics_dataframe)
    validate_fact_trait_word_counts(trait_word_counts_dataframe)

    effective_generated_at = generated_at or datetime.now(UTC).isoformat()
    analysis_base = _build_analysis_base(
        nlp_input_dataframe=nlp_input_dataframe,
        sample_leaders_dataframe=sample_leaders_dataframe,
        exposure_metrics_dataframe=exposure_metrics_dataframe,
    )
    scenario_exclusions = _build_scenario_exclusions(exposure_metrics_dataframe)
    metrics_dataframe = _build_mart_trait_metrics(
        analysis_base=analysis_base,
        trait_word_counts_dataframe=trait_word_counts_dataframe,
        scenario_exclusions=scenario_exclusions,
        generated_at=effective_generated_at,
    )
    top_terms_dataframe = _build_mart_trait_top_terms(
        analysis_base=analysis_base,
        trait_word_counts_dataframe=trait_word_counts_dataframe,
        scenario_exclusions=scenario_exclusions,
        generated_at=effective_generated_at,
    )
    candidate_metrics_dataframe = _build_mart_trait_candidate_metrics(
        analysis_base=analysis_base,
        trait_word_counts_dataframe=trait_word_counts_dataframe,
        scenario_exclusions=scenario_exclusions,
        generated_at=effective_generated_at,
    )
    qa_samples_dataframe = _build_mart_trait_qa_samples(
        analysis_base=analysis_base,
        trait_word_counts_dataframe=trait_word_counts_dataframe,
        generated_at=effective_generated_at,
    )
    return TraitMetricArtifacts(
        word_counts=trait_word_counts_dataframe,
        metrics=metrics_dataframe,
        top_terms=top_terms_dataframe,
        candidate_metrics=candidate_metrics_dataframe,
        qa_samples=qa_samples_dataframe,
    )


def materialize_trait_metric_artifacts(
    *,
    nlp_input_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    exposure_metrics_dataframe: pd.DataFrame,
    trait_lexicon_config: TraitLexiconConfig | None = None,
    trait_lexicon_path: Path | None = None,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> TraitMetricArtifacts:
    """Build and persist Silver and Gold trait lexicon artifacts.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        sample_leaders_dataframe: ``gold.sample_leaders`` rows.
        exposure_metrics_dataframe: ``gold.mart_exposure_metrics`` rows.
        trait_lexicon_config: Optional pre-loaded lexicon configuration.
        trait_lexicon_path: Optional trait lexicon JSON path. Ignored when
            ``trait_lexicon_config`` is supplied.
        silver_dir: Directory for the Silver word-count Parquet artifact.
        gold_dir: Directory for Gold dashboard Parquet artifacts.
        duckdb_path: DuckDB warehouse path.

    Returns:
        The artifacts that were written to Parquet and DuckDB.

    Raises:
        DataQualityError: If source or output validation fails.
        ValueError: If the lexicon configuration is invalid.
        RuntimeError: If DuckDB is unavailable while persisting the tables.
    """
    effective_lexicon_config = trait_lexicon_config or load_trait_lexicon(
        trait_lexicon_path
    )
    trait_word_counts_dataframe = build_fact_trait_word_counts(
        nlp_input_dataframe,
        effective_lexicon_config,
    )
    artifacts = build_trait_metric_artifacts(
        nlp_input_dataframe=nlp_input_dataframe,
        sample_leaders_dataframe=sample_leaders_dataframe,
        exposure_metrics_dataframe=exposure_metrics_dataframe,
        trait_word_counts_dataframe=trait_word_counts_dataframe,
    )

    _write_trait_artifact(
        artifacts.word_counts,
        parquet_path=silver_dir / "fact_trait_word_counts.parquet",
        schema_name="silver",
        table_name="fact_trait_word_counts",
        duckdb_path=duckdb_path,
    )
    _write_trait_artifact(
        artifacts.metrics,
        parquet_path=gold_dir / "mart_trait_metrics.parquet",
        schema_name="gold",
        table_name="mart_trait_metrics",
        duckdb_path=duckdb_path,
    )
    _write_trait_artifact(
        artifacts.top_terms,
        parquet_path=gold_dir / "mart_trait_top_terms.parquet",
        schema_name="gold",
        table_name="mart_trait_top_terms",
        duckdb_path=duckdb_path,
    )
    _write_trait_artifact(
        artifacts.candidate_metrics,
        parquet_path=gold_dir / "mart_trait_candidate_metrics.parquet",
        schema_name="gold",
        table_name="mart_trait_candidate_metrics",
        duckdb_path=duckdb_path,
    )
    _write_trait_artifact(
        artifacts.qa_samples,
        parquet_path=gold_dir / "mart_trait_qa_samples.parquet",
        schema_name="gold",
        table_name="mart_trait_qa_samples",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized trait lexicon artifacts word_rows=%d metric_rows=%d",
        len(artifacts.word_counts),
        len(artifacts.metrics),
    )
    return artifacts


def validate_fact_trait_word_counts(trait_word_counts_dataframe: pd.DataFrame) -> None:
    """Validate the Silver trait word-count contract.

    Args:
        trait_word_counts_dataframe: Candidate output table.

    Raises:
        DataQualityError: If required columns, key uniqueness, categories, tiers,
            counts, rates, or text metadata are invalid.
    """
    require_columns(
        dataframe=trait_word_counts_dataframe,
        required_columns=frozenset(FACT_TRAIT_WORD_COUNTS_COLUMNS),
        dataframe_name="fact_trait_word_counts",
    )
    validate_unique_key(
        dataframe=trait_word_counts_dataframe,
        key_columns=("mention_id", "trait_category", "trait_tier", "term"),
        dataframe_name="fact_trait_word_counts",
    )
    for column_name in (
        "mention_id",
        "leader_id",
        "canonical_article_id",
        "trait_category",
        "trait_tier",
        "term",
        "lexicon_version",
        "rationale",
    ):
        blank_values = trait_word_counts_dataframe[column_name].map(is_null_or_blank)
        if blank_values.any():
            raise DataQualityError(f"fact_trait_word_counts {column_name} has blanks")

    unsupported_categories = sorted(
        set(trait_word_counts_dataframe["trait_category"])
        - set(CONTROLLED_TRAIT_CATEGORIES)
    )
    if unsupported_categories:
        raise DataQualityError(
            "fact_trait_word_counts unsupported trait categories: "
            f"{unsupported_categories}"
        )
    unsupported_tiers = sorted(
        set(trait_word_counts_dataframe["trait_tier"]) - set(CONTROLLED_TRAIT_TIERS)
    )
    if unsupported_tiers:
        raise DataQualityError(
            "fact_trait_word_counts unsupported trait tiers: " f"{unsupported_tiers}"
        )

    counts = pd.to_numeric(trait_word_counts_dataframe["count"], errors="coerce")
    if counts.isna().any():
        raise DataQualityError("fact_trait_word_counts count has nulls")
    if (counts <= 0).any():
        raise DataQualityError("fact_trait_word_counts count must be positive")

    rates = pd.to_numeric(
        trait_word_counts_dataframe["count_per_1k_tokens"],
        errors="coerce",
    )
    if rates.isna().any():
        raise DataQualityError("fact_trait_word_counts count_per_1k_tokens has nulls")
    if (rates < 0).any():
        raise DataQualityError("fact_trait_word_counts count_per_1k_tokens is negative")


def _parse_trait_lexicon_payload(payload: Any) -> TraitLexiconConfig:
    """Parse and validate one trait lexicon JSON payload."""
    if not isinstance(payload, dict):
        raise ValueError("trait lexicon payload must be a JSON object")

    lexicon_version = str(payload.get("lexicon_version", "")).strip()
    terms_payload = payload.get("terms")
    if not lexicon_version:
        raise ValueError("lexicon_version must be non-blank")
    if not isinstance(terms_payload, list):
        raise ValueError("trait lexicon terms must be a list")

    terms: list[TraitLexiconTerm] = []
    seen_term_keys: set[tuple[str, str, str]] = set()
    for raw_term_payload in terms_payload:
        if not isinstance(raw_term_payload, dict):
            raise ValueError("each trait lexicon term must be an object")
        missing_fields = sorted(
            {"term", "category", "tier", "rationale"} - set(raw_term_payload)
        )
        if missing_fields:
            raise ValueError(
                "trait lexicon term missing required fields: "
                + ", ".join(missing_fields)
            )
        normalized_term = normalize_lexicon_text(raw_term_payload["term"])
        term_tokens = tuple(tokenize_lexicon_text(raw_term_payload["term"]))
        trait_category = normalize_lexicon_text(raw_term_payload["category"])
        trait_tier = normalize_lexicon_text(raw_term_payload["tier"])
        rationale = str(raw_term_payload["rationale"]).strip()
        trait_term = TraitLexiconTerm(
            trait_category=trait_category,
            trait_tier=trait_tier,
            term=normalized_term,
            rationale=rationale,
            tokens=term_tokens,
        )
        term_key = (trait_term.trait_category, trait_term.trait_tier, trait_term.term)
        if term_key in seen_term_keys:
            raise ValueError(
                "duplicate trait lexicon term within category and tier: " f"{term_key}"
            )
        seen_term_keys.add(term_key)
        terms.append(trait_term)

    return TraitLexiconConfig(
        lexicon_version=lexicon_version,
        terms=tuple(terms),
    )


def _validate_nlp_input_for_traits(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate Phase 0 rows needed by trait counting."""
    require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    validate_unique_key(
        dataframe=nlp_input_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention_nlp_input",
    )
    for column_name in ("mention_id", "leader_id", "canonical_article_id"):
        blank_values = nlp_input_dataframe[column_name].map(is_null_or_blank)
        if blank_values.any():
            raise DataQualityError(f"fact_mention_nlp_input {column_name} has blanks")
    if nlp_input_dataframe["eligible_for_lexicon"].isna().any():
        raise DataQualityError("fact_mention_nlp_input eligible_for_lexicon has nulls")

    word_counts = pd.to_numeric(
        nlp_input_dataframe["context_word_count"],
        errors="coerce",
    )
    if word_counts.isna().any() or (word_counts < 0).any():
        raise DataQualityError(
            "fact_mention_nlp_input context_word_count must be non-negative numeric"
        )

    eligible_rows = nlp_input_dataframe["eligible_for_lexicon"].astype(bool)
    blank_eligible_text = nlp_input_dataframe.loc[eligible_rows, "input_text"].map(
        is_null_or_blank
    )
    if blank_eligible_text.any():
        raise DataQualityError(
            "fact_mention_nlp_input eligible rows must have non-empty input_text"
        )


def _validate_sample_leaders(sample_leaders_dataframe: pd.DataFrame) -> None:
    """Validate sample leader fields used by trait marts."""
    require_columns(
        dataframe=sample_leaders_dataframe,
        required_columns=_REQUIRED_SAMPLE_COLUMNS,
        dataframe_name="sample_leaders",
    )
    validate_unique_key(
        dataframe=sample_leaders_dataframe,
        key_columns=("leader_id",),
        dataframe_name="sample_leaders",
    )


def _validate_exposure_metrics(exposure_metrics_dataframe: pd.DataFrame) -> None:
    """Validate exposure fields used by trait outlier scenarios."""
    require_columns(
        dataframe=exposure_metrics_dataframe,
        required_columns=_REQUIRED_EXPOSURE_COLUMNS,
        dataframe_name="mart_exposure_metrics",
    )
    validate_unique_key(
        dataframe=exposure_metrics_dataframe,
        key_columns=("leader_id",),
        dataframe_name="mart_exposure_metrics",
    )
    article_counts = pd.to_numeric(
        exposure_metrics_dataframe["article_count"],
        errors="coerce",
    )
    if article_counts.isna().any() or (article_counts < 0).any():
        raise DataQualityError("mart_exposure_metrics article_count must be >= 0")


def _count_trait_terms(
    input_tokens: list[str],
    trait_terms: tuple[TraitLexiconTerm, ...],
) -> Counter[TraitLexiconTerm]:
    """Count exact token or phrase matches for each trait term."""
    term_counts: Counter[TraitLexiconTerm] = Counter()
    for trait_term in trait_terms:
        term_length = len(trait_term.tokens)
        if term_length > len(input_tokens):
            continue
        for start_index in range(0, len(input_tokens) - term_length + 1):
            candidate_tokens = tuple(
                input_tokens[start_index : start_index + term_length]
            )
            if candidate_tokens == trait_term.tokens:
                term_counts[trait_term] += 1
    return term_counts


def _build_analysis_base(
    *,
    nlp_input_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    exposure_metrics_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Build one mention-level base with candidate and exposure metadata."""
    base_dataframe = nlp_input_dataframe.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "canonical_article_id",
            "input_text",
            "context_word_count",
        ],
    ].copy()
    sample_dataframe = sample_leaders_dataframe.loc[
        :, ["leader_id", "full_name", "gender", "commune_name"]
    ].copy()
    exposure_dataframe = exposure_metrics_dataframe.loc[
        :, ["leader_id", "article_count"]
    ].copy()
    base_dataframe = base_dataframe.merge(
        sample_dataframe,
        on="leader_id",
        how="left",
        validate="many_to_one",
    )
    if base_dataframe["gender"].isna().any():
        raise DataQualityError(
            "trait analysis has mentions missing from sample_leaders"
        )
    base_dataframe = base_dataframe.merge(
        exposure_dataframe,
        on="leader_id",
        how="left",
        validate="many_to_one",
    )
    if base_dataframe["article_count"].isna().any():
        raise DataQualityError(
            "trait analysis has mentions missing from mart_exposure_metrics"
        )
    return base_dataframe


def _build_scenario_exclusions(
    exposure_metrics_dataframe: pd.DataFrame,
) -> dict[str, set[str]]:
    """Return deterministic outlier exclusion sets by scenario."""
    if exposure_metrics_dataframe.empty:
        return {scenario_id: set() for scenario_id in CONTROLLED_TRAIT_SCENARIOS}

    exposure = exposure_metrics_dataframe.copy()
    overall_top = str(exposure.loc[exposure["article_count"].idxmax(), "leader_id"])
    male_exposure = exposure.loc[exposure["gender"].eq("M")]
    female_exposure = exposure.loc[exposure["gender"].eq("F")]
    male_top = (
        str(male_exposure.loc[male_exposure["article_count"].idxmax(), "leader_id"])
        if not male_exposure.empty
        else ""
    )
    female_top = (
        str(
            female_exposure.loc[
                female_exposure["article_count"].idxmax(),
                "leader_id",
            ]
        )
        if not female_exposure.empty
        else ""
    )
    return {
        "all": set(),
        "drop_top_overall": {overall_top} if overall_top else set(),
        "drop_top_each_gender": {
            leader_id for leader_id in (male_top, female_top) if leader_id
        },
    }


def _build_mart_trait_metrics(
    *,
    analysis_base: pd.DataFrame,
    trait_word_counts_dataframe: pd.DataFrame,
    scenario_exclusions: dict[str, set[str]],
    generated_at: str,
) -> pd.DataFrame:
    """Build gender-level trait metric rows for dashboard charts."""
    rows: list[dict[str, object]] = []
    for scenario_id in CONTROLLED_TRAIT_SCENARIOS:
        excluded_leaders = scenario_exclusions[scenario_id]
        scenario_base = analysis_base.loc[
            ~analysis_base["leader_id"].isin(excluded_leaders)
        ].copy()
        scenario_counts = trait_word_counts_dataframe.loc[
            ~trait_word_counts_dataframe["leader_id"].isin(excluded_leaders)
        ].copy()
        for trait_tier, gender, trait_category in itertools.product(
            CONTROLLED_TRAIT_TIERS,
            sorted(scenario_base["gender"].dropna().unique().tolist()),
            CONTROLLED_TRAIT_CATEGORIES,
        ):
            segment_base = scenario_base.loc[scenario_base["gender"].eq(gender)]
            segment_counts = scenario_counts.loc[
                scenario_counts["leader_id"].isin(segment_base["leader_id"])
                & scenario_counts["trait_tier"].eq(trait_tier)
                & scenario_counts["trait_category"].eq(trait_category)
            ]
            mention_count = int(len(segment_base))
            hit_mentions = int(segment_counts["mention_id"].nunique())
            term_hits = int(segment_counts["count"].sum())
            context_word_count = int(segment_base["context_word_count"].sum())
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "trait_tier": trait_tier,
                    "gender": gender,
                    "trait_category": trait_category,
                    "mention_count": mention_count,
                    "hit_mentions": hit_mentions,
                    "term_hits": term_hits,
                    "context_word_count": context_word_count,
                    "hits_per_1k_context_words": _safe_rate_per_1k(
                        term_hits,
                        context_word_count,
                    ),
                    "coverage_rate": _safe_share(hit_mentions, mention_count),
                    "evidence_level": _evidence_level(hit_mentions),
                    "generated_at": generated_at,
                }
            )
    return pd.DataFrame(rows, columns=MART_TRAIT_METRICS_COLUMNS)


def _build_mart_trait_top_terms(
    *,
    analysis_base: pd.DataFrame,
    trait_word_counts_dataframe: pd.DataFrame,
    scenario_exclusions: dict[str, set[str]],
    generated_at: str,
) -> pd.DataFrame:
    """Build ranked top-term rows by scenario, tier, gender, and category."""
    rows: list[dict[str, object]] = []
    for scenario_id in CONTROLLED_TRAIT_SCENARIOS:
        excluded_leaders = scenario_exclusions[scenario_id]
        scenario_base = analysis_base.loc[
            ~analysis_base["leader_id"].isin(excluded_leaders)
        ]
        scenario_counts = trait_word_counts_dataframe.loc[
            ~trait_word_counts_dataframe["leader_id"].isin(excluded_leaders)
        ].merge(
            scenario_base[["mention_id", "gender"]],
            on="mention_id",
            how="left",
            validate="many_to_one",
        )
        if scenario_counts.empty:
            continue
        grouped = (
            scenario_counts.groupby(
                ["trait_tier", "gender", "trait_category", "term"],
                dropna=False,
            )
            .agg(
                term_hits=("count", "sum"),
                hit_mentions=("mention_id", "nunique"),
            )
            .reset_index()
            .sort_values(
                [
                    "trait_tier",
                    "gender",
                    "trait_category",
                    "term_hits",
                    "hit_mentions",
                    "term",
                ],
                ascending=[True, True, True, False, False, True],
            )
        )
        grouped["rank"] = (
            grouped.groupby(["trait_tier", "gender", "trait_category"]).cumcount() + 1
        )
        for row in grouped.itertuples(index=False):
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "trait_tier": row.trait_tier,
                    "gender": row.gender,
                    "trait_category": row.trait_category,
                    "term": row.term,
                    "term_hits": int(row.term_hits),
                    "hit_mentions": int(row.hit_mentions),
                    "rank": int(row.rank),
                    "generated_at": generated_at,
                }
            )
    return pd.DataFrame(rows, columns=MART_TRAIT_TOP_TERMS_COLUMNS)


def _build_mart_trait_candidate_metrics(
    *,
    analysis_base: pd.DataFrame,
    trait_word_counts_dataframe: pd.DataFrame,
    scenario_exclusions: dict[str, set[str]],
    generated_at: str,
) -> pd.DataFrame:
    """Build candidate-level trait metric rows for drilldown tables."""
    rows: list[dict[str, object]] = []
    candidate_base = (
        analysis_base.groupby(
            ["leader_id", "full_name", "gender", "commune_name"],
            dropna=False,
        )
        .agg(
            mention_count=("mention_id", "count"),
            context_word_count=("context_word_count", "sum"),
            article_count=("article_count", "max"),
        )
        .reset_index()
    )
    for scenario_id in CONTROLLED_TRAIT_SCENARIOS:
        excluded_leaders = scenario_exclusions[scenario_id]
        scenario_candidates = candidate_base.loc[
            ~candidate_base["leader_id"].isin(excluded_leaders)
        ]
        scenario_counts = trait_word_counts_dataframe.loc[
            ~trait_word_counts_dataframe["leader_id"].isin(excluded_leaders)
        ]
        for candidate_row, trait_tier, trait_category in itertools.product(
            scenario_candidates.itertuples(index=False),
            CONTROLLED_TRAIT_TIERS,
            CONTROLLED_TRAIT_CATEGORIES,
        ):
            segment_counts = scenario_counts.loc[
                scenario_counts["leader_id"].eq(candidate_row.leader_id)
                & scenario_counts["trait_tier"].eq(trait_tier)
                & scenario_counts["trait_category"].eq(trait_category)
            ]
            hit_mentions = int(segment_counts["mention_id"].nunique())
            term_hits = int(segment_counts["count"].sum())
            rows.append(
                {
                    "scenario_id": scenario_id,
                    "trait_tier": trait_tier,
                    "leader_id": candidate_row.leader_id,
                    "full_name": candidate_row.full_name,
                    "gender": candidate_row.gender,
                    "commune_name": candidate_row.commune_name,
                    "trait_category": trait_category,
                    "article_count": int(candidate_row.article_count),
                    "mention_count": int(candidate_row.mention_count),
                    "hit_mentions": hit_mentions,
                    "term_hits": term_hits,
                    "context_word_count": int(candidate_row.context_word_count),
                    "hits_per_1k_context_words": _safe_rate_per_1k(
                        term_hits,
                        int(candidate_row.context_word_count),
                    ),
                    "coverage_rate": _safe_share(
                        hit_mentions,
                        int(candidate_row.mention_count),
                    ),
                    "generated_at": generated_at,
                }
            )
    return pd.DataFrame(rows, columns=MART_TRAIT_CANDIDATE_COLUMNS)


def _build_mart_trait_qa_samples(
    *,
    analysis_base: pd.DataFrame,
    trait_word_counts_dataframe: pd.DataFrame,
    generated_at: str,
) -> pd.DataFrame:
    """Build representative matched context rows for human QA."""
    if trait_word_counts_dataframe.empty:
        return pd.DataFrame(columns=MART_TRAIT_QA_SAMPLE_COLUMNS)

    sample_base = analysis_base.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "full_name",
            "gender",
            "input_text",
            "article_count",
        ],
    ]
    sample_counts = trait_word_counts_dataframe.merge(
        sample_base,
        on=["mention_id", "leader_id"],
        how="left",
        validate="many_to_one",
    )
    # Tie-break: prefer high-count matches and high-exposure leaders for QA review,
    # then use term and mention IDs for deterministic output.
    sample_counts = sample_counts.sort_values(
        [
            "trait_tier",
            "trait_category",
            "count",
            "article_count",
            "term",
            "mention_id",
        ],
        ascending=[True, True, False, False, True, True],
    )
    sample_counts = sample_counts.drop_duplicates(
        ["trait_tier", "trait_category", "mention_id", "term"]
    )
    sample_counts["sample_rank"] = (
        sample_counts.groupby(["trait_tier", "trait_category"]).cumcount() + 1
    )
    sample_counts = sample_counts.loc[
        sample_counts["sample_rank"].le(_QA_SAMPLES_PER_CATEGORY_TIER)
    ]
    rows = []
    for row in sample_counts.itertuples(index=False):
        rows.append(
            {
                "trait_tier": row.trait_tier,
                "trait_category": row.trait_category,
                "term": row.term,
                "gender": row.gender,
                "leader_id": row.leader_id,
                "full_name": row.full_name,
                "mention_id": row.mention_id,
                "context_excerpt": _excerpt(row.input_text),
                "rationale": row.rationale,
                "lexicon_version": row.lexicon_version,
                "generated_at": generated_at,
            }
        )
    return pd.DataFrame(rows, columns=MART_TRAIT_QA_SAMPLE_COLUMNS)


def _safe_rate_per_1k(count: int, denominator_words: int) -> float:
    """Return count per 1k words with a zero-denominator guard."""
    if denominator_words <= 0:
        return 0.0
    return float(count / denominator_words * 1000)


def _safe_share(numerator: int, denominator: int) -> float:
    """Return a share with a zero-denominator guard."""
    if denominator <= 0:
        return 0.0
    return float(numerator / denominator)


def _evidence_level(hit_mentions: int) -> str:
    """Classify dashboard evidence strength for a category segment."""
    if hit_mentions < _TABLE_ONLY_MIN_HIT_MENTIONS:
        return "table_only"
    if hit_mentions < _SPARSE_EVIDENCE_MIN_HIT_MENTIONS:
        return "sparse_evidence"
    return "chart_ready"


def _excerpt(value: object) -> str:
    """Return a short context excerpt for QA without persisting full articles."""
    text = str(value) if not is_null_or_blank(value) else ""
    if len(text) <= _QA_CONTEXT_EXCERPT_CHARS:
        return text
    return text[: _QA_CONTEXT_EXCERPT_CHARS - 3].rstrip() + "..."


def _write_trait_artifact(
    dataframe: pd.DataFrame,
    *,
    parquet_path: Path,
    schema_name: str,
    table_name: str,
    duckdb_path: Path,
) -> None:
    """Persist one trait artifact to Parquet and DuckDB."""
    write_parquet_table(dataframe, parquet_path)
    write_duckdb_table(
        dataframe=dataframe,
        schema_name=schema_name,
        table_name=table_name,
        duckdb_path=duckdb_path,
    )
