"""NLP enrichment contracts and scoring utilities."""

from src.nlp.input_contracts import (
    CONTROLLED_SKIP_REASONS,
    FACT_MENTION_NLP_INPUT_COLUMNS,
    build_fact_mention_nlp_input,
    compute_input_hash,
    materialize_fact_mention_nlp_input,
    validate_fact_mention_nlp_input,
)
from src.nlp.lexicon import (
    CONTROLLED_LEXICON_CATEGORIES,
    FACT_STEREOTYPE_WORD_COUNTS_COLUMNS,
    LexiconConfig,
    LexiconTerm,
    build_fact_stereotype_word_counts,
    load_stereotype_lexicon,
    materialize_fact_stereotype_word_counts,
    validate_fact_stereotype_word_counts,
)
from src.nlp.model_bundle import (
    ModelBundleConfig,
    build_model_bundle_config,
    resolve_model_device,
)
from src.nlp.normalization import (
    is_missing_scalar,
    is_null_or_blank,
    normalize_lexicon_text,
    tokenize_lexicon_text,
)

__all__ = [
    "CONTROLLED_SKIP_REASONS",
    "CONTROLLED_LEXICON_CATEGORIES",
    "FACT_MENTION_NLP_INPUT_COLUMNS",
    "FACT_STEREOTYPE_WORD_COUNTS_COLUMNS",
    "LexiconConfig",
    "LexiconTerm",
    "ModelBundleConfig",
    "build_fact_stereotype_word_counts",
    "build_fact_mention_nlp_input",
    "build_model_bundle_config",
    "compute_input_hash",
    "is_missing_scalar",
    "is_null_or_blank",
    "load_stereotype_lexicon",
    "materialize_fact_mention_nlp_input",
    "materialize_fact_stereotype_word_counts",
    "normalize_lexicon_text",
    "resolve_model_device",
    "tokenize_lexicon_text",
    "validate_fact_mention_nlp_input",
    "validate_fact_stereotype_word_counts",
]
