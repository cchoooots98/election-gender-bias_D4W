"""NLP enrichment contracts and scoring utilities."""

from src.nlp.input_contracts import (
    CONTROLLED_SKIP_REASONS,
    FACT_MENTION_NLP_INPUT_COLUMNS,
    build_fact_mention_nlp_input,
    compute_input_hash,
    materialize_fact_mention_nlp_input,
    validate_fact_mention_nlp_input,
)
from src.nlp.model_bundle import (
    ModelBundleConfig,
    build_model_bundle_config,
    resolve_model_device,
)

__all__ = [
    "CONTROLLED_SKIP_REASONS",
    "FACT_MENTION_NLP_INPUT_COLUMNS",
    "ModelBundleConfig",
    "build_fact_mention_nlp_input",
    "build_model_bundle_config",
    "compute_input_hash",
    "materialize_fact_mention_nlp_input",
    "resolve_model_device",
    "validate_fact_mention_nlp_input",
]
