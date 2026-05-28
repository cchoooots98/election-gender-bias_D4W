"""Target-aware tone and framing scoring with Natural Language Inference.

This module enriches ``silver.fact_mention_nlp_summary`` with candidate-aware
tone labels and Phase 4 frame labels while keeping Transformer imports lazy.
Tests inject mocked runners, so CI never needs GPU, internet, or model
downloads.
"""

from __future__ import annotations

import logging
import math
import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from src.config.settings import NLP_MODEL_CACHE_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp._validation import (
    pipeline_device_arg,
    require_columns,
    validate_unique_key,
)
from src.nlp.model_bundle import ModelBundleConfig, build_model_bundle_config
from src.nlp.normalization import is_null_or_blank
from src.nlp.sentiment import (
    FACT_MENTION_NLP_SUMMARY_COLUMNS,
    TransformerDependencyError,
    validate_fact_mention_nlp_summary,
)
from src.storage.tables import write_duckdb_table, write_parquet_table
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

SCORABLE_TARGET_TONE_LABELS: tuple[str, ...] = (
    "favorable",
    "unfavorable",
    "neutral",
)
CONTROLLED_TARGET_TONE_LABELS: tuple[str, ...] = (
    *SCORABLE_TARGET_TONE_LABELS,
    "unclassified",
)
SCORABLE_FRAME_LABELS: tuple[str, ...] = (
    "politique",
    "vie_privee",
    "apparence",
    "scandale",
    "personnalite",
    "securite",
)
CONTROLLED_FRAME_LABELS: tuple[str, ...] = (
    *SCORABLE_FRAME_LABELS,
    "unclassified",
)
NLI_TONE_MODEL_LABEL_BY_TONE_LABEL: dict[str, str] = {
    "favorable": "favorable",
    "unfavorable": "defavorable",
    "neutral": "neutre",
}
NLI_TONE_LABEL_BY_MODEL_LABEL: dict[str, str] = {
    model_label: tone_label
    for tone_label, model_label in NLI_TONE_MODEL_LABEL_BY_TONE_LABEL.items()
}
NLI_TONE_HYPOTHESIS_TEMPLATE_PATTERN = (
    "Le texte présente {candidate_name} de manière {{}}."
)

NLI_FRAME_MODEL_LABEL_BY_FRAME_LABEL: dict[str, str] = {
    "politique": (
        "le programme politique, la gouvernance ou l'action publique du candidat"
    ),
    "vie_privee": (
        "la vie privee, la famille ou la biographie personnelle du candidat"
    ),
    "apparence": "l'apparence, l'age ou la presentation physique du candidat",
    "scandale": (
        "une controverse, une affaire judiciaire ou un scandale impliquant "
        "le candidat"
    ),
    "personnalite": (
        "la personnalite, le caractere ou le style de leadership du candidat"
    ),
    "securite": "la securite, la police ou l'ordre public",
}
NLI_FRAME_LABEL_BY_MODEL_LABEL: dict[str, str] = {
    model_label: frame_label
    for frame_label, model_label in NLI_FRAME_MODEL_LABEL_BY_FRAME_LABEL.items()
}
NLI_FRAME_HYPOTHESIS_TEMPLATE = "Le texte discute {}."
FACT_MENTION_FRAME_SCORE_COLUMNS: tuple[str, ...] = (
    "mention_id",
    "frame_label",
    "frame_probability",
    "is_primary_frame",
    "passes_threshold",
    "nli_hypothesis",
    "nlp_model_bundle_version",
)

# The pinned cmarkea NLI revision currently publishes PyTorch .bin weights.
# Explicitly choosing that format and disabling conversion keeps Transformers
# from opening a background safetensors conversion thread during local inference.
_DISABLE_SAFETENSORS_CONVERSION_ENV_VAR = "DISABLE_SAFETENSORS_CONVERSION"
_NLI_USE_SAFETENSORS = False

_REQUIRED_NLP_INPUT_COLUMNS = frozenset(
    {
        "mention_id",
        "leader_id",
        "canonical_article_id",
        "input_text",
        "input_hash",
        "eligible_for_inference",
        "skip_reason",
    }
)
_REQUIRED_SAMPLE_LEADER_COLUMNS = frozenset({"leader_id", "full_name"})
_REQUIRED_FRAME_SCORE_COLUMNS = frozenset(FACT_MENTION_FRAME_SCORE_COLUMNS)
_CORE_IDENTIFIER_COLUMNS = (
    "mention_id",
    "leader_id",
    "canonical_article_id",
)


class NliModelLoadError(RuntimeError):
    """Raised when the NLI model cannot be loaded for a scoring run."""


@dataclass(frozen=True)
class ToneScoringInput:
    """One candidate-aware tone scoring input.

    Args:
        mention_id: Stable mention identifier used to reconcile predictions.
        input_text: Mention-level context text from the NLP input contract.
        candidate_name: Candidate display name from ``gold.sample_leaders``.
    """

    mention_id: str
    input_text: str
    candidate_name: str


@dataclass(frozen=True)
class TonePrediction:
    """One target-aware tone prediction for a mention context.

    Args:
        probabilities_by_label: Probability distribution keyed by controlled
            tone labels: ``favorable``, ``unfavorable``, and ``neutral``.
        was_truncated_to_max_length: Whether the tokenizer input exceeded the
            configured maximum token length before truncation.
    """

    probabilities_by_label: Mapping[str, float]
    was_truncated_to_max_length: bool = False


@dataclass(frozen=True)
class FrameScoringInput:
    """One frame scoring input.

    Args:
        mention_id: Stable mention identifier used to reconcile predictions.
        input_text: Mention-level context text from the NLP input contract.
    """

    mention_id: str
    input_text: str


@dataclass(frozen=True)
class FramePrediction:
    """One multi-label frame prediction for a mention context.

    Args:
        probabilities_by_label: Probability distribution keyed by controlled
            frame labels, excluding ``unclassified``.
        was_truncated_to_max_length: Whether tokenizer input exceeded the
            configured maximum token length before truncation.
    """

    probabilities_by_label: Mapping[str, float]
    was_truncated_to_max_length: bool = False


class ToneRunner(Protocol):
    """Protocol implemented by real and mocked NLI tone scorers."""

    def predict_batch(
        self,
        scoring_inputs: Sequence[ToneScoringInput],
    ) -> list[TonePrediction]:
        """Return tone predictions in the same order as ``scoring_inputs``."""


class FrameRunner(Protocol):
    """Protocol implemented by real and mocked NLI frame scorers."""

    def predict_batch(
        self,
        scoring_inputs: Sequence[FrameScoringInput],
    ) -> list[FramePrediction]:
        """Return frame predictions in the same order as ``scoring_inputs``."""


class HuggingFaceNliToneRunner:
    """Lazy Hugging Face adapter for candidate-aware French NLI tone.

    Args:
        model_bundle_config: Versioned model metadata used for loading the
            configured NLI model and tokenizer.
    """

    def __init__(self, model_bundle_config: ModelBundleConfig) -> None:
        self._model_bundle_config = model_bundle_config
        self._analyzer: Any | None = None

    def predict_batch(
        self,
        scoring_inputs: Sequence[ToneScoringInput],
    ) -> list[TonePrediction]:
        """Score mention contexts with the configured zero-shot NLI model.

        Args:
            scoring_inputs: Candidate-aware mention contexts.

        Returns:
            Ordered tone predictions.

        Raises:
            TransformerDependencyError: If ``transformers`` is not installed.
            NliModelLoadError: If model or tokenizer loading fails.
            RuntimeError: If the Hugging Face pipeline returns an unsupported
                output shape.
        """
        if not scoring_inputs:
            return []

        analyzer = self._get_analyzer()
        predictions: list[TonePrediction] = []
        candidate_labels = tuple(NLI_TONE_LABEL_BY_MODEL_LABEL)
        for scoring_input in scoring_inputs:
            hypothesis_template = build_tone_hypothesis_template(
                scoring_input.candidate_name
            )
            raw_result = analyzer(
                scoring_input.input_text,
                candidate_labels=candidate_labels,
                hypothesis_template=hypothesis_template,
                multi_label=False,
                truncation=True,
                max_length=self._model_bundle_config.max_token_length,
            )
            predictions.append(
                TonePrediction(
                    probabilities_by_label=_normalize_zero_shot_result(raw_result),
                    was_truncated_to_max_length=self._was_text_truncated(
                        scoring_input.input_text,
                        scoring_input.candidate_name,
                    ),
                )
            )
        return predictions

    def _get_analyzer(self) -> Any:
        """Load the Hugging Face zero-shot pipeline on first use."""
        if self._analyzer is not None:
            return self._analyzer

        self._analyzer = _load_huggingface_zero_shot_analyzer(self._model_bundle_config)
        return self._analyzer

    def _was_text_truncated(self, text: str, candidate_name: str) -> bool:
        """Return whether any NLI premise/hypothesis pair exceeds max length."""
        analyzer = self._get_analyzer()
        tokenizer = getattr(analyzer, "tokenizer", None)
        if tokenizer is None:
            return False

        hypothesis_template = build_tone_hypothesis_template(candidate_name)
        for model_label in NLI_TONE_LABEL_BY_MODEL_LABEL:
            encoded = tokenizer(
                text,
                hypothesis_template.format(model_label),
                add_special_tokens=True,
                truncation=False,
                verbose=False,
            )
            input_ids = encoded.get("input_ids", [])
            if len(input_ids) > self._model_bundle_config.max_token_length:
                return True
        return False


def _load_huggingface_zero_shot_analyzer(
    model_bundle_config: ModelBundleConfig,
) -> Any:
    """Load the configured Hugging Face NLI zero-shot pipeline."""
    try:
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
            pipeline,
        )
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise TransformerDependencyError(
            "transformers is required for NLP NLI scoring. Install the "
            "optional future stack with: pip install -r requirements-future.in"
        ) from exc

    try:
        cache_kwargs = (
            {"cache_dir": str(NLP_MODEL_CACHE_DIR)}
            if NLP_MODEL_CACHE_DIR is not None
            else {}
        )
        with _disable_safetensors_auto_conversion():
            tokenizer = AutoTokenizer.from_pretrained(
                model_bundle_config.nli_model_name,
                revision=model_bundle_config.nli_model_revision,
                use_fast=False,
                **cache_kwargs,
            )
            model = AutoModelForSequenceClassification.from_pretrained(
                model_bundle_config.nli_model_name,
                revision=model_bundle_config.nli_model_revision,
                use_safetensors=_NLI_USE_SAFETENSORS,
                **cache_kwargs,
            )
            return pipeline(
                task="zero-shot-classification",
                model=model,
                tokenizer=tokenizer,
                device=pipeline_device_arg(model_bundle_config.device),
            )
    except Exception as exc:
        raise NliModelLoadError(
            "Could not load the Hugging Face NLI model. The pinned cmarkea "
            "NLI revision is loaded from PyTorch .bin weights with "
            "use_safetensors=False and DISABLE_SAFETENSORS_CONVERSION=1 to "
            "avoid background safetensors conversion. If the model is already "
            "cached, retry with HF_HUB_OFFLINE=1. CamemBERT tokenizers require "
            "the optional SentencePiece dependency; install the future NLP "
            "stack with: pip install -r requirements-future.in"
        ) from exc


@contextmanager
def _disable_safetensors_auto_conversion() -> Iterator[None]:
    """Disable Hugging Face safetensors conversion only during NLI model load."""
    previous_value = os.environ.get(_DISABLE_SAFETENSORS_CONVERSION_ENV_VAR)
    os.environ[_DISABLE_SAFETENSORS_CONVERSION_ENV_VAR] = "1"
    try:
        yield
    finally:
        if previous_value is None:
            os.environ.pop(_DISABLE_SAFETENSORS_CONVERSION_ENV_VAR, None)
        else:
            os.environ[_DISABLE_SAFETENSORS_CONVERSION_ENV_VAR] = previous_value


class HuggingFaceNliFrameRunner:
    """Lazy Hugging Face adapter for French multi-label frame scoring.

    Args:
        model_bundle_config: Versioned model metadata used for loading the
            configured NLI model and tokenizer.
    """

    def __init__(self, model_bundle_config: ModelBundleConfig) -> None:
        self._model_bundle_config = model_bundle_config
        self._analyzer: Any | None = None

    def predict_batch(
        self,
        scoring_inputs: Sequence[FrameScoringInput],
    ) -> list[FramePrediction]:
        """Score mention contexts with the configured zero-shot NLI model.

        Args:
            scoring_inputs: Mention contexts to classify into controlled
                frame labels.

        Returns:
            Ordered frame predictions.

        Raises:
            TransformerDependencyError: If ``transformers`` is not installed.
            NliModelLoadError: If model or tokenizer loading fails.
            RuntimeError: If the Hugging Face pipeline returns an unsupported
                output shape.
        """
        if not scoring_inputs:
            return []

        analyzer = self._get_analyzer()
        predictions: list[FramePrediction] = []
        candidate_labels = tuple(NLI_FRAME_LABEL_BY_MODEL_LABEL)
        for scoring_input in scoring_inputs:
            raw_result = analyzer(
                scoring_input.input_text,
                candidate_labels=candidate_labels,
                hypothesis_template=NLI_FRAME_HYPOTHESIS_TEMPLATE,
                multi_label=True,
                truncation=True,
                max_length=self._model_bundle_config.max_token_length,
            )
            predictions.append(
                FramePrediction(
                    probabilities_by_label=_normalize_frame_zero_shot_result(
                        raw_result
                    ),
                    was_truncated_to_max_length=self._was_text_truncated(
                        scoring_input.input_text
                    ),
                )
            )
        return predictions

    def _get_analyzer(self) -> Any:
        """Load the Hugging Face zero-shot pipeline on first use."""
        if self._analyzer is not None:
            return self._analyzer

        self._analyzer = _load_huggingface_zero_shot_analyzer(self._model_bundle_config)
        return self._analyzer

    def _was_text_truncated(self, text: str) -> bool:
        """Return whether any NLI premise/hypothesis pair exceeds max length."""
        analyzer = self._get_analyzer()
        tokenizer = getattr(analyzer, "tokenizer", None)
        if tokenizer is None:
            return False

        for frame_label in SCORABLE_FRAME_LABELS:
            encoded = tokenizer(
                text,
                build_frame_hypothesis(frame_label),
                add_special_tokens=True,
                truncation=False,
                verbose=False,
            )
            input_ids = encoded.get("input_ids", [])
            if len(input_ids) > self._model_bundle_config.max_token_length:
                return True
        return False


def build_tone_hypothesis_template(candidate_name: str) -> str:
    """Build the exact target-aware NLI hypothesis template.

    Args:
        candidate_name: Candidate display name from ``gold.sample_leaders``.

    Returns:
        Hypothesis template with the zero-shot ``{}`` label placeholder.

    Raises:
        DataQualityError: If ``candidate_name`` is blank.
    """
    if is_null_or_blank(candidate_name):
        raise DataQualityError("candidate_name is required for tone hypotheses")
    return NLI_TONE_HYPOTHESIS_TEMPLATE_PATTERN.format(
        candidate_name=str(candidate_name).strip()
    )


def build_frame_hypothesis(frame_label: str) -> str:
    """Build the exact NLI hypothesis used for one controlled frame.

    Args:
        frame_label: Controlled frame label, excluding ``unclassified``.

    Returns:
        Full French hypothesis string sent to the zero-shot NLI pipeline.

    Raises:
        DataQualityError: If ``frame_label`` is not a supported scorable frame.
    """
    normalized_frame_label = str(frame_label).strip()
    if normalized_frame_label not in SCORABLE_FRAME_LABELS:
        raise DataQualityError(f"unsupported frame label: {normalized_frame_label}")
    model_label = NLI_FRAME_MODEL_LABEL_BY_FRAME_LABEL[normalized_frame_label]
    return NLI_FRAME_HYPOTHESIS_TEMPLATE.format(model_label)


def select_target_tone_label(
    probabilities_by_label: Mapping[str, float],
    *,
    threshold: float,
) -> tuple[str, float]:
    """Select the persisted target-aware tone label from NLI probabilities.

    Args:
        probabilities_by_label: Probability distribution keyed by
            ``favorable``, ``unfavorable``, and ``neutral``.
        threshold: Minimum probability required to persist a scored tone label.

    Returns:
        Tuple of selected label and selected probability. Low-confidence
        predictions return ``unclassified`` with the top probability retained
        for threshold auditability.

    Raises:
        DataQualityError: If labels or probabilities violate the tone contract.
        ValueError: If ``threshold`` is outside ``[0, 1]``.
    """
    threshold_value = float(threshold)
    if not math.isfinite(threshold_value) or not 0 <= threshold_value <= 1:
        raise ValueError("threshold must be between 0 and 1")

    probabilities_by_tone = _normalize_tone_probabilities(probabilities_by_label)
    top_label = max(
        SCORABLE_TARGET_TONE_LABELS,
        key=lambda tone_label: probabilities_by_tone[tone_label],
    )
    top_probability = float(probabilities_by_tone[top_label])
    if top_probability < threshold_value:
        return "unclassified", top_probability
    return top_label, top_probability


def select_primary_frame(
    probabilities_by_label: Mapping[str, float],
    *,
    threshold: float | None = None,
    thresholds_by_frame: Mapping[str, float] | None = None,
) -> tuple[str, float | None]:
    """Select the persisted primary frame from NLI probabilities.

    Args:
        probabilities_by_label: Probability distribution keyed by controlled
            frame labels, excluding ``unclassified``.
        threshold: Minimum probability required to persist a primary frame when
            the same threshold is used for every frame.
        thresholds_by_frame: Optional per-frame thresholds. When supplied, the
            selected top frame is compared with its own threshold.

    Returns:
        Tuple of selected frame label and selected probability. Low-confidence
        predictions return ``unclassified`` with ``None`` probability because
        ``unclassified`` is a fallback state, not a model-scored frame.

    Raises:
        DataQualityError: If labels or probabilities violate the frame contract.
        ValueError: If ``threshold`` is outside ``[0, 1]``.
    """
    probabilities_by_frame = _normalize_frame_probabilities(probabilities_by_label)
    normalized_thresholds = _normalize_frame_thresholds_for_selection(
        threshold=threshold,
        thresholds_by_frame=thresholds_by_frame,
    )
    top_label = max(
        SCORABLE_FRAME_LABELS,
        key=lambda frame_label: probabilities_by_frame[frame_label],
    )
    top_probability = float(probabilities_by_frame[top_label])
    if top_probability < normalized_thresholds[top_label]:
        return "unclassified", None
    return top_label, top_probability


def _normalize_frame_thresholds_for_selection(
    *,
    threshold: float | None,
    thresholds_by_frame: Mapping[str, float] | None,
) -> dict[str, float]:
    """Return a complete threshold mapping for frame selection."""
    if thresholds_by_frame is None:
        if threshold is None:
            raise ValueError("threshold is required when thresholds_by_frame is absent")
        threshold_value = float(threshold)
        if not math.isfinite(threshold_value) or not 0 <= threshold_value <= 1:
            raise ValueError("threshold must be between 0 and 1")
        return {frame_label: threshold_value for frame_label in SCORABLE_FRAME_LABELS}

    unsupported_labels = sorted(set(thresholds_by_frame) - set(SCORABLE_FRAME_LABELS))
    missing_labels = sorted(set(SCORABLE_FRAME_LABELS) - set(thresholds_by_frame))
    if unsupported_labels or missing_labels:
        raise DataQualityError(
            "frame thresholds must cover exactly the scorable frame labels; "
            f"unsupported={unsupported_labels} missing={missing_labels}"
        )

    normalized_thresholds: dict[str, float] = {}
    for frame_label in SCORABLE_FRAME_LABELS:
        threshold_value = float(thresholds_by_frame[frame_label])
        if not math.isfinite(threshold_value) or not 0 <= threshold_value <= 1:
            raise ValueError("frame thresholds must be between 0 and 1")
        normalized_thresholds[frame_label] = threshold_value
    return normalized_thresholds


def enrich_fact_mention_nlp_summary_with_tone(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    *,
    tone_runner: ToneRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
) -> pd.DataFrame:
    """Enrich an existing Phase 2 NLP summary with target-aware tone.

    Args:
        nlp_input_dataframe: Current ``silver.fact_mention_nlp_input`` rows.
        nlp_summary_dataframe: Existing Phase 2
            ``silver.fact_mention_nlp_summary`` rows.
        sample_leaders_dataframe: Current ``gold.sample_leaders`` rows. Only
            ``leader_id`` and ``full_name`` are used.
        tone_runner: Optional scorer implementation. Tests pass a mocked
            runner; production uses the lazy Hugging Face adapter.
        model_bundle_config: Optional model-bundle metadata override.

    Returns:
        DataFrame matching the ``silver.fact_mention_nlp_summary`` contract
        with ``target_tone_label`` and ``target_tone_probability`` populated
        for scoreable rows.

    Raises:
        DataQualityError: If source contracts, bundle lineage, candidate joins,
            runner outputs, or enriched tone fields violate the contract.
        TransformerDependencyError: If Transformer dependencies are missing
            when real NLI scoring is requested.
    """
    effective_model_bundle_config = model_bundle_config or build_model_bundle_config()
    _validate_phase3_sources(
        nlp_input_dataframe=nlp_input_dataframe,
        nlp_summary_dataframe=nlp_summary_dataframe,
        sample_leaders_dataframe=sample_leaders_dataframe,
        expected_bundle_version=effective_model_bundle_config.bundle_version,
    )

    enriched_summary_dataframe = nlp_summary_dataframe.loc[
        :, list(FACT_MENTION_NLP_SUMMARY_COLUMNS)
    ].copy()
    enriched_summary_dataframe["target_tone_label"] = "unclassified"
    enriched_summary_dataframe["target_tone_probability"] = None

    leader_lookup_dataframe = _build_leader_lookup(sample_leaders_dataframe)
    scoring_source_dataframe = _build_scoring_source_dataframe(
        nlp_input_dataframe=nlp_input_dataframe,
        nlp_summary_dataframe=enriched_summary_dataframe,
        leader_lookup_dataframe=leader_lookup_dataframe,
    )

    scoreable_dataframe = scoring_source_dataframe.loc[
        scoring_source_dataframe["eligible_for_tone"]
    ].reset_index(drop=True)
    if not scoreable_dataframe.empty:
        effective_runner = tone_runner or HuggingFaceNliToneRunner(
            effective_model_bundle_config
        )
        tone_updates_dataframe = _score_tone_rows(
            scoreable_dataframe=scoreable_dataframe,
            tone_runner=effective_runner,
            model_bundle_config=effective_model_bundle_config,
        )
        enriched_summary_dataframe = _apply_tone_updates(
            enriched_summary_dataframe,
            tone_updates_dataframe,
        )

    validate_fact_mention_nlp_summary(
        enriched_summary_dataframe,
        nlp_input_dataframe,
    )
    _validate_phase3_tone_contract(
        enriched_summary_dataframe,
        nlp_input_dataframe,
    )
    logger.info(
        "Enriched NLP summary with tone rows=%d scoreable=%d classified=%d bundle=%s",
        len(enriched_summary_dataframe),
        len(scoreable_dataframe),
        int(enriched_summary_dataframe["target_tone_label"].ne("unclassified").sum()),
        effective_model_bundle_config.bundle_version,
    )
    return enriched_summary_dataframe


def materialize_fact_mention_nlp_summary_with_tone(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    *,
    tone_runner: ToneRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> pd.DataFrame:
    """Build and persist Phase 3 tone-enriched NLP summary rows.

    Args:
        nlp_input_dataframe: Current ``silver.fact_mention_nlp_input`` rows.
        nlp_summary_dataframe: Existing Phase 2 NLP summary rows.
        sample_leaders_dataframe: Current ``gold.sample_leaders`` rows.
        tone_runner: Optional scorer implementation for tests.
        model_bundle_config: Optional model-bundle metadata override.
        silver_dir: Directory where the Silver Parquet artifact is written.
        duckdb_path: DuckDB warehouse path for the Silver table write.

    Returns:
        The DataFrame written to Parquet and DuckDB.

    Raises:
        DataQualityError: If validation fails before persistence.
        TransformerDependencyError: If NLI scoring is requested without
            optional Transformer dependencies.
        RuntimeError: If DuckDB is unavailable while persisting the table.
    """
    enriched_summary_dataframe = enrich_fact_mention_nlp_summary_with_tone(
        nlp_input_dataframe,
        nlp_summary_dataframe,
        sample_leaders_dataframe,
        tone_runner=tone_runner,
        model_bundle_config=model_bundle_config,
    )
    parquet_path = silver_dir / "fact_mention_nlp_summary.parquet"
    write_parquet_table(enriched_summary_dataframe, parquet_path)
    write_duckdb_table(
        dataframe=enriched_summary_dataframe,
        schema_name="silver",
        table_name="fact_mention_nlp_summary",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized tone-enriched NLP summary parquet_path=%s duckdb_path=%s rows=%d",
        parquet_path,
        duckdb_path,
        len(enriched_summary_dataframe),
    )
    return enriched_summary_dataframe


def enrich_fact_mention_nlp_summary_with_frames(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    *,
    frame_runner: FrameRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Enrich an existing NLP summary with Phase 4 frame scores.

    Args:
        nlp_input_dataframe: Current ``silver.fact_mention_nlp_input`` rows.
        nlp_summary_dataframe: Existing Phase 2 or Phase 3
            ``silver.fact_mention_nlp_summary`` rows.
        frame_runner: Optional scorer implementation. Tests pass a mocked
            runner; production uses the lazy Hugging Face adapter.
        model_bundle_config: Optional model-bundle metadata override.

    Returns:
        Tuple of the updated summary DataFrame and the
        ``silver.fact_mention_frame_score`` DataFrame.

    Raises:
        DataQualityError: If source contracts, bundle lineage, runner outputs,
            or frame-score fields violate the Phase 4 contract.
        TransformerDependencyError: If Transformer dependencies are missing
            when real NLI scoring is requested.
    """
    effective_model_bundle_config = model_bundle_config or build_model_bundle_config()
    _validate_phase4_sources(
        nlp_input_dataframe=nlp_input_dataframe,
        nlp_summary_dataframe=nlp_summary_dataframe,
        expected_bundle_version=effective_model_bundle_config.bundle_version,
    )

    enriched_summary_dataframe = nlp_summary_dataframe.loc[
        :, list(FACT_MENTION_NLP_SUMMARY_COLUMNS)
    ].copy()
    enriched_summary_dataframe["primary_frame_label"] = "unclassified"
    enriched_summary_dataframe["primary_frame_probability"] = None

    scoring_source_dataframe = _build_frame_scoring_source_dataframe(
        nlp_input_dataframe=nlp_input_dataframe,
        nlp_summary_dataframe=enriched_summary_dataframe,
    )
    scoreable_dataframe = scoring_source_dataframe.loc[
        scoring_source_dataframe["eligible_for_frame"]
    ].reset_index(drop=True)

    if scoreable_dataframe.empty:
        frame_updates_dataframe = pd.DataFrame(
            columns=[
                "mention_id",
                "primary_frame_label",
                "primary_frame_probability",
                "was_truncated_to_max_length",
            ]
        )
        frame_score_dataframe = pd.DataFrame(columns=FACT_MENTION_FRAME_SCORE_COLUMNS)
    else:
        effective_runner = frame_runner or HuggingFaceNliFrameRunner(
            effective_model_bundle_config
        )
        frame_updates_dataframe, frame_score_dataframe = _score_frame_rows(
            scoreable_dataframe=scoreable_dataframe,
            frame_runner=effective_runner,
            model_bundle_config=effective_model_bundle_config,
        )
        enriched_summary_dataframe = _apply_frame_updates(
            enriched_summary_dataframe,
            frame_updates_dataframe,
        )

    validate_fact_mention_nlp_summary(
        enriched_summary_dataframe,
        nlp_input_dataframe,
    )
    validate_fact_mention_frame_score(
        frame_score_dataframe,
        nlp_input_dataframe,
    )
    _validate_phase4_frame_contract(
        nlp_summary_dataframe=enriched_summary_dataframe,
        nlp_input_dataframe=nlp_input_dataframe,
        frame_score_dataframe=frame_score_dataframe,
    )
    logger.info(
        "Enriched NLP summary with frames summary_rows=%d scoreable=%d "
        "frame_rows=%d classified=%d bundle=%s",
        len(enriched_summary_dataframe),
        len(scoreable_dataframe),
        len(frame_score_dataframe),
        int(enriched_summary_dataframe["primary_frame_label"].ne("unclassified").sum()),
        effective_model_bundle_config.bundle_version,
    )
    return enriched_summary_dataframe, frame_score_dataframe


def materialize_fact_mention_nlp_summary_with_frames(
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    *,
    frame_runner: FrameRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build and persist Phase 4 frame-enriched Silver NLP outputs.

    Args:
        nlp_input_dataframe: Current ``silver.fact_mention_nlp_input`` rows.
        nlp_summary_dataframe: Existing NLP summary rows.
        frame_runner: Optional scorer implementation for tests.
        model_bundle_config: Optional model-bundle metadata override.
        silver_dir: Directory where Silver Parquet artifacts are written.
        duckdb_path: DuckDB warehouse path for Silver table writes.

    Returns:
        Tuple of the summary and frame-score DataFrames written to storage.

    Raises:
        DataQualityError: If validation fails before persistence.
        TransformerDependencyError: If NLI scoring is requested without
            optional Transformer dependencies.
        RuntimeError: If DuckDB is unavailable while persisting tables.
    """
    enriched_summary_dataframe, frame_score_dataframe = (
        enrich_fact_mention_nlp_summary_with_frames(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            frame_runner=frame_runner,
            model_bundle_config=model_bundle_config,
        )
    )
    summary_path = silver_dir / "fact_mention_nlp_summary.parquet"
    frame_score_path = silver_dir / "fact_mention_frame_score.parquet"
    write_parquet_table(enriched_summary_dataframe, summary_path)
    write_parquet_table(frame_score_dataframe, frame_score_path)
    write_duckdb_table(
        dataframe=enriched_summary_dataframe,
        schema_name="silver",
        table_name="fact_mention_nlp_summary",
        duckdb_path=duckdb_path,
    )
    write_duckdb_table(
        dataframe=frame_score_dataframe,
        schema_name="silver",
        table_name="fact_mention_frame_score",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized frame-enriched NLP summary summary_path=%s "
        "frame_score_path=%s duckdb_path=%s summary_rows=%d frame_rows=%d",
        summary_path,
        frame_score_path,
        duckdb_path,
        len(enriched_summary_dataframe),
        len(frame_score_dataframe),
    )
    return enriched_summary_dataframe, frame_score_dataframe


def validate_fact_mention_frame_score(
    frame_score_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame | None = None,
) -> None:
    """Validate the Phase 4 frame-score Silver output table.

    Args:
        frame_score_dataframe: Candidate frame-score output rows.
        nlp_input_dataframe: Optional source input table used to verify
            mention-level lineage.

    Raises:
        DataQualityError: If required columns, keys, probabilities, frame
        labels, booleans, primary-frame flags, or model metadata violate the
        contract.
    """
    require_columns(
        dataframe=frame_score_dataframe,
        required_columns=_REQUIRED_FRAME_SCORE_COLUMNS,
        dataframe_name="fact_mention_frame_score",
    )
    validate_unique_key(
        dataframe=frame_score_dataframe,
        key_columns=("mention_id", "frame_label"),
        dataframe_name="fact_mention_frame_score",
    )
    _validate_frame_score_required_values(frame_score_dataframe)
    _validate_frame_score_labels(frame_score_dataframe)
    _validate_frame_score_probabilities(frame_score_dataframe)
    _validate_frame_score_boolean_column(frame_score_dataframe, "is_primary_frame")
    _validate_frame_score_boolean_column(frame_score_dataframe, "passes_threshold")
    _validate_frame_score_primary_contract(frame_score_dataframe)
    if nlp_input_dataframe is not None:
        _validate_frame_score_matches_input(
            frame_score_dataframe,
            nlp_input_dataframe,
        )


def _validate_phase3_sources(
    *,
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    expected_bundle_version: str,
) -> None:
    """Validate Phase 3 source tables before model inference."""
    require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    validate_fact_mention_nlp_summary(nlp_summary_dataframe, nlp_input_dataframe)
    _validate_sample_leaders(sample_leaders_dataframe)
    _validate_summary_bundle_version(
        nlp_summary_dataframe,
        expected_bundle_version,
    )
    _validate_summary_leaders_have_names(
        nlp_summary_dataframe,
        sample_leaders_dataframe,
    )


def _validate_phase4_sources(
    *,
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    expected_bundle_version: str,
) -> None:
    """Validate Phase 4 source tables before frame inference."""
    require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    validate_fact_mention_nlp_summary(nlp_summary_dataframe, nlp_input_dataframe)
    _validate_summary_bundle_version(
        nlp_summary_dataframe,
        expected_bundle_version,
    )


def _validate_sample_leaders(sample_leaders_dataframe: pd.DataFrame) -> None:
    """Validate candidate names used for target-aware hypotheses."""
    require_columns(
        dataframe=sample_leaders_dataframe,
        required_columns=_REQUIRED_SAMPLE_LEADER_COLUMNS,
        dataframe_name="sample_leaders",
    )
    for column_name in ("leader_id", "full_name"):
        blank_values = sample_leaders_dataframe[column_name].map(is_null_or_blank)
        if blank_values.any():
            raise DataQualityError(f"sample_leaders has blank {column_name} values")
    validate_unique_key(
        dataframe=sample_leaders_dataframe,
        key_columns=("leader_id",),
        dataframe_name="sample_leaders",
    )


def _validate_summary_bundle_version(
    nlp_summary_dataframe: pd.DataFrame,
    expected_bundle_version: str,
) -> None:
    """Fail when Phase 2 summary rows came from a different model bundle."""
    actual_versions = (
        nlp_summary_dataframe["nlp_model_bundle_version"].astype(str).str.strip()
    )
    mismatched_versions = actual_versions.ne(expected_bundle_version)
    if mismatched_versions.any():
        examples = actual_versions.loc[mismatched_versions].drop_duplicates().tolist()
        raise DataQualityError(
            "fact_mention_nlp_summary bundle version does not match current "
            f"model config: expected={expected_bundle_version} actual={examples}"
        )


def _validate_summary_leaders_have_names(
    nlp_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
) -> None:
    """Fail when a summary leader has no candidate name in the cohort table."""
    sample_leader_ids = set(
        sample_leaders_dataframe["leader_id"].astype(str).str.strip()
    )
    summary_leader_ids = set(nlp_summary_dataframe["leader_id"].astype(str).str.strip())
    missing_leader_ids = sorted(summary_leader_ids - sample_leader_ids)
    if missing_leader_ids:
        raise DataQualityError(
            "fact_mention_nlp_summary has leaders missing from sample_leaders: "
            f"{missing_leader_ids[:5]}"
        )


def _build_leader_lookup(sample_leaders_dataframe: pd.DataFrame) -> pd.DataFrame:
    """Return a clean leader_id to full_name lookup."""
    leader_lookup_dataframe = sample_leaders_dataframe.loc[
        :, ["leader_id", "full_name"]
    ].copy()
    leader_lookup_dataframe["leader_id"] = leader_lookup_dataframe["leader_id"].map(
        lambda value: str(value).strip()
    )
    leader_lookup_dataframe["full_name"] = leader_lookup_dataframe["full_name"].map(
        lambda value: str(value).strip()
    )
    return leader_lookup_dataframe


def _build_scoring_source_dataframe(
    *,
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
    leader_lookup_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Join input, summary, and leader names for scoreable rows."""
    nlp_input_subset = nlp_input_dataframe.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "canonical_article_id",
            "input_text",
            "eligible_for_inference",
        ],
    ].copy()
    nlp_input_subset["eligible_for_inference"] = nlp_input_subset[
        "eligible_for_inference"
    ].astype(bool)

    scoring_source_dataframe = nlp_summary_dataframe.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "canonical_article_id",
            "nlp_enrichment_status",
        ],
    ].merge(
        nlp_input_subset,
        on=["mention_id", "leader_id", "canonical_article_id"],
        how="left",
        validate="one_to_one",
    )
    if scoring_source_dataframe["eligible_for_inference"].isna().any():
        raise DataQualityError(
            "fact_mention_nlp_summary has rows without matching NLP input rows"
        )

    scoring_source_dataframe = scoring_source_dataframe.merge(
        leader_lookup_dataframe,
        on="leader_id",
        how="left",
        validate="many_to_one",
    )
    if scoring_source_dataframe["full_name"].map(is_null_or_blank).any():
        raise DataQualityError(
            "fact_mention_nlp_summary has rows without sample_leaders full_name"
        )

    scoring_source_dataframe["eligible_for_tone"] = scoring_source_dataframe[
        "eligible_for_inference"
    ].astype(bool) & scoring_source_dataframe["nlp_enrichment_status"].eq("scored")
    return scoring_source_dataframe


def _build_frame_scoring_source_dataframe(
    *,
    nlp_input_dataframe: pd.DataFrame,
    nlp_summary_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Join input and summary rows for scoreable frame inference."""
    nlp_input_subset = nlp_input_dataframe.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "canonical_article_id",
            "input_text",
            "eligible_for_inference",
        ],
    ].copy()
    nlp_input_subset["eligible_for_inference"] = nlp_input_subset[
        "eligible_for_inference"
    ].astype(bool)

    scoring_source_dataframe = nlp_summary_dataframe.loc[
        :,
        [
            "mention_id",
            "leader_id",
            "canonical_article_id",
            "nlp_enrichment_status",
        ],
    ].merge(
        nlp_input_subset,
        on=["mention_id", "leader_id", "canonical_article_id"],
        how="left",
        validate="one_to_one",
    )
    if scoring_source_dataframe["eligible_for_inference"].isna().any():
        raise DataQualityError(
            "fact_mention_nlp_summary has rows without matching NLP input rows"
        )

    scoring_source_dataframe["eligible_for_frame"] = scoring_source_dataframe[
        "eligible_for_inference"
    ].astype(bool) & scoring_source_dataframe["nlp_enrichment_status"].eq("scored")
    return scoring_source_dataframe


def _score_tone_rows(
    *,
    scoreable_dataframe: pd.DataFrame,
    tone_runner: ToneRunner,
    model_bundle_config: ModelBundleConfig,
) -> pd.DataFrame:
    """Score rows in deterministic batches while preserving input order."""
    update_rows: list[dict[str, object]] = []
    scoreable_records = scoreable_dataframe.to_dict("records")
    for start_index in range(0, len(scoreable_records), model_bundle_config.batch_size):
        batch_records = scoreable_records[
            start_index : start_index + model_bundle_config.batch_size
        ]
        scoring_inputs = [
            ToneScoringInput(
                mention_id=str(record["mention_id"]).strip(),
                input_text=str(record["input_text"]),
                candidate_name=str(record["full_name"]).strip(),
            )
            for record in batch_records
        ]
        predictions = tone_runner.predict_batch(scoring_inputs)
        if len(predictions) != len(scoring_inputs):
            raise DataQualityError(
                "tone runner returned "
                f"{len(predictions)} predictions for {len(scoring_inputs)} inputs"
            )
        for scoring_input, prediction in zip(
            scoring_inputs,
            predictions,
            strict=True,
        ):
            selected_label, selected_probability = select_target_tone_label(
                prediction.probabilities_by_label,
                threshold=model_bundle_config.tone_threshold,
            )
            update_rows.append(
                {
                    "mention_id": scoring_input.mention_id,
                    "target_tone_label": selected_label,
                    "target_tone_probability": selected_probability,
                    "was_truncated_to_max_length": bool(
                        prediction.was_truncated_to_max_length
                    ),
                }
            )
    return pd.DataFrame(
        update_rows,
        columns=[
            "mention_id",
            "target_tone_label",
            "target_tone_probability",
            "was_truncated_to_max_length",
        ],
    )


def _score_frame_rows(
    *,
    scoreable_dataframe: pd.DataFrame,
    frame_runner: FrameRunner,
    model_bundle_config: ModelBundleConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Score frame rows in deterministic batches while preserving input order."""
    update_rows: list[dict[str, object]] = []
    frame_score_rows: list[dict[str, object]] = []
    scoreable_records = scoreable_dataframe.to_dict("records")
    for start_index in range(0, len(scoreable_records), model_bundle_config.batch_size):
        batch_records = scoreable_records[
            start_index : start_index + model_bundle_config.batch_size
        ]
        scoring_inputs = [
            FrameScoringInput(
                mention_id=str(record["mention_id"]).strip(),
                input_text=str(record["input_text"]),
            )
            for record in batch_records
        ]
        predictions = frame_runner.predict_batch(scoring_inputs)
        if len(predictions) != len(scoring_inputs):
            raise DataQualityError(
                "frame runner returned "
                f"{len(predictions)} predictions for {len(scoring_inputs)} inputs"
            )
        for scoring_input, prediction in zip(
            scoring_inputs,
            predictions,
            strict=True,
        ):
            probabilities_by_frame = _normalize_frame_probabilities(
                prediction.probabilities_by_label
            )
            selected_label, selected_probability = select_primary_frame(
                probabilities_by_frame,
                thresholds_by_frame=model_bundle_config.frame_thresholds,
            )
            update_rows.append(
                {
                    "mention_id": scoring_input.mention_id,
                    "primary_frame_label": selected_label,
                    "primary_frame_probability": selected_probability,
                    "was_truncated_to_max_length": bool(
                        prediction.was_truncated_to_max_length
                    ),
                }
            )
            for frame_label in SCORABLE_FRAME_LABELS:
                frame_probability = float(probabilities_by_frame[frame_label])
                frame_score_rows.append(
                    {
                        "mention_id": scoring_input.mention_id,
                        "frame_label": frame_label,
                        "frame_probability": frame_probability,
                        "is_primary_frame": selected_label == frame_label,
                        "passes_threshold": (
                            frame_probability
                            >= model_bundle_config.threshold_for_frame(frame_label)
                        ),
                        "nli_hypothesis": build_frame_hypothesis(frame_label),
                        "nlp_model_bundle_version": (
                            model_bundle_config.bundle_version
                        ),
                    }
                )
    frame_updates_dataframe = pd.DataFrame(
        update_rows,
        columns=[
            "mention_id",
            "primary_frame_label",
            "primary_frame_probability",
            "was_truncated_to_max_length",
        ],
    )
    frame_score_dataframe = pd.DataFrame(
        frame_score_rows,
        columns=FACT_MENTION_FRAME_SCORE_COLUMNS,
    )
    return frame_updates_dataframe, frame_score_dataframe


def _apply_tone_updates(
    nlp_summary_dataframe: pd.DataFrame,
    tone_updates_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Apply tone updates to the summary table without changing row order."""
    if tone_updates_dataframe.empty:
        return nlp_summary_dataframe

    enriched_summary_dataframe = nlp_summary_dataframe.copy()
    update_by_mention = tone_updates_dataframe.set_index("mention_id")
    summary_by_mention = enriched_summary_dataframe.set_index("mention_id", drop=False)
    update_ids = update_by_mention.index.tolist()

    summary_by_mention.loc[update_ids, "target_tone_label"] = update_by_mention[
        "target_tone_label"
    ]
    summary_by_mention.loc[update_ids, "target_tone_probability"] = update_by_mention[
        "target_tone_probability"
    ]
    existing_truncation = summary_by_mention.loc[
        update_ids,
        "was_truncated_to_max_length",
    ].astype(bool)
    summary_by_mention.loc[update_ids, "was_truncated_to_max_length"] = (
        existing_truncation
        | update_by_mention["was_truncated_to_max_length"].astype(bool)
    ).to_numpy()
    return summary_by_mention.reset_index(drop=True)[
        list(FACT_MENTION_NLP_SUMMARY_COLUMNS)
    ]


def _apply_frame_updates(
    nlp_summary_dataframe: pd.DataFrame,
    frame_updates_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Apply frame updates to the summary table without changing row order."""
    if frame_updates_dataframe.empty:
        return nlp_summary_dataframe

    enriched_summary_dataframe = nlp_summary_dataframe.copy()
    update_by_mention = frame_updates_dataframe.set_index("mention_id")
    summary_by_mention = enriched_summary_dataframe.set_index("mention_id", drop=False)
    update_ids = update_by_mention.index.tolist()

    summary_by_mention.loc[update_ids, "primary_frame_label"] = update_by_mention[
        "primary_frame_label"
    ]
    summary_by_mention.loc[update_ids, "primary_frame_probability"] = update_by_mention[
        "primary_frame_probability"
    ]
    existing_truncation = summary_by_mention.loc[
        update_ids,
        "was_truncated_to_max_length",
    ].astype(bool)
    summary_by_mention.loc[update_ids, "was_truncated_to_max_length"] = (
        existing_truncation
        | update_by_mention["was_truncated_to_max_length"].astype(bool)
    ).to_numpy()
    return summary_by_mention.reset_index(drop=True)[
        list(FACT_MENTION_NLP_SUMMARY_COLUMNS)
    ]


def _validate_phase3_tone_contract(
    nlp_summary_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame,
) -> None:
    """Validate tone field semantics after enrichment."""
    tone_contract_dataframe = nlp_summary_dataframe.loc[
        :,
        [
            "mention_id",
            "nlp_enrichment_status",
            "target_tone_label",
            "target_tone_probability",
        ],
    ].merge(
        nlp_input_dataframe.loc[:, ["mention_id", "eligible_for_inference"]],
        on="mention_id",
        how="left",
        validate="one_to_one",
    )
    scoreable_rows = tone_contract_dataframe["eligible_for_inference"].astype(
        bool
    ) & tone_contract_dataframe["nlp_enrichment_status"].eq("scored")
    missing_scoreable_probability = tone_contract_dataframe.loc[
        scoreable_rows,
        "target_tone_probability",
    ].isna()
    if missing_scoreable_probability.any():
        raise DataQualityError(
            "fact_mention_nlp_summary scoreable tone rows require probability"
        )

    non_scoreable_rows = ~scoreable_rows
    unexpected_non_scoreable_label = (
        tone_contract_dataframe.loc[non_scoreable_rows, "target_tone_label"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("unclassified")
    )
    unexpected_non_scoreable_probability = tone_contract_dataframe.loc[
        non_scoreable_rows,
        "target_tone_probability",
    ].notna()
    if (
        unexpected_non_scoreable_label.any()
        or unexpected_non_scoreable_probability.any()
    ):
        raise DataQualityError(
            "fact_mention_nlp_summary non-scoreable rows must keep tone unclassified"
        )


def _validate_phase4_frame_contract(
    *,
    nlp_summary_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame,
    frame_score_dataframe: pd.DataFrame,
) -> None:
    """Validate summary-frame reconciliation after Phase 4 enrichment."""
    frame_contract_dataframe = nlp_summary_dataframe.loc[
        :,
        [
            "mention_id",
            "nlp_enrichment_status",
            "primary_frame_label",
            "primary_frame_probability",
        ],
    ].merge(
        nlp_input_dataframe.loc[:, ["mention_id", "eligible_for_inference"]],
        on="mention_id",
        how="left",
        validate="one_to_one",
    )
    scoreable_rows = frame_contract_dataframe["eligible_for_inference"].astype(
        bool
    ) & frame_contract_dataframe["nlp_enrichment_status"].eq("scored")
    scoreable_mentions = set(
        frame_contract_dataframe.loc[scoreable_rows, "mention_id"].astype(str)
    )
    frame_score_mentions = set(frame_score_dataframe["mention_id"].astype(str))
    if frame_score_mentions != scoreable_mentions:
        raise DataQualityError(
            "fact_mention_frame_score mention coverage does not match scoreable "
            "NLP summary rows"
        )

    expected_frame_labels = set(SCORABLE_FRAME_LABELS)
    for mention_id, mention_frame_scores in frame_score_dataframe.groupby(
        "mention_id",
        sort=False,
    ):
        actual_frame_labels = set(mention_frame_scores["frame_label"].astype(str))
        if actual_frame_labels != expected_frame_labels:
            raise DataQualityError(
                "fact_mention_frame_score must contain exactly one row per "
                f"scorable frame for mention_id={mention_id}"
            )

    non_scoreable_rows = ~scoreable_rows
    unexpected_non_scoreable_label = (
        frame_contract_dataframe.loc[non_scoreable_rows, "primary_frame_label"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("unclassified")
    )
    unexpected_non_scoreable_probability = frame_contract_dataframe.loc[
        non_scoreable_rows,
        "primary_frame_probability",
    ].notna()
    if (
        unexpected_non_scoreable_label.any()
        or unexpected_non_scoreable_probability.any()
    ):
        raise DataQualityError(
            "fact_mention_nlp_summary non-scoreable rows must keep frame "
            "unclassified"
        )

    classified_rows = frame_contract_dataframe["primary_frame_label"].isin(
        SCORABLE_FRAME_LABELS
    )
    missing_classified_probability = frame_contract_dataframe.loc[
        classified_rows,
        "primary_frame_probability",
    ].isna()
    if missing_classified_probability.any():
        raise DataQualityError(
            "fact_mention_nlp_summary classified frame rows require probability"
        )

    unclassified_probability = frame_contract_dataframe.loc[
        frame_contract_dataframe["primary_frame_label"].eq("unclassified"),
        "primary_frame_probability",
    ].notna()
    if unclassified_probability.any():
        raise DataQualityError(
            "fact_mention_nlp_summary unclassified frame rows must not keep "
            "primary_frame_probability"
        )

    if frame_score_dataframe.empty:
        return
    primary_frame_scores = frame_score_dataframe.loc[
        frame_score_dataframe["is_primary_frame"].astype(bool)
    ]
    primary_lookup = primary_frame_scores.set_index("mention_id")["frame_label"]
    for summary_row in frame_contract_dataframe.loc[classified_rows].itertuples(
        index=False
    ):
        primary_frame_label = primary_lookup.get(summary_row.mention_id)
        if primary_frame_label != summary_row.primary_frame_label:
            raise DataQualityError(
                "fact_mention_nlp_summary primary_frame_label must match the "
                "primary frame-score row"
            )


def _validate_frame_score_required_values(
    frame_score_dataframe: pd.DataFrame,
) -> None:
    """Raise when frame-score required text fields are blank."""
    for column_name in (
        "mention_id",
        "frame_label",
        "nli_hypothesis",
        "nlp_model_bundle_version",
    ):
        blank_values = frame_score_dataframe[column_name].map(is_null_or_blank)
        if blank_values.any():
            raise DataQualityError(
                f"fact_mention_frame_score has blank {column_name} values"
            )


def _validate_frame_score_labels(frame_score_dataframe: pd.DataFrame) -> None:
    """Validate frame labels against the scorable vocabulary."""
    unsupported_labels = ~frame_score_dataframe["frame_label"].isin(
        SCORABLE_FRAME_LABELS
    )
    if unsupported_labels.any():
        examples = (
            frame_score_dataframe.loc[unsupported_labels, "frame_label"]
            .drop_duplicates()
            .tolist()
        )
        raise DataQualityError(
            f"fact_mention_frame_score unsupported frame labels: {examples}"
        )


def _validate_frame_score_probabilities(frame_score_dataframe: pd.DataFrame) -> None:
    """Validate frame probabilities are numeric probabilities."""
    numeric_probabilities = pd.to_numeric(
        frame_score_dataframe["frame_probability"],
        errors="coerce",
    )
    if numeric_probabilities.isna().any():
        raise DataQualityError(
            "fact_mention_frame_score frame_probability must be numeric"
        )
    if ((numeric_probabilities < 0) | (numeric_probabilities > 1)).any():
        raise DataQualityError(
            "fact_mention_frame_score frame_probability must be between 0 and 1"
        )


def _validate_frame_score_boolean_column(
    frame_score_dataframe: pd.DataFrame,
    column_name: str,
) -> None:
    """Validate frame-score boolean flags."""
    if frame_score_dataframe[column_name].isna().any():
        raise DataQualityError(f"fact_mention_frame_score {column_name} has nulls")
    if pd.api.types.is_bool_dtype(frame_score_dataframe[column_name]):
        return
    invalid_values = ~frame_score_dataframe[column_name].map(
        lambda value: isinstance(value, bool)
    )
    if invalid_values.any():
        raise DataQualityError(
            f"fact_mention_frame_score {column_name} must contain booleans"
        )


def _validate_frame_score_primary_contract(
    frame_score_dataframe: pd.DataFrame,
) -> None:
    """Validate primary-frame semantics inside the frame-score table."""
    if frame_score_dataframe.empty:
        return
    primary_counts = (
        frame_score_dataframe.loc[
            frame_score_dataframe["is_primary_frame"].astype(bool)
        ]
        .groupby("mention_id")
        .size()
    )
    if (primary_counts > 1).any():
        raise DataQualityError(
            "fact_mention_frame_score allows at most one primary frame per mention"
        )
    primary_without_threshold = frame_score_dataframe.loc[
        frame_score_dataframe["is_primary_frame"].astype(bool)
        & ~frame_score_dataframe["passes_threshold"].astype(bool)
    ]
    if not primary_without_threshold.empty:
        raise DataQualityError(
            "fact_mention_frame_score primary frames must pass threshold"
        )


def _validate_frame_score_matches_input(
    frame_score_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame,
) -> None:
    """Validate frame-score mention lineage against the NLP input table."""
    input_mentions = set(nlp_input_dataframe["mention_id"].astype(str))
    output_mentions = set(frame_score_dataframe["mention_id"].astype(str))
    if output_mentions - input_mentions:
        examples = sorted(output_mentions - input_mentions)[:5]
        raise DataQualityError(
            "fact_mention_frame_score has rows without matching NLP input: "
            f"{examples}"
        )


def _normalize_tone_probabilities(
    probabilities_by_label: Mapping[str, float],
) -> dict[str, float]:
    """Normalize and validate controlled tone-label probabilities."""
    probabilities_by_tone: dict[str, float] = {}
    for raw_label, raw_probability in probabilities_by_label.items():
        tone_label = str(raw_label).strip()
        if tone_label in probabilities_by_tone:
            raise DataQualityError(f"duplicate tone label: {tone_label}")
        if tone_label not in SCORABLE_TARGET_TONE_LABELS:
            raise DataQualityError(f"unsupported tone label: {tone_label}")
        try:
            probability = float(raw_probability)
        except (TypeError, ValueError) as exc:
            raise DataQualityError(
                f"tone probability for {tone_label} must be numeric"
            ) from exc
        if not 0 <= probability <= 1:
            raise DataQualityError(
                f"tone probability for {tone_label} must be between 0 and 1"
            )
        probabilities_by_tone[tone_label] = probability

    missing_labels = sorted(
        set(SCORABLE_TARGET_TONE_LABELS) - set(probabilities_by_tone)
    )
    if missing_labels:
        raise DataQualityError(f"tone probabilities missing labels: {missing_labels}")
    return probabilities_by_tone


def _normalize_frame_probabilities(
    probabilities_by_label: Mapping[str, float],
) -> dict[str, float]:
    """Normalize and validate controlled frame-label probabilities."""
    probabilities_by_frame: dict[str, float] = {}
    for raw_label, raw_probability in probabilities_by_label.items():
        frame_label = str(raw_label).strip()
        if frame_label in probabilities_by_frame:
            raise DataQualityError(f"duplicate frame label: {frame_label}")
        if frame_label not in SCORABLE_FRAME_LABELS:
            raise DataQualityError(f"unsupported frame label: {frame_label}")
        try:
            probability = float(raw_probability)
        except (TypeError, ValueError) as exc:
            raise DataQualityError(
                f"frame probability for {frame_label} must be numeric"
            ) from exc
        if not 0 <= probability <= 1:
            raise DataQualityError(
                f"frame probability for {frame_label} must be between 0 and 1"
            )
        probabilities_by_frame[frame_label] = probability

    missing_labels = sorted(set(SCORABLE_FRAME_LABELS) - set(probabilities_by_frame))
    if missing_labels:
        raise DataQualityError(f"frame probabilities missing labels: {missing_labels}")
    return probabilities_by_frame


def _normalize_zero_shot_result(raw_result: Any) -> dict[str, float]:
    """Normalize Hugging Face zero-shot output to controlled tone probabilities."""
    if isinstance(raw_result, list):
        if len(raw_result) != 1 or not isinstance(raw_result[0], dict):
            raise RuntimeError("Hugging Face NLI pipeline returned invalid batches")
        raw_result = raw_result[0]
    if not isinstance(raw_result, dict):
        raise RuntimeError("Hugging Face NLI pipeline returned non-dict output")

    raw_labels = raw_result.get("labels")
    raw_scores = raw_result.get("scores")
    if not isinstance(raw_labels, list) or not isinstance(raw_scores, list):
        raise RuntimeError("Hugging Face NLI pipeline returned invalid score fields")
    if len(raw_labels) != len(raw_scores):
        raise RuntimeError(
            "Hugging Face NLI pipeline returned mismatched labels/scores"
        )

    probabilities_by_label: dict[str, float] = {}
    for raw_label, raw_score in zip(raw_labels, raw_scores, strict=True):
        model_label = str(raw_label).strip()
        if model_label not in NLI_TONE_LABEL_BY_MODEL_LABEL:
            raise RuntimeError(f"Unsupported NLI tone model label: {model_label}")
        probabilities_by_label[NLI_TONE_LABEL_BY_MODEL_LABEL[model_label]] = float(
            raw_score
        )
    _normalize_tone_probabilities(probabilities_by_label)
    return probabilities_by_label


def _normalize_frame_zero_shot_result(raw_result: Any) -> dict[str, float]:
    """Normalize Hugging Face zero-shot output to frame probabilities."""
    if isinstance(raw_result, list):
        if len(raw_result) != 1 or not isinstance(raw_result[0], dict):
            raise RuntimeError("Hugging Face NLI pipeline returned invalid batches")
        raw_result = raw_result[0]
    if not isinstance(raw_result, dict):
        raise RuntimeError("Hugging Face NLI pipeline returned non-dict output")

    raw_labels = raw_result.get("labels")
    raw_scores = raw_result.get("scores")
    if not isinstance(raw_labels, list) or not isinstance(raw_scores, list):
        raise RuntimeError("Hugging Face NLI pipeline returned invalid score fields")
    if len(raw_labels) != len(raw_scores):
        raise RuntimeError(
            "Hugging Face NLI pipeline returned mismatched labels/scores"
        )

    probabilities_by_label: dict[str, float] = {}
    for raw_label, raw_score in zip(raw_labels, raw_scores, strict=True):
        model_label = str(raw_label).strip()
        if model_label not in NLI_FRAME_LABEL_BY_MODEL_LABEL:
            raise RuntimeError(f"Unsupported NLI frame model label: {model_label}")
        probabilities_by_label[NLI_FRAME_LABEL_BY_MODEL_LABEL[model_label]] = float(
            raw_score
        )
    _normalize_frame_probabilities(probabilities_by_label)
    return probabilities_by_label
