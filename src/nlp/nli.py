"""Phase 3 target-aware tone scoring with Natural Language Inference.

This module enriches ``silver.fact_mention_nlp_summary`` with candidate-aware
tone labels while keeping Transformer imports lazy. Tests inject mocked
``ToneRunner`` implementations, so CI never needs GPU, internet, or model
downloads.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from src.config.settings import SILVER_DIR, WAREHOUSE_PATH
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


class ToneRunner(Protocol):
    """Protocol implemented by real and mocked NLI tone scorers."""

    def predict_batch(
        self,
        scoring_inputs: Sequence[ToneScoringInput],
    ) -> list[TonePrediction]:
        """Return tone predictions in the same order as ``scoring_inputs``."""


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

        try:
            from transformers import (
                AutoModelForSequenceClassification,
                AutoTokenizer,
                pipeline,
            )
        except ImportError as exc:  # pragma: no cover - depends on environment
            raise TransformerDependencyError(
                "transformers is required for NLP NLI tone scoring. Install "
                "the optional future stack with: pip install -r "
                "requirements-future.in"
            ) from exc

        try:
            tokenizer = AutoTokenizer.from_pretrained(
                self._model_bundle_config.nli_model_name,
                revision=self._model_bundle_config.nli_model_revision,
                use_fast=False,
            )
            model = AutoModelForSequenceClassification.from_pretrained(
                self._model_bundle_config.nli_model_name,
                revision=self._model_bundle_config.nli_model_revision,
            )
            self._analyzer = pipeline(
                task="zero-shot-classification",
                model=model,
                tokenizer=tokenizer,
                device=pipeline_device_arg(self._model_bundle_config.device),
            )
        except Exception as exc:
            raise NliModelLoadError(
                "Could not load the Hugging Face NLI model. If the model is "
                "already cached, retry with HF_HUB_OFFLINE=1. CamemBERT "
                "tokenizers require the optional SentencePiece dependency; "
                "install the future NLP stack with: pip install -r "
                "requirements-future.in"
            ) from exc
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
