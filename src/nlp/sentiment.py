"""Phase 2 generic sentiment baseline for mention-level NLP inputs.

This module consumes ``silver.fact_mention_nlp_input`` and writes
``silver.fact_mention_nlp_summary``. It keeps Transformer imports lazy so the
default project environment remains runnable without installing the future NLP
stack.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from src.config.settings import NLP_MODEL_CACHE_DIR, SILVER_DIR, WAREHOUSE_PATH
from src.nlp._validation import (
    coerce_utc_timestamp,
    pipeline_device_arg,
    require_columns,
    validate_required_identifier_values,
    validate_unique_key,
)
from src.nlp.model_bundle import ModelBundleConfig, build_model_bundle_config
from src.nlp.normalization import is_null_or_blank
from src.storage.tables import write_duckdb_table, write_parquet_table
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

FACT_MENTION_NLP_SUMMARY_COLUMNS: tuple[str, ...] = (
    "mention_id",
    "leader_id",
    "canonical_article_id",
    "input_hash",
    "generic_sentiment_label",
    "generic_sentiment_score",
    "target_tone_label",
    "target_tone_probability",
    "primary_frame_label",
    "primary_frame_probability",
    "was_truncated_to_max_length",
    "nlp_enrichment_status",
    "nlp_model_bundle_version",
    "scored_at",
    "error_type",
)

SENTIMENT_STAR_LABELS: tuple[str, ...] = (
    "1 star",
    "2 stars",
    "3 stars",
    "4 stars",
    "5 stars",
)
CONTROLLED_NLP_ENRICHMENT_STATUSES: frozenset[str] = frozenset(
    {"scored", "skipped", "failed"}
)
CONTROLLED_TONE_LABELS: frozenset[str] = frozenset(
    {"favorable", "unfavorable", "neutral", "unclassified"}
)
CONTROLLED_FRAME_LABELS: frozenset[str] = frozenset(
    {
        "politique",
        "vie_privee",
        "apparence",
        "scandale",
        "personnalite",
        "securite",
        "unclassified",
    }
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
_REQUIRED_NLP_SUMMARY_COLUMNS = frozenset(FACT_MENTION_NLP_SUMMARY_COLUMNS)
_CORE_IDENTIFIER_COLUMNS = (
    "mention_id",
    "leader_id",
    "canonical_article_id",
)
_STAR_LABEL_PATTERN = re.compile(r"^\s*([1-5])\s+stars?\s*$", re.IGNORECASE)
_STAR_LABEL_BY_VALUE = {
    1: "1 star",
    2: "2 stars",
    3: "3 stars",
    4: "4 stars",
    5: "5 stars",
}


class TransformerDependencyError(RuntimeError):
    """Raised when Transformer scoring is requested without NLP dependencies."""


class SentimentModelLoadError(RuntimeError):
    """Raised when the sentiment model cannot be loaded for a scoring run."""


@dataclass(frozen=True)
class SentimentPrediction:
    """One generic sentiment prediction for a mention context.

    Args:
        label: Top model label, expected to be one of ``1 star`` through
            ``5 stars``.
        probabilities_by_label: Full label probability distribution from the
            text-classification model.
        was_truncated_to_max_length: Whether tokenizer input exceeded the
            configured maximum token length before truncation.
    """

    label: str
    probabilities_by_label: Mapping[str, float]
    was_truncated_to_max_length: bool = False


class SentimentRunner(Protocol):
    """Protocol implemented by real and mocked sentiment scorers."""

    def predict_batch(self, texts: Sequence[str]) -> list[SentimentPrediction]:
        """Return sentiment predictions in the same order as ``texts``."""


class HuggingFaceSentimentRunner:
    """Lazy Hugging Face adapter for the French sentiment baseline.

    Args:
        model_bundle_config: Versioned model metadata used for loading the
            configured sentiment model and tokenizer.
    """

    def __init__(self, model_bundle_config: ModelBundleConfig) -> None:
        self._model_bundle_config = model_bundle_config
        self._analyzer: Any | None = None

    def predict_batch(self, texts: Sequence[str]) -> list[SentimentPrediction]:
        """Score a batch of mention contexts with the configured HF model.

        Args:
            texts: Mention-level context strings.

        Returns:
            Ordered sentiment predictions.

        Raises:
            TransformerDependencyError: If ``transformers`` is not installed.
            SentimentModelLoadError: If model or tokenizer loading fails.
            RuntimeError: If the Hugging Face pipeline returns an unsupported
                output shape.
        """
        if not texts:
            return []

        analyzer = self._get_analyzer()
        raw_results = analyzer(
            list(texts),
            batch_size=self._model_bundle_config.batch_size,
            truncation=True,
            max_length=self._model_bundle_config.max_token_length,
            top_k=None,
        )
        result_batches = _normalize_huggingface_results(raw_results, len(texts))
        return [
            SentimentPrediction(
                label=_select_top_label(result_batch),
                probabilities_by_label={
                    str(score["label"]): float(score["score"]) for score in result_batch
                },
                was_truncated_to_max_length=self._was_text_truncated(text),
            )
            for text, result_batch in zip(texts, result_batches, strict=True)
        ]

    def _get_analyzer(self) -> Any:
        """Load the Hugging Face pipeline on first use."""
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
                "transformers is required for NLP sentiment scoring. Install "
                "the optional future stack with: pip install -r "
                "requirements-future.in"
            ) from exc

        try:
            cache_kwargs = (
                {"cache_dir": str(NLP_MODEL_CACHE_DIR)}
                if NLP_MODEL_CACHE_DIR is not None
                else {}
            )
            tokenizer = AutoTokenizer.from_pretrained(
                self._model_bundle_config.sentiment_model_name,
                revision=self._model_bundle_config.sentiment_model_revision,
                use_fast=False,
                **cache_kwargs,
            )
            model = AutoModelForSequenceClassification.from_pretrained(
                self._model_bundle_config.sentiment_model_name,
                revision=self._model_bundle_config.sentiment_model_revision,
                **cache_kwargs,
            )
            self._analyzer = pipeline(
                task="text-classification",
                model=model,
                tokenizer=tokenizer,
                device=pipeline_device_arg(self._model_bundle_config.device),
            )
        except Exception as exc:
            raise SentimentModelLoadError(
                "Could not load the Hugging Face sentiment model. If the model "
                "is already cached, retry with HF_HUB_OFFLINE=1. CamemBERT "
                "tokenizers require the optional SentencePiece dependency; "
                "install the future NLP stack with: pip install -r "
                "requirements-future.in"
            ) from exc
        return self._analyzer

    def _was_text_truncated(self, text: str) -> bool:
        """Return whether tokenizer length exceeds the configured max length."""
        analyzer = self._get_analyzer()
        tokenizer = getattr(analyzer, "tokenizer", None)
        if tokenizer is None:
            return False
        encoded = tokenizer(
            text,
            add_special_tokens=True,
            truncation=False,
            verbose=False,
        )
        input_ids = encoded.get("input_ids", [])
        return len(input_ids) > self._model_bundle_config.max_token_length


def compute_generic_sentiment_score(
    probabilities_by_label: Mapping[str, float],
) -> float:
    """Convert 1-5 star probabilities to a normalized generic score.

    Args:
        probabilities_by_label: Probability distribution keyed by star labels.

    Returns:
        Expected star score mapped from ``[1, 5]`` to ``[-1, 1]``.

    Raises:
        DataQualityError: If labels are missing, unsupported, out of range, or
            produce a normalized score outside ``[-1, 1]``.
    """
    probabilities_by_star = _normalize_probability_labels(probabilities_by_label)
    expected_star = sum(
        star_value * probability
        for star_value, probability in probabilities_by_star.items()
    )
    normalized_score = (expected_star - 3.0) / 2.0
    if not -1 <= normalized_score <= 1:
        raise DataQualityError(
            "generic_sentiment_score must be between -1 and 1; check model "
            "probabilities sum to a valid distribution"
        )
    return float(normalized_score)


def build_fact_mention_nlp_summary(
    nlp_input_dataframe: pd.DataFrame,
    *,
    sentiment_runner: SentimentRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
    scored_at: pd.Timestamp | str | None = None,
    continue_on_model_error: bool = True,
) -> pd.DataFrame:
    """Build the Phase 2 mention-level NLP summary table.

    Phase 2 only populates the generic sentiment baseline. The
    ``target_tone_*`` and ``primary_frame_*`` columns are placeholders reserved
    for Phase 3/4 Natural Language Inference outputs; label placeholders are
    persisted as ``unclassified`` and probability placeholders remain NULL.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        sentiment_runner: Optional scorer implementation. Tests pass a mocked
            runner; production uses the lazy Hugging Face adapter.
        model_bundle_config: Optional model-bundle metadata override.
        scored_at: UTC timestamp for scored rows. Defaults to current UTC time.
        continue_on_model_error: When true, row-level model runtime failures are
            written with ``nlp_enrichment_status = failed``. Dependency failures
            still raise because no requested scoring can run.

    Returns:
        DataFrame matching the ``silver.fact_mention_nlp_summary`` contract.

    Raises:
        DataQualityError: If input or output contracts fail.
        TransformerDependencyError: If Transformer dependencies are missing when
            scoring is requested.
    """
    _validate_nlp_input_for_sentiment(nlp_input_dataframe)
    effective_model_bundle_config = model_bundle_config or build_model_bundle_config()
    scoring_timestamp = coerce_utc_timestamp(scored_at)
    nlp_input_rows = nlp_input_dataframe.loc[
        :, list(_REQUIRED_NLP_INPUT_COLUMNS)
    ].copy()
    nlp_input_rows["eligible_for_inference"] = nlp_input_rows[
        "eligible_for_inference"
    ].astype(bool)

    skipped_rows = nlp_input_rows.loc[~nlp_input_rows["eligible_for_inference"]]
    eligible_rows = nlp_input_rows.loc[nlp_input_rows["eligible_for_inference"]]

    output_rows: list[dict[str, object]] = [
        _build_skipped_summary_row(
            input_row,
            effective_model_bundle_config.bundle_version,
        )
        for input_row in skipped_rows.to_dict("records")
    ]

    if not eligible_rows.empty:
        effective_runner = sentiment_runner or HuggingFaceSentimentRunner(
            effective_model_bundle_config
        )
        output_rows.extend(
            _score_eligible_rows(
                eligible_rows=eligible_rows,
                sentiment_runner=effective_runner,
                model_bundle_config=effective_model_bundle_config,
                scored_at=scoring_timestamp,
                continue_on_model_error=continue_on_model_error,
            )
        )

    nlp_summary_dataframe = pd.DataFrame(
        output_rows,
        columns=FACT_MENTION_NLP_SUMMARY_COLUMNS,
    )
    if not nlp_summary_dataframe.empty:
        order_by_mention = {
            mention_id: index
            for index, mention_id in enumerate(nlp_input_dataframe["mention_id"])
        }
        nlp_summary_dataframe = (
            nlp_summary_dataframe.assign(
                _input_order=nlp_summary_dataframe["mention_id"].map(order_by_mention)
            )
            .sort_values("_input_order", kind="stable")
            .drop(columns=["_input_order"])
            .reset_index(drop=True)
        )
    validate_fact_mention_nlp_summary(nlp_summary_dataframe, nlp_input_dataframe)
    logger.info(
        "Built NLP summary rows=%d scored=%d skipped=%d failed=%d bundle=%s",
        len(nlp_summary_dataframe),
        int((nlp_summary_dataframe["nlp_enrichment_status"] == "scored").sum()),
        int((nlp_summary_dataframe["nlp_enrichment_status"] == "skipped").sum()),
        int((nlp_summary_dataframe["nlp_enrichment_status"] == "failed").sum()),
        effective_model_bundle_config.bundle_version,
    )
    return nlp_summary_dataframe


def materialize_fact_mention_nlp_summary(
    nlp_input_dataframe: pd.DataFrame,
    *,
    sentiment_runner: SentimentRunner | None = None,
    model_bundle_config: ModelBundleConfig | None = None,
    silver_dir: Path = SILVER_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    scored_at: pd.Timestamp | str | None = None,
) -> pd.DataFrame:
    """Build and persist ``silver.fact_mention_nlp_summary``.

    Args:
        nlp_input_dataframe: ``silver.fact_mention_nlp_input`` rows.
        sentiment_runner: Optional scorer implementation for tests.
        model_bundle_config: Optional model-bundle metadata override.
        silver_dir: Directory where the Silver Parquet artifact is written.
        duckdb_path: DuckDB warehouse path for the Silver table write.
        scored_at: UTC timestamp for scored rows. Defaults to now.

    Returns:
        The DataFrame written to Parquet and DuckDB.

    Raises:
        DataQualityError: If input or output validation fails.
        TransformerDependencyError: If sentiment scoring is requested without
            optional Transformer dependencies.
        RuntimeError: If DuckDB is unavailable while persisting the table.
    """
    nlp_summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=sentiment_runner,
        model_bundle_config=model_bundle_config,
        scored_at=scored_at,
    )
    parquet_path = silver_dir / "fact_mention_nlp_summary.parquet"
    write_parquet_table(nlp_summary_dataframe, parquet_path)
    write_duckdb_table(
        dataframe=nlp_summary_dataframe,
        schema_name="silver",
        table_name="fact_mention_nlp_summary",
        duckdb_path=duckdb_path,
    )
    logger.info(
        "Materialized NLP summary parquet_path=%s duckdb_path=%s rows=%d",
        parquet_path,
        duckdb_path,
        len(nlp_summary_dataframe),
    )
    return nlp_summary_dataframe


def validate_fact_mention_nlp_summary(
    nlp_summary_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame | None = None,
) -> None:
    """Validate the mention-level NLP summary output table.

    Args:
        nlp_summary_dataframe: Candidate summary output.
        nlp_input_dataframe: Optional source input table used to verify row
            coverage and hash lineage.

    Raises:
        DataQualityError: If required columns, keys, statuses, sentiment fields,
            lineage, or model metadata violate the contract.
    """
    require_columns(
        dataframe=nlp_summary_dataframe,
        required_columns=_REQUIRED_NLP_SUMMARY_COLUMNS,
        dataframe_name="fact_mention_nlp_summary",
    )
    validate_required_identifier_values(
        dataframe=nlp_summary_dataframe,
        dataframe_name="fact_mention_nlp_summary",
        identifier_columns=_CORE_IDENTIFIER_COLUMNS,
    )
    validate_unique_key(
        dataframe=nlp_summary_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention_nlp_summary",
    )
    _validate_status_values(nlp_summary_dataframe)
    _validate_model_bundle_metadata(nlp_summary_dataframe)
    _validate_sentiment_columns(nlp_summary_dataframe)
    _validate_future_placeholder_columns(nlp_summary_dataframe)
    _validate_error_contract(nlp_summary_dataframe)
    _validate_truncation_column(nlp_summary_dataframe)
    if nlp_input_dataframe is not None:
        _validate_summary_matches_input(nlp_summary_dataframe, nlp_input_dataframe)


def _score_eligible_rows(
    *,
    eligible_rows: pd.DataFrame,
    sentiment_runner: SentimentRunner,
    model_bundle_config: ModelBundleConfig,
    scored_at: pd.Timestamp,
    continue_on_model_error: bool,
) -> list[dict[str, object]]:
    """Score eligible rows while preserving input order."""
    output_rows: list[dict[str, object]] = []
    eligible_records = eligible_rows.to_dict("records")
    for start_index in range(0, len(eligible_records), model_bundle_config.batch_size):
        batch_records = eligible_records[
            start_index : start_index + model_bundle_config.batch_size
        ]
        batch_texts = [str(record["input_text"]) for record in batch_records]
        try:
            predictions = sentiment_runner.predict_batch(batch_texts)
        except (SentimentModelLoadError, TransformerDependencyError):
            raise
        except Exception as exc:
            if not continue_on_model_error:
                raise
            logger.exception(
                "Sentiment batch failed; falling back to row-level failures "
                "start_index=%d size=%d",
                start_index,
                len(batch_records),
            )
            output_rows.extend(
                _score_batch_rows_individually(
                    batch_records=batch_records,
                    sentiment_runner=sentiment_runner,
                    model_bundle_config=model_bundle_config,
                    scored_at=scored_at,
                    batch_error=exc,
                )
            )
            continue

        if len(predictions) != len(batch_records):
            raise DataQualityError(
                "sentiment runner returned "
                f"{len(predictions)} predictions for {len(batch_records)} inputs"
            )
        output_rows.extend(
            _build_scored_summary_row(
                input_row=input_row,
                prediction=prediction,
                model_bundle_version=model_bundle_config.bundle_version,
                scored_at=scored_at,
            )
            for input_row, prediction in zip(
                batch_records,
                predictions,
                strict=True,
            )
        )
    return output_rows


def _score_batch_rows_individually(
    *,
    batch_records: list[dict[str, object]],
    sentiment_runner: SentimentRunner,
    model_bundle_config: ModelBundleConfig,
    scored_at: pd.Timestamp,
    batch_error: Exception,
) -> list[dict[str, object]]:
    """Retry a failed batch row by row to isolate recoverable model errors."""
    output_rows: list[dict[str, object]] = []
    for input_row in batch_records:
        try:
            predictions = sentiment_runner.predict_batch([str(input_row["input_text"])])
            if len(predictions) != 1:
                raise DataQualityError(
                    "sentiment runner returned "
                    f"{len(predictions)} predictions for one input"
                )
            output_rows.append(
                _build_scored_summary_row(
                    input_row=input_row,
                    prediction=predictions[0],
                    model_bundle_version=model_bundle_config.bundle_version,
                    scored_at=scored_at,
                )
            )
        except (SentimentModelLoadError, TransformerDependencyError):
            raise
        except Exception as exc:
            row_error = exc if not isinstance(exc, DataQualityError) else batch_error
            output_rows.append(
                _build_failed_summary_row(
                    input_row=input_row,
                    model_bundle_version=model_bundle_config.bundle_version,
                    scored_at=scored_at,
                    error_type=type(row_error).__name__,
                )
            )
    return output_rows


def _build_scored_summary_row(
    *,
    input_row: dict[str, object],
    prediction: SentimentPrediction,
    model_bundle_version: str,
    scored_at: pd.Timestamp,
) -> dict[str, object]:
    """Build one successfully scored summary row."""
    generic_sentiment_label = _normalize_star_label(prediction.label)
    generic_sentiment_score = compute_generic_sentiment_score(
        prediction.probabilities_by_label
    )
    return _base_summary_row(
        input_row=input_row,
        model_bundle_version=model_bundle_version,
        generic_sentiment_label=generic_sentiment_label,
        generic_sentiment_score=generic_sentiment_score,
        was_truncated_to_max_length=bool(prediction.was_truncated_to_max_length),
        nlp_enrichment_status="scored",
        scored_at=scored_at,
        error_type=None,
    )


def _build_skipped_summary_row(
    input_row: dict[str, object],
    model_bundle_version: str,
) -> dict[str, object]:
    """Build one skipped summary row for non-eligible model inputs."""
    return _base_summary_row(
        input_row=input_row,
        model_bundle_version=model_bundle_version,
        generic_sentiment_label=None,
        generic_sentiment_score=None,
        was_truncated_to_max_length=False,
        nlp_enrichment_status="skipped",
        scored_at=None,
        error_type=None,
    )


def _build_failed_summary_row(
    *,
    input_row: dict[str, object],
    model_bundle_version: str,
    scored_at: pd.Timestamp,
    error_type: str,
) -> dict[str, object]:
    """Build one failed summary row for recoverable model-runtime errors."""
    return _base_summary_row(
        input_row=input_row,
        model_bundle_version=model_bundle_version,
        generic_sentiment_label=None,
        generic_sentiment_score=None,
        was_truncated_to_max_length=False,
        nlp_enrichment_status="failed",
        scored_at=scored_at,
        error_type=error_type,
    )


def _base_summary_row(
    *,
    input_row: dict[str, object],
    model_bundle_version: str,
    generic_sentiment_label: str | None,
    generic_sentiment_score: float | None,
    was_truncated_to_max_length: bool,
    nlp_enrichment_status: str,
    scored_at: pd.Timestamp | None,
    error_type: str | None,
) -> dict[str, object]:
    """Build the shared row shape for Phase 2 summary outputs."""
    return {
        "mention_id": str(input_row["mention_id"]).strip(),
        "leader_id": str(input_row["leader_id"]).strip(),
        "canonical_article_id": str(input_row["canonical_article_id"]).strip(),
        "input_hash": input_row["input_hash"],
        "generic_sentiment_label": generic_sentiment_label,
        "generic_sentiment_score": generic_sentiment_score,
        "target_tone_label": "unclassified",
        "target_tone_probability": None,
        "primary_frame_label": "unclassified",
        "primary_frame_probability": None,
        "was_truncated_to_max_length": was_truncated_to_max_length,
        "nlp_enrichment_status": nlp_enrichment_status,
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": scored_at,
        "error_type": error_type,
    }


def _validate_nlp_input_for_sentiment(nlp_input_dataframe: pd.DataFrame) -> None:
    """Validate Phase 0 rows before sentiment scoring."""
    require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    validate_required_identifier_values(
        dataframe=nlp_input_dataframe,
        dataframe_name="fact_mention_nlp_input",
        identifier_columns=_CORE_IDENTIFIER_COLUMNS,
    )
    validate_unique_key(
        dataframe=nlp_input_dataframe,
        key_columns=("mention_id",),
        dataframe_name="fact_mention_nlp_input",
    )
    if nlp_input_dataframe["eligible_for_inference"].isna().any():
        raise DataQualityError(
            "fact_mention_nlp_input eligible_for_inference has nulls"
        )
    if not pd.api.types.is_bool_dtype(nlp_input_dataframe["eligible_for_inference"]):
        invalid_values = ~nlp_input_dataframe["eligible_for_inference"].map(
            lambda value: isinstance(value, bool)
        )
        if invalid_values.any():
            raise DataQualityError(
                "fact_mention_nlp_input eligible_for_inference must contain booleans"
            )

    eligible_rows = nlp_input_dataframe["eligible_for_inference"].astype(bool)
    blank_eligible_text = nlp_input_dataframe.loc[eligible_rows, "input_text"].map(
        is_null_or_blank
    )
    if blank_eligible_text.any():
        raise DataQualityError(
            "fact_mention_nlp_input inference-eligible rows must have input_text"
        )
    blank_eligible_hash = nlp_input_dataframe.loc[eligible_rows, "input_hash"].map(
        is_null_or_blank
    )
    if blank_eligible_hash.any():
        raise DataQualityError(
            "fact_mention_nlp_input inference-eligible rows must have input_hash"
        )


def _validate_status_values(nlp_summary_dataframe: pd.DataFrame) -> None:
    """Validate controlled enrichment statuses."""
    blank_status = nlp_summary_dataframe["nlp_enrichment_status"].map(is_null_or_blank)
    if blank_status.any():
        raise DataQualityError("fact_mention_nlp_summary status has blanks")
    unsupported_status = ~nlp_summary_dataframe["nlp_enrichment_status"].isin(
        CONTROLLED_NLP_ENRICHMENT_STATUSES
    )
    if unsupported_status.any():
        examples = (
            nlp_summary_dataframe.loc[unsupported_status, "nlp_enrichment_status"]
            .drop_duplicates()
            .tolist()
        )
        raise DataQualityError(
            f"fact_mention_nlp_summary unsupported statuses: {examples}"
        )


def _validate_model_bundle_metadata(nlp_summary_dataframe: pd.DataFrame) -> None:
    """Validate model bundle lineage on every summary row."""
    blank_bundle = nlp_summary_dataframe["nlp_model_bundle_version"].map(
        is_null_or_blank
    )
    if blank_bundle.any():
        raise DataQualityError(
            "fact_mention_nlp_summary nlp_model_bundle_version has blanks"
        )


def _validate_sentiment_columns(nlp_summary_dataframe: pd.DataFrame) -> None:
    """Validate label and normalized-score contracts."""
    scored_rows = nlp_summary_dataframe["nlp_enrichment_status"] == "scored"
    non_scored_rows = ~scored_rows
    missing_scored_label = nlp_summary_dataframe.loc[
        scored_rows,
        "generic_sentiment_label",
    ].map(is_null_or_blank)
    if missing_scored_label.any():
        raise DataQualityError(
            "fact_mention_nlp_summary scored rows require generic_sentiment_label"
        )
    missing_scored_score = nlp_summary_dataframe.loc[
        scored_rows,
        "generic_sentiment_score",
    ].isna()
    if missing_scored_score.any():
        raise DataQualityError(
            "fact_mention_nlp_summary scored rows require generic_sentiment_score"
        )

    non_null_labels = (
        nlp_summary_dataframe["generic_sentiment_label"]
        .dropna()
        .astype(str)
        .map(_normalize_star_label)
    )
    unsupported_labels = ~non_null_labels.isin(SENTIMENT_STAR_LABELS)
    if unsupported_labels.any():
        examples = non_null_labels.loc[unsupported_labels].drop_duplicates().tolist()
        raise DataQualityError(
            "fact_mention_nlp_summary unsupported sentiment labels: " f"{examples}"
        )

    non_null_scores = pd.to_numeric(
        nlp_summary_dataframe["generic_sentiment_score"].dropna(),
        errors="coerce",
    )
    if non_null_scores.isna().any():
        raise DataQualityError(
            "fact_mention_nlp_summary generic_sentiment_score has non-numeric values"
        )
    if ((non_null_scores < -1) | (non_null_scores > 1)).any():
        raise DataQualityError(
            "fact_mention_nlp_summary generic_sentiment_score must be between -1 and 1"
        )

    unexpected_non_scored_label = (
        nlp_summary_dataframe.loc[non_scored_rows, "generic_sentiment_label"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
    )
    unexpected_non_scored_score = nlp_summary_dataframe.loc[
        non_scored_rows,
        "generic_sentiment_score",
    ].notna()
    if unexpected_non_scored_label.any() or unexpected_non_scored_score.any():
        raise DataQualityError(
            "fact_mention_nlp_summary non-scored rows must not contain sentiment"
        )

    scored_missing_timestamp = nlp_summary_dataframe.loc[
        scored_rows, "scored_at"
    ].isna()
    if scored_missing_timestamp.any():
        raise DataQualityError("fact_mention_nlp_summary scored rows need scored_at")


def _validate_future_placeholder_columns(nlp_summary_dataframe: pd.DataFrame) -> None:
    """Validate controlled tone and frame columns."""
    non_null_tone_labels = nlp_summary_dataframe["target_tone_label"].dropna()
    unsupported_tones = ~non_null_tone_labels.isin(CONTROLLED_TONE_LABELS)
    if unsupported_tones.any():
        examples = (
            non_null_tone_labels.loc[unsupported_tones].drop_duplicates().tolist()
        )
        raise DataQualityError(
            f"fact_mention_nlp_summary unsupported tone labels: {examples}"
        )
    non_null_frame_labels = nlp_summary_dataframe["primary_frame_label"].dropna()
    unsupported_frames = ~non_null_frame_labels.isin(CONTROLLED_FRAME_LABELS)
    if unsupported_frames.any():
        examples = (
            non_null_frame_labels.loc[unsupported_frames].drop_duplicates().tolist()
        )
        raise DataQualityError(
            f"fact_mention_nlp_summary unsupported frame labels: {examples}"
        )
    for column_name in ("target_tone_probability", "primary_frame_probability"):
        values = pd.to_numeric(
            nlp_summary_dataframe[column_name].dropna(),
            errors="coerce",
        )
        if values.isna().any():
            raise DataQualityError(
                f"fact_mention_nlp_summary {column_name} has non-numeric values"
            )
        if ((values < 0) | (values > 1)).any():
            raise DataQualityError(
                f"fact_mention_nlp_summary {column_name} must be between 0 and 1"
            )


def _validate_error_contract(nlp_summary_dataframe: pd.DataFrame) -> None:
    """Validate error metadata for failed rows."""
    failed_rows = nlp_summary_dataframe["nlp_enrichment_status"] == "failed"
    missing_error = nlp_summary_dataframe.loc[failed_rows, "error_type"].map(
        is_null_or_blank
    )
    if missing_error.any():
        raise DataQualityError("fact_mention_nlp_summary failed rows need error_type")
    unexpected_error = (
        nlp_summary_dataframe.loc[~failed_rows, "error_type"]
        .fillna("")
        .astype(str)
        .str.strip()
        .ne("")
    )
    if unexpected_error.any():
        raise DataQualityError(
            "fact_mention_nlp_summary error_type is only allowed for failed rows"
        )


def _validate_truncation_column(nlp_summary_dataframe: pd.DataFrame) -> None:
    """Validate the tokenizer truncation audit flag."""
    if nlp_summary_dataframe["was_truncated_to_max_length"].isna().any():
        raise DataQualityError(
            "fact_mention_nlp_summary was_truncated_to_max_length has nulls"
        )
    if pd.api.types.is_bool_dtype(nlp_summary_dataframe["was_truncated_to_max_length"]):
        return
    invalid_values = ~nlp_summary_dataframe["was_truncated_to_max_length"].map(
        lambda value: isinstance(value, bool)
    )
    if invalid_values.any():
        raise DataQualityError(
            "fact_mention_nlp_summary was_truncated_to_max_length must contain booleans"
        )


def _validate_summary_matches_input(
    nlp_summary_dataframe: pd.DataFrame,
    nlp_input_dataframe: pd.DataFrame,
) -> None:
    """Validate row-level lineage against the source NLP input table."""
    _validate_nlp_input_for_sentiment(nlp_input_dataframe)
    input_mentions = set(nlp_input_dataframe["mention_id"])
    output_mentions = set(nlp_summary_dataframe["mention_id"])
    if output_mentions - input_mentions:
        examples = sorted(output_mentions - input_mentions)[:5]
        raise DataQualityError(
            "fact_mention_nlp_summary has output rows without matching NLP input: "
            f"{examples}"
        )
    if input_mentions - output_mentions:
        examples = sorted(input_mentions - output_mentions)[:5]
        raise DataQualityError(
            "fact_mention_nlp_summary missing rows for NLP input mentions: "
            f"{examples}"
        )

    input_hashes = nlp_input_dataframe[["mention_id", "input_hash"]].rename(
        columns={"input_hash": "input_hash_expected"}
    )
    lineage_dataframe = nlp_summary_dataframe[["mention_id", "input_hash"]].merge(
        input_hashes,
        on="mention_id",
        how="left",
        validate="one_to_one",
    )
    hash_mismatch = lineage_dataframe["input_hash"].fillna("") != lineage_dataframe[
        "input_hash_expected"
    ].fillna("")
    if hash_mismatch.any():
        raise DataQualityError(
            "fact_mention_nlp_summary input_hash does not match NLP input"
        )


def _normalize_probability_labels(
    probabilities_by_label: Mapping[str, float],
) -> dict[int, float]:
    """Normalize and validate star-label probabilities."""
    probabilities_by_star: dict[int, float] = {}
    for raw_label, raw_probability in probabilities_by_label.items():
        star_value = _parse_star_label(raw_label)
        if star_value in probabilities_by_star:
            raise DataQualityError(f"duplicate sentiment star label: {raw_label}")
        probability = float(raw_probability)
        if not 0 <= probability <= 1:
            raise DataQualityError(
                f"sentiment probability for {raw_label} must be between 0 and 1"
            )
        probabilities_by_star[star_value] = probability

    expected_stars = set(_STAR_LABEL_BY_VALUE)
    missing_stars = sorted(expected_stars - set(probabilities_by_star))
    if missing_stars:
        raise DataQualityError(
            f"sentiment probabilities missing stars: {missing_stars}"
        )
    return probabilities_by_star


def _parse_star_label(label: object) -> int:
    """Parse one controlled star label into an integer value."""
    match = _STAR_LABEL_PATTERN.match(str(label))
    if match is None:
        raise DataQualityError(f"unsupported sentiment label: {label}")
    return int(match.group(1))


def _normalize_star_label(label: object) -> str:
    """Return the canonical persisted star label."""
    return _STAR_LABEL_BY_VALUE[_parse_star_label(label)]


def _select_top_label(result_batch: Sequence[Mapping[str, object]]) -> str:
    """Return the highest-probability label from one HF result batch."""
    if not result_batch:
        raise RuntimeError("Hugging Face sentiment result batch is empty")
    top_score = max(result_batch, key=lambda result: float(result["score"]))
    return str(top_score["label"])


def _normalize_huggingface_results(
    raw_results: Any,
    expected_count: int,
) -> list[list[Mapping[str, object]]]:
    """Normalize Hugging Face pipeline output to one score list per input."""
    if expected_count == 1 and raw_results and isinstance(raw_results[0], dict):
        return [raw_results]
    if not isinstance(raw_results, list):
        raise RuntimeError("Hugging Face sentiment pipeline returned non-list output")
    if len(raw_results) != expected_count:
        raise RuntimeError(
            "Hugging Face sentiment pipeline returned "
            f"{len(raw_results)} outputs for {expected_count} inputs"
        )
    if any(not isinstance(result_batch, list) for result_batch in raw_results):
        raise RuntimeError("Hugging Face sentiment pipeline returned invalid batches")
    return raw_results
