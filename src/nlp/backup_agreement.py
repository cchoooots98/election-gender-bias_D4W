"""Backup-model agreement sample builder for governed NLI review."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.config.settings import GOLD_DIR, WAREHOUSE_PATH
from src.nlp._validation import require_columns, validate_unique_key
from src.nlp.model_bundle import ModelBundleConfig, build_model_bundle_config
from src.nlp.nli import (
    FrameRunner,
    FrameScoringInput,
    HuggingFaceNliFrameRunner,
    HuggingFaceNliToneRunner,
    ToneRunner,
    ToneScoringInput,
    select_primary_frame,
    select_target_tone_label,
)
from src.nlp.sentiment import (
    FACT_MENTION_NLP_SUMMARY_COLUMNS,
    validate_fact_mention_nlp_summary,
)
from src.storage.tables import write_duckdb_table, write_parquet_table
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

DEFAULT_BACKUP_SAMPLE_SIZE = 100
DEFAULT_BACKUP_RANDOM_SEED = 20260527

_REQUIRED_NLP_INPUT_COLUMNS = {
    "mention_id",
    "leader_id",
    "canonical_article_id",
    "input_text",
    "input_hash",
    "eligible_for_inference",
}
_REQUIRED_SAMPLE_COLUMNS = {"leader_id", "full_name"}


def build_backup_model_config(
    primary_model_bundle_config: ModelBundleConfig | None = None,
) -> ModelBundleConfig:
    """Build a model bundle that swaps the primary NLI model for the backup.

    Args:
        primary_model_bundle_config: Optional primary bundle used to preserve
            non-NLI scoring settings.

    Returns:
        Backup model bundle configuration.
    """
    primary_config = primary_model_bundle_config or build_model_bundle_config()
    return ModelBundleConfig(
        sentiment_model_name=primary_config.sentiment_model_name,
        sentiment_model_revision=primary_config.sentiment_model_revision,
        nli_model_name=primary_config.nli_backup_model_name,
        nli_model_revision=primary_config.nli_backup_model_revision,
        nli_backup_model_name=primary_config.nli_backup_model_name,
        nli_backup_model_revision=primary_config.nli_backup_model_revision,
        hypothesis_template_version=primary_config.hypothesis_template_version,
        tone_threshold=primary_config.tone_threshold,
        frame_threshold=primary_config.frame_threshold,
        max_token_length=primary_config.max_token_length,
        batch_size=primary_config.batch_size,
        device=primary_config.device,
        frame_thresholds=primary_config.frame_thresholds,
    )


def build_backup_summary_sample(
    nlp_input_dataframe: pd.DataFrame,
    primary_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    *,
    sample_size: int = DEFAULT_BACKUP_SAMPLE_SIZE,
    random_seed: int = DEFAULT_BACKUP_RANDOM_SEED,
    model_bundle_config: ModelBundleConfig | None = None,
    tone_runner: ToneRunner | None = None,
    frame_runner: FrameRunner | None = None,
    scored_at: datetime | None = None,
) -> pd.DataFrame:
    """Score a deterministic mention sample with the backup NLI model.

    Args:
        nlp_input_dataframe: Phase 0 NLP input rows.
        primary_summary_dataframe: Primary model summary rows.
        sample_leaders_dataframe: Sample leaders with candidate names.
        sample_size: Maximum scoreable mentions to run through the backup model.
        random_seed: Seed used for deterministic sample selection.
        model_bundle_config: Optional backup bundle override.
        tone_runner: Optional tone scorer for tests.
        frame_runner: Optional frame scorer for tests.
        scored_at: Optional UTC timestamp for deterministic tests.

    Returns:
        Full ``fact_mention_nlp_summary``-shaped DataFrame with backup model
        bundle lineage. Only sampled scoreable mentions receive backup tone and
        frame classifications; other rows remain unclassified.

    Raises:
        DataQualityError: If source contracts or scorer outputs are invalid.
        ValueError: If sampling parameters are invalid.
    """
    if sample_size <= 0:
        raise ValueError("sample_size must be positive")

    backup_config = model_bundle_config or build_backup_model_config()
    _validate_backup_sources(
        nlp_input_dataframe,
        primary_summary_dataframe,
        sample_leaders_dataframe,
    )

    backup_summary_dataframe = primary_summary_dataframe.loc[
        :, FACT_MENTION_NLP_SUMMARY_COLUMNS
    ].copy()
    backup_summary_dataframe["target_tone_label"] = "unclassified"
    backup_summary_dataframe["target_tone_probability"] = None
    backup_summary_dataframe["primary_frame_label"] = "unclassified"
    backup_summary_dataframe["primary_frame_probability"] = None
    backup_summary_dataframe["nlp_model_bundle_version"] = backup_config.bundle_version

    scoring_dataframe = _build_backup_scoring_dataframe(
        nlp_input_dataframe,
        primary_summary_dataframe,
        sample_leaders_dataframe,
    )
    sampled_dataframe = _sample_scoreable_mentions(
        scoring_dataframe,
        sample_size=sample_size,
        random_seed=random_seed,
    )
    if sampled_dataframe.empty:
        validate_fact_mention_nlp_summary(backup_summary_dataframe, nlp_input_dataframe)
        return backup_summary_dataframe

    effective_tone_runner = tone_runner or HuggingFaceNliToneRunner(backup_config)
    effective_frame_runner = frame_runner or HuggingFaceNliFrameRunner(backup_config)
    effective_scored_at = scored_at or datetime.now(UTC)

    tone_predictions = _score_backup_tone(
        sampled_dataframe,
        tone_runner=effective_tone_runner,
        model_bundle_config=backup_config,
    )
    frame_predictions = _score_backup_frames(
        sampled_dataframe,
        frame_runner=effective_frame_runner,
        model_bundle_config=backup_config,
    )
    backup_summary_dataframe = _apply_backup_predictions(
        backup_summary_dataframe,
        tone_predictions,
        frame_predictions,
        scored_at=effective_scored_at,
    )
    validate_fact_mention_nlp_summary(backup_summary_dataframe, nlp_input_dataframe)
    logger.info(
        "Backup NLI sample built sampled_mentions=%d backup_bundle=%s",
        len(sampled_dataframe),
        backup_config.bundle_version,
    )
    return backup_summary_dataframe


def materialize_backup_summary_sample(
    nlp_input_dataframe: pd.DataFrame,
    primary_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
    *,
    parquet_path: Path = GOLD_DIR / "nlp_backup_summary_sample.parquet",
    duckdb_path: Path = WAREHOUSE_PATH,
    sample_size: int = DEFAULT_BACKUP_SAMPLE_SIZE,
    random_seed: int = DEFAULT_BACKUP_RANDOM_SEED,
    model_bundle_config: ModelBundleConfig | None = None,
    tone_runner: ToneRunner | None = None,
    frame_runner: FrameRunner | None = None,
) -> pd.DataFrame:
    """Build and persist the backup-model summary sample.

    Args:
        nlp_input_dataframe: Phase 0 NLP input rows.
        primary_summary_dataframe: Primary model summary rows.
        sample_leaders_dataframe: Sample leaders with names.
        parquet_path: Output Parquet artifact path.
        duckdb_path: DuckDB warehouse path.
        sample_size: Maximum sampled mentions to score.
        random_seed: Deterministic sampling seed.
        model_bundle_config: Optional backup bundle override.
        tone_runner: Optional tone scorer for tests.
        frame_runner: Optional frame scorer for tests.

    Returns:
        Persisted backup summary DataFrame.
    """
    backup_summary_dataframe = build_backup_summary_sample(
        nlp_input_dataframe,
        primary_summary_dataframe,
        sample_leaders_dataframe,
        sample_size=sample_size,
        random_seed=random_seed,
        model_bundle_config=model_bundle_config,
        tone_runner=tone_runner,
        frame_runner=frame_runner,
    )
    write_parquet_table(backup_summary_dataframe, parquet_path)
    write_duckdb_table(
        dataframe=backup_summary_dataframe,
        schema_name="gold",
        table_name="nlp_backup_summary_sample",
        duckdb_path=duckdb_path,
    )
    return backup_summary_dataframe


def _validate_backup_sources(
    nlp_input_dataframe: pd.DataFrame,
    primary_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
) -> None:
    """Validate source tables before backup inference."""
    require_columns(
        dataframe=nlp_input_dataframe,
        required_columns=_REQUIRED_NLP_INPUT_COLUMNS,
        dataframe_name="fact_mention_nlp_input",
    )
    require_columns(
        dataframe=sample_leaders_dataframe,
        required_columns=_REQUIRED_SAMPLE_COLUMNS,
        dataframe_name="sample_leaders",
    )
    validate_fact_mention_nlp_summary(primary_summary_dataframe, nlp_input_dataframe)
    validate_unique_key(
        dataframe=sample_leaders_dataframe,
        key_columns=("leader_id",),
        dataframe_name="sample_leaders",
    )


def _build_backup_scoring_dataframe(
    nlp_input_dataframe: pd.DataFrame,
    primary_summary_dataframe: pd.DataFrame,
    sample_leaders_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """Join input, primary status, and candidate names for backup scoring."""
    scoring_dataframe = (
        nlp_input_dataframe.loc[
            :,
            [
                "mention_id",
                "leader_id",
                "canonical_article_id",
                "input_text",
                "eligible_for_inference",
            ],
        ]
        .merge(
            primary_summary_dataframe.loc[
                :,
                [
                    "mention_id",
                    "leader_id",
                    "canonical_article_id",
                    "nlp_enrichment_status",
                ],
            ],
            on=["mention_id", "leader_id", "canonical_article_id"],
            how="inner",
            validate="one_to_one",
        )
        .merge(
            sample_leaders_dataframe.loc[:, ["leader_id", "full_name"]],
            on="leader_id",
            how="left",
            validate="many_to_one",
        )
    )
    if scoring_dataframe["full_name"].isna().any():
        raise DataQualityError("backup scoring rows require candidate names")
    scoring_dataframe = scoring_dataframe.loc[
        scoring_dataframe["eligible_for_inference"].astype(bool)
        & scoring_dataframe["nlp_enrichment_status"].eq("scored")
    ].copy()
    return scoring_dataframe.sort_values("mention_id").reset_index(drop=True)


def _sample_scoreable_mentions(
    scoring_dataframe: pd.DataFrame,
    *,
    sample_size: int,
    random_seed: int,
) -> pd.DataFrame:
    """Return deterministic sample rows without replacing mentions."""
    if scoring_dataframe.empty or len(scoring_dataframe) <= sample_size:
        return scoring_dataframe.copy()
    rng = np.random.default_rng(random_seed)
    sampled_positions = sorted(
        rng.choice(len(scoring_dataframe), size=sample_size, replace=False).tolist()
    )
    return scoring_dataframe.iloc[sampled_positions].reset_index(drop=True)


def _score_backup_tone(
    scoring_dataframe: pd.DataFrame,
    *,
    tone_runner: ToneRunner,
    model_bundle_config: ModelBundleConfig,
) -> pd.DataFrame:
    """Run backup tone predictions over sampled rows."""
    scoring_inputs = [
        ToneScoringInput(
            mention_id=str(row.mention_id),
            input_text=str(row.input_text),
            candidate_name=str(row.full_name),
        )
        for row in scoring_dataframe.itertuples(index=False)
    ]
    predictions = tone_runner.predict_batch(scoring_inputs)
    if len(predictions) != len(scoring_inputs):
        raise DataQualityError("backup tone runner returned unexpected row count")
    rows: list[dict[str, object]] = []
    for scoring_input, prediction in zip(scoring_inputs, predictions, strict=True):
        selected_label, selected_probability = select_target_tone_label(
            prediction.probabilities_by_label,
            threshold=model_bundle_config.tone_threshold,
        )
        rows.append(
            {
                "mention_id": scoring_input.mention_id,
                "target_tone_label": selected_label,
                "target_tone_probability": selected_probability,
                "was_truncated_to_max_length": prediction.was_truncated_to_max_length,
            }
        )
    return pd.DataFrame(rows)


def _score_backup_frames(
    scoring_dataframe: pd.DataFrame,
    *,
    frame_runner: FrameRunner,
    model_bundle_config: ModelBundleConfig,
) -> pd.DataFrame:
    """Run backup frame predictions over sampled rows."""
    scoring_inputs = [
        FrameScoringInput(
            mention_id=str(row.mention_id),
            input_text=str(row.input_text),
        )
        for row in scoring_dataframe.itertuples(index=False)
    ]
    predictions = frame_runner.predict_batch(scoring_inputs)
    if len(predictions) != len(scoring_inputs):
        raise DataQualityError("backup frame runner returned unexpected row count")
    rows: list[dict[str, object]] = []
    for scoring_input, prediction in zip(scoring_inputs, predictions, strict=True):
        selected_label, selected_probability = select_primary_frame(
            prediction.probabilities_by_label,
            thresholds_by_frame=model_bundle_config.frame_thresholds,
        )
        rows.append(
            {
                "mention_id": scoring_input.mention_id,
                "primary_frame_label": selected_label,
                "primary_frame_probability": selected_probability,
                "was_truncated_to_max_length": prediction.was_truncated_to_max_length,
            }
        )
    return pd.DataFrame(rows)


def _apply_backup_predictions(
    backup_summary_dataframe: pd.DataFrame,
    tone_predictions: pd.DataFrame,
    frame_predictions: pd.DataFrame,
    *,
    scored_at: datetime,
) -> pd.DataFrame:
    """Apply sampled backup predictions to a full summary-shaped table."""
    backup_by_mention = backup_summary_dataframe.set_index("mention_id", drop=False)
    sampled_mentions = set(tone_predictions["mention_id"].astype(str)) | set(
        frame_predictions["mention_id"].astype(str)
    )
    if sampled_mentions:
        backup_by_mention.loc[list(sampled_mentions), "scored_at"] = pd.Timestamp(
            scored_at
        )

    for prediction_dataframe in (tone_predictions, frame_predictions):
        if prediction_dataframe.empty:
            continue
        prediction_by_mention = prediction_dataframe.set_index("mention_id")
        update_mentions = prediction_by_mention.index.tolist()
        for column_name in prediction_by_mention.columns:
            if column_name == "was_truncated_to_max_length":
                backup_by_mention.loc[update_mentions, column_name] = (
                    backup_by_mention.loc[update_mentions, column_name].astype(bool)
                    | prediction_by_mention[column_name].astype(bool)
                ).to_numpy()
            else:
                backup_by_mention.loc[update_mentions, column_name] = (
                    prediction_by_mention[column_name]
                )
    return backup_by_mention.reset_index(drop=True)[
        list(FACT_MENTION_NLP_SUMMARY_COLUMNS)
    ]
