"""Model bundle metadata for future local NLP inference.

This module does not import Transformer libraries at module import time. It
keeps the default repository lightweight while still making model provenance
auditable before any scorer is implemented.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from src.config.settings import (
    NLI_BACKUP_MODEL_NAME,
    NLI_BACKUP_MODEL_REVISION,
    NLI_MODEL_NAME,
    NLI_MODEL_REVISION,
    NLP_BATCH_SIZE,
    NLP_FRAME_THRESHOLD,
    NLP_HYPOTHESIS_TEMPLATE_VERSION,
    NLP_MAX_TOKEN_LENGTH,
    NLP_MODEL_DEVICE,
    NLP_TONE_THRESHOLD,
    SENTIMENT_MODEL_NAME,
    SENTIMENT_MODEL_REVISION,
)


@dataclass(frozen=True)
class ModelBundleConfig:
    """Versioned model-scoring configuration.

    Args:
        sentiment_model_name: HuggingFace model name for generic sentiment.
        sentiment_model_revision: HuggingFace revision for sentiment weights.
        nli_model_name: HuggingFace model name for tone and frame NLI scoring.
        nli_model_revision: HuggingFace revision for primary NLI weights.
        nli_backup_model_name: Optional agreement-check model name.
        nli_backup_model_revision: HuggingFace revision for the backup model.
        hypothesis_template_version: Version of the hypothesis text templates.
        tone_threshold: Confidence threshold for target-aware tone.
        frame_threshold: Confidence threshold for frame labels.
        max_token_length: Tokenizer maximum sequence length for model inputs.
        batch_size: Inference batch size.
        device: Resolved runtime device metadata.
    """

    sentiment_model_name: str
    sentiment_model_revision: str
    nli_model_name: str
    nli_model_revision: str
    nli_backup_model_name: str
    nli_backup_model_revision: str
    hypothesis_template_version: str
    tone_threshold: float
    frame_threshold: float
    max_token_length: int
    batch_size: int
    device: str

    def __post_init__(self) -> None:
        """Validate bundle metadata before deriving a version hash."""
        _require_non_blank(
            {
                "sentiment_model_name": self.sentiment_model_name,
                "sentiment_model_revision": self.sentiment_model_revision,
                "nli_model_name": self.nli_model_name,
                "nli_model_revision": self.nli_model_revision,
                "nli_backup_model_name": self.nli_backup_model_name,
                "nli_backup_model_revision": self.nli_backup_model_revision,
                "hypothesis_template_version": self.hypothesis_template_version,
                "device": self.device,
            }
        )
        _validate_probability_threshold("tone_threshold", self.tone_threshold)
        _validate_probability_threshold("frame_threshold", self.frame_threshold)
        if self.max_token_length <= 0:
            raise ValueError("max_token_length must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

    @property
    def bundle_version(self) -> str:
        """Return a deterministic short SHA-256 bundle identifier."""
        payload = json.dumps(
            self.to_metadata(include_bundle_version=False),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]

    def to_metadata(self, *, include_bundle_version: bool = True) -> dict[str, object]:
        """Return serializable model-bundle metadata.

        Args:
            include_bundle_version: Whether to include the derived short hash.

        Returns:
            Dictionary suitable for QA reports or future meta tables.
        """
        metadata: dict[str, object] = {
            "sentiment_model_name": self.sentiment_model_name,
            "sentiment_model_revision": self.sentiment_model_revision,
            "nli_model_name": self.nli_model_name,
            "nli_model_revision": self.nli_model_revision,
            "nli_backup_model_name": self.nli_backup_model_name,
            "nli_backup_model_revision": self.nli_backup_model_revision,
            "hypothesis_template_version": self.hypothesis_template_version,
            "tone_threshold": float(self.tone_threshold),
            "frame_threshold": float(self.frame_threshold),
            "max_token_length": int(self.max_token_length),
            "batch_size": int(self.batch_size),
            "device": self.device,
        }
        if include_bundle_version:
            metadata["nlp_model_bundle_version"] = self.bundle_version
        return metadata


def build_model_bundle_config(
    *,
    device_policy: str = NLP_MODEL_DEVICE,
) -> ModelBundleConfig:
    """Build the default model bundle metadata from project settings.

    Args:
        device_policy: ``auto`` detects CUDA lazily. Any other non-blank value is
            persisted as configured device metadata.

    Returns:
        Validated model bundle configuration.

    Raises:
        ValueError: If required model metadata or thresholds are invalid.
    """
    return ModelBundleConfig(
        sentiment_model_name=SENTIMENT_MODEL_NAME,
        sentiment_model_revision=SENTIMENT_MODEL_REVISION,
        nli_model_name=NLI_MODEL_NAME,
        nli_model_revision=NLI_MODEL_REVISION,
        nli_backup_model_name=NLI_BACKUP_MODEL_NAME,
        nli_backup_model_revision=NLI_BACKUP_MODEL_REVISION,
        hypothesis_template_version=NLP_HYPOTHESIS_TEMPLATE_VERSION,
        tone_threshold=NLP_TONE_THRESHOLD,
        frame_threshold=NLP_FRAME_THRESHOLD,
        max_token_length=NLP_MAX_TOKEN_LENGTH,
        batch_size=NLP_BATCH_SIZE,
        device=resolve_model_device(device_policy),
    )


def resolve_model_device(device_policy: str = NLP_MODEL_DEVICE) -> str:
    """Resolve runtime device metadata without importing torch eagerly.

    Args:
        device_policy: ``auto`` chooses ``cuda`` when ``torch.cuda.is_available``
            is true and otherwise falls back to ``cpu``. Other non-blank values
            are returned unchanged so operators can record explicit devices such
            as ``cuda:0``.

    Returns:
        Device string to persist in model-bundle metadata.

    Raises:
        ValueError: If ``device_policy`` is blank.
    """
    normalized_policy = str(device_policy).strip()
    if not normalized_policy:
        raise ValueError("device_policy must be non-blank")
    if normalized_policy.lower() != "auto":
        return normalized_policy
    return "cuda" if _cuda_available() else "cpu"


def _cuda_available() -> bool:
    """Return whether torch reports a CUDA device."""
    try:
        import torch
    except ImportError:
        return False
    return bool(torch.cuda.is_available())


def _require_non_blank(values_by_name: dict[str, str]) -> None:
    """Raise when required bundle metadata is blank."""
    blank_names = [
        name for name, value in values_by_name.items() if not str(value).strip()
    ]
    if blank_names:
        raise ValueError(f"Model bundle fields must be non-blank: {blank_names}")


def _validate_probability_threshold(name: str, value: float) -> None:
    """Raise when a configured threshold is outside the probability range."""
    if not 0 <= float(value) <= 1:
        raise ValueError(f"{name} must be between 0 and 1")
