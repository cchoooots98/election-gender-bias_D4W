"""Tests for NLP model bundle metadata governance."""

from __future__ import annotations

import sys

import pytest

from src.nlp.model_bundle import ModelBundleConfig, build_model_bundle_config


def _base_model_bundle_config(**overrides) -> ModelBundleConfig:
    """Build a valid model bundle config with optional test overrides."""
    values = {
        "sentiment_model_name": "cmarkea/distilcamembert-base-sentiment",
        "sentiment_model_revision": "abc123",
        "nli_model_name": "cmarkea/distilcamembert-base-nli",
        "nli_model_revision": "def456",
        "nli_backup_model_name": "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
        "nli_backup_model_revision": "ghi789",
        "hypothesis_template_version": "candidate_tone_frame_v1",
        "tone_threshold": 0.6,
        "frame_threshold": 0.6,
        "max_token_length": 512,
        "batch_size": 32,
        "device": "cpu",
    }
    values.update(overrides)
    return ModelBundleConfig(**values)


def test_model_bundle_version_is_deterministic():
    """Happy path: identical metadata yields the same bundle version."""
    first_config = _base_model_bundle_config()
    second_config = _base_model_bundle_config()

    assert first_config.bundle_version == second_config.bundle_version
    assert len(first_config.bundle_version) == 12


def test_model_bundle_version_changes_when_threshold_changes():
    """Regression: threshold edits must invalidate the bundle version."""
    base_config = _base_model_bundle_config()
    changed_config = _base_model_bundle_config(frame_threshold=0.7)

    assert base_config.bundle_version != changed_config.bundle_version


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("sentiment_model_name", ""),
        ("nli_model_revision", "  "),
        ("hypothesis_template_version", ""),
        ("device", ""),
    ],
)
def test_model_bundle_rejects_blank_required_fields(field_name, bad_value):
    """Error path: model provenance fields must not silently go blank."""
    with pytest.raises(ValueError, match="non-blank"):
        _base_model_bundle_config(**{field_name: bad_value})


@pytest.mark.parametrize(
    ("field_name", "bad_value", "message"),
    [
        ("tone_threshold", 1.5, "tone_threshold"),
        ("frame_threshold", -0.1, "frame_threshold"),
        ("max_token_length", 0, "max_token_length"),
        ("batch_size", 0, "batch_size"),
    ],
)
def test_model_bundle_rejects_invalid_numeric_fields(
    field_name,
    bad_value,
    message,
):
    """Error path: thresholds and runtime dimensions must be valid."""
    with pytest.raises(ValueError, match=message):
        _base_model_bundle_config(**{field_name: bad_value})


def test_model_bundle_metadata_includes_version_and_device():
    """Happy path: metadata is ready for future QA reports."""
    config = _base_model_bundle_config(device="cuda:0")

    metadata = config.to_metadata()

    assert metadata["device"] == "cuda:0"
    assert metadata["nlp_model_bundle_version"] == config.bundle_version


def test_build_model_bundle_config_falls_back_to_cpu_without_torch(monkeypatch):
    """Boundary: auto device stays import-light when torch is unavailable."""
    monkeypatch.setitem(sys.modules, "torch", None)

    config = build_model_bundle_config(device_policy="auto")

    assert config.device == "cpu"


def test_build_model_bundle_config_persists_explicit_device():
    """Happy path: configured device metadata is not overridden."""
    config = build_model_bundle_config(device_policy="cuda:0")

    assert config.device == "cuda:0"
