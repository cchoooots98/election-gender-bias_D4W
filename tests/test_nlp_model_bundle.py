"""Tests for NLP model bundle metadata governance."""

from __future__ import annotations

import sys

import pytest

from src.nlp.model_bundle import build_model_bundle_config


def test_model_bundle_version_is_deterministic(model_bundle_config_factory):
    """Happy path: identical metadata yields the same bundle version."""
    first_config = model_bundle_config_factory()
    second_config = model_bundle_config_factory()

    assert first_config.bundle_version == second_config.bundle_version
    assert len(first_config.bundle_version) == 12


def test_model_bundle_version_changes_when_threshold_changes(
    model_bundle_config_factory,
):
    """Regression: threshold edits must invalidate the bundle version."""
    base_config = model_bundle_config_factory()
    changed_config = model_bundle_config_factory(frame_threshold=0.7)

    assert base_config.bundle_version != changed_config.bundle_version


def test_model_bundle_version_changes_when_frame_threshold_map_changes(
    model_bundle_config_factory,
):
    """Regression: per-frame threshold edits must invalidate the bundle."""
    base_config = model_bundle_config_factory()
    changed_config = model_bundle_config_factory(
        frame_thresholds={
            "apparence": 0.6,
            "personnalite": 0.6,
            "politique": 0.75,
            "scandale": 0.6,
            "securite": 0.6,
            "vie_privee": 0.6,
        }
    )

    assert base_config.bundle_version != changed_config.bundle_version
    assert changed_config.threshold_for_frame("politique") == 0.75


def test_model_bundle_rejects_invalid_frame_threshold_map(
    model_bundle_config_factory,
):
    """Error path: unsupported frame labels fail before inference."""
    with pytest.raises(ValueError, match="unsupported"):
        model_bundle_config_factory(frame_thresholds={"unknown": 0.6})


@pytest.mark.parametrize(
    ("field_name", "bad_value"),
    [
        ("sentiment_model_name", ""),
        ("nli_model_revision", "  "),
        ("hypothesis_template_version", ""),
        ("device", ""),
    ],
)
def test_model_bundle_rejects_blank_required_fields(
    field_name,
    bad_value,
    model_bundle_config_factory,
):
    """Error path: model provenance fields must not silently go blank."""
    with pytest.raises(ValueError, match="non-blank"):
        model_bundle_config_factory(**{field_name: bad_value})


@pytest.mark.parametrize(
    ("field_name", "bad_value", "message"),
    [
        ("sentiment_model_revision", "main", "mutable alias"),
        ("nli_model_revision", "abc123", "40-character"),
        ("nli_backup_model_revision", "A" * 40, "lowercase"),
    ],
)
def test_model_bundle_rejects_mutable_or_malformed_revisions(
    field_name,
    bad_value,
    message,
    model_bundle_config_factory,
):
    """Regression: model bundle versions must pin immutable HF commits."""
    with pytest.raises(ValueError, match=message):
        model_bundle_config_factory(**{field_name: bad_value})


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
    model_bundle_config_factory,
):
    """Error path: thresholds and runtime dimensions must be valid."""
    with pytest.raises(ValueError, match=message):
        model_bundle_config_factory(**{field_name: bad_value})


def test_model_bundle_metadata_includes_version_and_device(
    model_bundle_config_factory,
):
    """Happy path: metadata is ready for future QA reports."""
    config = model_bundle_config_factory(device="cuda:0")

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
