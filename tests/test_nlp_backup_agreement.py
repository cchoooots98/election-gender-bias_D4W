"""Tests for backup-model NLI agreement sampling."""

from __future__ import annotations

import pandas as pd

from src.nlp.backup_agreement import (
    build_backup_model_config,
    build_backup_summary_sample,
)
from src.nlp.nli import FramePrediction, TonePrediction


class FixedToneRunner:
    """Mock tone runner returning favorable labels for every sampled row."""

    def predict_batch(self, scoring_inputs):
        """Return deterministic tone predictions."""
        return [
            TonePrediction(
                probabilities_by_label={
                    "favorable": 0.7,
                    "unfavorable": 0.1,
                    "neutral": 0.2,
                }
            )
            for _scoring_input in scoring_inputs
        ]


class FixedFrameRunner:
    """Mock frame runner returning politique labels for every sampled row."""

    def predict_batch(self, scoring_inputs):
        """Return deterministic frame predictions."""
        return [
            FramePrediction(
                probabilities_by_label={
                    "politique": 0.7,
                    "vie_privee": 0.05,
                    "apparence": 0.05,
                    "scandale": 0.05,
                    "personnalite": 0.1,
                    "securite": 0.05,
                }
            )
            for _scoring_input in scoring_inputs
        ]


def _nlp_input_dataframe() -> pd.DataFrame:
    """Return minimal valid NLP input rows."""
    return pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "article_language": "fr",
                "input_text": "Candidate context long enough for inference.",
                "input_hash": "hash-001",
                "context_word_count": 12,
                "eligible_for_lexicon": True,
                "eligible_for_inference": True,
                "skip_reason": None,
                "prepared_at": pd.Timestamp("2026-05-01T00:00:00Z"),
                "input_contract_version": "mention_context_v2",
            },
            {
                "mention_id": "mention-002",
                "leader_id": "leader-002",
                "canonical_article_id": "article-002",
                "article_language": "fr",
                "input_text": "Second candidate context long enough for inference.",
                "input_hash": "hash-002",
                "context_word_count": 12,
                "eligible_for_lexicon": True,
                "eligible_for_inference": True,
                "skip_reason": None,
                "prepared_at": pd.Timestamp("2026-05-01T00:00:00Z"),
                "input_contract_version": "mention_context_v2",
            },
        ]
    )


def _primary_summary_dataframe(model_bundle_version: str) -> pd.DataFrame:
    """Return primary model summary rows."""
    return pd.DataFrame(
        [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "input_hash": "hash-001",
                "generic_sentiment_label": "4 stars",
                "generic_sentiment_score": 0.2,
                "target_tone_label": "neutral",
                "target_tone_probability": 0.65,
                "primary_frame_label": "personnalite",
                "primary_frame_probability": 0.67,
                "was_truncated_to_max_length": False,
                "nlp_enrichment_status": "scored",
                "nlp_model_bundle_version": model_bundle_version,
                "scored_at": pd.Timestamp("2026-05-01T01:00:00Z"),
                "error_type": None,
            },
            {
                "mention_id": "mention-002",
                "leader_id": "leader-002",
                "canonical_article_id": "article-002",
                "input_hash": "hash-002",
                "generic_sentiment_label": "3 stars",
                "generic_sentiment_score": 0.0,
                "target_tone_label": "unclassified",
                "target_tone_probability": 0.5,
                "primary_frame_label": "unclassified",
                "primary_frame_probability": None,
                "was_truncated_to_max_length": False,
                "nlp_enrichment_status": "scored",
                "nlp_model_bundle_version": model_bundle_version,
                "scored_at": pd.Timestamp("2026-05-01T01:00:00Z"),
                "error_type": None,
            },
        ]
    )


def test_build_backup_summary_sample_scores_deterministic_sample(
    model_bundle_config_factory,
):
    """Happy path: backup sample writes full summary shape for QA comparison."""
    primary_config = model_bundle_config_factory()
    backup_config = build_backup_model_config(primary_config)
    sample_leaders_dataframe = pd.DataFrame(
        [
            {"leader_id": "leader-001", "full_name": "Candidate One"},
            {"leader_id": "leader-002", "full_name": "Candidate Two"},
        ]
    )

    backup_summary = build_backup_summary_sample(
        _nlp_input_dataframe(),
        _primary_summary_dataframe(primary_config.bundle_version),
        sample_leaders_dataframe,
        sample_size=2,
        model_bundle_config=backup_config,
        tone_runner=FixedToneRunner(),
        frame_runner=FixedFrameRunner(),
        scored_at=pd.Timestamp("2026-05-02T00:00:00Z"),
    )

    assert len(backup_summary) == 2
    assert set(backup_summary["nlp_model_bundle_version"]) == {
        backup_config.bundle_version
    }
    assert set(backup_summary["target_tone_label"]) == {"favorable"}
    assert set(backup_summary["primary_frame_label"]) == {"politique"}
