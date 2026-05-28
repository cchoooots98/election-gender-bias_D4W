"""End-to-end NLP contract tests for skipped empty mention contexts."""

from __future__ import annotations

import pandas as pd

from src.nlp.input_contracts import build_fact_mention_nlp_input
from src.nlp.lexicon import FACT_STEREOTYPE_WORD_COUNTS_COLUMNS
from src.nlp.nli import (
    FACT_MENTION_FRAME_SCORE_COLUMNS,
    enrich_fact_mention_nlp_summary_with_frames,
    enrich_fact_mention_nlp_summary_with_tone,
)
from src.nlp.qa import build_nlp_qa_report
from src.nlp.sentiment import build_fact_mention_nlp_summary


class FailIfCalledRunner:
    """Model runner stub that fails if skipped rows reach inference."""

    calls: list[object]

    def __init__(self) -> None:
        self.calls = []

    def predict_batch(self, inputs):
        """Record the unexpected call and fail the test immediately."""
        self.calls.append(inputs)
        raise AssertionError("empty-context rows must not reach model inference")


def test_empty_context_flows_from_phase0_to_phase5_without_model_calls(
    model_bundle_config_factory,
):
    """Regression: skipped empty contexts must remain auditable end to end."""
    model_bundle_config = model_bundle_config_factory()
    fact_mention_dataframe = pd.DataFrame(
        [
            {
                "mention_id": "mention-empty",
                "canonical_article_id": "article-001",
                "leader_id": "leader-001",
                "context_sentences": " \n\t ",
            }
        ]
    )
    fact_article_dataframe = pd.DataFrame(
        [{"canonical_article_id": "article-001", "language": "fr"}]
    )
    sample_leaders_dataframe = pd.DataFrame(
        [{"leader_id": "leader-001", "full_name": "Alice Martin"}]
    )

    nlp_input_dataframe = build_fact_mention_nlp_input(
        fact_mention_dataframe,
        fact_article_dataframe,
        prepared_at="2026-04-01T12:00:00Z",
    )
    sentiment_runner = FailIfCalledRunner()
    nlp_summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=sentiment_runner,
        model_bundle_config=model_bundle_config,
        scored_at="2026-04-02T10:00:00Z",
    )
    tone_runner = FailIfCalledRunner()
    tone_summary_dataframe = enrich_fact_mention_nlp_summary_with_tone(
        nlp_input_dataframe,
        nlp_summary_dataframe,
        sample_leaders_dataframe,
        tone_runner=tone_runner,
        model_bundle_config=model_bundle_config,
    )
    frame_runner = FailIfCalledRunner()
    final_summary_dataframe, frame_score_dataframe = (
        enrich_fact_mention_nlp_summary_with_frames(
            nlp_input_dataframe,
            tone_summary_dataframe,
            frame_runner=frame_runner,
            model_bundle_config=model_bundle_config,
        )
    )
    stereotype_dataframe = pd.DataFrame(columns=FACT_STEREOTYPE_WORD_COUNTS_COLUMNS)

    report = build_nlp_qa_report(
        nlp_input_dataframe,
        final_summary_dataframe,
        frame_score_dataframe,
        stereotype_dataframe,
        model_bundle_config=model_bundle_config,
    )

    final_row = final_summary_dataframe.iloc[0]
    assert nlp_input_dataframe.iloc[0]["skip_reason"] == "empty_context"
    assert final_row["nlp_enrichment_status"] == "skipped"
    assert final_row["target_tone_label"] == "unclassified"
    assert pd.isna(final_row["target_tone_probability"])
    assert final_row["primary_frame_label"] == "unclassified"
    assert pd.isna(final_row["primary_frame_probability"])
    assert sentiment_runner.calls == []
    assert tone_runner.calls == []
    assert frame_runner.calls == []
    assert tuple(frame_score_dataframe.columns) == FACT_MENTION_FRAME_SCORE_COLUMNS
    assert report["input_coverage"]["eligible_for_inference_mentions"] == 0
    assert report["output_coverage"]["tone"]["scoreable_mentions"] == 0
