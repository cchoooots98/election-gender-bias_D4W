"""Tests for Phase 3 target-aware NLI tone scoring."""

from __future__ import annotations

import sys

import pandas as pd
import pytest
from conftest import ConfigurableToneRunner

from src.nlp.nli import (
    HuggingFaceNliToneRunner,
    TonePrediction,
    ToneScoringInput,
    build_tone_hypothesis_template,
    enrich_fact_mention_nlp_summary_with_tone,
    materialize_fact_mention_nlp_summary_with_tone,
    select_target_tone_label,
)
from src.nlp.sentiment import (
    FACT_MENTION_NLP_SUMMARY_COLUMNS,
    TransformerDependencyError,
)
from src.transform._exceptions import DataQualityError


class EmptyToneRunner:
    """Mock scorer that violates the output-count contract."""

    def predict_batch(self, scoring_inputs):
        """Return no predictions to trigger row-count validation."""
        return []


def _tone_prediction(
    *,
    favorable: float = 0.80,
    unfavorable: float = 0.10,
    neutral: float = 0.10,
    was_truncated: bool = False,
) -> TonePrediction:
    """Return one deterministic tone prediction."""
    return TonePrediction(
        probabilities_by_label={
            "favorable": favorable,
            "unfavorable": unfavorable,
            "neutral": neutral,
        },
        was_truncated_to_max_length=was_truncated,
    )


def _nlp_input_dataframe(
    rows: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Return valid Phase 0 input rows for tone tests."""
    return pd.DataFrame(
        rows
        or [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "input_text": "Alice Martin presente son programme municipal local.",
                "input_hash": "hash-001",
                "eligible_for_inference": True,
                "skip_reason": None,
            }
        ]
    )


def _nlp_summary_dataframe(
    model_bundle_version: str,
    rows: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Return valid Phase 2 summary rows for tone tests."""
    return pd.DataFrame(
        rows
        or [
            _summary_row(
                mention_id="mention-001",
                leader_id="leader-001",
                canonical_article_id="article-001",
                input_hash="hash-001",
                model_bundle_version=model_bundle_version,
            )
        ],
        columns=FACT_MENTION_NLP_SUMMARY_COLUMNS,
    )


def _summary_row(
    *,
    mention_id: str,
    leader_id: str,
    canonical_article_id: str,
    input_hash: str | None,
    model_bundle_version: str,
    status: str = "scored",
    error_type: str | None = None,
) -> dict[str, object]:
    """Return one valid NLP summary row."""
    scored = status == "scored"
    return {
        "mention_id": mention_id,
        "leader_id": leader_id,
        "canonical_article_id": canonical_article_id,
        "input_hash": input_hash,
        "generic_sentiment_label": "4 stars" if scored else None,
        "generic_sentiment_score": 0.25 if scored else None,
        "target_tone_label": "unclassified",
        "target_tone_probability": None,
        "primary_frame_label": "unclassified",
        "primary_frame_probability": None,
        "was_truncated_to_max_length": False,
        "nlp_enrichment_status": status,
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": pd.Timestamp("2026-04-02T10:00:00Z") if scored else None,
        "error_type": error_type,
    }


def _sample_leaders_dataframe(
    rows: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Return sampled leader names for target-aware tone tests."""
    return pd.DataFrame(
        rows or [{"leader_id": "leader-001", "full_name": "Alice Martin"}]
    )


@pytest.mark.parametrize(
    ("expected_label", "probabilities"),
    [
        (
            "favorable",
            {"favorable": 0.81, "unfavorable": 0.10, "neutral": 0.09},
        ),
        (
            "unfavorable",
            {"favorable": 0.10, "unfavorable": 0.76, "neutral": 0.14},
        ),
        (
            "neutral",
            {"favorable": 0.15, "unfavorable": 0.15, "neutral": 0.70},
        ),
    ],
)
def test_select_target_tone_label_returns_confident_label(
    expected_label,
    probabilities,
):
    """Happy path: high-confidence NLI output becomes a controlled tone label."""
    label, probability = select_target_tone_label(probabilities, threshold=0.60)

    assert label == expected_label
    assert probability == pytest.approx(probabilities[expected_label])


def test_select_target_tone_label_keeps_low_confidence_probability():
    """Boundary: low-confidence tone is unclassified but still auditable."""
    label, probability = select_target_tone_label(
        {"favorable": 0.40, "unfavorable": 0.35, "neutral": 0.25},
        threshold=0.60,
    )

    assert label == "unclassified"
    assert probability == pytest.approx(0.40)


@pytest.mark.parametrize("invalid_threshold", [-0.01, 1.01, float("nan")])
def test_select_target_tone_label_raises_on_invalid_threshold(invalid_threshold):
    """Error path: thresholds must be finite probabilities."""
    with pytest.raises(ValueError, match="threshold"):
        select_target_tone_label(
            {"favorable": 0.8, "unfavorable": 0.1, "neutral": 0.1},
            threshold=invalid_threshold,
        )


@pytest.mark.parametrize(
    ("probabilities", "message"),
    [
        (
            {"favorable": 0.8, "unfavorable": 0.2},
            "missing labels",
        ),
        (
            {
                "favorable": 0.8,
                "unfavorable": 0.1,
                "neutral": 0.1,
                "positive": 0.5,
            },
            "unsupported tone label",
        ),
        (
            {"favorable": 1.2, "unfavorable": 0.0, "neutral": 0.0},
            "between 0 and 1",
        ),
    ],
)
def test_select_target_tone_label_rejects_malformed_probabilities(
    probabilities,
    message,
):
    """Error path: malformed model probabilities fail before persistence."""
    with pytest.raises(DataQualityError, match=message):
        select_target_tone_label(probabilities, threshold=0.60)


def test_build_tone_hypothesis_template_uses_exact_candidate_context():
    """Happy path: hypotheses bind the candidate name to the tone label."""
    template = build_tone_hypothesis_template("Alice Martin")

    assert template == "Le texte présente Alice Martin de manière {}."
    assert template.format("favorable") == (
        "Le texte présente Alice Martin de manière favorable."
    )


@pytest.mark.parametrize("blank_name", [None, "", "   "])
def test_build_tone_hypothesis_template_raises_on_blank_candidate_name(blank_name):
    """Error path: blank candidate names cannot produce valid hypotheses."""
    with pytest.raises(DataQualityError, match="candidate_name"):
        build_tone_hypothesis_template(blank_name)


def test_enrich_fact_mention_nlp_summary_with_tone_scores_rows(
    model_bundle_config_factory,
):
    """Happy path: mocked NLI tone output updates the existing summary row."""
    model_bundle_config = model_bundle_config_factory()
    runner = ConfigurableToneRunner(
        {"mention-001": _tone_prediction(was_truncated=True)}
    )

    enriched_dataframe = enrich_fact_mention_nlp_summary_with_tone(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(model_bundle_config.bundle_version),
        _sample_leaders_dataframe(),
        tone_runner=runner,
        model_bundle_config=model_bundle_config,
    )

    output_row = enriched_dataframe.iloc[0]
    assert output_row["generic_sentiment_label"] == "4 stars"
    assert output_row["generic_sentiment_score"] == pytest.approx(0.25)
    assert output_row["target_tone_label"] == "favorable"
    assert output_row["target_tone_probability"] == pytest.approx(0.80)
    assert bool(output_row["was_truncated_to_max_length"]) is True
    assert runner.calls == [["mention-001"]]


def test_enrich_fact_mention_nlp_summary_skips_non_scoreable_rows_without_model_call(
    model_bundle_config_factory,
):
    """Boundary: skipped Phase 2 rows keep unclassified tone without model calls."""
    model_bundle_config = model_bundle_config_factory()
    nlp_input_dataframe = _nlp_input_dataframe(
        [
            {
                "mention_id": "mention-001",
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "input_text": None,
                "input_hash": None,
                "eligible_for_inference": False,
                "skip_reason": "empty_context",
            }
        ]
    )
    nlp_summary_dataframe = _nlp_summary_dataframe(
        model_bundle_config.bundle_version,
        [
            _summary_row(
                mention_id="mention-001",
                leader_id="leader-001",
                canonical_article_id="article-001",
                input_hash=None,
                model_bundle_version=model_bundle_config.bundle_version,
                status="skipped",
            )
        ],
    )
    runner = ConfigurableToneRunner({"mention-001": _tone_prediction()})

    enriched_dataframe = enrich_fact_mention_nlp_summary_with_tone(
        nlp_input_dataframe,
        nlp_summary_dataframe,
        _sample_leaders_dataframe(),
        tone_runner=runner,
        model_bundle_config=model_bundle_config,
    )

    output_row = enriched_dataframe.iloc[0]
    assert output_row["target_tone_label"] == "unclassified"
    assert pd.isna(output_row["target_tone_probability"])
    assert runner.calls == []


def test_enrich_fact_mention_nlp_summary_preserves_input_order_across_batches(
    model_bundle_config_factory,
):
    """Regression: batch tone scoring must preserve mention order."""
    model_bundle_config = model_bundle_config_factory(batch_size=2)
    input_rows = []
    summary_rows = []
    sample_rows = []
    predictions = {}
    for index, label_values in enumerate(
        [
            {"favorable": 0.81, "unfavorable": 0.10, "neutral": 0.09},
            {"favorable": 0.10, "unfavorable": 0.79, "neutral": 0.11},
            {"favorable": 0.20, "unfavorable": 0.19, "neutral": 0.61},
        ],
        start=1,
    ):
        mention_id = f"mention-00{index}"
        leader_id = f"leader-00{index}"
        article_id = f"article-00{index}"
        input_rows.append(
            {
                "mention_id": mention_id,
                "leader_id": leader_id,
                "canonical_article_id": article_id,
                "input_text": f"Texte politique numero {index}",
                "input_hash": f"hash-00{index}",
                "eligible_for_inference": True,
                "skip_reason": None,
            }
        )
        summary_rows.append(
            _summary_row(
                mention_id=mention_id,
                leader_id=leader_id,
                canonical_article_id=article_id,
                input_hash=f"hash-00{index}",
                model_bundle_version=model_bundle_config.bundle_version,
            )
        )
        sample_rows.append({"leader_id": leader_id, "full_name": f"Candidate {index}"})
        predictions[mention_id] = TonePrediction(probabilities_by_label=label_values)
    runner = ConfigurableToneRunner(predictions)

    enriched_dataframe = enrich_fact_mention_nlp_summary_with_tone(
        _nlp_input_dataframe(input_rows),
        _nlp_summary_dataframe(model_bundle_config.bundle_version, summary_rows),
        _sample_leaders_dataframe(sample_rows),
        tone_runner=runner,
        model_bundle_config=model_bundle_config,
    )

    assert enriched_dataframe["mention_id"].tolist() == [
        "mention-001",
        "mention-002",
        "mention-003",
    ]
    assert enriched_dataframe["target_tone_label"].tolist() == [
        "favorable",
        "unfavorable",
        "neutral",
    ]
    assert runner.calls == [["mention-001", "mention-002"], ["mention-003"]]


def test_enrich_fact_mention_nlp_summary_rejects_bundle_mismatch(
    model_bundle_config_factory,
):
    """Regression: stale Phase 2 summaries must not be mixed with Phase 3 tone."""
    model_bundle_config = model_bundle_config_factory()

    with pytest.raises(DataQualityError, match="bundle version"):
        enrich_fact_mention_nlp_summary_with_tone(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe("stale-bundle"),
            _sample_leaders_dataframe(),
            tone_runner=ConfigurableToneRunner({"mention-001": _tone_prediction()}),
            model_bundle_config=model_bundle_config,
        )


def test_enrich_fact_mention_nlp_summary_rejects_duplicate_sample_leaders(
    model_bundle_config_factory,
):
    """Error path: candidate-name lookup must be many-to-one by leader_id."""
    model_bundle_config = model_bundle_config_factory()

    with pytest.raises(DataQualityError, match="duplicate key rows"):
        enrich_fact_mention_nlp_summary_with_tone(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            _sample_leaders_dataframe(
                [
                    {"leader_id": "leader-001", "full_name": "Alice Martin"},
                    {"leader_id": "leader-001", "full_name": "Alice M."},
                ]
            ),
            tone_runner=ConfigurableToneRunner({"mention-001": _tone_prediction()}),
            model_bundle_config=model_bundle_config,
        )


def test_enrich_fact_mention_nlp_summary_rejects_missing_sample_leader(
    model_bundle_config_factory,
):
    """Error path: scoreable rows require candidate names from sample_leaders."""
    model_bundle_config = model_bundle_config_factory()

    with pytest.raises(DataQualityError, match="missing from sample_leaders"):
        enrich_fact_mention_nlp_summary_with_tone(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            _sample_leaders_dataframe(
                [{"leader_id": "leader-999", "full_name": "Other Candidate"}]
            ),
            tone_runner=ConfigurableToneRunner({"mention-001": _tone_prediction()}),
            model_bundle_config=model_bundle_config,
        )


def test_enrich_fact_mention_nlp_summary_rejects_prediction_count_mismatch(
    model_bundle_config_factory,
):
    """Regression: NLI batch scoring must preserve input row count."""
    model_bundle_config = model_bundle_config_factory()

    with pytest.raises(DataQualityError, match="returned 0 predictions"):
        enrich_fact_mention_nlp_summary_with_tone(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            _sample_leaders_dataframe(),
            tone_runner=EmptyToneRunner(),
            model_bundle_config=model_bundle_config,
        )


def test_huggingface_nli_runner_raises_only_when_transformer_scoring_is_requested(
    monkeypatch,
    model_bundle_config_factory,
):
    """Regression: optional Transformer dependency remains lazy."""
    monkeypatch.setitem(sys.modules, "transformers", None)
    runner = HuggingFaceNliToneRunner(model_bundle_config_factory())

    with pytest.raises(TransformerDependencyError, match="requirements-future.in"):
        runner.predict_batch(
            [
                ToneScoringInput(
                    mention_id="mention-001",
                    input_text="Texte politique local.",
                    candidate_name="Alice Martin",
                )
            ]
        )


def test_materialize_fact_mention_nlp_summary_with_tone_writes_parquet_and_duckdb(
    tmp_path,
    model_bundle_config_factory,
):
    """Integration: Phase 3 persists the updated Silver summary contract."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    model_bundle_config = model_bundle_config_factory()

    materialized_dataframe = materialize_fact_mention_nlp_summary_with_tone(
        _nlp_input_dataframe(),
        _nlp_summary_dataframe(model_bundle_config.bundle_version),
        _sample_leaders_dataframe(),
        tone_runner=ConfigurableToneRunner({"mention-001": _tone_prediction()}),
        model_bundle_config=model_bundle_config,
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
    )

    parquet_path = silver_dir / "fact_mention_nlp_summary.parquet"
    assert parquet_path.exists()
    persisted_dataframe = pd.read_parquet(parquet_path)
    assert len(persisted_dataframe) == len(materialized_dataframe)

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_summary"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(materialized_dataframe)
