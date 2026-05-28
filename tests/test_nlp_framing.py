"""Tests for Phase 4 NLI frame scoring."""

from __future__ import annotations

import sys

import pandas as pd
import pytest
from conftest import ConfigurableFrameRunner

from src.nlp.nli import (
    FACT_MENTION_FRAME_SCORE_COLUMNS,
    SCORABLE_FRAME_LABELS,
    FramePrediction,
    FrameScoringInput,
    HuggingFaceNliFrameRunner,
    build_frame_hypothesis,
    enrich_fact_mention_nlp_summary_with_frames,
    materialize_fact_mention_nlp_summary_with_frames,
    select_primary_frame,
)
from src.nlp.sentiment import (
    FACT_MENTION_NLP_SUMMARY_COLUMNS,
    TransformerDependencyError,
)
from src.transform._exceptions import DataQualityError


class EmptyFrameRunner:
    """Mock scorer that violates the output-count contract."""

    def predict_batch(self, scoring_inputs):
        """Return no predictions to trigger row-count validation."""
        return []


def _frame_prediction(
    *,
    politique: float = 0.82,
    vie_privee: float = 0.12,
    apparence: float = 0.08,
    scandale: float = 0.04,
    personnalite: float = 0.20,
    securite: float = 0.11,
    was_truncated: bool = False,
) -> FramePrediction:
    """Return one deterministic frame prediction."""
    return FramePrediction(
        probabilities_by_label={
            "politique": politique,
            "vie_privee": vie_privee,
            "apparence": apparence,
            "scandale": scandale,
            "personnalite": personnalite,
            "securite": securite,
        },
        was_truncated_to_max_length=was_truncated,
    )


def _nlp_input_dataframe(
    rows: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Return valid Phase 0 input rows for frame tests."""
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
    """Return valid NLP summary rows for frame tests."""
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
        "target_tone_label": "favorable" if scored else "unclassified",
        "target_tone_probability": 0.75 if scored else None,
        "primary_frame_label": "unclassified",
        "primary_frame_probability": None,
        "was_truncated_to_max_length": False,
        "nlp_enrichment_status": status,
        "nlp_model_bundle_version": model_bundle_version,
        "scored_at": pd.Timestamp("2026-04-02T10:00:00Z") if scored else None,
        "error_type": error_type,
    }


def test_select_primary_frame_returns_confident_label():
    """Happy path: high-confidence NLI output becomes the primary frame."""
    label, probability = select_primary_frame(
        _frame_prediction().probabilities_by_label,
        threshold=0.60,
    )

    assert label == "politique"
    assert probability == pytest.approx(0.82)


def test_select_primary_frame_returns_unclassified_without_probability():
    """Boundary: low-confidence frames keep unclassified as a fallback state."""
    label, probability = select_primary_frame(
        _frame_prediction(politique=0.42, personnalite=0.41).probabilities_by_label,
        threshold=0.60,
    )

    assert label == "unclassified"
    assert probability is None


def test_select_primary_frame_handles_zero_and_one_thresholds():
    """Boundary: exact 0 and 1 thresholds should have explicit behavior."""
    label_at_zero, probability_at_zero = select_primary_frame(
        _frame_prediction(politique=0.42, personnalite=0.41).probabilities_by_label,
        threshold=0.0,
    )
    label_at_one, probability_at_one = select_primary_frame(
        _frame_prediction(politique=0.99, personnalite=0.01).probabilities_by_label,
        threshold=1.0,
    )

    assert label_at_zero == "politique"
    assert probability_at_zero == pytest.approx(0.42)
    assert label_at_one == "unclassified"
    assert probability_at_one is None


def test_select_primary_frame_uses_per_frame_thresholds():
    """Regression: each frame label can carry its own governance threshold."""
    label, probability = select_primary_frame(
        _frame_prediction(politique=0.82, personnalite=0.81).probabilities_by_label,
        thresholds_by_frame={
            "apparence": 0.6,
            "personnalite": 0.6,
            "politique": 0.9,
            "scandale": 0.6,
            "securite": 0.6,
            "vie_privee": 0.6,
        },
    )

    assert label == "unclassified"
    assert probability is None


@pytest.mark.parametrize("invalid_threshold", [-0.01, 1.01, float("nan")])
def test_select_primary_frame_raises_on_invalid_threshold(invalid_threshold):
    """Error path: frame thresholds must be finite probabilities."""
    with pytest.raises(ValueError, match="threshold"):
        select_primary_frame(
            _frame_prediction().probabilities_by_label,
            threshold=invalid_threshold,
        )


@pytest.mark.parametrize(
    ("probabilities", "message"),
    [
        (
            {
                "politique": 0.8,
                "vie_privee": 0.1,
                "apparence": 0.1,
                "scandale": 0.1,
                "personnalite": 0.1,
            },
            "missing labels",
        ),
        (
            {
                **_frame_prediction().probabilities_by_label,
                "unclassified": 0.1,
            },
            "unsupported frame label",
        ),
        (
            {
                **_frame_prediction().probabilities_by_label,
                "politique": 1.2,
            },
            "between 0 and 1",
        ),
        (
            {
                **_frame_prediction().probabilities_by_label,
                "politique": "high",
            },
            "must be numeric",
        ),
    ],
)
def test_select_primary_frame_rejects_malformed_probabilities(
    probabilities,
    message,
):
    """Error path: malformed model probabilities fail before persistence."""
    with pytest.raises(DataQualityError, match=message):
        select_primary_frame(probabilities, threshold=0.60)


def test_build_frame_hypothesis_uses_controlled_frame_text():
    """Happy path: each frame label maps to an auditable hypothesis."""
    hypothesis = build_frame_hypothesis("politique")

    assert hypothesis == (
        "Le texte discute le programme politique, la gouvernance ou "
        "l'action publique du candidat."
    )


def test_build_frame_hypothesis_rejects_unclassified_label():
    """Error path: unclassified is not sent as a model-scored frame."""
    with pytest.raises(DataQualityError, match="unsupported frame label"):
        build_frame_hypothesis("unclassified")


def test_enrich_fact_mention_nlp_summary_with_frames_scores_rows(
    model_bundle_config_factory,
):
    """Happy path: mocked NLI frame output updates summary and score table."""
    model_bundle_config = model_bundle_config_factory()
    runner = ConfigurableFrameRunner(
        {"mention-001": _frame_prediction(was_truncated=True)}
    )

    summary_dataframe, frame_score_dataframe = (
        enrich_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            frame_runner=runner,
            model_bundle_config=model_bundle_config,
        )
    )

    output_row = summary_dataframe.iloc[0]
    assert output_row["primary_frame_label"] == "politique"
    assert output_row["primary_frame_probability"] == pytest.approx(0.82)
    assert bool(output_row["was_truncated_to_max_length"]) is True
    assert frame_score_dataframe["frame_label"].tolist() == list(SCORABLE_FRAME_LABELS)
    assert "unclassified" not in set(frame_score_dataframe["frame_label"])
    assert len(frame_score_dataframe) == len(SCORABLE_FRAME_LABELS)
    assert frame_score_dataframe["passes_threshold"].sum() == 1
    assert frame_score_dataframe["is_primary_frame"].sum() == 1
    assert runner.calls == [["mention-001"]]


def test_enrich_fact_mention_nlp_summary_keeps_low_confidence_frame_scores(
    model_bundle_config_factory,
):
    """Boundary: below-threshold runs persist frame probabilities for QA."""
    model_bundle_config = model_bundle_config_factory(frame_threshold=0.90)
    runner = ConfigurableFrameRunner({"mention-001": _frame_prediction()})

    summary_dataframe, frame_score_dataframe = (
        enrich_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            frame_runner=runner,
            model_bundle_config=model_bundle_config,
        )
    )

    output_row = summary_dataframe.iloc[0]
    assert output_row["primary_frame_label"] == "unclassified"
    assert pd.isna(output_row["primary_frame_probability"])
    assert len(frame_score_dataframe) == len(SCORABLE_FRAME_LABELS)
    assert not frame_score_dataframe["passes_threshold"].any()
    assert not frame_score_dataframe["is_primary_frame"].any()


def test_enrich_fact_mention_nlp_summary_skips_non_scoreable_rows_without_model_call(
    model_bundle_config_factory,
):
    """Boundary: skipped and failed summary rows do not call the frame model."""
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
            },
            {
                "mention_id": "mention-002",
                "leader_id": "leader-002",
                "canonical_article_id": "article-002",
                "input_text": "Texte politique local avec un contexte suffisant.",
                "input_hash": "hash-002",
                "eligible_for_inference": True,
                "skip_reason": None,
            },
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
            ),
            _summary_row(
                mention_id="mention-002",
                leader_id="leader-002",
                canonical_article_id="article-002",
                input_hash="hash-002",
                model_bundle_version=model_bundle_config.bundle_version,
                status="failed",
                error_type="RuntimeError",
            ),
        ],
    )
    runner = ConfigurableFrameRunner({"mention-001": _frame_prediction()})

    summary_dataframe, frame_score_dataframe = (
        enrich_fact_mention_nlp_summary_with_frames(
            nlp_input_dataframe,
            nlp_summary_dataframe,
            frame_runner=runner,
            model_bundle_config=model_bundle_config,
        )
    )

    assert summary_dataframe["primary_frame_label"].tolist() == [
        "unclassified",
        "unclassified",
    ]
    assert frame_score_dataframe.empty
    assert frame_score_dataframe.columns.tolist() == list(
        FACT_MENTION_FRAME_SCORE_COLUMNS
    )
    assert runner.calls == []


def test_enrich_fact_mention_nlp_summary_preserves_input_order_across_batches(
    model_bundle_config_factory,
):
    """Regression: batch frame scoring must preserve mention order."""
    model_bundle_config = model_bundle_config_factory(batch_size=2)
    input_rows = []
    summary_rows = []
    predictions = {}
    expected_labels = ["politique", "scandale", "personnalite"]
    for index, frame_label in enumerate(expected_labels, start=1):
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
        probabilities = dict.fromkeys(SCORABLE_FRAME_LABELS, 0.05)
        probabilities[frame_label] = 0.83
        predictions[mention_id] = FramePrediction(probabilities_by_label=probabilities)
    runner = ConfigurableFrameRunner(predictions)

    summary_dataframe, frame_score_dataframe = (
        enrich_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(input_rows),
            _nlp_summary_dataframe(model_bundle_config.bundle_version, summary_rows),
            frame_runner=runner,
            model_bundle_config=model_bundle_config,
        )
    )

    assert summary_dataframe["mention_id"].tolist() == [
        "mention-001",
        "mention-002",
        "mention-003",
    ]
    assert summary_dataframe["primary_frame_label"].tolist() == expected_labels
    assert len(frame_score_dataframe) == 3 * len(SCORABLE_FRAME_LABELS)
    assert runner.calls == [["mention-001", "mention-002"], ["mention-003"]]


def test_enrich_fact_mention_nlp_summary_rejects_bundle_mismatch(
    model_bundle_config_factory,
):
    """Regression: stale summaries must not be mixed with Phase 4 framing."""
    model_bundle_config = model_bundle_config_factory()

    with pytest.raises(DataQualityError, match="bundle version"):
        enrich_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe("stale-bundle"),
            frame_runner=ConfigurableFrameRunner({"mention-001": _frame_prediction()}),
            model_bundle_config=model_bundle_config,
        )


def test_enrich_fact_mention_nlp_summary_rejects_stale_input_hash(
    model_bundle_config_factory,
):
    """Regression: stale summary hashes fail before Phase 4 scoring."""
    model_bundle_config = model_bundle_config_factory()
    summary_dataframe = _nlp_summary_dataframe(
        model_bundle_config.bundle_version,
        [
            _summary_row(
                mention_id="mention-001",
                leader_id="leader-001",
                canonical_article_id="article-001",
                input_hash="stale-hash",
                model_bundle_version=model_bundle_config.bundle_version,
            )
        ],
    )

    with pytest.raises(DataQualityError, match="input_hash"):
        enrich_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(),
            summary_dataframe,
            frame_runner=ConfigurableFrameRunner({"mention-001": _frame_prediction()}),
            model_bundle_config=model_bundle_config,
        )


def test_enrich_fact_mention_nlp_summary_rejects_prediction_count_mismatch(
    model_bundle_config_factory,
):
    """Regression: NLI batch scoring must preserve input row count."""
    model_bundle_config = model_bundle_config_factory()

    with pytest.raises(DataQualityError, match="returned 0 predictions"):
        enrich_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            frame_runner=EmptyFrameRunner(),
            model_bundle_config=model_bundle_config,
        )


def test_huggingface_nli_frame_runner_raises_only_when_scoring_is_requested(
    monkeypatch,
    model_bundle_config_factory,
):
    """Regression: optional Transformer dependency remains lazy."""
    monkeypatch.setitem(sys.modules, "transformers", None)
    runner = HuggingFaceNliFrameRunner(model_bundle_config_factory())

    with pytest.raises(TransformerDependencyError, match="requirements-future.in"):
        runner.predict_batch(
            [
                FrameScoringInput(
                    mention_id="mention-001",
                    input_text="Texte politique local.",
                )
            ]
        )


def test_huggingface_nli_frame_runner_loads_pytorch_bin_weights(
    fake_transformers_zero_shot,
    model_bundle_config_factory,
):
    """Regression: frame scoring uses the same explicit NLI weight format."""
    model_bundle_config = model_bundle_config_factory()
    runner = HuggingFaceNliFrameRunner(model_bundle_config)

    predictions = runner.predict_batch(
        [
            FrameScoringInput(
                mention_id="mention-001",
                input_text="Alice Martin presente son programme local.",
            )
        ]
    )

    model_kwargs = fake_transformers_zero_shot["model"][0]["kwargs"]
    tokenizer_kwargs = fake_transformers_zero_shot["tokenizer"][0]["kwargs"]
    assert len(fake_transformers_zero_shot["model"]) == 1
    assert (
        fake_transformers_zero_shot["model"][0]["disable_safetensors_conversion"] == "1"
    )
    assert model_kwargs["revision"] == model_bundle_config.nli_model_revision
    assert model_kwargs["use_safetensors"] is False
    assert tokenizer_kwargs == {
        "revision": model_bundle_config.nli_model_revision,
        "use_fast": False,
    }
    assert fake_transformers_zero_shot["pipeline"][0]["task"] == (
        "zero-shot-classification"
    )
    assert predictions[0].probabilities_by_label["politique"] == pytest.approx(0.90)


def test_materialize_fact_mention_nlp_summary_with_frames_writes_outputs(
    tmp_path,
    model_bundle_config_factory,
):
    """Integration: Phase 4 persists summary and frame-score Silver tables."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    model_bundle_config = model_bundle_config_factory()

    summary_dataframe, frame_score_dataframe = (
        materialize_fact_mention_nlp_summary_with_frames(
            _nlp_input_dataframe(),
            _nlp_summary_dataframe(model_bundle_config.bundle_version),
            frame_runner=ConfigurableFrameRunner({"mention-001": _frame_prediction()}),
            model_bundle_config=model_bundle_config,
            silver_dir=silver_dir,
            duckdb_path=duckdb_path,
        )
    )

    summary_path = silver_dir / "fact_mention_nlp_summary.parquet"
    frame_score_path = silver_dir / "fact_mention_frame_score.parquet"
    assert summary_path.exists()
    assert frame_score_path.exists()
    assert len(pd.read_parquet(summary_path)) == len(summary_dataframe)
    assert len(pd.read_parquet(frame_score_path)) == len(frame_score_dataframe)

    conn = duckdb.connect(str(duckdb_path))
    try:
        summary_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_summary"
        ).fetchone()[0]
        frame_score_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_frame_score"
        ).fetchone()[0]
    finally:
        conn.close()
    assert summary_count == len(summary_dataframe)
    assert frame_score_count == len(frame_score_dataframe)
