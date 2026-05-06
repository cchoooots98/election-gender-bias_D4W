"""Tests for Phase 2 generic NLP sentiment baseline."""

from __future__ import annotations

import sys

import pandas as pd
import pytest

from src.nlp.sentiment import (
    FACT_MENTION_NLP_SUMMARY_COLUMNS,
    HuggingFaceSentimentRunner,
    SentimentModelLoadError,
    TransformerDependencyError,
    build_fact_mention_nlp_summary,
    compute_generic_sentiment_score,
    materialize_fact_mention_nlp_summary,
    validate_fact_mention_nlp_summary,
)
from src.transform._exceptions import DataQualityError


class EmptyPredictionRunner:
    """Mock scorer that violates the output-count contract."""

    def predict_batch(self, texts):
        """Return no predictions to trigger row-count validation."""
        return []


class FailingRunner:
    """Mock scorer that simulates recoverable model-runtime failures."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def predict_batch(self, texts):
        """Raise a model-runtime error for every batch."""
        self.calls.append(list(texts))
        raise ValueError("model failed")


class ModelLoadFailingRunner:
    """Mock scorer that simulates a non-recoverable model loading failure."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def predict_batch(self, texts):
        """Raise a model-load error to verify fail-fast behavior."""
        self.calls.append(list(texts))
        raise SentimentModelLoadError("model could not load")


class RecordingTokenizer:
    """Tokenizer stub that records length-audit options."""

    def __init__(self, token_count: int) -> None:
        self.token_count = token_count
        self.calls: list[dict[str, object]] = []

    def __call__(self, text, **kwargs):
        """Return a synthetic token sequence and keep the call kwargs."""
        self.calls.append({"text": text, **kwargs})
        return {"input_ids": list(range(self.token_count))}


class AnalyzerWithRecordingTokenizer:
    """Analyzer stub exposing a tokenizer like a Hugging Face pipeline."""

    def __init__(self, tokenizer: RecordingTokenizer) -> None:
        self.tokenizer = tokenizer


def _eligible_nlp_input_dataframe(
    *,
    mention_id: str = "mention-001",
    input_text: str = "Alice Martin presente son programme municipal aux habitants.",
    eligible_for_inference: bool = True,
    input_hash: str | None = "hash-001",
    skip_reason: str | None = None,
) -> pd.DataFrame:
    """Return one NLP input row for sentiment tests."""
    return pd.DataFrame(
        [
            {
                "mention_id": mention_id,
                "leader_id": "leader-001",
                "canonical_article_id": "article-001",
                "input_text": input_text,
                "input_hash": input_hash,
                "eligible_for_inference": eligible_for_inference,
                "skip_reason": skip_reason,
            }
        ]
    )


def test_compute_generic_sentiment_score_uses_expected_star_formula():
    """Happy path: expected 1-5 star rating maps to a -1..1 baseline score."""
    score = compute_generic_sentiment_score(
        {
            "1 star": 0.10,
            "2 stars": 0.10,
            "3 stars": 0.20,
            "4 stars": 0.30,
            "5 stars": 0.30,
        }
    )

    assert score == pytest.approx(0.30)


def test_build_fact_mention_nlp_summary_scores_eligible_rows(
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Happy path: mocked model output becomes a scored Silver summary row."""
    nlp_input_dataframe = _eligible_nlp_input_dataframe()
    runner = sentiment_runner_factory(
        {
            "Alice Martin presente son programme municipal aux habitants.": (
                sentiment_prediction_factory(was_truncated=True)
            )
        }
    )
    model_bundle_config = model_bundle_config_factory()
    scored_at = pd.Timestamp("2026-04-02T10:00:00Z")

    summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config,
        scored_at=scored_at,
    )

    output_row = summary_dataframe.iloc[0]
    assert tuple(summary_dataframe.columns) == FACT_MENTION_NLP_SUMMARY_COLUMNS
    assert output_row["generic_sentiment_label"] == "5 stars"
    assert output_row["generic_sentiment_score"] == pytest.approx(0.30)
    assert output_row["target_tone_label"] == "unclassified"
    assert output_row["primary_frame_label"] == "unclassified"
    assert bool(output_row["was_truncated_to_max_length"]) is True
    assert output_row["nlp_enrichment_status"] == "scored"
    assert output_row["nlp_model_bundle_version"] == model_bundle_config.bundle_version
    assert output_row["scored_at"] == scored_at
    assert runner.calls == [
        ["Alice Martin presente son programme municipal aux habitants."]
    ]


def test_build_fact_mention_nlp_summary_skips_non_eligible_rows_without_model_call(
    model_bundle_config_factory,
):
    """Boundary: skipped inputs are preserved without invoking Transformers."""
    nlp_input_dataframe = _eligible_nlp_input_dataframe(
        input_text=None,
        eligible_for_inference=False,
        input_hash=None,
        skip_reason="empty_context",
    )
    runner = FailingRunner()

    summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(),
        scored_at="2026-04-02T10:00:00Z",
    )

    output_row = summary_dataframe.iloc[0]
    assert output_row["generic_sentiment_label"] is None
    assert pd.isna(output_row["generic_sentiment_score"])
    assert output_row["nlp_enrichment_status"] == "skipped"
    assert pd.isna(output_row["scored_at"])


def test_build_fact_mention_nlp_summary_writes_failed_rows_for_model_runtime_errors(
    model_bundle_config_factory,
):
    """Boundary: recoverable model errors are explicit failed rows."""
    summary_dataframe = build_fact_mention_nlp_summary(
        _eligible_nlp_input_dataframe(),
        sentiment_runner=FailingRunner(),
        model_bundle_config=model_bundle_config_factory(),
        scored_at="2026-04-02T10:00:00Z",
    )

    output_row = summary_dataframe.iloc[0]
    assert output_row["nlp_enrichment_status"] == "failed"
    assert output_row["error_type"] == "ValueError"
    assert output_row["generic_sentiment_label"] is None
    assert pd.isna(output_row["generic_sentiment_score"])


def test_build_fact_mention_nlp_summary_fails_fast_on_model_load_errors(
    model_bundle_config_factory,
):
    """Regression: model setup failures must not retry once per input row."""
    input_rows = [
        _eligible_nlp_input_dataframe(
            mention_id=f"mention-00{index}",
            input_text=f"Texte politique numero {index}",
            input_hash=f"hash-00{index}",
        )
        for index in range(1, 4)
    ]
    nlp_input_dataframe = pd.concat(input_rows, ignore_index=True)
    runner = ModelLoadFailingRunner()

    with pytest.raises(SentimentModelLoadError, match="could not load"):
        build_fact_mention_nlp_summary(
            nlp_input_dataframe,
            sentiment_runner=runner,
            model_bundle_config=model_bundle_config_factory(batch_size=2),
            scored_at="2026-04-02T10:00:00Z",
        )

    assert runner.calls == [["Texte politique numero 1", "Texte politique numero 2"]]


@pytest.mark.parametrize(
    ("probabilities", "message"),
    [
        (
            {
                "1 star": 0.1,
                "2 stars": 0.1,
                "3 stars": 0.2,
                "4 stars": 0.3,
            },
            "missing stars",
        ),
        (
            {
                "1 star": 0.1,
                "2 stars": 0.1,
                "3 stars": 0.2,
                "4 stars": 0.3,
                "5 stars": 1.2,
            },
            "between 0 and 1",
        ),
        (
            {
                "negative": 0.1,
                "2 stars": 0.1,
                "3 stars": 0.2,
                "4 stars": 0.3,
                "5 stars": 0.3,
            },
            "unsupported sentiment label",
        ),
    ],
)
def test_compute_generic_sentiment_score_rejects_malformed_probabilities(
    probabilities,
    message,
):
    """Error path: malformed model probabilities fail before persistence."""
    with pytest.raises(DataQualityError, match=message):
        compute_generic_sentiment_score(probabilities)


def test_build_fact_mention_nlp_summary_rejects_unsupported_top_label(
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Error path: top labels must use the controlled 1-5 star vocabulary."""
    runner = sentiment_runner_factory(
        {
            "Alice Martin presente son programme municipal aux habitants.": (
                sentiment_prediction_factory(label="positive")
            )
        }
    )

    with pytest.raises(DataQualityError, match="unsupported sentiment label"):
        build_fact_mention_nlp_summary(
            _eligible_nlp_input_dataframe(),
            sentiment_runner=runner,
            model_bundle_config=model_bundle_config_factory(),
        )


def test_build_fact_mention_nlp_summary_rejects_duplicate_mention_id(
    model_bundle_config_factory,
):
    """Regression: duplicate NLP input mention IDs fail before scoring."""
    nlp_input_dataframe = pd.concat(
        [_eligible_nlp_input_dataframe(), _eligible_nlp_input_dataframe()],
        ignore_index=True,
    )

    with pytest.raises(DataQualityError, match="duplicate key rows"):
        build_fact_mention_nlp_summary(
            nlp_input_dataframe,
            sentiment_runner=FailingRunner(),
            model_bundle_config=model_bundle_config_factory(),
        )


def test_build_fact_mention_nlp_summary_rejects_missing_input_columns(
    model_bundle_config_factory,
):
    """Error path: Phase 2 must fail fast on a broken Phase 0 contract."""
    nlp_input_dataframe = _eligible_nlp_input_dataframe().drop(columns=["input_hash"])

    with pytest.raises(DataQualityError, match="missing required columns"):
        build_fact_mention_nlp_summary(
            nlp_input_dataframe,
            sentiment_runner=FailingRunner(),
            model_bundle_config=model_bundle_config_factory(),
        )


def test_build_fact_mention_nlp_summary_rejects_prediction_count_mismatch(
    model_bundle_config_factory,
):
    """Regression: batch scoring must preserve input row count."""
    with pytest.raises(DataQualityError, match="returned 0 predictions"):
        build_fact_mention_nlp_summary(
            _eligible_nlp_input_dataframe(),
            sentiment_runner=EmptyPredictionRunner(),
            model_bundle_config=model_bundle_config_factory(),
        )


def test_build_fact_mention_nlp_summary_preserves_input_order_across_batches(
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Regression: batch scoring must preserve mention order."""
    input_rows = [
        _eligible_nlp_input_dataframe(
            mention_id=f"mention-00{index}",
            input_text=f"Texte politique numero {index}",
            input_hash=f"hash-00{index}",
        )
        for index in range(1, 4)
    ]
    nlp_input_dataframe = pd.concat(input_rows, ignore_index=True)
    runner = sentiment_runner_factory(
        {
            f"Texte politique numero {index}": sentiment_prediction_factory()
            for index in range(1, 4)
        }
    )

    summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(batch_size=2),
        scored_at="2026-04-02T10:00:00Z",
    )

    assert summary_dataframe["mention_id"].tolist() == [
        "mention-001",
        "mention-002",
        "mention-003",
    ]
    assert runner.calls == [
        ["Texte politique numero 1", "Texte politique numero 2"],
        ["Texte politique numero 3"],
    ]


def test_huggingface_runner_raises_only_when_transformer_scoring_is_requested(
    monkeypatch,
    model_bundle_config_factory,
):
    """Regression: optional Transformer dependency is lazy and actionable."""
    monkeypatch.setitem(sys.modules, "transformers", None)
    runner = HuggingFaceSentimentRunner(model_bundle_config_factory())

    with pytest.raises(TransformerDependencyError, match="requirements-future.in"):
        runner.predict_batch(["Texte politique local."])


def test_huggingface_runner_length_audit_suppresses_tokenizer_warning(
    model_bundle_config_factory,
):
    """Regression: truncation audits should not emit model-overflow warnings."""
    tokenizer = RecordingTokenizer(token_count=514)
    runner = HuggingFaceSentimentRunner(
        model_bundle_config_factory(max_token_length=512)
    )
    runner._analyzer = AnalyzerWithRecordingTokenizer(tokenizer)

    assert runner._was_text_truncated("Texte long") is True
    assert tokenizer.calls == [
        {
            "text": "Texte long",
            "add_special_tokens": True,
            "truncation": False,
            "verbose": False,
        }
    ]


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda dataframe: pd.concat([dataframe, dataframe], ignore_index=True),
            "duplicate key rows",
        ),
        (
            lambda dataframe: dataframe.assign(nlp_model_bundle_version=" "),
            "nlp_model_bundle_version",
        ),
        (
            lambda dataframe: dataframe.assign(generic_sentiment_score=1.5),
            "between -1 and 1",
        ),
        (
            lambda dataframe: dataframe.assign(nlp_enrichment_status="unknown"),
            "unsupported statuses",
        ),
        (
            lambda dataframe: dataframe.assign(
                nlp_enrichment_status="failed",
                generic_sentiment_label=None,
                generic_sentiment_score=None,
                error_type=None,
            ),
            "failed rows need error_type",
        ),
        (
            lambda dataframe: dataframe.assign(target_tone_label="positive"),
            "unsupported tone labels",
        ),
    ],
)
def test_validate_fact_mention_nlp_summary_rejects_contract_violations(
    mutator,
    message,
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Error path: output DQ gates fail independently."""
    runner = sentiment_runner_factory(
        {
            "Alice Martin presente son programme municipal aux habitants.": (
                sentiment_prediction_factory()
            )
        }
    )
    nlp_input_dataframe = _eligible_nlp_input_dataframe()
    summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(),
        scored_at="2026-04-02T10:00:00Z",
    )

    with pytest.raises(DataQualityError, match=message):
        validate_fact_mention_nlp_summary(
            mutator(summary_dataframe), nlp_input_dataframe
        )


def test_validate_fact_mention_nlp_summary_rejects_orphan_output_rows(
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Error path: summary rows must match the current NLP input table."""
    runner = sentiment_runner_factory(
        {
            "Alice Martin presente son programme municipal aux habitants.": (
                sentiment_prediction_factory()
            )
        }
    )
    nlp_input_dataframe = _eligible_nlp_input_dataframe()
    summary_dataframe = build_fact_mention_nlp_summary(
        nlp_input_dataframe,
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(),
        scored_at="2026-04-02T10:00:00Z",
    )

    summary_dataframe.loc[0, "mention_id"] = "orphan-mention"

    with pytest.raises(DataQualityError, match="without matching NLP input"):
        validate_fact_mention_nlp_summary(summary_dataframe, nlp_input_dataframe)


def test_materialize_fact_mention_nlp_summary_writes_parquet_and_duckdb(
    tmp_path,
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Integration: Phase 2 can materialize its Silver artifact and table."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    runner = sentiment_runner_factory(
        {
            "Alice Martin presente son programme municipal aux habitants.": (
                sentiment_prediction_factory()
            )
        }
    )

    materialized_dataframe = materialize_fact_mention_nlp_summary(
        _eligible_nlp_input_dataframe(),
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        scored_at="2026-04-02T10:00:00Z",
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


def test_materialize_fact_mention_nlp_summary_is_idempotent(
    tmp_path,
    model_bundle_config_factory,
    sentiment_prediction_factory,
    sentiment_runner_factory,
):
    """Regression: repeated materialization replaces rows instead of appending."""
    duckdb = pytest.importorskip("duckdb")
    silver_dir = tmp_path / "silver"
    duckdb_path = tmp_path / "warehouse.duckdb"
    runner = sentiment_runner_factory(
        {
            "Alice Martin presente son programme municipal aux habitants.": (
                sentiment_prediction_factory()
            )
        }
    )

    materialize_fact_mention_nlp_summary(
        _eligible_nlp_input_dataframe(),
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        scored_at="2026-04-02T10:00:00Z",
    )
    materialized_dataframe = materialize_fact_mention_nlp_summary(
        _eligible_nlp_input_dataframe(),
        sentiment_runner=runner,
        model_bundle_config=model_bundle_config_factory(),
        silver_dir=silver_dir,
        duckdb_path=duckdb_path,
        scored_at="2026-04-02T10:00:00Z",
    )

    conn = duckdb.connect(str(duckdb_path))
    try:
        table_count = conn.execute(
            "SELECT COUNT(*) FROM silver.fact_mention_nlp_summary"
        ).fetchone()[0]
    finally:
        conn.close()
    assert table_count == len(materialized_dataframe) == 1
