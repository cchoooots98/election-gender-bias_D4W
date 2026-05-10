"""Shared pytest fixtures for the election-gender-bias test suite.

Fixtures defined here are available to all test files without explicit import —
pytest discovers conftest.py automatically and injects fixtures by parameter name.

Design principles:
- Tests must be hermetic: no network calls, no disk I/O, no GPU.
- All external dependencies (DuckDB files, HTTP APIs, HuggingFace models)
  are replaced with in-memory or mocked equivalents.
- Fixtures use yield so teardown (conn.close()) runs even if the test fails.

Portfolio note: a well-designed conftest.py is a signal of test engineering
maturity. Hiring managers reading this file will see that tests are isolated,
reproducible, and don't rely on external state.
"""

import pandas as pd
import pytest

from src.nlp.model_bundle import ModelBundleConfig
from src.nlp.nli import FramePrediction, TonePrediction
from src.nlp.sentiment import SentimentPrediction

try:
    import duckdb
except ImportError:  # pragma: no cover - depends on local test environment
    duckdb = None


class ConfigurableSentimentRunner:
    """Mock scorer returning configured predictions in input order."""

    def __init__(
        self,
        predictions_by_text: dict[str, SentimentPrediction],
    ) -> None:
        self.predictions_by_text = predictions_by_text
        self.calls: list[list[str]] = []

    def predict_batch(self, texts):
        """Return configured predictions for the requested texts."""
        self.calls.append(list(texts))
        return [self.predictions_by_text[text] for text in texts]


class ConfigurableToneRunner:
    """Mock NLI scorer returning configured predictions in input order."""

    def __init__(self, predictions_by_mention_id: dict[str, TonePrediction]) -> None:
        self.predictions_by_mention_id = predictions_by_mention_id
        self.calls: list[list[str]] = []

    def predict_batch(self, scoring_inputs):
        """Return configured predictions for the requested mention IDs."""
        self.calls.append(
            [scoring_input.mention_id for scoring_input in scoring_inputs]
        )
        return [
            self.predictions_by_mention_id[scoring_input.mention_id]
            for scoring_input in scoring_inputs
        ]


class ConfigurableFrameRunner:
    """Mock NLI frame scorer returning configured predictions in input order."""

    def __init__(self, predictions_by_mention_id: dict[str, FramePrediction]) -> None:
        self.predictions_by_mention_id = predictions_by_mention_id
        self.calls: list[list[str]] = []

    def predict_batch(self, scoring_inputs):
        """Return configured predictions for the requested mention IDs."""
        self.calls.append(
            [scoring_input.mention_id for scoring_input in scoring_inputs]
        )
        return [
            self.predictions_by_mention_id[scoring_input.mention_id]
            for scoring_input in scoring_inputs
        ]


@pytest.fixture
def model_bundle_config_factory():
    """Return a factory for valid model-bundle test configurations."""

    def _build_model_bundle_config(**overrides) -> ModelBundleConfig:
        values = {
            "sentiment_model_name": "cmarkea/distilcamembert-base-sentiment",
            "sentiment_model_revision": "a" * 40,
            "nli_model_name": "cmarkea/distilcamembert-base-nli",
            "nli_model_revision": "b" * 40,
            "nli_backup_model_name": "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
            "nli_backup_model_revision": "c" * 40,
            "hypothesis_template_version": "candidate_tone_frame_v2",
            "tone_threshold": 0.6,
            "frame_threshold": 0.6,
            "max_token_length": 512,
            "batch_size": 32,
            "device": "cpu",
        }
        values.update(overrides)
        return ModelBundleConfig(**values)

    return _build_model_bundle_config


@pytest.fixture
def sentiment_prediction_factory():
    """Return a factory for valid mock sentiment predictions."""

    def _build_sentiment_prediction(
        *,
        label: str = "5 stars",
        was_truncated: bool = False,
        probabilities: dict[str, float] | None = None,
    ) -> SentimentPrediction:
        return SentimentPrediction(
            label=label,
            probabilities_by_label=probabilities
            or {
                "1 star": 0.10,
                "2 stars": 0.10,
                "3 stars": 0.20,
                "4 stars": 0.30,
                "5 stars": 0.30,
            },
            was_truncated_to_max_length=was_truncated,
        )

    return _build_sentiment_prediction


@pytest.fixture
def sentiment_runner_factory():
    """Return a factory for configurable mock sentiment runners."""

    def _build_runner(
        predictions_by_text: dict[str, SentimentPrediction],
    ) -> ConfigurableSentimentRunner:
        return ConfigurableSentimentRunner(predictions_by_text)

    return _build_runner


@pytest.fixture
def read_pipeline_meta_run():
    """Return a helper for reading the latest meta_run row for one pipeline."""

    def _read_pipeline_meta_run(duckdb_path, flow_name: str):
        if duckdb is None:
            pytest.skip("duckdb is not installed in this test environment")

        conn = duckdb.connect(str(duckdb_path))
        try:
            return conn.execute(
                """
                SELECT status, rows_ingested, error_count
                FROM meta.meta_run
                WHERE flow_name = ?
                ORDER BY end_ts DESC
                LIMIT 1
                """,
                [flow_name],
            ).fetchone()
        finally:
            conn.close()

    return _read_pipeline_meta_run


@pytest.fixture
def duckdb_conn():
    """In-memory DuckDB connection — resets between tests.

    Why in-memory: each test gets a completely clean database state.
    No leftover rows from a previous test can cause false positives or
    false negatives. This is the DuckDB equivalent of wrapping a SQL
    test in BEGIN / ROLLBACK.

    Yields:
        An open DuckDB connection pointed at ':memory:'.
    """
    if duckdb is None:
        pytest.skip("duckdb is not installed in this test environment")

    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def sample_candidate_df() -> pd.DataFrame:
    """Minimal candidate DataFrame matching the dim_candidate_leader schema.

    Contains exactly two rows — one male, one female — the minimum required
    for any gender-comparison test. Enough to verify logic without inflating
    test setup cost.

    Returns:
        DataFrame with columns matching silver.dim_candidate_leader.
    """
    return pd.DataFrame(
        {
            "leader_id": ["abc123def456abc1", "def456abc123def4"],
            "full_name": ["Jean Dupont", "Marie Durand"],
            "gender": ["M", "F"],
            "commune_insee": ["75056", "69123"],
            "same_name_candidate_count": [1, 1],
            "list_nuance": ["DVC", "DVG"],
            "nuance_group": ["divers", "gauche"],
            "is_incumbent": [True, False],
            "incumbent_match_score": [0.93, 0.0],
            "incumbent_match_auditable": [True, False],
            "advanced_to_tour2": pd.Series([True, False], dtype="boolean"),
        }
    )


@pytest.fixture
def sample_article_df() -> pd.DataFrame:
    """Minimal article DataFrame matching the fact_article schema.

    Three articles: two mention the male candidate, one the female candidate.
    This intentional 2:1 asymmetry lets exposure-metric tests assert that
    gender differences are correctly detected (not just that counting works).

    Returns:
        DataFrame with columns matching silver.fact_article.
    """
    return pd.DataFrame(
        {
            "article_id": ["a1", "a2", "a3"],
            "url": [
                "https://example.com/article-1",
                "https://example.com/article-2",
                "https://example.com/article-3",
            ],
            "title": [
                "Jean Dupont en tête dans Paris",
                "Interview exclusive de Jean Dupont",
                "Marie Durand présente son programme économique",
            ],
            "body_text": [
                "Le candidat Jean Dupont mène la campagne à Paris avec un programme ambitieux.",
                "Jean Dupont répond aux questions des journalistes sur sa vision de la ville.",
                "Marie Durand a détaillé ses propositions pour l'économie locale de Lyon.",
            ],
            "published_at": pd.to_datetime(
                ["2026-03-01", "2026-03-05", "2026-03-03"]
            ).tz_localize("UTC"),
            "domain": ["lefigaro.fr", "lemonde.fr", "liberation.fr"],
            # Hive-style partition column — matches bronze/ directory partitioning.
            "partition_date": ["2026-03-01", "2026-03-05", "2026-03-03"],
        }
    )
