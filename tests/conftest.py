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

try:
    import duckdb
except ImportError:  # pragma: no cover - depends on local test environment
    duckdb = None


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
            "commune_name": ["Paris", "Lyon"],
            "list_name": ["Liste Avenir Paris", "Liste Demain Lyon"],
            "is_incumbent": [True, False],
            # Timezone-aware timestamp: required by the silver schema.
            # Using a fixed date avoids test failures caused by time-of-day.
            "_ingested_at": pd.Timestamp("2026-02-15T00:00:00", tz="UTC"),
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
