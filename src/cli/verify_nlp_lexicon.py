"""Verify persisted Phase 1 NLP lexicon artifacts."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.config.settings import WAREHOUSE_PATH

logger = logging.getLogger(__name__)

_REQUIRED_TABLES: tuple[tuple[str, str], ...] = (
    ("silver", "fact_stereotype_word_counts"),
    ("silver", "fact_trait_word_counts"),
    ("gold", "mart_trait_metrics"),
    ("gold", "mart_trait_top_terms"),
    ("gold", "mart_trait_candidate_metrics"),
    ("gold", "mart_trait_qa_samples"),
)


@dataclass(frozen=True)
class NlpLexiconVerificationSummary:
    """Row-count summary for persisted NLP lexicon artifacts.

    Args:
        stereotype_rows: Rows in ``silver.fact_stereotype_word_counts``.
        stereotype_mentions: Distinct mentions with stereotype seed terms.
        stereotype_categories: Distinct stereotype categories matched.
        stereotype_total_terms: Sum of stereotype term counts.
        trait_rows: Rows in ``silver.fact_trait_word_counts``.
        trait_mentions: Distinct mentions with trait terms.
        trait_categories: Distinct trait categories matched.
        trait_total_terms: Sum of trait term counts.
        trait_metric_rows: Rows in ``gold.mart_trait_metrics``.
        scenario_count: Distinct trait outlier scenarios.
        tier_count: Distinct trait tiers.
        qa_sample_rows: Rows in ``gold.mart_trait_qa_samples``.
    """

    stereotype_rows: int
    stereotype_mentions: int
    stereotype_categories: int
    stereotype_total_terms: int
    trait_rows: int
    trait_mentions: int
    trait_categories: int
    trait_total_terms: int
    trait_metric_rows: int
    scenario_count: int
    tier_count: int
    qa_sample_rows: int


def verify_nlp_lexicon_artifacts(
    duckdb_path: Path = WAREHOUSE_PATH,
) -> NlpLexiconVerificationSummary:
    """Verify that Phase 1 NLP lexicon artifacts exist and are queryable.

    Args:
        duckdb_path: Path to the DuckDB warehouse file.

    Returns:
        Summary of persisted Silver and Gold lexicon artifact row counts.

    Raises:
        FileNotFoundError: If the DuckDB warehouse file does not exist.
        RuntimeError: If DuckDB is unavailable, a required table is missing, or
            a required artifact query fails.
    """
    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB warehouse does not exist: {duckdb_path}")

    duckdb = _import_duckdb()
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        try:
            _require_tables(conn)
            stereotype_summary = conn.execute(
                """
                select
                    count(*) as rows,
                    count(distinct mention_id) as mentions_with_terms,
                    count(distinct lexicon_category) as categories_with_terms,
                    coalesce(sum(count), 0) as total_term_count
                from silver.fact_stereotype_word_counts
                """
            ).fetchone()
            trait_summary = conn.execute(
                """
                select
                    count(*) as rows,
                    count(distinct mention_id) as mentions_with_terms,
                    count(distinct trait_category) as categories_with_terms,
                    coalesce(sum(count), 0) as total_term_count
                from silver.fact_trait_word_counts
                """
            ).fetchone()
            trait_metric_summary = conn.execute(
                """
                select
                    count(*) as metric_rows,
                    count(distinct scenario_id) as scenarios,
                    count(distinct trait_tier) as tiers
                from gold.mart_trait_metrics
                """
            ).fetchone()
            qa_sample_rows = conn.execute(
                """
                select count(*) as qa_sample_rows
                from gold.mart_trait_qa_samples
                """
            ).fetchone()
        except duckdb.Error as exc:
            raise RuntimeError("Failed to query NLP lexicon artifacts") from exc
    finally:
        conn.close()

    return NlpLexiconVerificationSummary(
        stereotype_rows=_as_int(stereotype_summary[0]),
        stereotype_mentions=_as_int(stereotype_summary[1]),
        stereotype_categories=_as_int(stereotype_summary[2]),
        stereotype_total_terms=_as_int(stereotype_summary[3]),
        trait_rows=_as_int(trait_summary[0]),
        trait_mentions=_as_int(trait_summary[1]),
        trait_categories=_as_int(trait_summary[2]),
        trait_total_terms=_as_int(trait_summary[3]),
        trait_metric_rows=_as_int(trait_metric_summary[0]),
        scenario_count=_as_int(trait_metric_summary[1]),
        tier_count=_as_int(trait_metric_summary[2]),
        qa_sample_rows=_as_int(qa_sample_rows[0]),
    )


def main(argv: list[str] | None = None) -> int:
    """Run the verification CLI and return a process exit code."""
    args = _parse_args(argv)
    try:
        summary = verify_nlp_lexicon_artifacts(args.duckdb_path)
    except (FileNotFoundError, RuntimeError, ValueError):
        logger.exception("NLP lexicon verification failed")
        return 1

    logger.info(
        "NLP lexicon verification passed stereotype_rows=%d "
        "stereotype_mentions=%d stereotype_categories=%d "
        "stereotype_total_terms=%d trait_rows=%d trait_mentions=%d "
        "trait_categories=%d trait_total_terms=%d trait_metric_rows=%d "
        "scenarios=%d tiers=%d qa_sample_rows=%d",
        summary.stereotype_rows,
        summary.stereotype_mentions,
        summary.stereotype_categories,
        summary.stereotype_total_terms,
        summary.trait_rows,
        summary.trait_mentions,
        summary.trait_categories,
        summary.trait_total_terms,
        summary.trait_metric_rows,
        summary.scenario_count,
        summary.tier_count,
        summary.qa_sample_rows,
    )
    return 0


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for NLP lexicon verification."""
    parser = argparse.ArgumentParser(
        description="Verify persisted Phase 1 NLP lexicon artifacts.",
    )
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=WAREHOUSE_PATH,
        metavar="PATH",
        help="Path to the DuckDB warehouse.",
    )
    return parser.parse_args(argv)


def _import_duckdb() -> Any:
    """Import DuckDB lazily so argument parsing remains lightweight."""
    try:
        import duckdb
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise RuntimeError(
            "duckdb is required to verify NLP lexicon artifacts"
        ) from exc
    return duckdb


def _require_tables(conn: Any) -> None:
    """Fail fast when any required lexicon artifact table is missing."""
    missing_tables = []
    for schema_name, table_name in _REQUIRED_TABLES:
        table_exists = conn.execute(
            """
            select count(*) > 0
            from information_schema.tables
            where table_schema = ?
                and table_name = ?
            """,
            [schema_name, table_name],
        ).fetchone()[0]
        if not table_exists:
            missing_tables.append(f"{schema_name}.{table_name}")
    if missing_tables:
        raise RuntimeError(
            "Missing NLP lexicon artifact tables: " + ", ".join(missing_tables)
        )


def _as_int(value: object) -> int:
    """Convert aggregate query values into plain integers."""
    if value is None:
        return 0
    return int(value)


if __name__ == "__main__":
    raise SystemExit(main())
