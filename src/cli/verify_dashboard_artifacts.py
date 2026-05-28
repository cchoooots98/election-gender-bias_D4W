"""Verify dashboard readiness artifacts."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.config.settings import GOLD_DIR

logger = logging.getLogger(__name__)

REQUIRED_DASHBOARD_ARTIFACTS: tuple[str, ...] = (
    "sample_leaders.parquet",
    "mart_exposure_metrics.parquet",
    "mart_regression_results.parquet",
    "mart_bootstrap_ci.parquet",
    "mart_analysis_summary.parquet",
    "sample_manifest.json",
    "news_corpus_qa_report.json",
)


@dataclass(frozen=True)
class DashboardArtifactSummary:
    """Readiness summary for dashboard artifacts.

    Attributes:
        gold_dir: Artifact directory that was checked.
        artifact_count: Number of required artifacts found.
        sample_leader_count: Row count in ``sample_leaders.parquet``.
        warning_count: Number of non-fatal artifact health warnings.
        warnings: Non-fatal artifact health warning messages.
    """

    gold_dir: Path
    artifact_count: int
    sample_leader_count: int
    warning_count: int
    warnings: tuple[str, ...]


def verify_dashboard_artifacts(
    gold_dir: Path = GOLD_DIR,
    *,
    expected_sample_leaders: int = 36,
) -> DashboardArtifactSummary:
    """Verify that required dashboard artifacts exist and have expected shape.

    Args:
        gold_dir: Directory containing Gold dashboard artifacts.
        expected_sample_leaders: Expected row count for ``sample_leaders``.

    Returns:
        Summary of verified artifact counts.

    Raises:
        FileNotFoundError: If any required artifact is missing.
        RuntimeError: If an artifact cannot be read or violates row-count
            expectations.
    """
    missing_artifacts = [
        artifact_name
        for artifact_name in REQUIRED_DASHBOARD_ARTIFACTS
        if not (gold_dir / artifact_name).exists()
    ]
    if missing_artifacts:
        raise FileNotFoundError(
            "Missing dashboard artifacts: " + ", ".join(missing_artifacts)
        )

    sample_leaders_path = gold_dir / "sample_leaders.parquet"
    try:
        sample_leaders = pd.read_parquet(sample_leaders_path)
        json_reports = {
            json_artifact: json.loads(
                (gold_dir / json_artifact).read_text(encoding="utf-8")
            )
            for json_artifact in ["sample_manifest.json", "news_corpus_qa_report.json"]
        }
    except (OSError, ValueError, ImportError) as exc:
        raise RuntimeError("Failed to read dashboard artifacts") from exc

    sample_leader_count = int(len(sample_leaders))
    if sample_leader_count != expected_sample_leaders:
        raise RuntimeError(
            "sample_leaders row-count mismatch: "
            f"expected {expected_sample_leaders}, observed {sample_leader_count}"
        )
    warnings = _build_artifact_health_warnings(gold_dir, json_reports)

    return DashboardArtifactSummary(
        gold_dir=gold_dir,
        artifact_count=len(REQUIRED_DASHBOARD_ARTIFACTS),
        sample_leader_count=sample_leader_count,
        warning_count=len(warnings),
        warnings=tuple(warnings),
    )


def _build_artifact_health_warnings(
    gold_dir: Path,
    json_reports: dict[str, dict[str, object]],
) -> list[str]:
    """Return non-fatal dashboard artifact health warnings."""
    warnings: list[str] = []
    exposure_path = gold_dir / "mart_exposure_metrics.parquet"
    regression_path = gold_dir / "mart_regression_results.parquet"
    bootstrap_path = gold_dir / "mart_bootstrap_ci.parquet"
    if (
        exposure_path.exists()
        and regression_path.exists()
        and regression_path.stat().st_mtime < exposure_path.stat().st_mtime
    ):
        raise RuntimeError(
            "mart_regression_results.parquet is older than "
            "mart_exposure_metrics.parquet; rerun the news corpus regression "
            "artifacts before publishing the dashboard."
        )
    if (
        exposure_path.exists()
        and bootstrap_path.exists()
        and bootstrap_path.stat().st_mtime < exposure_path.stat().st_mtime
    ):
        raise RuntimeError(
            "mart_bootstrap_ci.parquet is older than mart_exposure_metrics.parquet; "
            "rerun bootstrap artifacts before publishing the dashboard."
        )

    news_qa = json_reports.get("news_corpus_qa_report.json", {})
    qa_section = news_qa.get("qa", {})
    if isinstance(qa_section, dict):
        warnings.extend(str(warning) for warning in qa_section.get("warnings", []))

    nlp_qa_path = gold_dir / "nlp_qa_report.json"
    if nlp_qa_path.exists():
        nlp_qa_report = json.loads(nlp_qa_path.read_text(encoding="utf-8"))
        corpus_mention_count = _coerce_optional_int(
            qa_section.get("mention_count") if isinstance(qa_section, dict) else None
        )
        nlp_input_total = _coerce_optional_int(
            nlp_qa_report.get("input_coverage", {}).get("total_mentions")
        )
        source_tables = nlp_qa_report.get("source_tables", {})
        if isinstance(source_tables, dict):
            nlp_input_source = source_tables.get("silver.fact_mention_nlp_input", {})
            if isinstance(nlp_input_source, dict):
                nlp_input_total = nlp_input_total or _coerce_optional_int(
                    nlp_input_source.get("rows")
                )
        if (
            corpus_mention_count is not None
            and nlp_input_total is not None
            and corpus_mention_count != nlp_input_total
        ):
            raise RuntimeError(
                "nlp_qa_report.json NLP input row count does not match "
                "news_corpus_qa_report.json mention_count; rerun NLP input and "
                "downstream NLP artifacts before publishing the dashboard."
            )
        model_bundle = nlp_qa_report.get("model_bundle", {})
        if (
            isinstance(model_bundle, dict)
            and model_bundle.get("matches_current_config") is False
        ):
            raise RuntimeError(
                "nlp_qa_report.json model bundle does not match current config"
            )
        blessed_comparison = nlp_qa_report.get("blessed_bundle_comparison", {})
        if (
            isinstance(blessed_comparison, dict)
            and blessed_comparison.get("status") == "differs"
        ):
            raise RuntimeError("nlp_qa_report.json differs from blessed bundle")
    return warnings


def _coerce_optional_int(value: object) -> int | None:
    """Return an integer count when a report field is present and numeric."""
    if value is None or isinstance(value, dict | list | tuple | set):
        return None
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def main(argv: list[str] | None = None) -> int:
    """Run the dashboard artifact readiness check."""
    parser = argparse.ArgumentParser(
        description="Verify required Streamlit dashboard artifacts.",
    )
    parser.add_argument(
        "--gold-dir",
        type=Path,
        default=GOLD_DIR,
        help="Directory containing dashboard Gold artifacts.",
    )
    parser.add_argument(
        "--expected-sample-leaders",
        type=int,
        default=36,
        help="Expected sample_leaders row count.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        summary = verify_dashboard_artifacts(
            args.gold_dir,
            expected_sample_leaders=args.expected_sample_leaders,
        )
    except (FileNotFoundError, RuntimeError) as exc:
        logger.error("Dashboard artifact verification failed: %s", exc)
        return 1

    logger.info(
        "Dashboard artifact verification passed gold_dir=%s artifacts=%d "
        "sample_leaders=%d warnings=%d",
        summary.gold_dir,
        summary.artifact_count,
        summary.sample_leader_count,
        summary.warning_count,
    )
    for warning in summary.warnings:
        logger.warning("Dashboard artifact health warning: %s", warning)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
