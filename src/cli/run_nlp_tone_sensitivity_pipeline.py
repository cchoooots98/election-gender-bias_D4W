"""Console script entrypoint for tone threshold sensitivity analysis."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import GOLD_DIR, SILVER_DIR
from src.nlp.tone_sensitivity import DEFAULT_TONE_SENSITIVITY_THRESHOLDS
from src.orchestration.nlp_tone_sensitivity_pipeline import (
    run_nlp_tone_sensitivity_pipeline,
)

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for tone sensitivity analysis."""
    parser = argparse.ArgumentParser(
        description="Audit target-aware tone coverage across probability thresholds.",
    )
    parser.add_argument(
        "--nlp-summary-path",
        type=Path,
        default=SILVER_DIR / "fact_mention_nlp_summary.parquet",
        metavar="PATH",
        help="Path to tone-enriched silver fact_mention_nlp_summary Parquet.",
    )
    parser.add_argument(
        "--sample-leaders-path",
        type=Path,
        default=GOLD_DIR / "sample_leaders.parquet",
        metavar="PATH",
        help="Path to gold sample_leaders Parquet.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=GOLD_DIR / "nlp_tone_sensitivity_report.json",
        metavar="PATH",
        help="Output path for the JSON QA report.",
    )
    parser.add_argument(
        "--parquet-path",
        type=Path,
        default=GOLD_DIR / "nlp_tone_threshold_sensitivity.parquet",
        metavar="PATH",
        help="Output path for the long-form threshold table.",
    )
    parser.add_argument(
        "--thresholds",
        type=float,
        nargs="+",
        default=list(DEFAULT_TONE_SENSITIVITY_THRESHOLDS),
        metavar="P",
        help="Probability thresholds to audit.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the tone sensitivity pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_tone_sensitivity_pipeline(
            nlp_summary_path=args.nlp_summary_path,
            sample_leaders_path=args.sample_leaders_path,
            report_path=args.report_path,
            parquet_path=args.parquet_path,
            thresholds=args.thresholds,
        )
    except Exception:
        logger.exception("NLP tone sensitivity pipeline exited with failure")
        return 1

    logger.info(
        "NLP tone sensitivity pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
