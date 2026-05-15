"""Console script entrypoint for the Phase 5 NLP QA report."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import GOLD_DIR, SILVER_DIR
from src.nlp.qa import DEFAULT_NLP_QA_THRESHOLDS
from src.orchestration.nlp_qa_pipeline import run_nlp_qa_pipeline

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Phase 5 NLP QA reporting."""
    parser = argparse.ArgumentParser(
        description="Build the governed Phase 5 NLP QA report.",
    )
    parser.add_argument(
        "--nlp-input-path",
        type=Path,
        default=SILVER_DIR / "fact_mention_nlp_input.parquet",
        metavar="PATH",
        help="Path to silver fact_mention_nlp_input Parquet.",
    )
    parser.add_argument(
        "--nlp-summary-path",
        type=Path,
        default=SILVER_DIR / "fact_mention_nlp_summary.parquet",
        metavar="PATH",
        help="Path to silver fact_mention_nlp_summary Parquet.",
    )
    parser.add_argument(
        "--frame-score-path",
        type=Path,
        default=SILVER_DIR / "fact_mention_frame_score.parquet",
        metavar="PATH",
        help="Path to silver fact_mention_frame_score Parquet.",
    )
    parser.add_argument(
        "--stereotype-word-counts-path",
        type=Path,
        default=SILVER_DIR / "fact_stereotype_word_counts.parquet",
        metavar="PATH",
        help="Path to silver fact_stereotype_word_counts Parquet.",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=GOLD_DIR / "nlp_qa_report.json",
        metavar="PATH",
        help="Output path for the unified NLP QA report JSON.",
    )
    parser.add_argument(
        "--backup-summary-path",
        type=Path,
        default=None,
        metavar="PATH",
        help="Optional precomputed backup-model summary Parquet.",
    )
    parser.add_argument(
        "--thresholds",
        type=float,
        nargs="+",
        default=list(DEFAULT_NLP_QA_THRESHOLDS),
        metavar="P",
        help="Probability thresholds to audit for tone and framing coverage.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the NLP QA pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_qa_pipeline(
            nlp_input_path=args.nlp_input_path,
            nlp_summary_path=args.nlp_summary_path,
            frame_score_path=args.frame_score_path,
            stereotype_word_counts_path=args.stereotype_word_counts_path,
            report_path=args.report_path,
            backup_summary_path=args.backup_summary_path,
            thresholds=args.thresholds,
        )
    except Exception:
        logger.exception("NLP QA pipeline exited with failure")
        return 1

    logger.info(
        "NLP QA pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
