"""Console script entrypoint for the Phase 3 NLP tone pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import GOLD_DIR, SILVER_DIR
from src.orchestration.nlp_tone_pipeline import run_nlp_tone_pipeline

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Phase 3 tone enrichment."""
    parser = argparse.ArgumentParser(
        description="Enrich silver.fact_mention_nlp_summary with target-aware tone.",
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
        help="Path to existing silver fact_mention_nlp_summary Parquet.",
    )
    parser.add_argument(
        "--sample-leaders-path",
        type=Path,
        default=GOLD_DIR / "sample_leaders.parquet",
        metavar="PATH",
        help="Path to gold sample_leaders Parquet.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the NLP tone pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_tone_pipeline(
            nlp_input_path=args.nlp_input_path,
            nlp_summary_path=args.nlp_summary_path,
            sample_leaders_path=args.sample_leaders_path,
        )
    except Exception:
        logger.exception("NLP tone pipeline exited with failure")
        return 1

    logger.info(
        "NLP tone pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
