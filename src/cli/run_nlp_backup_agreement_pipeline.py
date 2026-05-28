"""Console script entrypoint for the backup NLI agreement sample."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import GOLD_DIR, SILVER_DIR
from src.nlp.backup_agreement import (
    DEFAULT_BACKUP_RANDOM_SEED,
    DEFAULT_BACKUP_SAMPLE_SIZE,
)
from src.orchestration.nlp_backup_agreement_pipeline import (
    run_nlp_backup_agreement_pipeline,
)

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for backup-model agreement sampling."""
    parser = argparse.ArgumentParser(
        description="Build the governed backup NLI agreement sample.",
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
        help="Path to primary silver fact_mention_nlp_summary Parquet.",
    )
    parser.add_argument(
        "--sample-leaders-path",
        type=Path,
        default=GOLD_DIR / "sample_leaders.parquet",
        metavar="PATH",
        help="Path to gold sample_leaders Parquet.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=GOLD_DIR / "nlp_backup_summary_sample.parquet",
        metavar="PATH",
        help="Output path for backup summary sample Parquet.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_BACKUP_SAMPLE_SIZE,
        help="Maximum scoreable mentions to run through the backup model.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=DEFAULT_BACKUP_RANDOM_SEED,
        help="Deterministic sampling seed.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the backup agreement pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_backup_agreement_pipeline(
            nlp_input_path=args.nlp_input_path,
            nlp_summary_path=args.nlp_summary_path,
            sample_leaders_path=args.sample_leaders_path,
            output_path=args.output_path,
            sample_size=args.sample_size,
            random_seed=args.random_seed,
        )
    except Exception:
        logger.exception("NLP backup agreement pipeline exited with failure")
        return 1

    logger.info(
        "NLP backup agreement pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
