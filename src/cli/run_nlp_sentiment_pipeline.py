"""Console script entrypoint for the Phase 2 NLP sentiment pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import SILVER_DIR
from src.orchestration.nlp_sentiment_pipeline import run_nlp_sentiment_pipeline

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Phase 2 sentiment materialization."""
    parser = argparse.ArgumentParser(
        description="Materialize silver.fact_mention_nlp_summary from NLP input rows.",
    )
    parser.add_argument(
        "--nlp-input-path",
        type=Path,
        default=SILVER_DIR / "fact_mention_nlp_input.parquet",
        metavar="PATH",
        help="Path to silver fact_mention_nlp_input Parquet.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the NLP sentiment pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_sentiment_pipeline(nlp_input_path=args.nlp_input_path)
    except Exception:
        logger.exception("NLP sentiment pipeline exited with failure")
        return 1

    logger.info(
        "NLP sentiment pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
