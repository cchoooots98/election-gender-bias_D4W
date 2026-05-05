"""Console script entrypoint for the Phase 1 NLP lexicon pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import SILVER_DIR
from src.orchestration.nlp_lexicon_pipeline import run_nlp_lexicon_pipeline

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Phase 1 lexicon materialization."""
    parser = argparse.ArgumentParser(
        description=(
            "Materialize silver.fact_stereotype_word_counts from NLP input rows."
        ),
    )
    parser.add_argument(
        "--nlp-input-path",
        type=Path,
        default=SILVER_DIR / "fact_mention_nlp_input.parquet",
        metavar="PATH",
        help="Path to silver fact_mention_nlp_input Parquet.",
    )
    parser.add_argument(
        "--lexicon-path",
        type=Path,
        default=None,
        metavar="PATH",
        help="Optional custom stereotype lexicon JSON path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the NLP lexicon pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_lexicon_pipeline(
            nlp_input_path=args.nlp_input_path,
            lexicon_path=args.lexicon_path,
        )
    except Exception:
        logger.exception("NLP lexicon pipeline exited with failure")
        return 1

    logger.info(
        "NLP lexicon pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
