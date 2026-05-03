"""Console script entrypoint for the Phase 0 NLP input pipeline."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from src.config.settings import SILVER_DIR
from src.orchestration.nlp_input_pipeline import run_nlp_input_pipeline

logger = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments for Phase 0 NLP input materialization."""
    parser = argparse.ArgumentParser(
        description="Materialize silver.fact_mention_nlp_input from Silver facts.",
    )
    parser.add_argument(
        "--fact-mention-path",
        type=Path,
        default=SILVER_DIR / "fact_mention.parquet",
        metavar="PATH",
        help="Path to silver fact_mention Parquet.",
    )
    parser.add_argument(
        "--fact-article-path",
        type=Path,
        default=SILVER_DIR / "fact_article.parquet",
        metavar="PATH",
        help="Path to silver fact_article Parquet.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the NLP input pipeline and return a process exit code."""
    args = _parse_args(argv)
    try:
        result = run_nlp_input_pipeline(
            fact_mention_path=args.fact_mention_path,
            fact_article_path=args.fact_article_path,
        )
    except Exception:
        logger.exception("NLP input pipeline exited with failure")
        return 1

    logger.info(
        "NLP input pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
