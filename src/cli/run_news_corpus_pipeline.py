"""Console script entrypoint for the enterprise news corpus pipeline."""

from __future__ import annotations

import logging

from src.orchestration.news_corpus_pipeline import run_news_corpus_pipeline

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the news corpus pipeline and return a process exit code."""
    try:
        result = run_news_corpus_pipeline()
    except Exception:
        logger.exception("News corpus pipeline exited with failure")
        return 1

    logger.info(
        "News corpus pipeline finished status=%s batch_id=%s artifacts=%d",
        result.status,
        result.batch_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
