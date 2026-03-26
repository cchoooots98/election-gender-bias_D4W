"""Console script entrypoint for the hybrid news benchmark runner."""

from __future__ import annotations

import logging

from src.orchestration.news_benchmark_pipeline import run_news_benchmark_pipeline

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the news benchmark pipeline and return a process exit code."""
    try:
        result = run_news_benchmark_pipeline()
    except Exception:
        logger.exception("News benchmark pipeline exited with failure")
        return 1

    logger.info(
        "News benchmark pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
