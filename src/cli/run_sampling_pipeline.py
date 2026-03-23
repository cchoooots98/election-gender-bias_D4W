"""Console script entrypoint for the runnable sampling pipeline."""

from __future__ import annotations

import logging

from src.orchestration.sampling_pipeline import run_sampling_pipeline

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the sampling pipeline and return a process exit code."""
    try:
        result = run_sampling_pipeline()
    except Exception:
        logger.exception("Sampling pipeline exited with failure")
        return 1

    logger.info(
        "Sampling pipeline finished status=%s run_id=%s artifacts=%d",
        result.status,
        result.run_id,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
