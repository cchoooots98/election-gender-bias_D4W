"""Console script entrypoint for the low-frequency GDELT backfill runner."""

from __future__ import annotations

import logging

from src.orchestration.gdelt_backfill_pipeline import run_gdelt_backfill_pipeline

logger = logging.getLogger(__name__)


def main() -> int:
    """Run the GDELT backfill pipeline and return a process exit code."""
    try:
        result = run_gdelt_backfill_pipeline()
    except Exception:
        logger.exception("GDELT backfill pipeline exited with failure")
        return 1

    logger.info(
        "GDELT backfill finished status=%s run_id=%s queries=%d hits=%d artifacts=%d",
        result.status,
        result.run_id,
        result.query_count,
        result.hit_count,
        len(result.artifact_paths),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
