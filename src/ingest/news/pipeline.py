"""Multi-source discovery pipeline for the sampled cohort."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

from src.config.settings import (
    ANALYSIS_END_DATE,
    ANALYSIS_START_DATE,
    BRONZE_DIR,
    NEWS_PROVIDER_ORDER,
    RAW_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.models import (
    CandidateQueryCase,
    NewsIngestRunResult,
    ProviderQueryResult,
)
from src.ingest.news.providers import get_provider_runner
from src.ingest.news.providers.curated import (
    CuratedSourceBundle,
    fetch_curated_source_bundle,
    match_candidate_against_curated_bundle,
)
from src.ingest.news.queries import build_candidate_query_case
from src.ingest.news.storage import (
    persist_provider_query_result,
    persist_raw_documents,
)

logger = logging.getLogger(__name__)


def _resolve_window_bounds(
    *,
    start_date: str | date = ANALYSIS_START_DATE,
    end_date: str | date = ANALYSIS_END_DATE,
    window_start: str | date | None = None,
    window_end: str | date | None = None,
) -> tuple[str | date, str | date]:
    """Normalize backward-compatible ingest window arguments."""
    return (
        window_start if window_start is not None else start_date,
        window_end if window_end is not None else end_date,
    )


def _load_candidate_cases(
    sample_manifest_path: Path,
    *,
    query_start: str | date,
    query_end: str | date,
) -> list[CandidateQueryCase]:
    """Load candidate cases from the manifest with the requested window."""
    if not sample_manifest_path.exists():
        raise FileNotFoundError(
            f"Sample manifest not found: {sample_manifest_path}. "
            "Run the sampling pipeline first."
        )

    with open(sample_manifest_path, encoding="utf-8") as file_handle:
        manifest = json.load(file_handle)
    return [
        build_candidate_query_case(candidate_row, query_start, query_end)
        for candidate_row in manifest["candidates"]
    ]


def _apply_provider_window_policy(
    candidate_case: CandidateQueryCase,
    provider_name: str,
) -> CandidateQueryCase:
    """Apply provider-specific window rules before execution.

    GNews free-plan availability lags publication by roughly half a day. A
    one-day rolling overlap prevents late-evening articles from being missed in
    the next scheduled run; downstream canonical URL deduplication collapses the
    duplicate discoveries safely.
    """
    if provider_name != "gnews":
        return candidate_case

    if (candidate_case.window_end - candidate_case.window_start).days != 1:
        return candidate_case

    return replace(
        candidate_case,
        window_start=candidate_case.window_start - timedelta(days=1),
    )


def _record_provider_result(
    *,
    candidate_case: CandidateQueryCase,
    provider_result: ProviderQueryResult,
    provider_result_persister: Callable[
        ..., tuple[ProviderQueryResult, tuple[Path, ...]]
    ],
    raw_dir: Path,
    bronze_dir: Path,
    duckdb_path: Path,
    raw_document_index: dict[str, tuple[str, str, str]] | None = None,
    persist_raw: bool = True,
) -> tuple[ProviderQueryResult, tuple[Path, ...]]:
    """Persist one candidate/provider result with optional shared raw-document lookup."""
    return provider_result_persister(
        candidate_case,
        provider_result,
        raw_dir=raw_dir,
        bronze_dir=bronze_dir,
        duckdb_path=duckdb_path,
        raw_document_index=raw_document_index,
        persist_raw=persist_raw,
    )


def run_news_ingest(
    sample_manifest_path: Path,
    start_date: str | date = ANALYSIS_START_DATE,
    end_date: str | date = ANALYSIS_END_DATE,
    window_start: str | date | None = None,
    window_end: str | date | None = None,
    provider_order: tuple[str, ...] = NEWS_PROVIDER_ORDER,
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    provider_runner_resolver: Callable[
        [str], Callable[[CandidateQueryCase], ProviderQueryResult]
    ] = get_provider_runner,
    provider_result_persister: Callable[
        ..., tuple[ProviderQueryResult, tuple[Path, ...]]
    ] = persist_provider_query_result,
    raw_document_persister: Callable[
        ..., tuple[dict[str, tuple[str, str, str]], tuple[Path, ...]]
    ] = persist_raw_documents,
    curated_source_fetcher: Callable[
        ..., CuratedSourceBundle
    ] = fetch_curated_source_bundle,
    curated_candidate_matcher: Callable[
        ..., ProviderQueryResult
    ] = match_candidate_against_curated_bundle,
) -> NewsIngestRunResult:
    """Run multi-source news discovery for every candidate in ``sample_manifest.json``."""
    query_start, query_end = _resolve_window_bounds(
        start_date=start_date,
        end_date=end_date,
        window_start=window_start,
        window_end=window_end,
    )
    candidate_cases = _load_candidate_cases(
        sample_manifest_path,
        query_start=query_start,
        query_end=query_end,
    )

    artifact_paths: list[str] = []
    query_count = 0
    hit_count = 0
    error_count = 0

    for provider_name in provider_order:
        if provider_name == "curated":
            shared_bundle = curated_source_fetcher(
                window_start=candidate_cases[0].window_start,
                window_end=candidate_cases[0].window_end,
            )
            shared_raw_index: dict[str, tuple[str, str, str]] = {}
            if shared_bundle.raw_documents:
                shared_raw_index, shared_raw_paths = raw_document_persister(
                    provider="curated",
                    provider_tier="tier1_curated",
                    raw_documents=shared_bundle.raw_documents,
                    window_start=candidate_cases[0].window_start.isoformat(),
                    window_end=candidate_cases[0].window_end.isoformat(),
                    raw_dir=raw_dir,
                    duckdb_path=duckdb_path,
                )
                artifact_paths.extend(str(path) for path in shared_raw_paths)

            for candidate_case in candidate_cases:
                provider_result = curated_candidate_matcher(
                    shared_bundle,
                    candidate_case,
                    include_raw_documents=False,
                )
                persisted_result, persisted_paths = _record_provider_result(
                    candidate_case=candidate_case,
                    provider_result=provider_result,
                    provider_result_persister=provider_result_persister,
                    raw_dir=raw_dir,
                    bronze_dir=bronze_dir,
                    duckdb_path=duckdb_path,
                    raw_document_index=shared_raw_index,
                    persist_raw=False,
                )
                query_count += 1
                hit_count += len(persisted_result.hits)
                if persisted_result.status in {"rate_limited", "error"}:
                    error_count += 1
                artifact_paths.extend(str(path) for path in persisted_paths)
            continue

        provider_runner = provider_runner_resolver(provider_name)
        for candidate_case in candidate_cases:
            provider_case = _apply_provider_window_policy(candidate_case, provider_name)
            provider_result = provider_runner(provider_case)
            persisted_result, persisted_paths = _record_provider_result(
                candidate_case=provider_case,
                provider_result=provider_result,
                provider_result_persister=provider_result_persister,
                raw_dir=raw_dir,
                bronze_dir=bronze_dir,
                duckdb_path=duckdb_path,
            )
            query_count += 1
            hit_count += len(persisted_result.hits)
            if persisted_result.status in {"rate_limited", "error"}:
                error_count += 1
            artifact_paths.extend(str(path) for path in persisted_paths)

    status = "partial" if error_count else "success"
    logger.info(
        "Hybrid news ingest complete status=%s queries=%d hits=%d errors=%d",
        status,
        query_count,
        hit_count,
        error_count,
    )
    return NewsIngestRunResult(
        status=status,
        error_count=error_count,
        query_count=query_count,
        hit_count=hit_count,
        artifact_paths=tuple(artifact_paths),
    )


def run_gdelt_ingest(
    sample_manifest_path: Path,
    start_date: str | date = ANALYSIS_START_DATE,
    end_date: str | date = ANALYSIS_END_DATE,
    window_start: str | date | None = None,
    window_end: str | date | None = None,
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> NewsIngestRunResult:
    """Compatibility wrapper that runs the news ingest against GDELT only."""
    return run_news_ingest(
        sample_manifest_path=sample_manifest_path,
        start_date=start_date,
        end_date=end_date,
        window_start=window_start,
        window_end=window_end,
        provider_order=("gdelt",),
        raw_dir=raw_dir,
        bronze_dir=bronze_dir,
        duckdb_path=duckdb_path,
    )
