"""Benchmark runner for multi-source news discovery."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

try:
    import trafilatura
except ImportError:  # pragma: no cover - exercised via fetch-status fallback tests
    trafilatura = None

from src.config.settings import (
    BRONZE_DIR,
    DQ_MIN_ARTICLE_TEXT_LENGTH,
    GOLD_DIR,
    NEWS_BENCHMARK_PROVIDER_ORDER,
    RAW_DIR,
    SCRAPE_REQUEST_TIMEOUT_SECONDS,
    WAREHOUSE_PATH,
)
from src.ingest.news.models import (
    ArticleFetchResult,
    BenchmarkCase,
    BenchmarkRunResult,
    CandidateQueryCase,
    ProviderQueryResult,
)
from src.ingest.news.normalize import (
    build_fact_article_frames,
    canonicalize_url,
    compute_duplicate_rate,
)
from src.ingest.news.providers import get_provider_runner
from src.ingest.news.providers.curated import (
    CuratedSourceBundle,
    fetch_curated_source_bundle,
    match_candidate_against_curated_bundle,
)
from src.ingest.news.storage import persist_provider_query_result, persist_raw_documents

logger = logging.getLogger(__name__)


def load_benchmark_manifest(
    benchmark_manifest_path: Path,
) -> tuple[list[CandidateQueryCase], list[BenchmarkCase], tuple[str, ...]]:
    """Load the committed benchmark manifest YAML."""
    with open(benchmark_manifest_path, encoding="utf-8") as file_handle:
        manifest = yaml.safe_load(file_handle) or {}

    benchmark_window = manifest["benchmark_window"]
    start_date = benchmark_window["start_date"]
    end_date = benchmark_window["end_date"]
    candidate_cases = [
        CandidateQueryCase(
            leader_id=str(candidate["leader_id"]),
            full_name=str(candidate["full_name"]),
            commune_name=str(candidate["commune_name"]),
            dep_code=str(candidate["dep_code"]),
            city_size_bucket=str(candidate["city_size_bucket"]),
            window_start=date.fromisoformat(start_date),
            window_end=date.fromisoformat(end_date),
        )
        for candidate in manifest["candidates"]
    ]
    benchmark_cases = [
        BenchmarkCase(
            benchmark_case_id=str(case_row["benchmark_case_id"]),
            leader_id=str(case_row["leader_id"]),
            expected_outlet_key=str(case_row["expected_outlet_key"]),
            manual_article_url=case_row.get("manual_article_url"),
            manual_title=case_row.get("manual_title"),
            manual_published_at=(
                date.fromisoformat(case_row["manual_published_at"])
                if case_row.get("manual_published_at")
                else None
            ),
        )
        for case_row in manifest.get("manual_truth_articles", [])
    ]
    return candidate_cases, benchmark_cases, tuple(manifest["curated_outlets"])


def _fetch_article_text(article_url: str) -> ArticleFetchResult:
    """Fetch article body text for benchmark success-rate measurement."""
    try:
        response = requests.get(article_url, timeout=SCRAPE_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException:
        return ArticleFetchResult(
            canonical_url=canonicalize_url(article_url),
            fetch_status="http_error",
            body_text="",
        )

    if trafilatura is None:
        return ArticleFetchResult(
            canonical_url=canonicalize_url(article_url),
            fetch_status="extractor_unavailable",
            body_text="",
        )

    extracted_text = trafilatura.extract(response.text) or ""

    if len(extracted_text.strip()) < DQ_MIN_ARTICLE_TEXT_LENGTH:
        return ArticleFetchResult(
            canonical_url=canonicalize_url(article_url),
            fetch_status="too_short",
            body_text=extracted_text,
        )
    return ArticleFetchResult(
        canonical_url=canonicalize_url(article_url),
        fetch_status="success",
        body_text=extracted_text,
    )


def _build_results_dataframe(
    discovery_df: pd.DataFrame,
    benchmark_cases: list[BenchmarkCase],
    provider_query_rows: list[dict[str, object]],
    article_fetch_results: dict[str, ArticleFetchResult],
) -> pd.DataFrame:
    """Build the per-discovery benchmark results table."""
    benchmark_case_df = pd.DataFrame(
        [
            {
                "benchmark_case_id": case.benchmark_case_id,
                "leader_id": case.leader_id,
                "expected_outlet_key": case.expected_outlet_key,
                "manual_article_url": case.manual_article_url,
                "manual_title": case.manual_title,
                "manual_published_at": case.manual_published_at,
                "truth_canonical_url": (
                    canonicalize_url(case.manual_article_url)
                    if case.manual_article_url
                    else None
                ),
            }
            for case in benchmark_cases
        ]
    )
    if discovery_df.empty:
        return pd.DataFrame(
            columns=[
                *discovery_df.columns.tolist(),
                "article_fetch_status",
                "benchmark_case_id",
                "expected_outlet_key",
                "manual_article_url",
                "manual_title",
                "manual_published_at",
                "truth_canonical_url",
                "is_truth_match",
                "provider_status",
                "provider_error_type",
                "provider_warning_count",
                "provider_hit_count",
            ]
        )

    discovery_with_fetch = discovery_df.copy()
    discovery_with_fetch["article_fetch_status"] = discovery_with_fetch[
        "canonical_url"
    ].map(
        lambda canonical_url: article_fetch_results.get(
            canonical_url,
            ArticleFetchResult(
                canonical_url=canonical_url,
                fetch_status="not_fetched",
                body_text="",
            ),
        ).fetch_status
    )
    if benchmark_case_df.empty:
        provider_query_df = pd.DataFrame(provider_query_rows).rename(
            columns={"provider_tier": "query_provider_tier"}
        )
        if provider_query_df.empty:
            return discovery_with_fetch
        return discovery_with_fetch.merge(
            provider_query_df,
            on=["leader_id", "provider"],
            how="left",
            validate="many_to_one",
        )

    provider_query_df = pd.DataFrame(provider_query_rows).rename(
        columns={"provider_tier": "query_provider_tier"}
    )
    results_df = discovery_with_fetch.merge(
        benchmark_case_df,
        left_on=["leader_id", "outlet_key", "canonical_url"],
        right_on=["leader_id", "expected_outlet_key", "truth_canonical_url"],
        how="left",
        validate="many_to_one",
    )
    results_df["is_truth_match"] = results_df["benchmark_case_id"].notna()
    if provider_query_df.empty:
        return results_df
    return results_df.merge(
        provider_query_df,
        on=["leader_id", "provider"],
        how="left",
        validate="many_to_one",
    )


def _build_provider_query_frame(
    provider_query_rows: list[dict[str, object]],
) -> pd.DataFrame:
    """Convert provider-query audit rows into a deterministic DataFrame."""
    if not provider_query_rows:
        return pd.DataFrame(
            columns=[
                "leader_id",
                "full_name",
                "commune_name",
                "provider",
                "provider_tier",
                "provider_status",
                "provider_error_type",
                "provider_warning_count",
                "provider_hit_count",
                "request_url",
            ]
        )
    return pd.DataFrame(provider_query_rows)


def _build_outlet_diagnostics_frame(
    outlet_diagnostic_rows: list[dict[str, object]],
) -> pd.DataFrame:
    """Convert per-outlet diagnostics into a deterministic DataFrame."""
    if not outlet_diagnostic_rows:
        return pd.DataFrame(
            columns=[
                "leader_id",
                "full_name",
                "commune_name",
                "provider",
                "provider_tier",
                "outlet_key",
                "display_name",
                "connector_capability",
                "outlet_status",
                "documents_fetched",
                "documents_parsed",
                "entry_count",
                "window_entry_count",
                "hit_count",
                "warning_count",
                "error_types",
                "request_urls",
            ]
        )
    return pd.DataFrame(outlet_diagnostic_rows)


def _build_summary_metrics(
    provider_results: list[ProviderQueryResult],
    benchmark_cases: list[BenchmarkCase],
    all_hits,
    results_df: pd.DataFrame,
    article_fetch_results: dict[str, ArticleFetchResult],
) -> dict[str, object]:
    """Compute run-level benchmark metrics."""
    truth_cases = [case for case in benchmark_cases if case.manual_article_url]
    truth_total = len(truth_cases)
    matched_truth_urls = set()
    if not results_df.empty and "truth_canonical_url" in results_df.columns:
        # fillna(False) treats NA as non-match — correct for left-join result columns.
        is_truth_match = results_df["is_truth_match"].fillna(False)
        matched_truth_urls = set(
            results_df.loc[is_truth_match, "truth_canonical_url"].dropna().tolist()
        )

    unique_article_fetches = list(article_fetch_results.values())
    successful_fetches = [
        fetch_result
        for fetch_result in unique_article_fetches
        if fetch_result.fetch_status == "success"
    ]

    source_overlap_count = None
    if not results_df.empty and "truth_canonical_url" in results_df.columns:
        is_truth_match = results_df["is_truth_match"].fillna(False)
        matched_rows = results_df.loc[is_truth_match]
        if not matched_rows.empty:
            overlap_series = matched_rows.groupby("truth_canonical_url")[
                "provider"
            ].nunique()
            source_overlap_count = float(overlap_series.mean())

    successful_provider_queries = [
        result
        for result in provider_results
        if result.status in {"success_hits", "success_zero"}
    ]
    provider_availability_rate = (
        len(successful_provider_queries) / len(provider_results)
        if provider_results
        else None
    )

    recall = (len(matched_truth_urls) / truth_total) if truth_total else None
    duplicate_rate = compute_duplicate_rate(all_hits)
    article_fetch_success_rate = (
        len(successful_fetches) / len(unique_article_fetches)
        if unique_article_fetches
        else None
    )
    return {
        "truth_article_count": truth_total,
        "matched_truth_article_count": len(matched_truth_urls),
        "recall": recall,
        "duplicate_rate": duplicate_rate,
        "article_fetch_success_rate": article_fetch_success_rate,
        "source_overlap_count": source_overlap_count,
        "provider_availability_rate": provider_availability_rate,
    }


def run_news_benchmark(
    benchmark_manifest_path: Path,
    provider_order: tuple[str, ...] = NEWS_BENCHMARK_PROVIDER_ORDER,
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    gold_dir: Path = GOLD_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    pipeline_run_id: str | None = None,
    provider_runner_resolver: Callable[
        [str], Callable[[CandidateQueryCase], ProviderQueryResult]
    ] = get_provider_runner,
    provider_result_persister: Callable[
        ..., tuple[ProviderQueryResult, tuple[Path, ...]]
    ] = persist_provider_query_result,
    raw_document_persister: Callable[
        ..., tuple[dict[str, tuple[str, str, str]], tuple[Path, ...]]
    ] = persist_raw_documents,
    article_fetcher: Callable[[str], ArticleFetchResult] = _fetch_article_text,
    curated_source_fetcher: Callable[..., CuratedSourceBundle] = fetch_curated_source_bundle,
    curated_candidate_matcher: Callable[..., ProviderQueryResult] = match_candidate_against_curated_bundle,
) -> BenchmarkRunResult:
    """Run the fixed multi-source benchmark and persist its artifacts."""
    run_id = pipeline_run_id or str(uuid.uuid4())
    candidate_cases, benchmark_cases, _ = load_benchmark_manifest(
        benchmark_manifest_path
    )
    artifact_paths: list[Path] = []
    provider_results: list[ProviderQueryResult] = []
    provider_query_rows: list[dict[str, object]] = []
    outlet_diagnostic_rows: list[dict[str, object]] = []
    all_hits = []
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
                artifact_paths.extend(shared_raw_paths)

            for candidate_case in candidate_cases:
                provider_result = curated_candidate_matcher(
                    shared_bundle,
                    candidate_case,
                    include_raw_documents=False,
                )
                provider_results.append(provider_result)
                if provider_result.status in {"rate_limited", "error"}:
                    error_count += 1
                persisted_result, persisted_paths = provider_result_persister(
                    candidate_case,
                    provider_result,
                    raw_dir=raw_dir,
                    bronze_dir=bronze_dir,
                    duckdb_path=duckdb_path,
                    raw_document_index=shared_raw_index,
                    persist_raw=False,
                )
                artifact_paths.extend(persisted_paths)
                all_hits.extend(persisted_result.hits)
                provider_query_rows.append(
                    {
                        "leader_id": candidate_case.leader_id,
                        "full_name": candidate_case.full_name,
                        "commune_name": candidate_case.commune_name,
                        "provider": provider_name,
                        "provider_tier": persisted_result.provider_tier,
                        "provider_status": persisted_result.status,
                        "provider_error_type": persisted_result.error_type,
                        "provider_warning_count": persisted_result.warning_count,
                        "provider_hit_count": len(persisted_result.hits),
                        "request_url": persisted_result.request_url,
                    }
                )
                for outlet_diagnostic in persisted_result.outlet_diagnostics:
                    outlet_diagnostic_rows.append(
                        {
                            "leader_id": candidate_case.leader_id,
                            "full_name": candidate_case.full_name,
                            "commune_name": candidate_case.commune_name,
                            "provider": provider_name,
                            "provider_tier": persisted_result.provider_tier,
                            "outlet_key": outlet_diagnostic.outlet_key,
                            "display_name": outlet_diagnostic.display_name,
                            "connector_capability": outlet_diagnostic.connector_capability,
                            "outlet_status": outlet_diagnostic.status,
                            "documents_fetched": outlet_diagnostic.documents_fetched,
                            "documents_parsed": outlet_diagnostic.documents_parsed,
                            "entry_count": outlet_diagnostic.entry_count,
                            "window_entry_count": outlet_diagnostic.window_entry_count,
                            "hit_count": outlet_diagnostic.hit_count,
                            "warning_count": outlet_diagnostic.warning_count,
                            "error_types": list(outlet_diagnostic.error_types),
                            "request_urls": list(outlet_diagnostic.request_urls),
                        }
                    )
            continue

        for candidate_case in candidate_cases:
            provider_runner = provider_runner_resolver(provider_name)
            provider_result = provider_runner(candidate_case)
            provider_results.append(provider_result)
            if provider_result.status in {"rate_limited", "error"}:
                error_count += 1
            persisted_result, persisted_paths = provider_result_persister(
                candidate_case,
                provider_result,
                raw_dir=raw_dir,
                bronze_dir=bronze_dir,
                duckdb_path=duckdb_path,
            )
            artifact_paths.extend(persisted_paths)
            all_hits.extend(persisted_result.hits)
            provider_query_rows.append(
                {
                    "leader_id": candidate_case.leader_id,
                    "full_name": candidate_case.full_name,
                    "commune_name": candidate_case.commune_name,
                    "provider": provider_name,
                    "provider_tier": persisted_result.provider_tier,
                    "provider_status": persisted_result.status,
                    "provider_error_type": persisted_result.error_type,
                    "provider_warning_count": persisted_result.warning_count,
                    "provider_hit_count": len(persisted_result.hits),
                    "request_url": persisted_result.request_url,
                }
            )
            for outlet_diagnostic in persisted_result.outlet_diagnostics:
                outlet_diagnostic_rows.append(
                    {
                        "leader_id": candidate_case.leader_id,
                        "full_name": candidate_case.full_name,
                        "commune_name": candidate_case.commune_name,
                        "provider": provider_name,
                        "provider_tier": persisted_result.provider_tier,
                        "outlet_key": outlet_diagnostic.outlet_key,
                        "display_name": outlet_diagnostic.display_name,
                        "connector_capability": outlet_diagnostic.connector_capability,
                        "outlet_status": outlet_diagnostic.status,
                        "documents_fetched": outlet_diagnostic.documents_fetched,
                        "documents_parsed": outlet_diagnostic.documents_parsed,
                        "entry_count": outlet_diagnostic.entry_count,
                        "window_entry_count": outlet_diagnostic.window_entry_count,
                        "hit_count": outlet_diagnostic.hit_count,
                        "warning_count": outlet_diagnostic.warning_count,
                        "error_types": list(outlet_diagnostic.error_types),
                        "request_urls": list(outlet_diagnostic.request_urls),
                    }
                )

    article_fetch_results: dict[str, ArticleFetchResult] = {}
    for search_hit in all_hits:
        canonical_url = canonicalize_url(search_hit.article_url)
        if canonical_url in article_fetch_results:
            continue
        article_fetch_results[canonical_url] = article_fetcher(search_hit.article_url)

    fact_article_df, discovery_df = build_fact_article_frames(
        all_hits,
        article_fetch_results,
    )
    provider_query_df = _build_provider_query_frame(provider_query_rows)
    outlet_diagnostic_df = _build_outlet_diagnostics_frame(outlet_diagnostic_rows)
    results_df = _build_results_dataframe(
        discovery_df=discovery_df,
        benchmark_cases=benchmark_cases,
        provider_query_rows=provider_query_rows,
        article_fetch_results=article_fetch_results,
    )
    summary_metrics = _build_summary_metrics(
        provider_results=provider_results,
        benchmark_cases=benchmark_cases,
        all_hits=all_hits,
        results_df=results_df,
        article_fetch_results=article_fetch_results,
    )
    status = "partial" if error_count else "success"

    gold_dir.mkdir(parents=True, exist_ok=True)
    results_path = gold_dir / "news_source_benchmark_results.parquet"
    summary_path = gold_dir / "news_source_benchmark_summary.json"
    provider_query_path = gold_dir / "news_source_benchmark_provider_queries.parquet"
    outlet_diagnostic_path = (
        gold_dir / "news_source_benchmark_outlet_diagnostics.parquet"
    )
    fact_article_path = gold_dir / "news_source_benchmark_fact_article.parquet"
    discovery_path = gold_dir / "news_source_benchmark_fact_article_discovery.parquet"

    pq.write_table(pa.Table.from_pandas(results_df), results_path, compression="snappy")
    pq.write_table(
        pa.Table.from_pandas(provider_query_df),
        provider_query_path,
        compression="snappy",
    )
    pq.write_table(
        pa.Table.from_pandas(outlet_diagnostic_df),
        outlet_diagnostic_path,
        compression="snappy",
    )
    pq.write_table(
        pa.Table.from_pandas(fact_article_df), fact_article_path, compression="snappy"
    )
    pq.write_table(
        pa.Table.from_pandas(discovery_df), discovery_path, compression="snappy"
    )
    with open(summary_path, "w", encoding="utf-8") as file_handle:
        json.dump(
            {
                "run_id": run_id,
                "status": status,
                "error_count": error_count,
                "summary_metrics": summary_metrics,
            },
            file_handle,
            ensure_ascii=False,
            indent=2,
        )

    artifact_paths.extend(
        [
            results_path,
            summary_path,
            provider_query_path,
            outlet_diagnostic_path,
            fact_article_path,
            discovery_path,
        ]
    )
    logger.info(
        "News benchmark complete run_id=%s status=%s rows=%d",
        run_id,
        status,
        len(results_df),
    )
    return BenchmarkRunResult(
        run_id=run_id,
        status=status,
        error_count=error_count,
        results_row_count=len(results_df),
        artifact_paths=tuple(str(path) for path in artifact_paths),
        summary_metrics=summary_metrics,
    )
