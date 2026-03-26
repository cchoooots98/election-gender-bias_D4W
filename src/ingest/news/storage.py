"""Raw + bronze persistence helpers for news ingestion."""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config.settings import (
    BRONZE_DIR,
    FILESYSTEM_PATH_BUDGET,
    RAW_DIR,
    WAREHOUSE_PATH,
)
from src.ingest.news.models import (
    CandidateQueryCase,
    ProviderQueryResult,
    RawDocument,
    SearchHit,
)
from src.ingest.news.normalize import now_iso_utc, sanitize_request_url
from src.observability.run_logger import log_source_snapshot

logger = logging.getLogger(__name__)

_BRONZE_SEARCH_HIT_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("provider", pa.string()),
        pa.field("provider_tier", pa.string()),
        pa.field("outlet_key", pa.string()),
        pa.field("article_url", pa.string()),
        pa.field("title", pa.string()),
        pa.field("published_at", pa.timestamp("us", tz="UTC")),
        pa.field("domain", pa.string()),
        pa.field("language", pa.string()),
        pa.field("raw_payload_path", pa.string()),
        pa.field("query_text", pa.string()),
        pa.field("query_strategy", pa.string()),
        pa.field("leader_id", pa.string()),
        pa.field("full_name", pa.string()),
        pa.field("commune_name", pa.string()),
        pa.field("dep_code", pa.string()),
        pa.field("city_size_bucket", pa.string()),
        pa.field("window_start", pa.string()),
        pa.field("window_end", pa.string()),
        pa.field("_query_status", pa.string()),
        pa.field("_error_type", pa.string()),
        pa.field("_source_url", pa.string()),
        pa.field("_ingested_at", pa.string()),
        pa.field("_source_hash", pa.string()),
    ]
)

_BRONZE_QUERY_AUDIT_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("provider", pa.string()),
        pa.field("provider_tier", pa.string()),
        pa.field("leader_id", pa.string()),
        pa.field("full_name", pa.string()),
        pa.field("commune_name", pa.string()),
        pa.field("dep_code", pa.string()),
        pa.field("city_size_bucket", pa.string()),
        pa.field("window_start", pa.string()),
        pa.field("window_end", pa.string()),
        pa.field("provider_status", pa.string()),
        pa.field("provider_error_type", pa.string()),
        pa.field("provider_warning_count", pa.int64()),
        pa.field("provider_hit_count", pa.int64()),
        pa.field("request_url", pa.string()),
        pa.field("_ingested_at", pa.string()),
    ]
)


def _compute_file_md5(file_path: Path) -> str:
    """Compute a stable MD5 hash for a file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(8 * 1024 * 1024), b""):
            md5.update(chunk)
    return md5.hexdigest()


def _stable_md5(text: str) -> str:
    """Build a deterministic MD5 digest for short storage keys."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def _normalize_path_token(value: str, *, max_length: int) -> str:
    """Normalize free-text values into short filesystem-safe tokens."""
    normalized = re.sub(r"[^0-9a-z]+", "_", value.strip().lower()).strip("_")
    if not normalized:
        return "unknown"
    return normalized[:max_length]


def _absolute_path(path: Path) -> Path:
    """Return an absolute path without requiring the target to exist."""
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _validate_path_budget(path: Path, *, artifact_kind: str) -> None:
    """Fail fast when an artifact path exceeds the portable filesystem budget."""
    absolute_path = _absolute_path(path)
    path_length = len(str(absolute_path))
    if path_length <= FILESYSTEM_PATH_BUDGET:
        return

    raise ValueError(
        f"Path budget exceeded for {artifact_kind}: "
        f"path_length={path_length} budget={FILESYSTEM_PATH_BUDGET} "
        f"path={absolute_path}. Shorten the root output directory or "
        "reduce storage-key verbosity."
    )


def _build_raw_scope(raw_document: RawDocument) -> str:
    """Build a stable raw-data directory segment for one raw document."""
    if raw_document.storage_key_name and raw_document.storage_key:
        normalized_name = _normalize_path_token(
            raw_document.storage_key_name,
            max_length=8,
        )
        normalized_value = _normalize_path_token(
            raw_document.storage_key, max_length=12
        )
        value_hash = _stable_md5(raw_document.storage_key)[:8]
        return f"scope={normalized_name}_{normalized_value}_{value_hash}"
    return "scope=shared"


def _build_raw_window_segment(window_start: str, window_end: str) -> str:
    """Build one short deterministic window partition segment."""
    return f"window={window_start.replace('-', '')}_{window_end.replace('-', '')}"


def _build_raw_filename(raw_document: RawDocument) -> str:
    """Build a short raw payload filename while preserving full metadata in JSON."""
    document_hash = _stable_md5(raw_document.raw_document_key)[:16]
    extension = "json" if raw_document.content_type == "json" else "xml"
    return f"doc_{document_hash}.{extension}"


def persist_raw_documents(
    *,
    provider: str,
    provider_tier: str,
    raw_documents: tuple[RawDocument, ...],
    window_start: str,
    window_end: str,
    raw_dir: Path = RAW_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
) -> tuple[dict[str, tuple[str, str, str]], tuple[Path, ...]]:
    """Persist raw provider payloads once and return a lookup by raw document key."""
    artifact_paths: list[Path] = []
    raw_document_index: dict[str, tuple[str, str, str]] = {}

    for raw_document in raw_documents:
        raw_path = (
            raw_dir
            / "news"
            / f"provider={provider}"
            / _build_raw_window_segment(window_start, window_end)
            / _build_raw_scope(raw_document)
            / _build_raw_filename(raw_document)
        )
        _validate_path_budget(raw_path, artifact_kind="raw news payload")
        raw_path.parent.mkdir(parents=True, exist_ok=True)

        sanitized_request_url = sanitize_request_url(raw_document.source_url)
        payload_wrapper = {
            "provider": provider,
            "provider_tier": provider_tier,
            "window_start": window_start,
            "window_end": window_end,
            "raw_document_key": raw_document.raw_document_key,
            "request_url": sanitized_request_url,
            "content_type": raw_document.content_type,
            "row_count": raw_document.row_count,
            "payload": raw_document.payload,
        }
        if raw_document.storage_key_name and raw_document.storage_key:
            payload_wrapper[raw_document.storage_key_name] = raw_document.storage_key

        with open(raw_path, "w", encoding="utf-8") as file_handle:
            json.dump(payload_wrapper, file_handle, ensure_ascii=False, indent=2)

        source_hash = _compute_file_md5(raw_path)
        raw_document_index[raw_document.raw_document_key] = (
            str(raw_path),
            sanitized_request_url,
            source_hash,
        )
        artifact_paths.append(raw_path)
        log_source_snapshot(
            source_key=f"news:{provider}:{raw_document.raw_document_key}",
            source_url=sanitized_request_url,
            source_hash=source_hash,
            raw_file_path=raw_path,
            row_count=raw_document.row_count,
            duckdb_path=duckdb_path,
        )

    return raw_document_index, tuple(artifact_paths)


def _build_search_hit_frame(
    *,
    candidate_case: CandidateQueryCase,
    provider_result: ProviderQueryResult,
    raw_document_index: dict[str, tuple[str, str, str]],
    ingested_at: str,
) -> tuple[pd.DataFrame, tuple[SearchHit, ...]]:
    """Build the bronze search-hit frame and resolve raw payload paths."""
    updated_hits: list[SearchHit] = []
    rows: list[dict[str, object]] = []

    for search_hit in provider_result.hits:
        raw_path, source_url, source_hash = raw_document_index.get(
            search_hit.raw_payload_path,
            (search_hit.raw_payload_path, "", ""),
        )
        updated_hit = dataclasses.replace(search_hit, raw_payload_path=raw_path)
        updated_hits.append(updated_hit)
        rows.append(
            {
                "provider": updated_hit.provider,
                "provider_tier": updated_hit.provider_tier,
                "outlet_key": updated_hit.outlet_key,
                "article_url": updated_hit.article_url,
                "title": updated_hit.title,
                "published_at": updated_hit.published_at,
                "domain": updated_hit.domain,
                "language": updated_hit.language,
                "raw_payload_path": updated_hit.raw_payload_path,
                "query_text": updated_hit.query_text,
                "query_strategy": updated_hit.query_strategy,
                "leader_id": updated_hit.leader_id,
                "full_name": updated_hit.full_name,
                "commune_name": updated_hit.commune_name,
                "dep_code": updated_hit.dep_code,
                "city_size_bucket": updated_hit.city_size_bucket,
                "window_start": candidate_case.window_start.isoformat(),
                "window_end": candidate_case.window_end.isoformat(),
                "_query_status": provider_result.status,
                "_error_type": provider_result.error_type or "",
                "_source_url": source_url,
                "_ingested_at": ingested_at,
                "_source_hash": source_hash,
            }
        )

    if not rows:
        return (
            pd.DataFrame(columns=[field.name for field in _BRONZE_SEARCH_HIT_SCHEMA]),
            tuple(updated_hits),
        )

    return pd.DataFrame(rows), tuple(updated_hits)


def _build_query_audit_frame(
    *,
    candidate_case: CandidateQueryCase,
    provider_result: ProviderQueryResult,
    ingested_at: str,
) -> pd.DataFrame:
    """Build one candidate-level provider audit row."""
    row = {
        "provider": provider_result.provider,
        "provider_tier": provider_result.provider_tier,
        "leader_id": candidate_case.leader_id,
        "full_name": candidate_case.full_name,
        "commune_name": candidate_case.commune_name,
        "dep_code": candidate_case.dep_code,
        "city_size_bucket": candidate_case.city_size_bucket,
        "window_start": candidate_case.window_start.isoformat(),
        "window_end": candidate_case.window_end.isoformat(),
        "provider_status": provider_result.status,
        "provider_error_type": provider_result.error_type or "",
        "provider_warning_count": provider_result.warning_count,
        "provider_hit_count": len(provider_result.hits),
        "request_url": sanitize_request_url(provider_result.request_url),
        "_ingested_at": ingested_at,
    }
    return pd.DataFrame([row])


def persist_provider_query_result(
    candidate_case: CandidateQueryCase,
    provider_result: ProviderQueryResult,
    raw_dir: Path = RAW_DIR,
    bronze_dir: Path = BRONZE_DIR,
    duckdb_path: Path = WAREHOUSE_PATH,
    raw_document_index: dict[str, tuple[str, str, str]] | None = None,
    persist_raw: bool = True,
) -> tuple[ProviderQueryResult, tuple[Path, ...]]:
    """Persist bronze search hits and query audit for one candidate/provider query."""
    artifact_paths: list[Path] = []
    window_start = candidate_case.window_start.isoformat()
    window_end = candidate_case.window_end.isoformat()

    document_index = raw_document_index or {}
    if persist_raw and provider_result.raw_documents:
        document_index, raw_artifacts = persist_raw_documents(
            provider=provider_result.provider,
            provider_tier=provider_result.provider_tier,
            raw_documents=provider_result.raw_documents,
            window_start=window_start,
            window_end=window_end,
            raw_dir=raw_dir,
            duckdb_path=duckdb_path,
        )
        artifact_paths.extend(raw_artifacts)

    ingested_at = now_iso_utc()
    bronze_df, updated_hits = _build_search_hit_frame(
        candidate_case=candidate_case,
        provider_result=provider_result,
        raw_document_index=document_index,
        ingested_at=ingested_at,
    )
    query_audit_df = _build_query_audit_frame(
        candidate_case=candidate_case,
        provider_result=provider_result,
        ingested_at=ingested_at,
    )

    base_dir = (
        bronze_dir
        / f"window_start={window_start}"
        / f"window_end={window_end}"
        / f"provider={provider_result.provider}"
        / f"leader_id={candidate_case.leader_id}"
    )
    hits_path = base_dir / "news_search_results.parquet"
    query_audit_path = base_dir / "news_provider_query.parquet"
    _validate_path_budget(hits_path, artifact_kind="bronze search hits")
    _validate_path_budget(query_audit_path, artifact_kind="bronze query audit")
    hits_path.parent.mkdir(parents=True, exist_ok=True)

    pq.write_table(
        pa.Table.from_pandas(bronze_df, schema=_BRONZE_SEARCH_HIT_SCHEMA),
        hits_path,
        compression="snappy",
    )
    pq.write_table(
        pa.Table.from_pandas(query_audit_df, schema=_BRONZE_QUERY_AUDIT_SCHEMA),
        query_audit_path,
        compression="snappy",
    )
    artifact_paths.extend([hits_path, query_audit_path])

    logger.info(
        "Persisted provider result provider=%s leader_id=%s hits=%d status=%s",
        provider_result.provider,
        candidate_case.leader_id,
        len(updated_hits),
        provider_result.status,
    )
    updated_result = dataclasses.replace(provider_result, hits=updated_hits)
    return updated_result, tuple(artifact_paths)
