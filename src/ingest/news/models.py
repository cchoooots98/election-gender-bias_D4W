"""Shared dataclasses for hybrid news ingestion."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

ProviderQueryStatus = Literal["success_hits", "success_zero", "rate_limited", "error"]
OutletDiagnosticStatus = Literal[
    "success_hits",
    "zero_hits",
    "zero_entries",
    "parsed",
    "fetched",
    "blocked",
]
OutletConnectorCapability = Literal[
    "realtime_only",
    "historical_capable",
    "browser_required",
]


@dataclass(frozen=True)
class CandidateQueryCase:
    """Candidate-specific search input shared across providers."""

    leader_id: str
    full_name: str
    commune_name: str
    dep_code: str
    city_size_bucket: str
    window_start: date
    window_end: date


@dataclass(frozen=True)
class RawDocument:
    """One raw provider payload to persist in the raw layer."""

    raw_document_key: str
    source_url: str
    payload: Any
    row_count: int
    partition_date: str
    content_type: str = "json"
    storage_key_name: str = ""
    storage_key: str = ""


@dataclass(frozen=True)
class SearchHit:
    """One discovered article hit before deduplication."""

    provider: str
    provider_tier: str
    outlet_key: str
    article_url: str
    title: str
    published_at: datetime | None
    domain: str
    language: str
    raw_payload_path: str
    query_text: str
    query_strategy: str
    leader_id: str = ""
    full_name: str = ""
    commune_name: str = ""
    dep_code: str = ""
    city_size_bucket: str = ""


@dataclass(frozen=True)
class OutletDiagnostic:
    """Per-outlet connector status for one candidate query."""

    outlet_key: str
    display_name: str
    connector_capability: OutletConnectorCapability
    status: OutletDiagnosticStatus
    documents_fetched: int = 0
    documents_parsed: int = 0
    entry_count: int = 0
    window_entry_count: int = 0
    hit_count: int = 0
    warning_count: int = 0
    error_types: tuple[str, ...] = field(default_factory=tuple)
    request_urls: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class ProviderQueryResult:
    """Normalized contract returned by every provider adapter."""

    provider: str
    provider_tier: str
    status: ProviderQueryStatus
    hits: tuple[SearchHit, ...] = field(default_factory=tuple)
    request_url: str = ""
    error_type: str | None = None
    error_message: str | None = None
    raw_documents: tuple[RawDocument, ...] = field(default_factory=tuple)
    warning_count: int = 0
    outlet_diagnostics: tuple[OutletDiagnostic, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class BenchmarkCase:
    """One manually validated truth article in the benchmark denominator."""

    benchmark_case_id: str
    leader_id: str
    expected_outlet_key: str
    manual_article_url: str | None
    manual_title: str | None
    manual_published_at: date | None


@dataclass(frozen=True)
class ArticleFetchResult:
    """Article-body extraction outcome for one canonical article URL."""

    canonical_url: str
    fetch_status: str
    body_text: str


@dataclass(frozen=True)
class BenchmarkRunResult:
    """Summary of one benchmark execution."""

    run_id: str
    status: str
    error_count: int
    results_row_count: int
    artifact_paths: tuple[str, ...]
    summary_metrics: dict[str, Any]


@dataclass(frozen=True)
class NewsIngestRunResult:
    """Summary of one multi-source discovery ingest run."""

    status: str
    error_count: int
    query_count: int
    hit_count: int
    artifact_paths: tuple[str, ...]
