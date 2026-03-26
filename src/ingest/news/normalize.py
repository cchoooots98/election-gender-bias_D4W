"""Canonicalization and silver-ready normalization helpers for news hits."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd

from src.ingest.news.models import ArticleFetchResult, SearchHit

_TRACKING_QUERY_PARAMS = frozenset(
    {
        "fbclid",
        "gclid",
        "igshid",
        "mc_cid",
        "mc_eid",
        "ref",
        "ref_src",
        "source",
        "utm_campaign",
        "utm_content",
        "utm_medium",
        "utm_source",
        "utm_term",
    }
)
_SENSITIVE_QUERY_PARAMS = frozenset(
    {
        "access_token",
        "api_key",
        "apikey",
        "key",
        "token",
    }
)


def canonicalize_url(url: str) -> str:
    """Canonicalize a URL for cross-provider duplicate detection."""
    parsed = urlparse(url.strip())
    normalized_host = parsed.netloc.lower().removeprefix("www.")
    normalized_path = parsed.path.rstrip("/") or "/"
    filtered_params = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in _TRACKING_QUERY_PARAMS
    ]
    normalized_query = urlencode(filtered_params, doseq=True)
    return urlunparse(
        (
            parsed.scheme.lower() or "https",
            normalized_host,
            normalized_path,
            "",
            normalized_query,
            "",
        )
    )


def sanitize_request_url(url: str) -> str:
    """Redact sensitive query parameters before persisting request metadata."""
    parsed = urlparse(url.strip())
    filtered_params = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in _SENSITIVE_QUERY_PARAMS
    ]
    normalized_query = urlencode(filtered_params, doseq=True)
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            normalized_query,
            parsed.fragment,
        )
    )


def _stable_md5(text: str) -> str:
    """Build a deterministic MD5 key from a text input."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def build_fact_article_frames(
    search_hits: Iterable[SearchHit],
    article_fetch_results: Mapping[str, ArticleFetchResult] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convert raw search hits into canonical article and discovery tables."""
    fetch_results = article_fetch_results or {}
    search_hits_list = list(search_hits)
    article_columns = [
        "article_id",
        "url",
        "canonical_url",
        "title",
        "body_text",
        "published_at",
        "domain",
        "language",
        "fetch_status",
        "is_duplicate",
        "canonical_article_id",
        "partition_date",
    ]
    discovery_columns = [
        "discovery_id",
        "article_id",
        "leader_id",
        "provider",
        "provider_tier",
        "outlet_key",
        "article_url",
        "canonical_url",
        "title",
        "published_at",
        "domain",
        "language",
        "raw_payload_path",
        "query_text",
        "query_strategy",
        "partition_date",
    ]

    if not search_hits_list:
        return pd.DataFrame(columns=article_columns), pd.DataFrame(
            columns=discovery_columns
        )

    canonical_to_hit: dict[str, SearchHit] = {}
    discovery_rows: list[dict[str, object]] = []
    for search_hit in search_hits_list:
        canonical_url = canonicalize_url(search_hit.article_url)
        canonical_to_hit.setdefault(canonical_url, search_hit)
        article_id = _stable_md5(canonical_url)
        partition_date = (
            search_hit.published_at.date().isoformat()
            if search_hit.published_at is not None
            else ""
        )
        discovery_key = (
            f"{article_id}:{search_hit.provider}:{search_hit.outlet_key}:"
            f"{search_hit.raw_payload_path}:{search_hit.query_text}"
        )
        discovery_rows.append(
            {
                "discovery_id": _stable_md5(discovery_key),
                "article_id": article_id,
                "leader_id": search_hit.leader_id,
                "provider": search_hit.provider,
                "provider_tier": search_hit.provider_tier,
                "outlet_key": search_hit.outlet_key,
                "article_url": search_hit.article_url,
                "canonical_url": canonical_url,
                "title": search_hit.title,
                "published_at": search_hit.published_at,
                "domain": search_hit.domain,
                "language": search_hit.language,
                "raw_payload_path": search_hit.raw_payload_path,
                "query_text": search_hit.query_text,
                "query_strategy": search_hit.query_strategy,
                "partition_date": partition_date,
            }
        )

    article_rows: list[dict[str, object]] = []
    for canonical_url, canonical_hit in canonical_to_hit.items():
        fetch_result = fetch_results.get(
            canonical_url,
            ArticleFetchResult(
                canonical_url=canonical_url,
                fetch_status="not_fetched",
                body_text="",
            ),
        )
        partition_date = (
            canonical_hit.published_at.date().isoformat()
            if canonical_hit.published_at is not None
            else ""
        )
        article_rows.append(
            {
                "article_id": _stable_md5(canonical_url),
                "url": canonical_hit.article_url,
                "canonical_url": canonical_url,
                "title": canonical_hit.title,
                "body_text": fetch_result.body_text,
                "published_at": canonical_hit.published_at,
                "domain": canonical_hit.domain,
                "language": canonical_hit.language,
                "fetch_status": fetch_result.fetch_status,
                "is_duplicate": False,
                "canonical_article_id": None,
                "partition_date": partition_date,
            }
        )

    return pd.DataFrame(article_rows), pd.DataFrame(discovery_rows)


def compute_duplicate_rate(search_hits: Iterable[SearchHit]) -> float | None:
    """Compute duplicate-discovery rate after URL canonicalization."""
    hits_list = list(search_hits)
    if not hits_list:
        return None
    unique_canonical_urls = {canonicalize_url(hit.article_url) for hit in hits_list}
    return 1 - (len(unique_canonical_urls) / len(hits_list))


def now_iso_utc() -> str:
    """Return the current UTC timestamp as an ISO-8601 string."""
    return datetime.now(UTC).isoformat()
