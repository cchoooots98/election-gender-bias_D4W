"""URL canonicalization and lightweight normalization utilities.

These helpers are consumed by provider adapters, storage, and the benchmark
runner.  They do **not** produce Silver-layer DataFrames — use the functions
in ``corpus.py`` (``build_fact_article``, ``build_fact_article_discovery``,
``build_article_source_from_search_hits``) for that purpose.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
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
        and key.lower() not in _SENSITIVE_QUERY_PARAMS
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


def stable_md5(text: str) -> str:
    """Build a deterministic MD5 key from a text input."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()


# Backward-compatible private alias for older imports inside this package.
_stable_md5 = stable_md5


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


def _build_compat_article_fetch_results(
    search_hits: list[SearchHit] | tuple[SearchHit, ...],
) -> dict[str, ArticleFetchResult]:
    """Synthesize minimal fetch results for compatibility-only callers.

    The benchmark suite historically called ``build_fact_article_frames`` with
    search hits only. The new corpus contract expects fetched article bodies, so
    this helper creates deterministic placeholder bodies from the hit metadata.
    That preserves the old import surface without reintroducing duplicate ETL
    logic into ``normalize.py``.
    """
    fetch_results: dict[str, ArticleFetchResult] = {}
    for hit in search_hits:
        canonical_url = canonicalize_url(hit.article_url)
        body_text = hit.title.strip() or hit.query_text.strip() or canonical_url
        fetch_results.setdefault(
            canonical_url,
            ArticleFetchResult(
                canonical_url=canonical_url,
                fetch_status="synthetic_search_hit_body",
                body_text=body_text,
            ),
        )
    return fetch_results


def build_fact_article_frames(
    search_hits: list[SearchHit] | tuple[SearchHit, ...],
    article_fetch_results: dict[str, ArticleFetchResult] | None = None,
    provider_query_rows: list[dict[str, object]] | None = None,
    *,
    batch_id: str = "compat_build_fact_article_frames",
    source_system: str = "compat_provider",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compatibility wrapper for benchmark callers expecting article frames.

    ``corpus.py`` owns the canonical DataFrame-building logic. This wrapper
    keeps the older import path stable for tests and lightweight scripts while
    delegating all real work to the corpus-layer contracts.
    """
    from src.ingest.news.corpus import (
        build_article_source_from_search_hits,
        build_fact_article,
        build_fact_article_discovery,
    )

    effective_fetch_results = (
        article_fetch_results or _build_compat_article_fetch_results(search_hits)
    )
    fact_article_source_df = build_article_source_from_search_hits(
        search_hits=search_hits,
        article_fetch_results=effective_fetch_results,
        batch_id=batch_id,
        source_system=source_system,
    )
    fact_article_df = build_fact_article(fact_article_source_df)
    discovery_df = build_fact_article_discovery(
        search_hits=search_hits,
        provider_query_rows=provider_query_rows,
    )
    return fact_article_df, discovery_df
