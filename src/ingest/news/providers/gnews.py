"""Tier 3 GNews provider adapter."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from urllib.parse import urlparse

import requests

from src.config.settings import (
    GNEWS_API_KEY,
    GNEWS_MAX_ARTICLES_PER_PAGE,
    GNEWS_MAX_PAGES_PER_QUERY,
    SCRAPE_REQUEST_TIMEOUT_SECONDS,
    SCRAPE_RETRY_BACKOFF_SECONDS,
    SCRAPE_RETRY_MAX_ATTEMPTS,
)
from src.ingest.news.models import (
    CandidateQueryCase,
    ProviderQueryResult,
    RawDocument,
    SearchHit,
)
from src.ingest.news.normalize import canonicalize_url, sanitize_request_url
from src.ingest.news.queries import build_generic_news_query

logger = logging.getLogger(__name__)

_PROVIDER = "gnews"
_PROVIDER_TIER = "tier3_api"
_GNEWS_SEARCH_URL = "https://gnews.io/api/v4/search"
_RATE_LIMIT_BACKOFF_SECONDS = 30


def _parse_gnews_datetime(value: str) -> datetime | None:
    """Parse a GNews RFC3339 timestamp into UTC."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        logger.warning("Failed to parse GNews publishedAt=%r", value)
        return None


def _empty_result(
    *,
    status: str,
    request_urls: list[str] | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    hits: list[SearchHit] | None = None,
    raw_documents: list[RawDocument] | None = None,
) -> ProviderQueryResult:
    """Build a standardized provider result, preserving partial pages when present."""
    return ProviderQueryResult(
        provider=_PROVIDER,
        provider_tier=_PROVIDER_TIER,
        status=status,  # type: ignore[arg-type]
        hits=tuple(hits or ()),
        request_url=";".join(request_urls or ()),
        error_type=error_type,
        error_message=error_message,
        raw_documents=tuple(raw_documents or ()),
    )


def search_gnews_candidate(candidate_case: CandidateQueryCase) -> ProviderQueryResult:
    """Search GNews for one candidate using paginated result collection."""
    if not GNEWS_API_KEY:
        return _empty_result(
            status="error",
            error_type="missing_api_key",
            error_message="GNEWS_API_KEY is not configured.",
        )

    query_text = build_generic_news_query(
        candidate_case.full_name,
        candidate_case.commune_name,
    )
    base_params = {
        "q": query_text,
        "lang": "fr",
        "country": "fr",
        "from": candidate_case.window_start.isoformat(),
        "to": candidate_case.window_end.isoformat(),
        "max": GNEWS_MAX_ARTICLES_PER_PAGE,
    }
    headers = {"X-Api-Key": GNEWS_API_KEY}

    request_urls: list[str] = []
    raw_documents: list[RawDocument] = []
    hits: list[SearchHit] = []
    seen_canonical_urls: set[str] = set()

    for page in range(1, GNEWS_MAX_PAGES_PER_QUERY + 1):
        params = {**base_params, "page": page}
        for attempt in range(1, SCRAPE_RETRY_MAX_ATTEMPTS + 1):
            try:
                response = requests.get(
                    _GNEWS_SEARCH_URL,
                    params=params,
                    headers=headers,
                    timeout=SCRAPE_REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                payload = response.json()
                sanitized_request_url = sanitize_request_url(response.url)
                request_urls.append(sanitized_request_url)

                articles = payload.get("articles") or []
                raw_document_key = (
                    f"search_{candidate_case.window_start.isoformat()}_"
                    f"{candidate_case.leader_id}_page_{page}"
                )
                raw_documents.append(
                    RawDocument(
                        raw_document_key=raw_document_key,
                        source_url=sanitized_request_url,
                        payload=payload,
                        row_count=len(articles),
                        partition_date=candidate_case.window_start.isoformat(),
                        storage_key_name="leader_id",
                        storage_key=candidate_case.leader_id,
                    )
                )

                for article in articles:
                    article_url = article.get("url") or ""
                    canonical_url = canonicalize_url(article_url)
                    if not article_url or canonical_url in seen_canonical_urls:
                        continue
                    seen_canonical_urls.add(canonical_url)
                    hits.append(
                        SearchHit(
                            provider=_PROVIDER,
                            provider_tier=_PROVIDER_TIER,
                            outlet_key=urlparse(article_url)
                            .netloc.lower()
                            .removeprefix("www."),
                            article_url=article_url,
                            title=article.get("title") or "",
                            published_at=_parse_gnews_datetime(
                                article.get("publishedAt") or ""
                            ),
                            domain=urlparse(article_url)
                            .netloc.lower()
                            .removeprefix("www."),
                            language="fr",
                            raw_payload_path=raw_document_key,
                            query_text=query_text,
                            query_strategy="gnews_search",
                            leader_id=candidate_case.leader_id,
                            full_name=candidate_case.full_name,
                            commune_name=candidate_case.commune_name,
                            dep_code=candidate_case.dep_code,
                            city_size_bucket=candidate_case.city_size_bucket,
                        )
                    )

                if len(articles) < GNEWS_MAX_ARTICLES_PER_PAGE:
                    return _empty_result(
                        status="success_hits" if hits else "success_zero",
                        request_urls=request_urls,
                        hits=hits,
                        raw_documents=raw_documents,
                    )
                break

            except requests.HTTPError as exc:
                status_code = exc.response.status_code
                request_url = (
                    sanitize_request_url(exc.response.url)
                    if exc.response is not None
                    else ""
                )
                if request_url:
                    request_urls.append(request_url)
                if status_code == 429:
                    wait_seconds = _RATE_LIMIT_BACKOFF_SECONDS * attempt
                    if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                        time.sleep(wait_seconds)
                        continue
                    return _empty_result(
                        status="rate_limited",
                        request_urls=request_urls,
                        error_type="http_429",
                        error_message="GNews returned HTTP 429 after retries.",
                        hits=hits,
                        raw_documents=raw_documents,
                    )
                return _empty_result(
                    status="error",
                    request_urls=request_urls,
                    error_type=f"http_{status_code}",
                    error_message=f"GNews returned HTTP {status_code}.",
                    hits=hits,
                    raw_documents=raw_documents,
                )

            except requests.Timeout:
                wait_seconds = int(SCRAPE_RETRY_BACKOFF_SECONDS) * attempt
                if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                    time.sleep(wait_seconds)
                    continue
                return _empty_result(
                    status="error",
                    request_urls=request_urls,
                    error_type="timeout",
                    error_message="GNews timed out after retries.",
                    hits=hits,
                    raw_documents=raw_documents,
                )

            except requests.ConnectionError:
                wait_seconds = int(SCRAPE_RETRY_BACKOFF_SECONDS) * attempt
                if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                    time.sleep(wait_seconds)
                    continue
                return _empty_result(
                    status="error",
                    request_urls=request_urls,
                    error_type="connection_error",
                    error_message="GNews connection failed after retries.",
                    hits=hits,
                    raw_documents=raw_documents,
                )

            except (ValueError, KeyError) as exc:
                return _empty_result(
                    status="error",
                    request_urls=request_urls,
                    error_type="parse_error",
                    error_message=f"Failed to parse GNews response: {exc!r}",
                    hits=hits,
                    raw_documents=raw_documents,
                )

    return _empty_result(
        status="success_hits" if hits else "success_zero",
        request_urls=request_urls,
        hits=hits,
        raw_documents=raw_documents,
    )
