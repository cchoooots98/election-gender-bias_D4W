"""Tier 2 GDELT provider adapter."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta

import requests

from src.config.settings import (
    GDELT_MAX_RECORDS,
    GDELT_REQUEST_DELAY_SECONDS,
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
from src.ingest.news.normalize import sanitize_request_url
from src.ingest.news.queries import build_gdelt_query

logger = logging.getLogger(__name__)

_PROVIDER = "gdelt"
_PROVIDER_TIER = "tier2_gdelt"
_GDELT_BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
_DEFAULT_WINDOW_DAYS = 7
_NEAR_CAP_THRESHOLD = int(GDELT_MAX_RECORDS * 0.95)
_RATE_LIMIT_BACKOFF_SECONDS = 30
_SEENDATE_FMT = "%Y%m%dT%H%M%SZ"


def _fmt_gdelt_dt(dt: datetime) -> str:
    """Format UTC datetimes for the GDELT request contract."""
    return dt.strftime("%Y%m%d%H%M%S")


def _parse_gdelt_seendate(seendate: str) -> datetime | None:
    """Parse the undocumented GDELT ``seendate`` wire format."""
    if not seendate:
        return None
    try:
        return datetime.strptime(seendate, _SEENDATE_FMT).replace(tzinfo=UTC)
    except ValueError:
        logger.warning("Failed to parse GDELT seendate=%r", seendate)
        return None


def _empty_result(
    status: str,
    request_url: str = "",
    error_type: str | None = None,
    error_message: str | None = None,
) -> ProviderQueryResult:
    """Build a standardized empty provider result."""
    return ProviderQueryResult(
        provider=_PROVIDER,
        provider_tier=_PROVIDER_TIER,
        status=status,  # type: ignore[arg-type]
        request_url=request_url,
        error_type=error_type,
        error_message=error_message,
    )


def _fetch_gdelt_window(
    candidate_case: CandidateQueryCase,
    query_text: str,
    query_strategy: str,
    window_start: datetime,
    window_end: datetime,
) -> ProviderQueryResult:
    """Fetch one GDELT window and map it to the internal provider contract."""
    params = {
        "query": query_text,
        "mode": "artlist",
        "maxrecords": str(GDELT_MAX_RECORDS),
        "startdatetime": _fmt_gdelt_dt(window_start),
        "enddatetime": _fmt_gdelt_dt(window_end),
        "format": "json",
        "sourcelang": "French",
    }

    for attempt in range(1, SCRAPE_RETRY_MAX_ATTEMPTS + 1):
        try:
            response = requests.get(
                _GDELT_BASE_URL,
                params=params,
                timeout=SCRAPE_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = response.json()
            articles = payload.get("articles") or []
            raw_document_key = (
                f"{query_strategy}_{window_start.date().isoformat()}_"
                f"{window_end.date().isoformat()}"
            )
            raw_document = RawDocument(
                raw_document_key=raw_document_key,
                source_url=sanitize_request_url(response.url),
                payload=payload,
                row_count=len(articles),
                partition_date=window_start.date().isoformat(),
                storage_key_name="leader_id",
                storage_key=candidate_case.leader_id,
            )
            hits = tuple(
                SearchHit(
                    provider=_PROVIDER,
                    provider_tier=_PROVIDER_TIER,
                    outlet_key=(article.get("domain") or "")
                    .lower()
                    .removeprefix("www.")
                    or _PROVIDER,
                    article_url=article.get("url") or "",
                    title=article.get("title") or "",
                    published_at=_parse_gdelt_seendate(article.get("seendate") or ""),
                    domain=(article.get("domain") or "").lower().removeprefix("www."),
                    language=(article.get("language") or "fr").lower(),
                    raw_payload_path=raw_document_key,
                    query_text=query_text,
                    query_strategy=query_strategy,
                    leader_id=candidate_case.leader_id,
                    full_name=candidate_case.full_name,
                    commune_name=candidate_case.commune_name,
                    dep_code=candidate_case.dep_code,
                    city_size_bucket=candidate_case.city_size_bucket,
                )
                for article in articles
            )
            status = "success_hits" if hits else "success_zero"
            return ProviderQueryResult(
                provider=_PROVIDER,
                provider_tier=_PROVIDER_TIER,
                status=status,
                hits=hits,
                request_url=sanitize_request_url(response.url),
                raw_documents=(raw_document,),
            )

        except requests.HTTPError as exc:
            status_code = exc.response.status_code
            request_url = (
                sanitize_request_url(exc.response.url)
                if exc.response is not None
                else ""
            )
            if status_code == 429:
                wait_seconds = _RATE_LIMIT_BACKOFF_SECONDS * attempt
                if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                    logger.warning(
                        "GDELT rate limited leader_id=%s attempt=%d/%d wait=%ds",
                        candidate_case.leader_id,
                        attempt,
                        SCRAPE_RETRY_MAX_ATTEMPTS,
                        wait_seconds,
                    )
                    time.sleep(wait_seconds)
                    continue
                return _empty_result(
                    status="rate_limited",
                    request_url=request_url,
                    error_type="http_429",
                    error_message="GDELT returned HTTP 429 after retries.",
                )
            return _empty_result(
                status="error",
                request_url=request_url,
                error_type=f"http_{status_code}",
                error_message=f"GDELT returned HTTP {status_code}.",
            )

        except requests.Timeout:
            wait_seconds = int(SCRAPE_RETRY_BACKOFF_SECONDS) * attempt
            if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                logger.warning(
                    "GDELT timeout leader_id=%s attempt=%d/%d wait=%ds",
                    candidate_case.leader_id,
                    attempt,
                    SCRAPE_RETRY_MAX_ATTEMPTS,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            return _empty_result(
                status="error",
                error_type="timeout",
                error_message="GDELT timed out after retries.",
            )

        except requests.ConnectionError:
            wait_seconds = int(SCRAPE_RETRY_BACKOFF_SECONDS) * attempt
            if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                logger.warning(
                    "GDELT connection error leader_id=%s attempt=%d/%d wait=%ds",
                    candidate_case.leader_id,
                    attempt,
                    SCRAPE_RETRY_MAX_ATTEMPTS,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            return _empty_result(
                status="error",
                error_type="connection_error",
                error_message="GDELT connection failed after retries.",
            )

        except (ValueError, KeyError) as exc:
            return _empty_result(
                status="error",
                error_type="parse_error",
                error_message=f"Failed to parse GDELT response: {exc!r}",
            )

    return _empty_result(
        status="error",
        error_type="unexpected_retry_exit",
        error_message="GDELT retry loop exited unexpectedly.",
    )


def search_gdelt_candidate(candidate_case: CandidateQueryCase) -> ProviderQueryResult:
    """Search GDELT for one candidate using adaptive windowing and mode fallback."""
    precise_query = build_gdelt_query(
        candidate_case.full_name,
        candidate_case.commune_name,
        mode="precise",
    )
    relaxed_query = build_gdelt_query(
        candidate_case.full_name,
        candidate_case.commune_name,
        mode="relaxed",
    )

    period_start_dt = datetime(
        candidate_case.window_start.year,
        candidate_case.window_start.month,
        candidate_case.window_start.day,
        0,
        0,
        0,
        tzinfo=UTC,
    )
    period_end_dt = datetime(
        candidate_case.window_end.year,
        candidate_case.window_end.month,
        candidate_case.window_end.day,
        23,
        59,
        59,
        tzinfo=UTC,
    )

    probe_result = _fetch_gdelt_window(
        candidate_case,
        precise_query,
        "precise",
        period_start_dt,
        period_end_dt,
    )
    time.sleep(GDELT_REQUEST_DELAY_SECONDS)
    if probe_result.status in {"rate_limited", "error"}:
        return probe_result

    active_query = precise_query
    active_strategy = "precise"
    if probe_result.status == "success_zero":
        relaxed_probe = _fetch_gdelt_window(
            candidate_case,
            relaxed_query,
            "relaxed",
            period_start_dt,
            period_end_dt,
        )
        time.sleep(GDELT_REQUEST_DELAY_SECONDS)
        if relaxed_probe.status in {"rate_limited", "error"}:
            return relaxed_probe
        if relaxed_probe.status == "success_zero":
            return relaxed_probe
        active_query = relaxed_query
        active_strategy = "relaxed"

    collected_hits: list[SearchHit] = []
    collected_raw_documents: list[RawDocument] = []
    request_url = ""
    window_start_date = candidate_case.window_start

    while window_start_date <= candidate_case.window_end:
        window_end_date = min(
            window_start_date + timedelta(days=_DEFAULT_WINDOW_DAYS - 1),
            candidate_case.window_end,
        )
        window_result = _fetch_gdelt_window(
            candidate_case,
            active_query,
            active_strategy,
            datetime(
                window_start_date.year,
                window_start_date.month,
                window_start_date.day,
                0,
                0,
                0,
                tzinfo=UTC,
            ),
            datetime(
                window_end_date.year,
                window_end_date.month,
                window_end_date.day,
                23,
                59,
                59,
                tzinfo=UTC,
            ),
        )
        time.sleep(GDELT_REQUEST_DELAY_SECONDS)
        request_url = window_result.request_url or request_url
        if window_result.status in {"rate_limited", "error"}:
            return ProviderQueryResult(
                provider=_PROVIDER,
                provider_tier=_PROVIDER_TIER,
                status=window_result.status,
                hits=tuple(collected_hits),
                request_url=request_url,
                error_type=window_result.error_type,
                error_message=window_result.error_message,
                raw_documents=tuple(collected_raw_documents),
            )

        if len(window_result.hits) >= _NEAR_CAP_THRESHOLD:
            sub_date = window_start_date
            while sub_date <= window_end_date:
                sub_result = _fetch_gdelt_window(
                    candidate_case,
                    active_query,
                    active_strategy,
                    datetime(
                        sub_date.year, sub_date.month, sub_date.day, 0, 0, 0, tzinfo=UTC
                    ),
                    datetime(
                        sub_date.year,
                        sub_date.month,
                        sub_date.day,
                        23,
                        59,
                        59,
                        tzinfo=UTC,
                    ),
                )
                time.sleep(GDELT_REQUEST_DELAY_SECONDS)
                request_url = sub_result.request_url or request_url
                if sub_result.status in {"rate_limited", "error"}:
                    return ProviderQueryResult(
                        provider=_PROVIDER,
                        provider_tier=_PROVIDER_TIER,
                        status=sub_result.status,
                        hits=tuple(collected_hits),
                        request_url=request_url,
                        error_type=sub_result.error_type,
                        error_message=sub_result.error_message,
                        raw_documents=tuple(collected_raw_documents),
                    )
                collected_hits.extend(sub_result.hits)
                collected_raw_documents.extend(sub_result.raw_documents)
                sub_date += timedelta(days=1)
        else:
            collected_hits.extend(window_result.hits)
            collected_raw_documents.extend(window_result.raw_documents)

        window_start_date = window_end_date + timedelta(days=1)

    return ProviderQueryResult(
        provider=_PROVIDER,
        provider_tier=_PROVIDER_TIER,
        status="success_hits" if collected_hits else "success_zero",
        hits=tuple(collected_hits),
        request_url=request_url,
        raw_documents=tuple(collected_raw_documents),
    )
