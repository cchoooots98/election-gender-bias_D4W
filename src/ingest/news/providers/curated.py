"""Tier 1 curated outlet connector using RSS and sitemap feeds."""

from __future__ import annotations

import html
import logging
import re
import time
import xml.etree.ElementTree as ET
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime

import requests

from src.config.news_outlets import ACTIVE_CURATED_OUTLET_KEYS, OUTLET_CATALOG
from src.config.settings import (
    SCRAPE_REQUEST_TIMEOUT_SECONDS,
    SCRAPE_RETRY_BACKOFF_SECONDS,
    SCRAPE_RETRY_MAX_ATTEMPTS,
)
from src.ingest.news.models import (
    CandidateQueryCase,
    OutletDiagnostic,
    ProviderQueryResult,
    RawDocument,
    SearchHit,
)
from src.ingest.news.normalize import sanitize_request_url
from src.ingest.news.queries import (
    build_generic_news_query,
    entry_matches_candidate,
    entry_needs_candidate_verification,
)

logger = logging.getLogger(__name__)

_PROVIDER = "curated"
_PROVIDER_TIER = "tier1_curated"
_RATE_LIMIT_BACKOFF_SECONDS = 30
_MAX_SITEMAP_RECURSION_DEPTH = 2
_MAX_CHILD_SITEMAPS_PER_SOURCE = 20
_MAX_ARTICLE_VERIFICATION_FETCHES = 15
_HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
_SCRIPT_STYLE_PATTERN = re.compile(r"(?is)<(script|style).*?>.*?</\1>")


@dataclass(frozen=True)
class CuratedFetchedDocument:
    """One fetched outlet document with parsed entries."""

    outlet_key: str
    raw_document: RawDocument
    entries: tuple[dict[str, object], ...]


@dataclass(frozen=True)
class CuratedOutletSnapshot:
    """Candidate-agnostic fetch stats for one outlet during one window."""

    outlet_key: str
    display_name: str
    connector_capability: str
    documents_fetched: int
    documents_parsed: int
    entry_count: int
    window_entry_count: int
    warning_count: int
    error_types: tuple[str, ...]
    request_urls: tuple[str, ...]


@dataclass(frozen=True)
class CuratedSourceBundle:
    """Window-scoped curated source snapshot shared across candidate matches."""

    raw_documents: tuple[RawDocument, ...]
    fetched_documents: tuple[CuratedFetchedDocument, ...]
    outlet_snapshots: tuple[CuratedOutletSnapshot, ...]
    request_urls: tuple[str, ...]
    warning_count: int
    successful_documents: int


def _parse_feed_datetime(value: str) -> datetime | None:
    """Parse RSS, Atom, or sitemap timestamps into UTC-aware datetimes."""
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except (TypeError, ValueError, IndexError):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            logger.warning("Failed to parse feed datetime=%r", value)
            return None


def _iter_rss_entries(xml_text: str) -> Iterable[dict[str, object]]:
    """Yield RSS and Atom entries as normalized dicts."""
    root = ET.fromstring(xml_text)
    yield from _iter_rss_entries_from_root(root)


def _iter_rss_entries_from_root(root: ET.Element) -> Iterable[dict[str, object]]:
    """Yield RSS and Atom entries from a parsed XML root."""
    for item in root.findall(".//item"):
        yield {
            "title": item.findtext("title", default=""),
            "link": item.findtext("link", default=""),
            "published_at": _parse_feed_datetime(item.findtext("pubDate", default="")),
        }
    for entry in root.findall(".//{*}entry"):
        link_element = entry.find("{*}link")
        href = link_element.attrib.get("href", "") if link_element is not None else ""
        yield {
            "title": entry.findtext("{*}title", default=""),
            "link": href,
            "published_at": _parse_feed_datetime(
                entry.findtext("{*}updated", default="")
                or entry.findtext("{*}published", default="")
            ),
        }


def _iter_sitemap_entries_from_root(root: ET.Element) -> Iterable[dict[str, object]]:
    """Yield sitemap URL entries from a parsed XML root."""
    for url_node in root.findall(".//{*}url"):
        link = url_node.findtext("{*}loc", default="")
        yield {
            "title": link,
            "link": link,
            "published_at": _parse_feed_datetime(
                url_node.findtext("{*}lastmod", default="")
            ),
        }


def _iter_sitemap_index_entries_from_root(
    root: ET.Element,
) -> Iterable[tuple[str, datetime | None]]:
    """Yield (url, lastmod) pairs from a sitemap index XML root."""
    for sitemap_node in root.findall(".//{*}sitemap"):
        child_url = sitemap_node.findtext("{*}loc", default="")
        if not child_url:
            continue
        lastmod_str = sitemap_node.findtext("{*}lastmod", default="")
        lastmod = _parse_feed_datetime(lastmod_str) if lastmod_str else None
        yield child_url, lastmod


def _iter_sitemap_index_urls_from_root(root: ET.Element) -> Iterable[str]:
    """Yield child sitemap URLs from a sitemap index XML root."""
    for url, _ in _iter_sitemap_index_entries_from_root(root):
        yield url


def _is_sitemap_index(root: ET.Element) -> bool:
    """Return whether the parsed XML root is a sitemap index."""
    return root.tag.endswith("sitemapindex") or bool(root.findall(".//{*}sitemap"))


def _fetch_xml_document(url: str) -> tuple[str | None, str | None, str | None]:
    """Fetch one XML or HTML document with retry semantics."""
    for attempt in range(1, SCRAPE_RETRY_MAX_ATTEMPTS + 1):
        try:
            response = requests.get(url, timeout=SCRAPE_REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            return response.text, sanitize_request_url(response.url), None
        except requests.HTTPError as exc:
            status_code = exc.response.status_code
            response_url = (
                sanitize_request_url(exc.response.url)
                if exc.response is not None
                else sanitize_request_url(url)
            )
            if status_code == 429:
                wait_seconds = _RATE_LIMIT_BACKOFF_SECONDS * attempt
                if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                    time.sleep(wait_seconds)
                    continue
                return (None, response_url, "http_429")
            return (None, response_url, f"http_{status_code}")
        except requests.Timeout:
            wait_seconds = int(SCRAPE_RETRY_BACKOFF_SECONDS) * attempt
            if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                time.sleep(wait_seconds)
                continue
            return None, sanitize_request_url(url), "timeout"
        except requests.ConnectionError:
            wait_seconds = int(SCRAPE_RETRY_BACKOFF_SECONDS) * attempt
            if attempt < SCRAPE_RETRY_MAX_ATTEMPTS:
                time.sleep(wait_seconds)
                continue
            return None, sanitize_request_url(url), "connection_error"
    return None, sanitize_request_url(url), "unexpected_retry_exit"


def _entry_within_window_bounds(
    published_at: datetime | None,
    *,
    window_start: date,
    window_end: date,
) -> bool:
    """Return whether one feed entry belongs to the requested window."""
    if published_at is None:
        return True
    published_date = published_at.astimezone(UTC).date()
    return window_start <= published_date <= window_end


def _strip_html_for_match(document_text: str) -> str:
    """Reduce HTML into coarse plain text for candidate verification."""
    without_scripts = _SCRIPT_STYLE_PATTERN.sub(" ", document_text)
    without_tags = _HTML_TAG_PATTERN.sub(" ", without_scripts)
    return html.unescape(without_tags)


def _fetch_article_verification_text(article_url: str) -> tuple[str, str, str | None]:
    """Fetch one article page and return coarse text for body-level verification."""
    document_text, resolved_url, error_type = _fetch_xml_document(article_url)
    if document_text is None:
        return "", resolved_url or article_url, error_type
    return _strip_html_for_match(document_text), resolved_url or article_url, None


def _build_raw_document(
    *,
    outlet_key: str,
    raw_document_index: int,
    source_url: str,
    payload: str,
    row_count: int,
    window_start: date,
) -> RawDocument:
    """Create a raw-document audit record with deterministic naming."""
    raw_document_key = (
        f"{outlet_key.replace('.', '_')}_{raw_document_index}_"
        f"{window_start.isoformat()}"
    )
    return RawDocument(
        raw_document_key=raw_document_key,
        source_url=source_url,
        payload=payload,
        row_count=row_count,
        partition_date=window_start.isoformat(),
        content_type="xml",
        storage_key_name="outlet_key",
        storage_key=outlet_key,
    )


def _append_hits_from_entries(
    *,
    entries: Iterable[dict[str, object]],
    outlet_key: str,
    query_text: str,
    candidate_case: CandidateQueryCase,
    raw_document_key: str,
    hits: list[SearchHit],
    verification_cache: dict[str, tuple[bool, str]],
    verification_fetch_count: int,
) -> tuple[int, int, int]:
    """Screen feed entries and append candidate hits, using body verification when needed."""
    window_entry_count = 0
    hit_count = 0

    for entry in entries:
        published_at = entry.get("published_at")
        published_dt = published_at if isinstance(published_at, datetime) else None
        if not _entry_within_window_bounds(
            published_dt,
            window_start=candidate_case.window_start,
            window_end=candidate_case.window_end,
        ):
            continue
        window_entry_count += 1

        article_url = str(entry.get("link") or "")
        title = str(entry.get("title") or "")
        if not article_url:
            continue

        search_text = f"{title} {article_url}"
        query_strategy = "rss_sitemap_filter"
        verified_url = article_url

        if entry_matches_candidate(search_text, candidate_case):
            is_match = True
        elif entry_needs_candidate_verification(search_text, candidate_case):
            cached_verification = verification_cache.get(article_url)
            if (
                cached_verification is None
                and verification_fetch_count < _MAX_ARTICLE_VERIFICATION_FETCHES
            ):
                verification_text, resolved_url, _ = _fetch_article_verification_text(
                    article_url
                )
                verification_fetch_count += 1
                cached_verification = (
                    entry_matches_candidate(verification_text, candidate_case),
                    resolved_url,
                )
                verification_cache[article_url] = cached_verification
            if cached_verification is None:
                is_match = False
            else:
                is_match, verified_url = cached_verification
                query_strategy = "rss_sitemap_body_verify"
        else:
            is_match = False

        if not is_match:
            continue

        hit_count += 1
        hits.append(
            SearchHit(
                provider=_PROVIDER,
                provider_tier=_PROVIDER_TIER,
                outlet_key=outlet_key,
                article_url=verified_url,
                title=title,
                published_at=published_dt,
                domain=outlet_key,
                language="fr",
                raw_payload_path=raw_document_key,
                query_text=query_text,
                query_strategy=query_strategy,
                leader_id=candidate_case.leader_id,
                full_name=candidate_case.full_name,
                commune_name=candidate_case.commune_name,
                dep_code=candidate_case.dep_code,
                city_size_bucket=candidate_case.city_size_bucket,
            )
        )

    return verification_fetch_count, window_entry_count, hit_count


def _count_window_entries(
    entries: Iterable[dict[str, object]],
    *,
    window_start: date,
    window_end: date,
) -> int:
    """Count entries that fall inside the requested window."""
    return sum(
        1
        for entry in entries
        if _entry_within_window_bounds(
            entry.get("published_at")
            if isinstance(entry.get("published_at"), datetime)
            else None,
            window_start=window_start,
            window_end=window_end,
        )
    )


def _build_outlet_diagnostic(
    *,
    outlet_snapshot: CuratedOutletSnapshot,
    hit_count: int,
) -> OutletDiagnostic:
    """Build a candidate-specific diagnostic row from a shared outlet snapshot."""
    if hit_count > 0:
        status = "success_hits"
    elif outlet_snapshot.documents_fetched == 0:
        status = "blocked"
    elif outlet_snapshot.documents_parsed == 0:
        status = "fetched"
    elif outlet_snapshot.entry_count == 0:
        status = "zero_entries"
    elif outlet_snapshot.window_entry_count == 0:
        status = "parsed"
    else:
        status = "zero_hits"

    return OutletDiagnostic(
        outlet_key=outlet_snapshot.outlet_key,
        display_name=outlet_snapshot.display_name,
        connector_capability=outlet_snapshot.connector_capability,
        status=status,
        documents_fetched=outlet_snapshot.documents_fetched,
        documents_parsed=outlet_snapshot.documents_parsed,
        entry_count=outlet_snapshot.entry_count,
        window_entry_count=outlet_snapshot.window_entry_count,
        hit_count=hit_count,
        warning_count=outlet_snapshot.warning_count,
        error_types=outlet_snapshot.error_types,
        request_urls=outlet_snapshot.request_urls,
    )


def fetch_curated_source_bundle(
    *,
    window_start: date,
    window_end: date,
) -> CuratedSourceBundle:
    """Fetch all curated outlet documents once for a shared ingest window."""
    raw_documents: list[RawDocument] = []
    fetched_documents: list[CuratedFetchedDocument] = []
    request_urls: list[str] = []
    warning_count = 0
    successful_documents = 0
    outlet_snapshots: list[CuratedOutletSnapshot] = []

    for outlet_key in ACTIVE_CURATED_OUTLET_KEYS:
        outlet = OUTLET_CATALOG[outlet_key]
        outlet_request_urls: list[str] = []
        outlet_documents_fetched = 0
        outlet_documents_parsed = 0
        outlet_entry_count = 0
        outlet_window_entry_count = 0
        outlet_warning_count = 0
        outlet_error_types: set[str] = set()

        for source_url in outlet.rss_urls:
            document_text, resolved_url, error_type = _fetch_xml_document(source_url)
            request_url = resolved_url or sanitize_request_url(source_url)
            request_urls.append(request_url)
            outlet_request_urls.append(request_url)
            if document_text is None:
                if error_type is not None:
                    warning_count += 1
                    outlet_warning_count += 1
                    outlet_error_types.add(error_type)
                continue

            successful_documents += 1
            outlet_documents_fetched += 1
            try:
                entries = list(_iter_rss_entries(document_text))
            except ET.ParseError:
                logger.warning(
                    "Curated connector parse error outlet=%s source_url=%s",
                    outlet_key,
                    source_url,
                )
                warning_count += 1
                outlet_warning_count += 1
                outlet_error_types.add("parse_error")
                continue

            outlet_documents_parsed += 1
            outlet_entry_count += len(entries)
            outlet_window_entry_count += _count_window_entries(
                entries,
                window_start=window_start,
                window_end=window_end,
            )
            raw_document = _build_raw_document(
                outlet_key=outlet_key,
                raw_document_index=len(raw_documents) + 1,
                source_url=request_url,
                payload=document_text,
                row_count=len(entries),
                window_start=window_start,
            )
            raw_documents.append(raw_document)
            fetched_documents.append(
                CuratedFetchedDocument(
                    outlet_key=outlet_key,
                    raw_document=raw_document,
                    entries=tuple(entries),
                )
            )

        for source_url in outlet.sitemap_urls:
            pending_sitemaps: deque[tuple[str, int]] = deque([(source_url, 0)])
            visited_sitemaps: set[str] = set()

            while pending_sitemaps:
                current_url, depth = pending_sitemaps.popleft()
                if current_url in visited_sitemaps:
                    continue
                visited_sitemaps.add(current_url)

                document_text, resolved_url, error_type = _fetch_xml_document(
                    current_url
                )
                request_url = resolved_url or sanitize_request_url(current_url)
                request_urls.append(request_url)
                outlet_request_urls.append(request_url)
                if document_text is None:
                    if error_type is not None:
                        warning_count += 1
                        outlet_warning_count += 1
                        outlet_error_types.add(error_type)
                    continue

                successful_documents += 1
                outlet_documents_fetched += 1
                try:
                    root = ET.fromstring(document_text)
                except ET.ParseError:
                    logger.warning(
                        "Curated connector parse error outlet=%s source_url=%s",
                        outlet_key,
                        current_url,
                    )
                    warning_count += 1
                    outlet_warning_count += 1
                    outlet_error_types.add("parse_error")
                    continue

                outlet_documents_parsed += 1
                if _is_sitemap_index(root):
                    child_entries = list(_iter_sitemap_index_entries_from_root(root))
                    window_cutoff: date = window_start - timedelta(days=1)
                    child_urls = [
                        url
                        for url, lastmod in child_entries
                        if lastmod is None or lastmod.date() >= window_cutoff
                    ]
                    if depth < _MAX_SITEMAP_RECURSION_DEPTH:
                        for child_url in child_urls[:_MAX_CHILD_SITEMAPS_PER_SOURCE]:
                            pending_sitemaps.append((child_url, depth + 1))
                    raw_documents.append(
                        _build_raw_document(
                            outlet_key=outlet_key,
                            raw_document_index=len(raw_documents) + 1,
                            source_url=request_url,
                            payload=document_text,
                            row_count=len(child_urls),
                            window_start=window_start,
                        )
                    )
                    continue

                entries = list(_iter_sitemap_entries_from_root(root))
                outlet_entry_count += len(entries)
                outlet_window_entry_count += _count_window_entries(
                    entries,
                    window_start=window_start,
                    window_end=window_end,
                )
                raw_document = _build_raw_document(
                    outlet_key=outlet_key,
                    raw_document_index=len(raw_documents) + 1,
                    source_url=request_url,
                    payload=document_text,
                    row_count=len(entries),
                    window_start=window_start,
                )
                raw_documents.append(raw_document)
                fetched_documents.append(
                    CuratedFetchedDocument(
                        outlet_key=outlet_key,
                        raw_document=raw_document,
                        entries=tuple(entries),
                    )
                )

        outlet_snapshots.append(
            CuratedOutletSnapshot(
                outlet_key=outlet.outlet_key,
                display_name=outlet.display_name,
                connector_capability=outlet.connector_capability,
                documents_fetched=outlet_documents_fetched,
                documents_parsed=outlet_documents_parsed,
                entry_count=outlet_entry_count,
                window_entry_count=outlet_window_entry_count,
                warning_count=outlet_warning_count,
                error_types=tuple(sorted(outlet_error_types)),
                request_urls=tuple(dict.fromkeys(outlet_request_urls)),
            )
        )

    return CuratedSourceBundle(
        raw_documents=tuple(raw_documents),
        fetched_documents=tuple(fetched_documents),
        outlet_snapshots=tuple(outlet_snapshots),
        request_urls=tuple(dict.fromkeys(request_urls)),
        warning_count=warning_count,
        successful_documents=successful_documents,
    )


def match_candidate_against_curated_bundle(
    bundle: CuratedSourceBundle,
    candidate_case: CandidateQueryCase,
    *,
    include_raw_documents: bool = False,
) -> ProviderQueryResult:
    """Match one candidate against a shared curated source bundle."""
    if bundle.successful_documents == 0:
        return ProviderQueryResult(
            provider=_PROVIDER,
            provider_tier=_PROVIDER_TIER,
            status="error",
            request_url=";".join(bundle.request_urls),
            error_type="all_connectors_failed",
            error_message="No curated RSS or sitemap connector succeeded.",
            warning_count=bundle.warning_count,
            raw_documents=bundle.raw_documents if include_raw_documents else (),
            outlet_diagnostics=tuple(
                _build_outlet_diagnostic(outlet_snapshot=snapshot, hit_count=0)
                for snapshot in bundle.outlet_snapshots
            ),
        )

    query_text = build_generic_news_query(
        candidate_case.full_name,
        candidate_case.commune_name,
    )
    hits: list[SearchHit] = []
    verification_cache: dict[str, tuple[bool, str]] = {}
    verification_fetch_count = 0
    hit_count_by_outlet = {snapshot.outlet_key: 0 for snapshot in bundle.outlet_snapshots}

    for fetched_document in bundle.fetched_documents:
        verification_fetch_count, _, hit_count = _append_hits_from_entries(
            entries=fetched_document.entries,
            outlet_key=fetched_document.outlet_key,
            query_text=query_text,
            candidate_case=candidate_case,
            raw_document_key=fetched_document.raw_document.raw_document_key,
            hits=hits,
            verification_cache=verification_cache,
            verification_fetch_count=verification_fetch_count,
        )
        hit_count_by_outlet[fetched_document.outlet_key] += hit_count

    outlet_diagnostics = tuple(
        _build_outlet_diagnostic(
            outlet_snapshot=snapshot,
            hit_count=hit_count_by_outlet.get(snapshot.outlet_key, 0),
        )
        for snapshot in bundle.outlet_snapshots
    )

    return ProviderQueryResult(
        provider=_PROVIDER,
        provider_tier=_PROVIDER_TIER,
        status="success_hits" if hits else "success_zero",
        hits=tuple(hits),
        request_url=";".join(bundle.request_urls),
        raw_documents=bundle.raw_documents if include_raw_documents else (),
        warning_count=bundle.warning_count,
        outlet_diagnostics=outlet_diagnostics,
    )


def search_curated_outlets(candidate_case: CandidateQueryCase) -> ProviderQueryResult:
    """Search committed RSS and sitemap connectors for one candidate."""
    bundle = fetch_curated_source_bundle(
        window_start=candidate_case.window_start,
        window_end=candidate_case.window_end,
    )
    return match_candidate_against_curated_bundle(
        bundle,
        candidate_case,
        include_raw_documents=True,
    )
