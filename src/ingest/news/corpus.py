"""Europresse-first news corpus contracts and normalization helpers.

This module centers the corpus on restricted archive exports rather than live
provider discovery. Europresse batches may arrive as CSV, XLSX, HTML, TXT, or
PDF files rather than stable public URLs, so URL-based identity can no longer
be the primary article contract.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pandas as pd

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - depends on local environment
    BeautifulSoup = None

try:
    import trafilatura
except ImportError:  # pragma: no cover - depends on local environment
    trafilatura = None

try:
    from pdfminer.high_level import extract_text as _pdfminer_extract_text
    from pdfminer.pdfparser import PDFException
    from pdfminer.psparser import PSException

    _PDFMINER_AVAILABLE = True
    _PDFMINER_EXCEPTIONS: tuple[type[BaseException], ...] = (
        PDFException,
        PSException,
        OSError,
        UnicodeError,
        ValueError,
    )
except ImportError:  # pragma: no cover - optional dependency; install pdfminer-six
    _PDFMINER_AVAILABLE = False
    _PDFMINER_EXCEPTIONS = (OSError, UnicodeError, ValueError)

from src.config.settings import (
    ANALYSIS_END_DATE,
    ANALYSIS_START_DATE,
    DQ_MAX_NULL_RATE,
    DQ_MIN_ARTICLE_TEXT_LENGTH,
    NEWS_PDF_INSPECTION_MAX_PAGES,
    SCRAPE_REQUEST_TIMEOUT_SECONDS,
)
from src.ingest.news.models import (
    ArticleFetchResult,
    ImportBatchFile,
    ImportBatchInspection,
    NewsImportManifest,
    SearchHit,
)
from src.ingest.news.normalize import (
    canonicalize_url,
    now_iso_utc,
)
from src.ingest.news.normalize import (
    stable_md5 as _stable_md5,
)
from src.ingest.news.queries import normalize_text_for_match
from src.transform._exceptions import DataQualityError

logger = logging.getLogger(__name__)

_PARSER_VERSION = "news_corpus_v1"
_DEFAULT_ANALYSIS_WINDOW_START = date.fromisoformat(ANALYSIS_START_DATE)
_DEFAULT_ANALYSIS_WINDOW_END = date.fromisoformat(ANALYSIS_END_DATE)
_TABLE_EXTENSIONS = {".csv", ".xlsx"}
_DOCUMENT_EXTENSIONS = {".html", ".htm", ".txt"}
_PDF_EXTENSIONS = {".pdf"}
_SENTENCE_BOUNDARY_PATTERN = re.compile(r"(?<=[.!?])\s+|\n+")
# Legacy PostScript text operator — only matches old-style PDFs; modern CIDFont
# PDFs (e.g. Europresse exports) use Unicode-mapped hex streams invisible to this regex.
_PDF_TEXT_PATTERN = re.compile(rb"\(([^()]*)\)\s*Tj")
_HTTP_URL_PATTERN = re.compile(r"https?://", re.IGNORECASE)
_HTTP_URL_WITH_COMMON_SUFFIX_PATTERN = re.compile(
    r"https?://.+?\.(?:html?|php|aspx?)(?:[?#][^<>\s\"']*)?",
    re.IGNORECASE,
)
_HTTP_URL_FALLBACK_PATTERN = re.compile(r"https?://[^<>\s\"']+", re.IGNORECASE)
_HTTP_URL_CONTINUATION_PATTERN = re.compile(r"^[A-Za-z0-9._~:/?#\[\]@!$&'()*+,;=%-]+$")
# ── Europresse batch-PDF article structure ────────────────────────────────────
# CEDROM-SNi Europresse exports embed multiple articles per PDF.  pdfminer maps
# the copyright bullet (•, U+2022) that Europresse uses as a prefix for metadata
# fields.  The actual character in the raw PDF glyphs resolves to U+2022.
#
# Each article's header, in order:
#   • YYYY Outlet. Tous droits réservés.    ← copyright line
#   news•YYYYMMDD•PR•HASH                  ← article ID
#   [Source name / type block — first occurrence per source only]
#   [French day name] D Month YYYY          ← date with day-name prefix
#   Outlet name                             ← outlet
#   • p. PAGE_REF                           ← page reference
#   • N words                               ← word-count anchor (our primary split key)
#
# After the anchor: Page/code reference lines, then title, author, body text.
# Body ends at the CEDROM-SNi boilerplate footer: "This document is destined…".
_EUROPRESSE_WORD_COUNT_PATTERN = re.compile(
    r"(?:\u2022|\u00e2\u20ac\u00a2)\s+(\d+)\s+words"
)
_EUROPRESSE_DOCUMENT_COUNT_PATTERN = re.compile(
    r"\b(\d+)\s+documents?\b",
    re.IGNORECASE,
)
# Day-name prefix is optional. Some Europresse web-reference headers localize
# the UI date in English even when the article body is French.
_EUROPRESSE_DATE_PATTERN = re.compile(
    r"(?:(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+)?"
    r"(\d{1,2}\s+(?:janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t"
    r"|septembre|octobre|novembre|d[eé]cembre)\s+\d{4})"
    r"|(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+)?"
    r"((?:January|February|March|April|May|June|July|August|September|October"
    r"|November|December)\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)
_EUROPRESSE_FOOTER_PATTERN = re.compile(
    r"This document is destined for the exclusive use",
    re.IGNORECASE,
)
# Page reference lines that appear between the anchor and the article title.
# Europresse prints page codes either on their own line ("lyoe18") or inline
# with the label ("Page lyon23") when multiple page references exist.
# Also appears mid-article when an article spans multiple PDF pages.
_EUROPRESSE_PAGE_REF_PATTERN = re.compile(
    r"^\s*(?:Page(?:\s+\w+)?|[a-z]{3,6}\d{1,3})\s*$",
    re.IGNORECASE,
)
# Europresse metadata labels printed at the top of each article's page.
# Each label is immediately followed by its value on the next line.
# "Origin" may span two value lines (city + geographic suffix).
_EUROPRESSE_METADATA_LABEL_PATTERN = re.compile(
    r"^\s*(?:Source\s+name|Source\s+type|Periodicity|Geographical\s+coverage|Origin)\s*$",
    re.IGNORECASE,
)
# Lines to skip unconditionally in the post-anchor preamble (no following value
# line).  These include CEDROM-SNi legal boilerplate, page headers, article IDs,
# known metadata values, and standalone French date lines.
_EUROPRESSE_SKIP_LINE_PATTERN = re.compile(
    r"^\s*(?:"
    r"Saved\s+documents.*|"  # page header: "Saved documents by…"
    r"(?:news|web)\W.*\d{8}.*|"  # article ID: "news•20251109•PR•…"
    r".*Tous\s+droits\s+r\S*serv\S*s.*|"  # copyright line
    r"The\s+present\s+document.*|"  # CEDROM-SNi copyright sentence (line 1)
    r"protected\s+under.*|"  # copyright sentence (line 2)
    r"and\s+conventions[.;]?\s*|"  # copyright sentence (line 3)
    r"used\s+for\s+any\s+other.*|"  # copyright sentence (line 4)
    r"Read\s+more\s*|"  # web-article "Read more" link text
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday).*\d{4}.*|"
    # Standalone French date lines (optional weekday prefix):
    r"(?:(?:lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)\s+)?"
    r"\d{1,2}\s+(?:janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t"
    r"|septembre|octobre|novembre|d[eé]cembre)\s+\d{4}|"
    r"Daily|Weekly|Monthly|Irregular|Continuously|"  # periodicity values
    r"Regional|National|International|"  # coverage values
    r"Press.*|"  # source-type values
    r"Newspapers|Magazines"  # standalone source-type values
    r")\s*$",
    re.IGNORECASE,
)
_EUROPRESSE_TITLE_TERMINATOR_PATTERN = re.compile(r'[.!?;:»"”]$')
_EUROPRESSE_NAME_TOKEN_PATTERN = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ'’.-]+")
_EUROPRESSE_PHOTO_CREDIT_PATTERN = re.compile(
    r"\bPhoto\s+[A-ZÀ-ÖØ-Þ][\wÀ-ÖØ-öø-ÿ'’.-]*"
    r"(?:\s+[A-ZÀ-ÖØ-Þ][\wÀ-ÖØ-öø-ÿ'’.-]*){0,4}\s*\.?",
    re.IGNORECASE,
)
_EUROPRESSE_SEGMENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    IndexError,
    TypeError,
    ValueError,
)
_PAYLOAD_TEXT_PREVIEW_CHARS = 500
_FRENCH_STOPWORDS = frozenset(
    {
        "a",
        "au",
        "aux",
        "avec",
        "ce",
        "ces",
        "dans",
        "de",
        "des",
        "du",
        "elle",
        "en",
        "et",
        "est",
        "la",
        "le",
        "les",
        "maire",
        "municipales",
        "pour",
        "que",
        "qui",
        "sur",
        "une",
        "ville",
    }
)
_FRENCH_MONTH_NUMBERS = {
    "janvier": "01",
    "fevrier": "02",
    "fvrier": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "aout": "08",
    "aot": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "decembre": "12",
    "dcembre": "12",
}
_FRENCH_WEEKDAYS = (
    "lundi",
    "mardi",
    "mercredi",
    "jeudi",
    "vendredi",
    "samedi",
    "dimanche",
)
_BRONZE_SOURCE_COLUMNS = [
    "batch_id",
    "source_system",
    "source_record_id",
    "local_record_key",
    "source_record_hash",
    "source_native_payload",
    "raw_title",
    "raw_body_text",
    "raw_published_at",
    "raw_outlet",
    "raw_article_url",
    "raw_author",
    "raw_language",
    "raw_file_path",
    "raw_file_type",
    "import_classification",
    "parser_name",
    "parser_version",
    "rights_class",
    "_ingested_at",
]
_SOURCE_REJECTED_COLUMNS = [
    "source_record_id",
    "batch_id",
    "source_system",
    "raw_file_path",
    "raw_title",
    "raw_published_at",
    "raw_outlet",
    "raw_article_url",
    "_rejection_reason",
]
_FACT_ARTICLE_SOURCE_COLUMNS = [
    "article_source_id",
    "batch_id",
    "source_system",
    "source_record_id",
    "source_record_hash",
    "title",
    "title_normalized",
    "body_text",
    "body_text_hash",
    "has_full_text",
    "published_at_normalized",
    "published_date",
    "outlet_name",
    "outlet_name_normalized",
    "article_url",
    "canonical_url",
    "author",
    "language",
    "acquisition_method",
    "parser_status",
    "rights_class",
    "raw_file_path",
    "raw_file_type",
    "import_classification",
    "parser_name",
    "parser_version",
    "source_native_payload",
    "_ingested_at",
]
_FACT_ARTICLE_COLUMNS = [
    "canonical_article_id",
    "duplicate_group_id",
    "dedup_method",
    "canonical_url",
    "representative_url",
    "representative_source_record_id",
    "title",
    "title_normalized",
    "body_text",
    "body_text_hash",
    "has_full_text",
    "published_at",
    "published_date",
    "domain",
    "outlet_name",
    "outlet_name_normalized",
    "language",
    "rights_class",
    "source_record_count",
    "source_system_count",
    "source_systems",
    "acquisition_methods",
    "partition_date",
]
_WEB_FETCH_CACHE_COLUMNS = [
    "canonical_url",
    "source_url",
    "fetch_status",
    "http_status",
    "body_text",
    "body_text_hash",
    "body_text_preview",
    "body_text_length",
    "fetched_at",
    "extractor_name",
    "extractor_version",
    "error_type",
]
_DISCOVERY_COLUMNS = [
    "discovery_id",
    "canonical_article_id",
    "leader_id",
    "provider",
    "provider_tier",
    "provider_status",
    "provider_error_type",
    "provider_warning_count",
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


def _derive_rights_class(access_level: str) -> str:
    """Map batch access-level labels to the restricted/public rights contract."""
    normalized = normalize_text_for_match(access_level)
    if any(token in normalized for token in ("restricted", "subscriber", "license")):
        return "restricted_local"
    if "public" in normalized:
        return "public"
    return "restricted_local"


def _deserialize_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string into a UTC-aware datetime."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clean_text(value: str) -> str:
    """Normalize free text into stable whitespace without altering semantics."""
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _coerce_optional_str(value: object) -> str:
    """Convert nullable scalars into a safe string for JSON and Parquet outputs."""
    if value is None:
        return ""
    if isinstance(value, list | tuple | set | dict):
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        return ""
    return str(value)


def _normalize_payload(payload: dict[str, object]) -> str:
    """Serialize a source-native payload deterministically for hashing and audit."""
    return json.dumps(
        _summarize_payload_for_storage(payload),
        ensure_ascii=False,
        sort_keys=True,
    )


def _summarize_payload_for_storage(payload: object) -> object:
    """Redact large payload text while keeping a stable audit summary.

    Import payloads are stored for provenance, not for reproducing full article
    text. Large string fields are therefore reduced to previews plus length so
    the persisted audit artifact stays compliant with the repository's
    data-minimisation promise.
    """
    if isinstance(payload, dict):
        return {
            str(key): _summarize_payload_for_storage(value)
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [_summarize_payload_for_storage(value) for value in payload]
    if isinstance(payload, str):
        cleaned_value = _clean_text(payload)
        if len(cleaned_value) <= _PAYLOAD_TEXT_PREVIEW_CHARS:
            return cleaned_value
        return {
            "text_preview": cleaned_value[:_PAYLOAD_TEXT_PREVIEW_CHARS],
            "text_length": len(cleaned_value),
            "truncated": True,
        }
    return payload


def _derive_domain(article_url: str, outlet_name_normalized: str) -> str:
    """Prefer the URL host when present, otherwise fall back to outlet name."""
    if article_url:
        return urlparse(article_url).netloc.lower().removeprefix("www.")
    return outlet_name_normalized


def _detect_french_language(raw_language: str, title: str, body_text: str) -> str:
    """Apply a lightweight French-language heuristic without network or models."""
    normalized_language = normalize_text_for_match(raw_language)
    if normalized_language in {"fr", "fra", "francais", "francais fr", "french"}:
        return "fr"

    tokenized_text = normalize_text_for_match(f"{title} {body_text}").split()
    if not tokenized_text:
        return "unknown"

    stopword_hits = sum(1 for token in tokenized_text if token in _FRENCH_STOPWORDS)
    if stopword_hits >= 3 and (stopword_hits / len(tokenized_text)) >= 0.03:
        return "fr"
    return "unknown"


def _parse_french_literal_date(value: str) -> pd.Timestamp | pd.NaT:
    """Parse French calendar literals without depending on system locale."""
    normalized_value = normalize_text_for_match(value)
    date_tokens = normalized_value.split()
    if date_tokens and date_tokens[0] in _FRENCH_WEEKDAYS:
        date_tokens = date_tokens[1:]
    if len(date_tokens) < 3:
        return pd.NaT

    day_token = date_tokens[0]
    year_token = date_tokens[-1]
    month_token = "".join(date_tokens[1:-1])
    if not re.fullmatch(r"\d{1,2}", day_token):
        return pd.NaT
    if not re.fullmatch(r"\d{4}", year_token):
        return pd.NaT

    month_number = _FRENCH_MONTH_NUMBERS.get(month_token)
    if month_number is None:
        return pd.NaT

    iso_date = f"{year_token}-{month_number}-{int(day_token):02d}"
    return pd.to_datetime(
        iso_date,
        utc=True,
        errors="coerce",
        format="%Y-%m-%d",
    )


def _parse_timestamp(value: str) -> pd.Timestamp | pd.NaT:
    """Parse article timestamps with French exports in mind."""
    cleaned_value = _clean_text(value)
    if not cleaned_value:
        return pd.NaT

    parsed_french_literal = _parse_french_literal_date(cleaned_value)
    if not pd.isna(parsed_french_literal):
        return parsed_french_literal

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", cleaned_value):
        parsed = pd.to_datetime(
            cleaned_value,
            utc=True,
            errors="coerce",
            format="%Y-%m-%d",
        )
    else:
        parsed = pd.to_datetime(
            cleaned_value,
            utc=True,
            errors="coerce",
            dayfirst=True,
        )
    if pd.isna(parsed):
        return pd.NaT
    return parsed


def _pick_table_column(
    columns: list[str],
    *,
    include: tuple[str, ...],
    exclude: tuple[str, ...] = (),
) -> str | None:
    """Find the best matching column using token inclusion/exclusion heuristics."""
    for column_name in columns:
        normalized = normalize_text_for_match(column_name)
        if not normalized:
            continue
        if not any(token in normalized for token in include):
            continue
        if any(token in normalized for token in exclude):
            continue
        return column_name
    return None


def _resolve_table_columns(table_df: pd.DataFrame) -> dict[str, str | None]:
    """Map source columns to the canonical article fields expected downstream."""
    columns = [str(column_name) for column_name in table_df.columns]
    return {
        "title": _pick_table_column(
            columns,
            include=("title", "headline", "titre"),
            exclude=("journal",),
        ),
        "body": _pick_table_column(
            columns,
            include=(
                "full text",
                "texte",
                "content",
                "body",
                "article",
                "document",
                "text",
            ),
            exclude=("title", "headline", "url", "date"),
        ),
        "published_at": _pick_table_column(
            columns,
            include=("date", "published", "publication", "parution"),
            exclude=("url", "journal"),
        ),
        "outlet": _pick_table_column(
            columns,
            include=(
                "journal",
                "source",
                "media",
                "newspaper",
                "publication",
                "outlet",
            ),
            exclude=("date", "published"),
        ),
        "article_url": _pick_table_column(
            columns,
            include=("url", "link", "lien", "permalink"),
        ),
        "author": _pick_table_column(columns, include=("author", "auteur", "byline")),
        "language": _pick_table_column(columns, include=("language", "langue")),
    }


def _fallback_body_from_row(
    row: pd.Series,
    *,
    resolved_columns: dict[str, str | None],
) -> str:
    """Reconstruct body text when the export has no obvious dedicated text column."""
    excluded_columns = {
        value for value in resolved_columns.values() if value is not None
    }
    body_candidates = []
    for column_name, value in row.items():
        if column_name in excluded_columns:
            continue
        cell_value = _clean_text(_coerce_optional_str(value))
        if len(cell_value) < 40:
            continue
        body_candidates.append(cell_value)
    return "\n".join(body_candidates)


def _extract_html_document_fields(document_text: str) -> dict[str, str]:
    """Extract title/body/date from local HTML exports without network calls."""
    if trafilatura is not None:
        extracted_text = trafilatura.extract(
            document_text,
            include_comments=False,
            include_tables=False,
            favor_precision=True,
        )
    else:  # pragma: no cover - fallback depends on local environment
        extracted_text = None

    title = ""
    published_at = ""
    if BeautifulSoup is not None:
        soup = BeautifulSoup(document_text, "html.parser")
        if soup.title and soup.title.string:
            title = _clean_text(soup.title.string)
        if not title:
            heading = soup.find(["h1", "h2"])
            if heading is not None:
                title = _clean_text(heading.get_text(" ", strip=True))
        date_meta = (
            soup.find("meta", attrs={"property": "article:published_time"})
            or soup.find("meta", attrs={"name": "date"})
            or soup.find("meta", attrs={"name": "publish-date"})
        )
        if date_meta is not None:
            published_at = _coerce_optional_str(date_meta.get("content"))
        body_text = _clean_text(extracted_text or soup.get_text("\n", strip=True))
    else:  # pragma: no cover - fallback depends on local environment
        body_text = _clean_text(extracted_text or document_text)

    return {
        "title": title,
        "body_text": body_text,
        "published_at": published_at,
    }


def _trim_extracted_url(raw_url: str) -> str:
    """Return a cleaned URL extracted from wrapped PDF text."""
    compact_url = re.sub(r"\s+", "", raw_url)
    common_suffix_match = _HTTP_URL_WITH_COMMON_SUFFIX_PATTERN.search(compact_url)
    if common_suffix_match is not None:
        return common_suffix_match.group(0).rstrip(".,);:]")
    fallback_match = _HTTP_URL_FALLBACK_PATTERN.search(compact_url)
    if fallback_match is None:
        return ""
    return fallback_match.group(0).rstrip(".,);:]")


def _extract_first_url_from_lines(lines: list[str]) -> str:
    """Extract the first URL from Europresse lines with PDF line wrapping."""
    for line_index, line in enumerate(lines):
        if _HTTP_URL_PATTERN.search(line) is None:
            continue

        fragments: list[str] = []
        for candidate_line in lines[line_index : line_index + 24]:
            stripped_line = candidate_line.strip()
            if not stripped_line:
                continue
            if fragments and re.match(
                r"^(This document|Web-?sites|are not hosted|Page\s|Source\s)",
                stripped_line,
                re.IGNORECASE,
            ):
                break
            if not fragments:
                url_start = _HTTP_URL_PATTERN.search(stripped_line)
                if url_start is None:
                    continue
                stripped_line = stripped_line[url_start.start() :]
            elif _HTTP_URL_CONTINUATION_PATTERN.fullmatch(stripped_line) is None:
                break
            fragments.append(stripped_line)

            candidate_url = _trim_extracted_url("".join(fragments))
            if candidate_url and re.search(
                r"\.(?:html?|php|aspx?)(?:[?#].*)?$",
                candidate_url,
                re.IGNORECASE,
            ):
                return candidate_url

        candidate_url = _trim_extracted_url("".join(fragments))
        if candidate_url:
            return candidate_url
    return ""


def _extract_first_url_from_text(text: str) -> str:
    """Extract a URL from one-line or line-preserving article text."""
    return _extract_first_url_from_lines(str(text or "").splitlines())


def _extract_pdf_text(file_path: Path, *, maxpages: int = 0) -> str:
    """Extract text from a PDF file with line-break structure preserved.

    pdfminer.six handles CIDFont/ToUnicode glyph mapping used by modern press
    archive exports such as Europresse.  The legacy regex fallback handles older
    PostScript-style PDFs whose text appears as ``(string) Tj`` operators.

    The returned string preserves newlines so downstream callers can perform
    line-based parsing (e.g. Europresse article segmentation).  Callers that
    need a single-line clean string should apply ``_clean_text()`` themselves.

    Args:
        file_path: Path to the PDF file.
        maxpages: Maximum number of pages to extract. ``0`` means all pages.

    Returns:
        Extracted text with newlines preserved, or empty string on failure.
    """
    if _PDFMINER_AVAILABLE:
        try:
            raw_text = _pdfminer_extract_text(str(file_path), maxpages=maxpages)
            if raw_text and raw_text.strip():
                # Normalize form-feed page separators to newlines for uniform parsing.
                return raw_text.replace("\x0c", "\n")
        except _PDFMINER_EXCEPTIONS as exc:
            logger.warning(
                "pdfminer failed on %s: %s — falling back to legacy regex extractor",
                file_path.name,
                exc,
            )
    # Legacy fallback: old PostScript-style PDFs with ``(string) Tj`` operators.
    pdf_bytes = file_path.read_bytes()
    extracted_chunks = [
        match.decode("latin-1", errors="ignore")
        for match in _PDF_TEXT_PATTERN.findall(pdf_bytes)
    ]
    return " ".join(extracted_chunks)


def _pdf_has_text_layer(file_path: Path) -> bool:
    """Detect whether a PDF contains extractable text.

    Tries pdfminer.six first (handles modern CIDFont encodings), then falls back
    to scanning for legacy PostScript ``(string) Tj`` operators.

    Args:
        file_path: Path to the PDF file.

    Returns:
        True if any text could be extracted from the PDF.
    """
    if _PDFMINER_AVAILABLE:
        try:
            raw_text = _pdfminer_extract_text(str(file_path))
            if raw_text and raw_text.strip():
                return True
            # pdfminer returned empty — fall through to the legacy check.
        except _PDFMINER_EXCEPTIONS as exc:
            logger.debug("pdfminer could not parse %s: %s", file_path.name, exc)
    # Legacy fallback: scan for PostScript-style text operators.
    pdf_bytes = file_path.read_bytes()
    if b"/Font" not in pdf_bytes:
        return False
    return bool(_PDF_TEXT_PATTERN.search(pdf_bytes))


def _is_europresse_format(text: str) -> bool:
    """Detect whether extracted PDF text is a structured Europresse export.

    Europresse alert PDFs (CEDROM-SNi) embed one or many articles per file.  Each article
    header contains a ``• N words`` word-count line with a U+2022 bullet prefix.
    The full segmenter must recover a dated article so arbitrary PDFs with a
    coincidental word-count bullet are not misclassified.

    Args:
        text: Raw text extracted from the PDF (newlines preserved).

    Returns:
        True if the segmenter recovers at least one article with structured
        Europresse content.
    """
    if _is_europresse_cover_or_summary(text):
        return True

    articles = _segment_europresse_articles(text)
    if any(
        article.get("published_at")
        and article.get("title")
        and article.get("declared_word_count")
        for article in articles
    ):
        return True
    return len(articles) >= 2 and all(
        article.get("title") and article.get("declared_word_count")
        for article in articles
    )


def _is_europresse_cover_or_summary(text: str) -> bool:
    """Detect Europresse cover pages that precede article-level anchors.

    Args:
        text: Raw text extracted from the PDF inspection sample.

    Returns:
        True when the sample contains the Europresse export cover contract and
        a declared document count.
    """
    declared_document_count = _extract_europresse_declared_document_count(text)
    if declared_document_count is None:
        return False

    normalized_text = _clean_text(text).lower()
    if "saved documents" not in normalized_text:
        return False
    return "cedrom-sni" in normalized_text or "exclusive use" in normalized_text


def _extract_europresse_declared_document_count(full_text: str) -> int | None:
    """Read the declared document count from the Europresse cover/summary pages."""
    header_window = full_text[:4_000]
    document_count_match = _EUROPRESSE_DOCUMENT_COUNT_PATTERN.search(header_window)
    if document_count_match is None:
        return None
    return int(document_count_match.group(1))


def _extract_europresse_date_literal(date_match: re.Match[str]) -> str:
    """Return the localized Europresse date literal captured by a regex match."""
    for matched_group in date_match.groups():
        if matched_group:
            return matched_group
    return ""


def _looks_like_europresse_byline(line: str) -> bool:
    """Detect compact author-credit lines so they do not leak into article text."""
    cleaned_line = _clean_text(line)
    if not cleaned_line or cleaned_line.lower().startswith("photo "):
        return False

    raw_tokens = _EUROPRESSE_NAME_TOKEN_PATTERN.findall(cleaned_line)
    if not 2 <= len(raw_tokens) <= 4:
        return False
    if any(any(character.isdigit() for character in token) for token in raw_tokens):
        return False

    connective_tokens = {"d", "de", "du", "des", "la", "le", "les"}
    capitalized_token_count = 0

    for token in raw_tokens:
        normalized_token = normalize_text_for_match(token)
        if not normalized_token:
            return False
        if normalized_token in connective_tokens:
            continue
        if token.isupper() or token[0].isupper():
            capitalized_token_count += 1
            continue
        return False

    return capitalized_token_count >= 2 and cleaned_line[-1] not in ".!?:;"


def _looks_like_europresse_title_continuation(line: str) -> bool:
    """Return whether a short line is likely to continue a wrapped headline."""
    cleaned_line = _clean_text(line)
    normalized_line = normalize_text_for_match(cleaned_line)
    if not normalized_line or _looks_like_europresse_byline(cleaned_line):
        return False

    word_count = len(normalized_line.split())
    if word_count > 14:
        return False
    if cleaned_line.lower().startswith("photo "):
        return False
    return not (cleaned_line.endswith(".") and word_count >= 6)


def _clean_europresse_body_text(body_lines: list[str]) -> str:
    """Collapse segmented body lines while removing layout-only Europresse noise."""
    body_text = " ".join(_clean_text(line) for line in body_lines if _clean_text(line))
    body_text = _EUROPRESSE_PHOTO_CREDIT_PATTERN.sub("", body_text)
    # pdfminer sometimes splits drop caps into ``V illeurbanne`` or ``L e``.
    body_text = re.sub(r"\b([A-Z])\s+([a-zà-öø-ÿ]{2,})\b", r"\1\2", body_text)
    body_text = re.sub(r"\s+([,.;:!?»])", r"\1", body_text)
    return _clean_text(body_text)


def _extract_europresse_title_and_body(
    effective_lines: list[str],
) -> tuple[str, str]:
    """Split Europresse content lines into a stitched title and cleaned body."""
    if not effective_lines:
        return "", ""

    title_parts = [_clean_text(effective_lines[0])]
    body_start_idx = 1

    for line_index, line in enumerate(effective_lines[1:4], start=1):
        if _EUROPRESSE_TITLE_TERMINATOR_PATTERN.search(title_parts[-1]):
            break
        if not _looks_like_europresse_title_continuation(line):
            break
        title_parts.append(_clean_text(line))
        body_start_idx = line_index + 1

    body_lines = effective_lines[body_start_idx:]
    if body_lines and _looks_like_europresse_byline(body_lines[0]):
        body_lines = body_lines[1:]

    return _clean_text(" ".join(title_parts)), _clean_europresse_body_text(body_lines)


def _build_europresse_parse_error_article(
    *,
    article_index: int,
    declared_word_count: str,
    error: BaseException,
) -> dict[str, str]:
    """Build a quarantinable row when one Europresse article segment fails."""
    return {
        "outlet": "",
        "published_at": "",
        "title": "",
        "body_text": "",
        "article_url": "",
        "declared_word_count": declared_word_count,
        "article_index": str(article_index),
        "parse_error": type(error).__name__,
    }


def _segment_europresse_articles(full_text: str) -> list[dict[str, str]]:
    """Segment a multi-article Europresse batch PDF into per-article records.

    CEDROM-SNi Europresse alert exports embed multiple articles per PDF.  Each
    article has a structured header that ends with a ``• N words`` line.  Article
    body text follows and ends at the CEDROM-SNi boilerplate footer
    (``This document is destined for the exclusive use``).

    Segmentation strategy:
    - ``• N words`` is the anchor: everything after it (until the footer) is
      title + author + body text.
    - French or English UI date (with day-name prefix) in the 800 chars before
      the anchor gives the publication date.
    - The first non-bullet non-empty line after the date gives the outlet name.
    - ``Page`` and page-code lines immediately after the anchor are skipped.
    - The first 1–2 non-empty post-Page lines become the title; the rest is body.

    Args:
        full_text: Raw text extracted from a multi-article Europresse PDF,
            with page breaks normalized to newlines.

    Returns:
        List of article dicts with keys: ``outlet``, ``published_at``,
        ``title``, ``body_text``, ``article_url``, ``declared_word_count``,
        ``article_index``.
    """
    wc_matches = list(_EUROPRESSE_WORD_COUNT_PATTERN.finditer(full_text))
    if not wc_matches:
        return []

    # Pre-compute footer positions so we can do O(1) lookup per article.
    footer_positions = [
        m.start() for m in _EUROPRESSE_FOOTER_PATTERN.finditer(full_text)
    ]

    articles: list[dict[str, str]] = []

    for art_num, wc_match in enumerate(wc_matches):
        wc_start = wc_match.start()
        wc_end = wc_match.end()
        declared_word_count = wc_match.group(1)

        # ── Date and outlet: scan 800 chars before the word-count anchor ─────
        header_region = full_text[max(0, wc_start - 800) : wc_start]

        # Date: last French-date occurrence in the header region (closest to anchor).
        article_date = ""
        last_date_end = -1
        for date_match in _EUROPRESSE_DATE_PATTERN.finditer(header_region):
            article_date = _extract_europresse_date_literal(date_match)
            last_date_end = date_match.end()

        # Outlet: first non-empty, non-bullet line after the date in the header region.
        # Europresse prints some outlet names across two lines:
        #   (a) "L'intern@ute (site web) -\nL'Internaute" — trailing dash signals continuation.
        #   (b) "France 3 Régions (site web\nréf.) - France 3 Regions" — unclosed paren.
        # In both cases we join the next non-empty non-bullet line to produce the full name.
        outlet = ""
        if last_date_end >= 0:
            after_date = header_region[last_date_end:]
            non_empty_lines = [
                line.strip()
                for line in after_date.splitlines()
                if line.strip()
                and not line.strip().startswith("\u2022")
                and len(line.strip()) > 2
            ]
            if non_empty_lines:
                outlet = non_empty_lines[0]
                needs_continuation = outlet.endswith("-") or outlet.count(
                    "("
                ) > outlet.count(")")
                if needs_continuation and len(non_empty_lines) > 1:
                    outlet = f"{outlet} {non_empty_lines[1]}".strip()

        # ── Title and body: everything from after the anchor to the footer ───
        # Body ends at the first boilerplate footer that follows the anchor.
        body_end_pos = len(full_text)
        for fp in footer_positions:
            if fp > wc_end:
                body_end_pos = fp
                break

        content_text = full_text[wc_end:body_end_pos]
        content_lines = content_text.splitlines()
        try:
            article_url = _extract_first_url_from_lines(content_lines)
        except _EUROPRESSE_SEGMENT_EXCEPTIONS as exc:
            logger.warning(
                "Failed to extract Europresse article URL article_index=%s error=%s",
                art_num,
                type(exc).__name__,
            )
            articles.append(
                _build_europresse_parse_error_article(
                    article_index=art_num,
                    declared_word_count=declared_word_count,
                    error=exc,
                )
            )
            continue

        # ── Skip post-anchor preamble before the article title ───────────────
        # Europresse places several preamble blocks after the word-count anchor:
        # (a) Page reference lines: "Page", "lyoe18", "Page lyon25".
        # (b) Boilerplate: page headers, article IDs, CEDROM-SNi copyright.
        # (c) Web outlet reference lines: "Actu.fr (site web réf.) - Actu (FR)".
        #     These may wrap across two lines; the line continuing after a web-
        #     ref line is also skipped when it is short (≤ 40 chars).
        # (d) Metadata block: "Source name" → outlet value (up to 2 lines) →
        #     "Source type" → value → "Periodicity" → value →
        #     "Geographical coverage" → value → "Origin" → 1–2 value lines.
        #     "Origin" skip count is determined by lookahead: if the next non-
        #     empty line ends with a hyphen it spans two lines.
        #
        # Rule for skip_value_lines > 0: if the line is itself a metadata
        # label, override and re-process as a label (handles outlets whose
        # name is shorter than the reserved skip window).
        skip_value_lines = 0  # lines to consume after a metadata label
        outlet_continuation = 0  # remaining lines for a split web outlet ref
        content_start_idx = 0

        for i, line in enumerate(content_lines):
            stripped = line.strip()
            if not stripped:
                continue

            # (i) Web outlet reference continuation (short lines after a ref).
            # outlet_continuation is set when a web-ref line is first detected.
            if outlet_continuation > 0 and len(stripped) <= 55:
                outlet_continuation -= 1
                content_start_idx = i + 1
                continue
            outlet_continuation = 0

            # (ii) Consume expected value lines after a metadata label.
            if skip_value_lines > 0:
                if _EUROPRESSE_METADATA_LABEL_PATTERN.match(stripped):
                    # The label's value was shorter than reserved — reset and
                    # fall through to process this line as a label.
                    skip_value_lines = 0
                else:
                    skip_value_lines -= 1
                    content_start_idx = i + 1
                    # If this value line begins a web-ref that spans more lines,
                    # set the continuation counter for subsequent fragments.
                    if re.search(
                        r"site\s+web", stripped, re.IGNORECASE
                    ) or stripped.endswith("(site"):
                        outlet_continuation = 2
                    continue

            # (iii) Page references.
            if _EUROPRESSE_PAGE_REF_PATTERN.match(stripped):
                content_start_idx = i + 1
                continue

            # (iv) Boilerplate and known metadata values.
            if _EUROPRESSE_SKIP_LINE_PATTERN.match(stripped):
                content_start_idx = i + 1
                continue

            # (v) Web outlet reference lines (single line with "site web"
            #     or first fragment of a split line ending with "(site").
            if re.search(r"site\s+web", stripped, re.IGNORECASE) or stripped.endswith(
                "(site"
            ):
                outlet_continuation = 2  # allow up to 2 continuation lines
                content_start_idx = i + 1
                continue

            # (vi) Metadata labels — consume their value line(s).
            if _EUROPRESSE_METADATA_LABEL_PATTERN.match(stripped):
                if re.match(r"^\s*Source\s+name\s*$", stripped, re.IGNORECASE):
                    # Outlet names can span two lines; reserve 2 skip slots.
                    skip_value_lines = 2
                elif re.match(r"^\s*Origin\s*$", stripped, re.IGNORECASE):
                    # Lookahead: multi-line origin ends with a trailing hyphen.
                    skip_value_lines = 1
                    for j in range(i + 1, min(i + 4, len(content_lines))):
                        next_s = content_lines[j].strip()
                        if next_s:
                            if next_s.endswith("-"):
                                skip_value_lines = 2
                            break
                else:
                    skip_value_lines = 1
                content_start_idx = i + 1
                continue

            # This line is article content — stop skipping preamble.
            break

        # Collect effective content lines, stripping any mid-article page
        # references that appear at the top of continuation pages.
        effective_lines = [
            line.strip()
            for line in content_lines[content_start_idx:]
            if line.strip() and not _EUROPRESSE_PAGE_REF_PATTERN.match(line.strip())
        ]

        # Split content into a stitched headline and a cleaned body payload.
        try:
            title, body_text = _extract_europresse_title_and_body(effective_lines)
        except _EUROPRESSE_SEGMENT_EXCEPTIONS as exc:
            logger.warning(
                "Failed to parse Europresse article body article_index=%s error=%s",
                art_num,
                type(exc).__name__,
            )
            articles.append(
                _build_europresse_parse_error_article(
                    article_index=art_num,
                    declared_word_count=declared_word_count,
                    error=exc,
                )
            )
            continue

        articles.append(
            {
                "outlet": outlet,
                "published_at": article_date,
                "title": title,
                "body_text": body_text,
                "article_url": article_url,
                "declared_word_count": declared_word_count,
                "article_index": str(art_num),
            }
        )

    return articles


def _build_source_record(
    *,
    manifest: NewsImportManifest,
    file_path: Path,
    file_type: str,
    classification: str,
    local_record_key: str,
    source_native_payload: dict[str, object],
    raw_title: str,
    raw_body_text: str,
    raw_published_at: str,
    raw_outlet: str,
    raw_article_url: str,
    raw_author: str,
    raw_language: str,
    parser_name: str,
) -> dict[str, object]:
    """Build one bronze news-source-record row from a parsed export unit."""
    payload_json = _normalize_payload(source_native_payload)
    source_record_hash = _stable_md5(payload_json)
    source_record_id = _stable_md5(
        f"{manifest.batch_id}|{file_path}|{local_record_key}|{source_record_hash}"
    )
    return {
        "batch_id": manifest.batch_id,
        "source_system": manifest.source_system,
        "source_record_id": source_record_id,
        "local_record_key": local_record_key,
        "source_record_hash": source_record_hash,
        "source_native_payload": payload_json,
        "raw_title": _clean_text(raw_title),
        "raw_body_text": _clean_text(raw_body_text),
        "raw_published_at": _clean_text(raw_published_at),
        "raw_outlet": _clean_text(raw_outlet),
        "raw_article_url": _clean_text(raw_article_url),
        "raw_author": _clean_text(raw_author),
        "raw_language": _clean_text(raw_language),
        "raw_file_path": str(file_path),
        "raw_file_type": file_type,
        "import_classification": classification,
        "parser_name": parser_name,
        "parser_version": _PARSER_VERSION,
        "rights_class": _derive_rights_class(manifest.access_level),
        "_ingested_at": now_iso_utc(),
    }


def write_news_import_manifest(
    manifest: NewsImportManifest,
    manifest_path: Path,
) -> Path:
    """Write a news import manifest to JSON.

    Args:
        manifest: Manifest contract to persist.
        manifest_path: Target JSON path.

    Returns:
        The written manifest path.
    """
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "batch_id": manifest.batch_id,
        "source_system": manifest.source_system,
        "window_start": manifest.window_start.isoformat(),
        "window_end": manifest.window_end.isoformat(),
        "exported_at": manifest.exported_at.astimezone(UTC).isoformat(),
        "operator": manifest.operator,
        "access_level": manifest.access_level,
        "file_paths": list(manifest.file_paths),
        "notes": manifest.notes,
    }
    with open(manifest_path, "w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, ensure_ascii=False, indent=2)
    return manifest_path


def load_news_import_manifest(manifest_path: Path) -> NewsImportManifest:
    """Load a news import manifest from JSON.

    Args:
        manifest_path: Path to ``news_import_manifest.json``.

    Returns:
        Parsed batch manifest.

    Raises:
        FileNotFoundError: If the manifest path does not exist.
        ValueError: If required fields are missing.
    """
    if not manifest_path.exists():
        raise FileNotFoundError(f"News import manifest not found: {manifest_path}")

    with open(manifest_path, encoding="utf-8") as file_handle:
        payload = json.load(file_handle)

    required_fields = {
        "batch_id",
        "source_system",
        "window_start",
        "window_end",
        "exported_at",
        "operator",
        "access_level",
        "file_paths",
        "notes",
    }
    missing_fields = sorted(required_fields - set(payload))
    if missing_fields:
        raise ValueError(
            "News import manifest is missing required fields: "
            + ", ".join(missing_fields)
        )

    return NewsImportManifest(
        batch_id=str(payload["batch_id"]),
        source_system=str(payload["source_system"]),
        window_start=date.fromisoformat(str(payload["window_start"])),
        window_end=date.fromisoformat(str(payload["window_end"])),
        exported_at=_deserialize_datetime(str(payload["exported_at"])),
        operator=str(payload["operator"]),
        access_level=str(payload["access_level"]),
        file_paths=tuple(str(path) for path in payload["file_paths"]),
        notes=str(payload.get("notes", "")),
    )


def inspect_import_batch(manifest: NewsImportManifest) -> ImportBatchInspection:
    """Inspect an import batch and classify its files before parsing.

    Args:
        manifest: Batch contract describing the files to inspect.

    Returns:
        Inspection result with one row per file plus parser mix counts.

    Raises:
        FileNotFoundError: If any referenced file path is missing.
    """
    inspected_files: list[ImportBatchFile] = []
    parser_mix = {
        "table_export": 0,
        "document_export": 0,
        "pdf_text_layer": 0,
        "pdf_europresse_batch": 0,
        "unsupported": 0,
    }

    total_files = len(manifest.file_paths)
    for file_index, file_path_str in enumerate(manifest.file_paths, start=1):
        file_path = Path(file_path_str)
        if not file_path.exists():
            raise FileNotFoundError(f"News import file not found: {file_path}")

        suffix = file_path.suffix.lower()
        if suffix in _TABLE_EXTENSIONS:
            inspected = ImportBatchFile(
                path=str(file_path),
                classification="table_export",
                file_type=suffix.lstrip("."),
            )
        elif suffix in _DOCUMENT_EXTENSIONS:
            inspected = ImportBatchFile(
                path=str(file_path),
                classification="document_export",
                file_type=suffix.lstrip("."),
            )
        elif suffix in _PDF_EXTENSIONS:
            logger.info(
                "Inspecting PDF import file index=%d total=%d path=%s size_mb=%.2f",
                file_index,
                total_files,
                file_path,
                file_path.stat().st_size / 1_000_000,
            )
            # Recall first: default inspection reads the full PDF because large
            # Europresse exports may put only cover pages before article anchors.
            pdf_text = _extract_pdf_text(
                file_path,
                maxpages=NEWS_PDF_INSPECTION_MAX_PAGES,
            )
            has_text_layer = bool(pdf_text.strip())
            if has_text_layer and _is_europresse_format(pdf_text):
                pdf_classification = "pdf_europresse_batch"
            elif has_text_layer:
                pdf_classification = "pdf_text_layer"
            else:
                pdf_classification = "unsupported"
            inspected = ImportBatchFile(
                path=str(file_path),
                classification=pdf_classification,
                file_type="pdf",
                has_text_layer=has_text_layer,
                reason="" if has_text_layer else "pdf has no detectable text layer",
            )
        else:
            inspected = ImportBatchFile(
                path=str(file_path),
                classification="unsupported",
                file_type=suffix.lstrip(".") or "unknown",
                reason=f"unsupported extension: {suffix or 'none'}",
            )

        parser_mix[inspected.classification] += 1
        inspected_files.append(inspected)

    return ImportBatchInspection(
        batch_id=manifest.batch_id,
        source_system=manifest.source_system,
        files=tuple(inspected_files),
        parser_mix=parser_mix,
    )


def parse_import_batch(
    manifest: NewsImportManifest,
    inspection: ImportBatchInspection,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse a classified import batch into bronze records and quarantine rows.

    Args:
        manifest: Batch-level import contract.
        inspection: Inspection result produced by ``inspect_import_batch``.

    Returns:
        Tuple of ``(bronze_news_source_record_df, unsupported_df)``.
    """
    bronze_rows: list[dict[str, object]] = []
    unsupported_rows: list[dict[str, object]] = []

    for inspected_file in inspection.files:
        file_path = Path(inspected_file.path)

        if inspected_file.classification == "table_export":
            if inspected_file.file_type == "csv":
                sheet_map = {"csv": pd.read_csv(file_path, dtype=str).fillna("")}
            else:
                sheet_map = {
                    str(sheet_name): sheet_df.fillna("")
                    for sheet_name, sheet_df in pd.read_excel(
                        file_path,
                        sheet_name=None,
                        dtype=str,
                    ).items()
                }

            for sheet_name, sheet_df in sheet_map.items():
                resolved_columns = _resolve_table_columns(sheet_df)
                for row_index, row in sheet_df.iterrows():
                    row_payload = {
                        "sheet_name": sheet_name,
                        "row_index": int(row_index),
                        "row": {
                            str(column_name): _coerce_optional_str(value)
                            for column_name, value in row.items()
                        },
                        "resolved_columns": resolved_columns,
                    }
                    body_text = _clean_text(
                        _coerce_optional_str(
                            row.get(resolved_columns["body"])
                            if resolved_columns["body"] is not None
                            else ""
                        )
                        or _fallback_body_from_row(
                            row,
                            resolved_columns=resolved_columns,
                        )
                    )
                    title = _clean_text(
                        _coerce_optional_str(
                            row.get(resolved_columns["title"])
                            if resolved_columns["title"] is not None
                            else ""
                        )
                    )
                    if not title and body_text:
                        title = body_text.split(".")[0][:140].strip()

                    bronze_rows.append(
                        _build_source_record(
                            manifest=manifest,
                            file_path=file_path,
                            file_type=inspected_file.file_type,
                            classification=inspected_file.classification,
                            local_record_key=f"{sheet_name}:{row_index}",
                            source_native_payload=row_payload,
                            raw_title=title,
                            raw_body_text=body_text,
                            raw_published_at=_coerce_optional_str(
                                row.get(resolved_columns["published_at"])
                                if resolved_columns["published_at"] is not None
                                else ""
                            ),
                            raw_outlet=_coerce_optional_str(
                                row.get(resolved_columns["outlet"])
                                if resolved_columns["outlet"] is not None
                                else manifest.source_system
                            ),
                            raw_article_url=_coerce_optional_str(
                                row.get(resolved_columns["article_url"])
                                if resolved_columns["article_url"] is not None
                                else ""
                            ),
                            raw_author=_coerce_optional_str(
                                row.get(resolved_columns["author"])
                                if resolved_columns["author"] is not None
                                else ""
                            ),
                            raw_language=_coerce_optional_str(
                                row.get(resolved_columns["language"])
                                if resolved_columns["language"] is not None
                                else ""
                            ),
                            parser_name="parse_table_export",
                        )
                    )
            continue

        if inspected_file.classification == "document_export":
            raw_text = file_path.read_text(encoding="utf-8", errors="ignore")
            if inspected_file.file_type == "txt":
                non_empty_lines = [
                    line.strip() for line in raw_text.splitlines() if line.strip()
                ]
                title = non_empty_lines[0] if non_empty_lines else file_path.stem
                extracted = {
                    "title": title,
                    "body_text": raw_text,
                    "published_at": "",
                }
                parser_name = "parse_text_document"
            else:
                extracted = _extract_html_document_fields(raw_text)
                parser_name = "parse_html_document"

            bronze_rows.append(
                _build_source_record(
                    manifest=manifest,
                    file_path=file_path,
                    file_type=inspected_file.file_type,
                    classification=inspected_file.classification,
                    local_record_key=file_path.name,
                    source_native_payload={
                        "file_name": file_path.name,
                        "body_preview": _clean_text(extracted["body_text"])[:500],
                    },
                    raw_title=extracted["title"] or file_path.stem,
                    raw_body_text=extracted["body_text"],
                    raw_published_at=extracted["published_at"],
                    raw_outlet=manifest.source_system,
                    raw_article_url="",
                    raw_author="",
                    raw_language="",
                    parser_name=parser_name,
                )
            )
            continue

        if inspected_file.classification == "pdf_europresse_batch":
            # One Europresse PDF → N individual article bronze rows.
            # _extract_pdf_text preserves newlines for line-based segmentation.
            logger.info("Extracting full Europresse PDF text path=%s", file_path)
            full_pdf_text = _extract_pdf_text(file_path)
            segmented_articles = _segment_europresse_articles(full_pdf_text)
            declared_document_count = _extract_europresse_declared_document_count(
                full_pdf_text
            )
            if declared_document_count is not None and declared_document_count != len(
                segmented_articles
            ):
                raise DataQualityError(
                    f"Europresse PDF {file_path.name} declared "
                    f"{declared_document_count} documents but segmented "
                    f"{len(segmented_articles)} articles"
                )
            logger.info(
                "Parsed Europresse PDF %s: %d articles segmented",
                file_path.name,
                len(segmented_articles),
            )
            for article in segmented_articles:
                article_body = _clean_text(article["body_text"])
                article_title = _clean_text(article["title"]) or file_path.stem
                bronze_rows.append(
                    _build_source_record(
                        manifest=manifest,
                        file_path=file_path,
                        file_type=inspected_file.file_type,
                        classification=inspected_file.classification,
                        local_record_key=(
                            f"{file_path.name}:article_{article['article_index']}"
                        ),
                        source_native_payload={
                            "file_name": file_path.name,
                            "article_index": article["article_index"],
                            "declared_word_count": article["declared_word_count"],
                            "parse_error": article.get("parse_error", ""),
                            "body_preview": article_body[:_PAYLOAD_TEXT_PREVIEW_CHARS],
                        },
                        raw_title=article_title,
                        raw_body_text=article_body,
                        raw_published_at=article["published_at"],
                        raw_outlet=article["outlet"] or manifest.source_system,
                        raw_article_url=article.get("article_url", ""),
                        raw_author="",
                        raw_language="",
                        parser_name="parse_europresse_pdf_batch",
                    )
                )
            continue

        if inspected_file.classification == "pdf_text_layer":
            # Single-document PDF: treat the whole file as one article record.
            # _extract_pdf_text preserves newlines; _clean_text collapses to one line.
            logger.info("Extracting full single-document PDF text path=%s", file_path)
            extracted_text = _clean_text(_extract_pdf_text(file_path))
            title = extracted_text.split(".")[0][:140].strip() or file_path.stem
            bronze_rows.append(
                _build_source_record(
                    manifest=manifest,
                    file_path=file_path,
                    file_type=inspected_file.file_type,
                    classification=inspected_file.classification,
                    local_record_key=file_path.name,
                    source_native_payload={
                        "file_name": file_path.name,
                        "body_preview": extracted_text[:_PAYLOAD_TEXT_PREVIEW_CHARS],
                    },
                    raw_title=title,
                    raw_body_text=extracted_text,
                    raw_published_at="",
                    raw_outlet=manifest.source_system,
                    raw_article_url="",
                    raw_author="",
                    raw_language="",
                    parser_name="parse_pdf_text_layer",
                )
            )
            continue

        unsupported_rows.append(
            {
                "batch_id": manifest.batch_id,
                "source_system": manifest.source_system,
                "raw_file_path": str(file_path),
                "raw_file_type": inspected_file.file_type,
                "import_classification": inspected_file.classification,
                "_rejection_reason": inspected_file.reason or "unsupported file",
            }
        )

    bronze_df = pd.DataFrame(bronze_rows, columns=_BRONZE_SOURCE_COLUMNS)
    unsupported_df = pd.DataFrame(
        unsupported_rows,
        columns=[
            "batch_id",
            "source_system",
            "raw_file_path",
            "raw_file_type",
            "import_classification",
            "_rejection_reason",
        ],
    )
    return bronze_df, unsupported_df


def _looks_like_web_reference_stub(
    *,
    body_text: str,
    article_url: str,
    outlet_name: str,
) -> bool:
    """Return whether a Europresse row points to web text rather than containing it."""
    if not article_url:
        return False
    normalized_body = normalize_text_for_match(body_text)
    normalized_outlet = normalize_text_for_match(outlet_name)
    return "read more" in normalized_body and (
        "this document contains links" in normalized_body
        or "web sites" in normalized_body
        or "site web ref" in normalized_outlet
    )


def _empty_web_fetch_cache() -> pd.DataFrame:
    """Return an empty cache frame with the documented web-fetch schema."""
    return pd.DataFrame(columns=_WEB_FETCH_CACHE_COLUMNS)


def load_news_web_fetch_cache(cache_path: Path) -> pd.DataFrame:
    """Load the local web-fetch cache, returning an empty frame when absent.

    Args:
        cache_path: Local Parquet path for the ignored web-fetch cache.

    Returns:
        Cache DataFrame using the documented web-fetch schema.
    """
    if not cache_path.exists():
        return _empty_web_fetch_cache()
    cache_df = pd.read_parquet(cache_path)
    for column_name in _WEB_FETCH_CACHE_COLUMNS:
        if column_name not in cache_df.columns:
            cache_df[column_name] = None
    return cache_df[_WEB_FETCH_CACHE_COLUMNS].copy()


def write_news_web_fetch_cache(cache_df: pd.DataFrame, cache_path: Path) -> Path:
    """Persist the local web-fetch cache in the ignored data directory.

    Args:
        cache_df: Cache rows using the documented web-fetch schema.
        cache_path: Local Parquet path for the ignored web-fetch cache.

    Returns:
        Path where the cache was written.
    """
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_df[_WEB_FETCH_CACHE_COLUMNS].to_parquet(cache_path, index=False)
    return cache_path


def _build_web_fetch_cache_row(
    *,
    canonical_url: str,
    source_url: str,
    fetch_status: str,
    http_status: int | None,
    body_text: str,
    error_type: str | None,
) -> dict[str, object]:
    """Build one cache row for a web-fetch attempt."""
    cleaned_body = _clean_text(body_text)
    return {
        "canonical_url": canonical_url,
        "source_url": source_url,
        "fetch_status": fetch_status,
        "http_status": http_status,
        "body_text": cleaned_body or None,
        "body_text_hash": _stable_md5(cleaned_body) if cleaned_body else None,
        "body_text_preview": cleaned_body[:_PAYLOAD_TEXT_PREVIEW_CHARS] or None,
        "body_text_length": len(cleaned_body),
        "fetched_at": now_iso_utc(),
        "extractor_name": "trafilatura",
        "extractor_version": getattr(trafilatura, "__version__", None),
        "error_type": error_type,
    }


def _fetch_web_article(cache_key: str, source_url: str) -> dict[str, object]:
    """Fetch and extract one web article with explicit failure metadata."""
    if trafilatura is None:
        return _build_web_fetch_cache_row(
            canonical_url=cache_key,
            source_url=source_url,
            fetch_status="failed",
            http_status=None,
            body_text="",
            error_type="missing_trafilatura",
        )

    request = Request(
        source_url,
        headers={"User-Agent": "Mozilla/5.0 (compatible; research-etl/1.0)"},
    )
    try:
        with urlopen(request, timeout=SCRAPE_REQUEST_TIMEOUT_SECONDS) as response:
            http_status = getattr(response, "status", None)
            charset = response.headers.get_content_charset() or "utf-8"
            html_text = response.read().decode(charset, errors="replace")
    except HTTPError as exc:
        return _build_web_fetch_cache_row(
            canonical_url=cache_key,
            source_url=source_url,
            fetch_status="failed",
            http_status=exc.code,
            body_text="",
            error_type=f"http_{exc.code}",
        )
    except (OSError, URLError, UnicodeError) as exc:
        return _build_web_fetch_cache_row(
            canonical_url=cache_key,
            source_url=source_url,
            fetch_status="failed",
            http_status=None,
            body_text="",
            error_type=type(exc).__name__,
        )

    extracted_text = trafilatura.extract(
        html_text,
        include_comments=False,
        include_tables=False,
        favor_precision=True,
    )
    cleaned_text = _clean_text(extracted_text or "")
    if len(cleaned_text) < DQ_MIN_ARTICLE_TEXT_LENGTH:
        return _build_web_fetch_cache_row(
            canonical_url=cache_key,
            source_url=source_url,
            fetch_status="short_text",
            http_status=http_status,
            body_text=cleaned_text,
            error_type="body_text_too_short",
        )
    return _build_web_fetch_cache_row(
        canonical_url=cache_key,
        source_url=source_url,
        fetch_status="success",
        http_status=http_status,
        body_text=cleaned_text,
        error_type=None,
    )


def _has_usable_cached_body(cache_row: pd.Series) -> bool:
    """Return whether a cached row can support offline full-text reuse."""
    return (
        str(cache_row.get("fetch_status") or "") == "success"
        and int(cache_row.get("body_text_length") or 0) >= DQ_MIN_ARTICLE_TEXT_LENGTH
        and bool(_clean_text(_coerce_optional_str(cache_row.get("body_text"))))
    )


def _apply_web_body_to_source_row(
    fact_article_source_df: pd.DataFrame,
    row_index: int,
    body_text: str,
    *,
    acquisition_method: str,
) -> None:
    """Mutate one article-source row with a full-text web extraction."""
    cleaned_body = _clean_text(body_text)
    fact_article_source_df.at[row_index, "body_text"] = cleaned_body
    fact_article_source_df.at[row_index, "body_text_hash"] = _stable_md5(cleaned_body)
    fact_article_source_df.at[row_index, "has_full_text"] = True
    fact_article_source_df.at[row_index, "acquisition_method"] = acquisition_method
    fact_article_source_df.at[row_index, "parser_status"] = "parsed"


def _apply_metadata_only_to_source_row(
    fact_article_source_df: pd.DataFrame,
    row_index: int,
) -> None:
    """Mutate one article-source row into a metadata-only web reference."""
    fact_article_source_df.at[row_index, "body_text"] = None
    fact_article_source_df.at[row_index, "body_text_hash"] = None
    fact_article_source_df.at[row_index, "has_full_text"] = False
    fact_article_source_df.at[row_index, "acquisition_method"] = "url_metadata_only"
    fact_article_source_df.at[row_index, "parser_status"] = "metadata_only"


def enrich_article_sources_with_web_cache(
    fact_article_source_df: pd.DataFrame,
    *,
    cache_path: Path,
    enable_web_scrape: bool = False,
) -> tuple[pd.DataFrame, dict[str, int], bool]:
    """Reuse cached web extractions and optionally fetch missing web bodies.

    Args:
        fact_article_source_df: Silver article-source rows after PDF parsing.
        cache_path: Local ignored Parquet cache for web fetch results.
        enable_web_scrape: Whether this run may perform new network fetches.

    Returns:
        Tuple of enriched rows, QA metrics, and whether the cache was written.
    """
    enriched_df = fact_article_source_df.copy()
    if enriched_df.empty:
        return (
            enriched_df,
            {
                "web_scrape_queued_count": 0,
                "web_scrape_cache_hit_count": 0,
                "web_scrape_success_count": 0,
                "url_metadata_only_count": 0,
                "web_scrape_failure_count": 0,
            },
            False,
        )

    metrics = {
        "web_scrape_queued_count": 0,
        "web_scrape_cache_hit_count": 0,
        "web_scrape_success_count": 0,
        "url_metadata_only_count": 0,
        "web_scrape_failure_count": 0,
    }
    cache_df = load_news_web_fetch_cache(cache_path)
    cache_lookup = {
        str(row["canonical_url"]): row
        for _, row in cache_df.drop_duplicates(
            subset=["canonical_url"],
            keep="last",
        ).iterrows()
        if _clean_text(_coerce_optional_str(row.get("canonical_url")))
    }
    new_cache_rows: list[dict[str, object]] = []

    for row_index, row in enriched_df.iterrows():
        article_url = _clean_text(_coerce_optional_str(row.get("article_url")))
        canonical_url = _clean_text(_coerce_optional_str(row.get("canonical_url")))
        if not article_url or not canonical_url:
            continue
        if bool(row.get("has_full_text", True)):
            continue

        metrics["web_scrape_queued_count"] += 1
        cached_row = cache_lookup.get(canonical_url)
        if cached_row is not None and _has_usable_cached_body(cached_row):
            _apply_web_body_to_source_row(
                enriched_df,
                row_index,
                _coerce_optional_str(cached_row.get("body_text")),
                acquisition_method="web_scrape",
            )
            metrics["web_scrape_cache_hit_count"] += 1
            continue

        if enable_web_scrape:
            fetched_row = _fetch_web_article(canonical_url, article_url)
            cache_lookup[canonical_url] = pd.Series(fetched_row)
            new_cache_rows.append(fetched_row)
            if (
                fetched_row["fetch_status"] == "success"
                and int(fetched_row["body_text_length"]) >= DQ_MIN_ARTICLE_TEXT_LENGTH
            ):
                _apply_web_body_to_source_row(
                    enriched_df,
                    row_index,
                    _coerce_optional_str(fetched_row["body_text"]),
                    acquisition_method="web_scrape",
                )
                metrics["web_scrape_success_count"] += 1
            else:
                _apply_metadata_only_to_source_row(enriched_df, row_index)
                metrics["web_scrape_failure_count"] += 1
                metrics["url_metadata_only_count"] += 1
            continue

        _apply_metadata_only_to_source_row(enriched_df, row_index)
        metrics["url_metadata_only_count"] += 1

    cache_written = False
    if new_cache_rows:
        merged_cache_df = pd.DataFrame(
            cache_df.to_dict("records") + new_cache_rows,
            columns=_WEB_FETCH_CACHE_COLUMNS,
        )
        merged_cache_df = merged_cache_df.drop_duplicates(
            subset=["canonical_url"],
            keep="last",
        )
        write_news_web_fetch_cache(merged_cache_df, cache_path)
        cache_written = True

    return enriched_df, metrics, cache_written


def build_fact_article_source(
    bronze_news_source_record_df: pd.DataFrame,
    *,
    window_start: date | None = _DEFAULT_ANALYSIS_WINDOW_START,
    window_end: date | None = _DEFAULT_ANALYSIS_WINDOW_END,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize bronze source records into silver-ready article-source rows.

    Articles whose publication date falls outside ``[window_start, window_end]``
    are quarantined to the rejected table with reason
    ``"published_date outside analysis window"``.  This enforces the project's
    analysis scope contract at the silver boundary.

    Bronze records are never modified: the window filter is applied here, not
    upstream, so the bronze layer always retains the full raw extract. The
    analysis window is required at this Silver boundary; omitting it is a
    contract violation because it changes the study denominator.

    Args:
        bronze_news_source_record_df: Parsed bronze source records.
        window_start: Inclusive lower bound for ``published_date``.
            Defaults to the configured project analysis start date.
        window_end: Inclusive upper bound for ``published_date``.
            Defaults to the configured project analysis end date.

    Returns:
        Tuple of ``(fact_article_source_df, rejected_df)``.
    """
    if window_start is None or window_end is None:
        raise ValueError("window_start and window_end are required for source DQ")

    if bronze_news_source_record_df.empty:
        return (
            pd.DataFrame(columns=_FACT_ARTICLE_SOURCE_COLUMNS),
            pd.DataFrame(columns=_SOURCE_REJECTED_COLUMNS),
        )

    normalized_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, object]] = []

    for row in bronze_news_source_record_df.to_dict("records"):
        body_text = _clean_text(_coerce_optional_str(row["raw_body_text"]))
        title = _clean_text(_coerce_optional_str(row["raw_title"]))
        published_at = _parse_timestamp(_coerce_optional_str(row["raw_published_at"]))
        outlet_name = _clean_text(
            _coerce_optional_str(row["raw_outlet"])
            or _coerce_optional_str(row["source_system"])
        )
        article_url = _clean_text(_coerce_optional_str(row["raw_article_url"]))
        if not article_url:
            article_url = _extract_first_url_from_text(body_text)
        canonical_url = canonicalize_url(article_url) if article_url else None
        language = _detect_french_language(
            _coerce_optional_str(row["raw_language"]),
            title,
            body_text,
        )
        is_web_reference_stub = _looks_like_web_reference_stub(
            body_text=body_text,
            article_url=article_url,
            outlet_name=outlet_name,
        )
        has_full_text = (
            len(body_text) >= DQ_MIN_ARTICLE_TEXT_LENGTH and not is_web_reference_stub
        )
        body_text_too_short_without_url = (
            len(body_text) < DQ_MIN_ARTICLE_TEXT_LENGTH and not article_url
        )

        rejection_reasons = []
        if body_text_too_short_without_url:
            rejection_reasons.append("body_text too short")
        if published_at is pd.NaT:
            rejection_reasons.append("published_at unparseable")
        elif (window_start is not None and published_at.date() < window_start) or (
            window_end is not None and published_at.date() > window_end
        ):
            rejection_reasons.append(
                f"published_date {published_at.date()} outside analysis window "
                f"[{window_start}, {window_end}]"
            )
        if language != "fr":
            rejection_reasons.append("language is not French")
        if not normalize_text_for_match(title):
            rejection_reasons.append("title missing")

        if rejection_reasons:
            rejected_rows.append(
                {
                    "source_record_id": row["source_record_id"],
                    "batch_id": row["batch_id"],
                    "source_system": row["source_system"],
                    "raw_file_path": row["raw_file_path"],
                    "raw_title": title,
                    "raw_published_at": row["raw_published_at"],
                    "raw_outlet": outlet_name,
                    "raw_article_url": article_url,
                    "_rejection_reason": "; ".join(rejection_reasons),
                }
            )
            continue

        persisted_body_text = body_text if has_full_text else None
        persisted_body_text_hash = _stable_md5(body_text) if has_full_text else None
        acquisition_method = (
            "url_metadata_only"
            if not has_full_text
            else (
                "restricted_export"
                if row["rights_class"] == "restricted_local"
                else "supplemental_import"
            )
        )
        parser_status = "parsed" if has_full_text else "metadata_only"

        normalized_rows.append(
            {
                "article_source_id": row["source_record_id"],
                "batch_id": row["batch_id"],
                "source_system": row["source_system"],
                "source_record_id": row["source_record_id"],
                "source_record_hash": row["source_record_hash"],
                "title": title,
                "title_normalized": normalize_text_for_match(title),
                "body_text": persisted_body_text,
                "body_text_hash": persisted_body_text_hash,
                "has_full_text": has_full_text,
                "published_at_normalized": published_at,
                "published_date": published_at.date().isoformat(),
                "outlet_name": outlet_name,
                "outlet_name_normalized": normalize_text_for_match(outlet_name),
                "article_url": article_url or None,
                "canonical_url": canonical_url,
                "author": _clean_text(_coerce_optional_str(row["raw_author"])) or None,
                "language": language,
                "acquisition_method": acquisition_method,
                "parser_status": parser_status,
                "rights_class": row["rights_class"],
                "raw_file_path": row["raw_file_path"],
                "raw_file_type": row["raw_file_type"],
                "import_classification": row["import_classification"],
                "parser_name": row["parser_name"],
                "parser_version": row["parser_version"],
                "source_native_payload": row["source_native_payload"],
                "_ingested_at": row["_ingested_at"],
            }
        )

    accepted_df = pd.DataFrame(normalized_rows, columns=_FACT_ARTICLE_SOURCE_COLUMNS)
    rejected_df = pd.DataFrame(rejected_rows, columns=_SOURCE_REJECTED_COLUMNS)

    reject_rate = len(rejected_df) / max(len(bronze_news_source_record_df), 1)
    if reject_rate > DQ_MAX_NULL_RATE:
        raise DataQualityError(
            f"fact_article_source reject rate {reject_rate:.1%} exceeds threshold "
            f"{DQ_MAX_NULL_RATE:.1%}"
        )

    return accepted_df, rejected_df


def build_fact_article(
    fact_article_source_df: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse normalized source rows into canonical article records.

    Args:
        fact_article_source_df: Silver article-source rows.

    Returns:
        Canonical ``fact_article`` DataFrame.

    Raises:
        DataQualityError: If canonical IDs are duplicated after aggregation.
    """
    if fact_article_source_df.empty:
        return pd.DataFrame(columns=_FACT_ARTICLE_COLUMNS)

    working_df = fact_article_source_df.copy()
    working_df["content_signature_id"] = working_df.apply(
        lambda row: _stable_md5(
            "|".join(
                [
                    _coerce_optional_str(row["title_normalized"]),
                    _coerce_optional_str(row["outlet_name_normalized"]),
                    _coerce_optional_str(row["published_date"]),
                    _coerce_optional_str(row["body_text_hash"]),
                ]
            )
        ),
        axis=1,
    )
    working_df["url_group_id"] = working_df["canonical_url"].apply(
        lambda value: (
            _stable_md5(value) if _clean_text(_coerce_optional_str(value)) else None
        )
    )
    content_to_url_group = (
        working_df.dropna(subset=["url_group_id"])
        .drop_duplicates(subset=["content_signature_id"])
        .set_index("content_signature_id")["url_group_id"]
        .to_dict()
    )
    working_df["duplicate_group_id"] = working_df.apply(
        lambda row: _coerce_optional_str(row["url_group_id"])
        or content_to_url_group.get(row["content_signature_id"])
        or row["content_signature_id"],
        axis=1,
    )
    working_df["dedup_method"] = working_df.apply(
        lambda row: (
            (
                "url_canonicalization"
                if _clean_text(_coerce_optional_str(row["url_group_id"]))
                else "content_signature"
            )
            if row["duplicate_group_id"] == row["content_signature_id"]
            else "hybrid_url_content"
        ),
        axis=1,
    )
    working_df["canonical_article_id"] = working_df["duplicate_group_id"]

    article_rows = []
    grouped = working_df.sort_values(
        by=["has_full_text", "published_at_normalized", "article_source_id"],
        ascending=[False, True, True],
        na_position="last",
    ).groupby("duplicate_group_id", dropna=False)
    for duplicate_group_id, group_df in grouped:
        representative = group_df.iloc[0]
        article_rows.append(
            {
                "canonical_article_id": duplicate_group_id,
                "duplicate_group_id": duplicate_group_id,
                "dedup_method": representative["dedup_method"],
                "canonical_url": representative["canonical_url"],
                "representative_url": representative["article_url"],
                "representative_source_record_id": representative["source_record_id"],
                "title": representative["title"],
                "title_normalized": representative["title_normalized"],
                "body_text": representative["body_text"],
                "body_text_hash": representative["body_text_hash"],
                "has_full_text": bool(representative["has_full_text"]),
                "published_at": representative["published_at_normalized"],
                "published_date": representative["published_date"],
                "domain": _derive_domain(
                    _coerce_optional_str(representative["article_url"]),
                    _coerce_optional_str(representative["outlet_name_normalized"]),
                ),
                "outlet_name": representative["outlet_name"],
                "outlet_name_normalized": representative["outlet_name_normalized"],
                "language": representative["language"],
                "rights_class": representative["rights_class"],
                "source_record_count": int(group_df["source_record_id"].nunique()),
                "source_system_count": int(group_df["source_system"].nunique()),
                "source_systems": ",".join(
                    sorted(group_df["source_system"].astype(str).unique())
                ),
                "acquisition_methods": ",".join(
                    sorted(group_df["acquisition_method"].astype(str).unique())
                ),
                "partition_date": representative["published_date"],
            }
        )

    fact_article_df = pd.DataFrame(article_rows, columns=_FACT_ARTICLE_COLUMNS)
    if fact_article_df["canonical_article_id"].duplicated().any():
        raise DataQualityError(
            "fact_article contains duplicate canonical_article_id values"
        )
    return fact_article_df


def build_fact_article_discovery(
    search_hits: list[SearchHit] | tuple[SearchHit, ...],
    provider_query_rows: list[dict[str, object]] | None = None,
) -> pd.DataFrame:
    """Convert provider search hits into the canonical discovery contract.

    Args:
        search_hits: Provider hit list returned by discovery adapters.
        provider_query_rows: Optional provider audit rows keyed by leader/provider.

    Returns:
        ``fact_article_discovery`` DataFrame.
    """
    if not search_hits:
        return pd.DataFrame(columns=_DISCOVERY_COLUMNS)

    query_lookup = {
        (
            _coerce_optional_str(row.get("leader_id")),
            _coerce_optional_str(row.get("provider")),
        ): row
        for row in (provider_query_rows or [])
    }

    discovery_rows: list[dict[str, object]] = []
    for hit in search_hits:
        canonical_url = canonicalize_url(hit.article_url)
        provider_row = query_lookup.get((hit.leader_id, hit.provider), {})
        partition_date = (
            hit.published_at.date().isoformat() if hit.published_at is not None else ""
        )
        canonical_article_id = _stable_md5(canonical_url)
        discovery_rows.append(
            {
                "discovery_id": _stable_md5(
                    "|".join(
                        [
                            canonical_article_id,
                            hit.leader_id,
                            hit.provider,
                            hit.query_strategy,
                            hit.raw_payload_path,
                        ]
                    )
                ),
                "canonical_article_id": canonical_article_id,
                "leader_id": hit.leader_id,
                "provider": hit.provider,
                "provider_tier": hit.provider_tier,
                "provider_status": provider_row.get("provider_status", "success_hits"),
                "provider_error_type": provider_row.get("provider_error_type"),
                "provider_warning_count": int(
                    provider_row.get("provider_warning_count", 0) or 0
                ),
                "outlet_key": hit.outlet_key,
                "article_url": hit.article_url,
                "canonical_url": canonical_url,
                "title": hit.title,
                "published_at": hit.published_at,
                "domain": hit.domain,
                "language": hit.language,
                "raw_payload_path": hit.raw_payload_path,
                "query_text": hit.query_text,
                "query_strategy": hit.query_strategy,
                "partition_date": partition_date,
            }
        )

    return pd.DataFrame(discovery_rows, columns=_DISCOVERY_COLUMNS)


def build_article_source_from_search_hits(
    search_hits: list[SearchHit] | tuple[SearchHit, ...],
    article_fetch_results: dict[str, ArticleFetchResult] | None = None,
    *,
    batch_id: str,
    source_system: str = "supplemental_provider",
) -> pd.DataFrame:
    """Convert fetched provider hits into the article-source contract.

    Args:
        search_hits: Provider hit list.
        article_fetch_results: Canonical URL keyed fetch results with body text.
        batch_id: Import batch identifier used for provenance.
        source_system: Logical source system label.

    Returns:
        ``fact_article_source``-compatible DataFrame. Rows with empty bodies are
        dropped because they cannot support downstream mention extraction.
    """
    fetch_results = article_fetch_results or {}
    source_rows: list[dict[str, object]] = []

    for hit in search_hits:
        canonical_url = canonicalize_url(hit.article_url)
        fetch_result = fetch_results.get(canonical_url)
        if fetch_result is None or not _clean_text(fetch_result.body_text):
            continue

        title = _clean_text(hit.title)
        body_text = _clean_text(fetch_result.body_text)
        published_at = hit.published_at
        if published_at is None:
            continue
        source_rows.append(
            {
                "article_source_id": _stable_md5(
                    f"{batch_id}|{canonical_url}|{hit.provider}|{hit.leader_id}"
                ),
                "batch_id": batch_id,
                "source_system": source_system,
                "source_record_id": _stable_md5(
                    f"{batch_id}|{canonical_url}|{hit.raw_payload_path}"
                ),
                "source_record_hash": _stable_md5(body_text),
                "title": title,
                "title_normalized": normalize_text_for_match(title),
                "body_text": body_text,
                "body_text_hash": _stable_md5(body_text),
                "has_full_text": True,
                "published_at_normalized": published_at,
                "published_date": published_at.date().isoformat(),
                "outlet_name": hit.outlet_key,
                "outlet_name_normalized": normalize_text_for_match(hit.outlet_key),
                "article_url": hit.article_url,
                "canonical_url": canonical_url,
                "author": None,
                "language": hit.language,
                "acquisition_method": "supplemental_import",
                "parser_status": fetch_result.fetch_status,
                "rights_class": "public",
                "raw_file_path": hit.raw_payload_path,
                "raw_file_type": "json",
                "import_classification": "provider_fetch",
                "parser_name": "provider_article_fetch",
                "parser_version": _PARSER_VERSION,
                "source_native_payload": json.dumps(
                    {"provider": hit.provider, "query_text": hit.query_text},
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "_ingested_at": now_iso_utc(),
            }
        )

    return pd.DataFrame(source_rows, columns=_FACT_ARTICLE_SOURCE_COLUMNS)


def split_sentences(text: str) -> list[str]:
    """Split text into coarse sentence units for candidate-context extraction."""
    return [
        segment.strip()
        for segment in _SENTENCE_BOUNDARY_PATTERN.split(text)
        if segment.strip()
    ]
