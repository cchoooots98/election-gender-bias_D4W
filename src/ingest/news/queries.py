"""Shared query and normalization helpers for news providers."""

from __future__ import annotations

import re
import unicodedata
from datetime import date

from src.ingest.news.models import CandidateQueryCase

_MULTISPACE_PATTERN = re.compile(r"\s+")
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")
_ELECTION_KEYWORDS = ("municipales", "maire", "mairie", "liste")


def _deaccent(text: str) -> str:
    """Remove diacritical marks from a Unicode string."""
    return "".join(
        char
        for char in unicodedata.normalize("NFD", text)
        if unicodedata.category(char) != "Mn"
    )


def normalize_text_for_match(text: str) -> str:
    """Normalize text for deterministic matching and deduplication."""
    deaccented = _deaccent(text or "")
    lowered = deaccented.lower()
    cleaned = _NON_ALNUM_PATTERN.sub(" ", lowered)
    return _MULTISPACE_PATTERN.sub(" ", cleaned).strip()


def _parse_family_given(full_name: str) -> tuple[str, str]:
    """Parse ``FAMILY Given`` into family and given name."""
    tokens = full_name.strip().split()
    if len(tokens) == 1:
        return tokens[0], ""
    return " ".join(tokens[:-1]), tokens[-1]


def _normalize_tokens(text: str, *, min_length: int = 3) -> tuple[str, ...]:
    """Tokenize normalized text into stable match terms."""
    return tuple(
        token
        for token in normalize_text_for_match(text).split()
        if len(token) >= min_length
    )


def _any_token_present(normalized_text: str, tokens: tuple[str, ...]) -> bool:
    """Return whether any token is present in the normalized text."""
    return any(token in normalized_text for token in tokens)


def build_candidate_aliases(full_name: str) -> dict[str, str]:
    """Generate candidate aliases that increase source recall."""
    family_name, given_name = _parse_family_given(full_name)
    family_title = family_name.title()
    natural_accented = f"{given_name} {family_title}".strip()
    natural_deaccented = _deaccent(natural_accented)
    normalized = " ".join(
        natural_deaccented.replace("-", " ").replace("'", " ").split()
    )
    return {
        "official": full_name,
        "natural_accented": natural_accented,
        "natural_deaccented": natural_deaccented,
        "normalized": normalized,
    }


def build_gdelt_query(full_name: str, commune_name: str, mode: str = "precise") -> str:
    """Build a GDELT full-text query from the shared alias contract."""
    if mode not in {"precise", "relaxed"}:
        raise ValueError(
            f"Unknown query mode: {mode!r}. Must be 'precise' or 'relaxed'."
        )

    aliases = build_candidate_aliases(full_name)
    if mode == "precise":
        return (
            f'"{aliases["natural_accented"]}" AND "{commune_name}" '
            f"AND (municipales OR maire OR mairie OR liste)"
        )
    return f'"{aliases["natural_deaccented"]}" AND "{commune_name}"'


def build_generic_news_query(full_name: str, commune_name: str) -> str:
    """Build a provider-agnostic search query for API and feed sources."""
    aliases = build_candidate_aliases(full_name)
    return (
        f'"{aliases["natural_accented"]}" "{commune_name}" '
        "(municipales OR maire OR mairie OR liste)"
    )


def build_candidate_match_terms(case: CandidateQueryCase) -> dict[str, tuple[str, ...]]:
    """Build deterministic local-filter terms for RSS and sitemap entries."""
    aliases = build_candidate_aliases(case.full_name)
    family_name, given_name = _parse_family_given(case.full_name)
    full_name_variants = tuple(
        sorted(
            {
                normalize_text_for_match(aliases["natural_accented"]),
                normalize_text_for_match(aliases["natural_deaccented"]),
                normalize_text_for_match(aliases["normalized"]),
            }
        )
    )
    return {
        "full_name_variants": full_name_variants,
        "surname_tokens": _normalize_tokens(family_name),
        "given_tokens": _normalize_tokens(given_name),
        "commune_tokens": _normalize_tokens(case.commune_name.replace("-", " ")),
        "election_keywords": _ELECTION_KEYWORDS,
    }


def _score_candidate_entry_match(
    entry_text: str, case: CandidateQueryCase
) -> dict[str, object]:
    """Compute deterministic local-match signals for one feed or article text."""
    normalized_entry = normalize_text_for_match(entry_text)
    terms = build_candidate_match_terms(case)
    has_full_name = any(
        variant and variant in normalized_entry
        for variant in terms["full_name_variants"]
    )
    has_surname = _any_token_present(normalized_entry, terms["surname_tokens"])
    has_given_name = _any_token_present(normalized_entry, terms["given_tokens"])
    has_commune = _any_token_present(normalized_entry, terms["commune_tokens"])
    has_election_context = any(
        keyword in normalized_entry for keyword in terms["election_keywords"]
    )

    score = 0
    if has_full_name:
        score += 5
    if has_surname:
        score += 2
    if has_given_name:
        score += 1
    if has_commune:
        score += 2
    if has_election_context:
        score += 1

    return {
        "normalized_entry": normalized_entry,
        "score": score,
        "has_full_name": has_full_name,
        "has_surname": has_surname,
        "has_given_name": has_given_name,
        "has_commune": has_commune,
        "has_election_context": has_election_context,
    }


def entry_matches_candidate(entry_text: str, case: CandidateQueryCase) -> bool:
    """Return whether text explicitly names or strongly anchors the candidate."""
    match_details = _score_candidate_entry_match(entry_text, case)
    if match_details["has_full_name"]:
        return True
    if match_details["has_surname"] and match_details["has_given_name"]:
        return True
    if match_details["has_surname"] and match_details["has_commune"]:
        return True
    return bool(match_details["score"] >= 5 and match_details["has_surname"])


def entry_needs_candidate_verification(
    entry_text: str, case: CandidateQueryCase
) -> bool:
    """Return whether text is relevant enough to justify fetching article body text."""
    match_details = _score_candidate_entry_match(entry_text, case)
    if entry_matches_candidate(entry_text, case):
        return True
    if match_details["has_commune"] and match_details["has_election_context"]:
        return True
    if match_details["has_surname"] and match_details["has_election_context"]:
        return True
    return bool(match_details["score"] >= 3)


def build_candidate_query_case(
    candidate_row: dict[str, object],
    start_date: str | date,
    end_date: str | date,
) -> CandidateQueryCase:
    """Create a validated candidate query case from manifest-like input."""
    required_fields = (
        "leader_id",
        "full_name",
        "commune_name",
        "dep_code",
        "city_size_bucket",
    )
    missing_fields = [
        field_name
        for field_name in required_fields
        if not str(candidate_row.get(field_name) or "").strip()
    ]
    if missing_fields:
        raise ValueError(
            "Candidate query case is missing required fields: "
            + ", ".join(sorted(missing_fields))
        )

    query_start = (
        date.fromisoformat(start_date) if isinstance(start_date, str) else start_date
    )
    query_end = date.fromisoformat(end_date) if isinstance(end_date, str) else end_date
    return CandidateQueryCase(
        leader_id=str(candidate_row["leader_id"]),
        full_name=str(candidate_row["full_name"]),
        commune_name=str(candidate_row["commune_name"]),
        dep_code=str(candidate_row["dep_code"]),
        city_size_bucket=str(candidate_row["city_size_bucket"]),
        window_start=query_start,
        window_end=query_end,
    )
