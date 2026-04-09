"""Shared query and normalization helpers for news providers."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date

from src.ingest.news.models import CandidateQueryCase

_MULTISPACE_PATTERN = re.compile(r"\s+")
_NON_ALNUM_PATTERN = re.compile(r"[^a-z0-9]+")

# Canonical municipal election context keywords, shared across all matching layers.
# Pre-filter (queries.py) and body-text resolution (matching.py) must use the
# same set so that recall and precision are consistent end-to-end.
# "election" is included because body text often uses the generic form while
# article titles use the more specific "municipales".
ELECTION_KEYWORDS: tuple[str, ...] = (
    "municipales",
    "maire",
    "mairie",
    "liste",
    "election",
)

# Module-internal alias for backward compatibility with existing call sites in
# this file that reference the private name.
_ELECTION_KEYWORDS = ELECTION_KEYWORDS


@dataclass(frozen=True)
class CandidateNameParts:
    """Structured representation of one candidate full name."""

    official_full_name: str
    family_name: str
    given_name: str

    @property
    def natural_order_name(self) -> str:
        """Return the display order used by news sources."""
        return f"{self.given_name} {self.family_name.title()}".strip()


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


def _looks_like_official_family_token(token: str) -> bool:
    """Return whether one token belongs to the leading uppercase family block."""
    folded_token = (
        _deaccent(token).replace("-", "").replace("'", "").replace("\u2019", "").strip()
    )
    if not folded_token:
        return False

    letter_characters = "".join(
        character for character in folded_token if character.isalpha()
    )
    return bool(letter_characters) and letter_characters == letter_characters.upper()


def parse_candidate_full_name(full_name: str) -> CandidateNameParts:
    """Parse one candidate name into family and given components.

    The official project contract stores candidate names as ``FAMILY Given``.
    Family names are preserved in uppercase in the source data, which allows the
    parser to keep multi-token family names and multi-token given names intact.

    For manually provided names that do not follow the official casing contract,
    the function falls back to natural-order parsing (``Given Family``).

    Args:
        full_name: Candidate name in official or natural display order.

    Returns:
        Structured candidate name parts.

    Raises:
        ValueError: If the name is blank or the token sequence is ambiguous.
    """
    normalized_full_name = " ".join(str(full_name or "").split())
    if not normalized_full_name:
        raise ValueError("Candidate full name must not be blank.")

    tokens = normalized_full_name.split()
    if len(tokens) == 1:
        return CandidateNameParts(
            official_full_name=normalized_full_name,
            family_name=tokens[0],
            given_name="",
        )

    leading_family_token_count = 0
    for token in tokens:
        if not _looks_like_official_family_token(token):
            break
        leading_family_token_count += 1

    if 0 < leading_family_token_count < len(tokens):
        return CandidateNameParts(
            official_full_name=normalized_full_name,
            family_name=" ".join(tokens[:leading_family_token_count]),
            given_name=" ".join(tokens[leading_family_token_count:]),
        )

    if leading_family_token_count == len(tokens):
        raise ValueError(
            "Candidate full name is ambiguous because every token looks like an "
            f"uppercase family token: {normalized_full_name!r}"
        )

    return CandidateNameParts(
        official_full_name=normalized_full_name,
        family_name=tokens[-1],
        given_name=" ".join(tokens[:-1]),
    )


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
    name_parts = parse_candidate_full_name(full_name)
    natural_accented = name_parts.natural_order_name
    natural_deaccented = _deaccent(natural_accented)
    normalized = " ".join(
        natural_deaccented.replace("-", " ").replace("'", " ").split()
    )
    return {
        "official": name_parts.official_full_name,
        "natural_accented": natural_accented,
        "natural_deaccented": natural_deaccented,
        "normalized": normalized,
    }


def build_gdelt_query(full_name: str, commune_name: str, mode: str = "precise") -> str:
    """Build a GDELT full-text query from the shared alias contract.

    Precise mode uses the accented candidate name and accented commune name for
    high-precision retrieval. Relaxed mode deaccents both the candidate name AND
    the commune name so that articles spelling either without diacritics are
    still retrieved — important for communes like Besançon or Châteauroux whose
    names appear both with and without accents in French news corpora.
    """
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
    # Relaxed mode: deaccent the commune name as well as the candidate name so
    # that both accented and ASCII spellings of place names are matched.
    commune_deaccented = _deaccent(commune_name)
    return f'"{aliases["natural_deaccented"]}" AND "{commune_deaccented}"'


def _build_phrase_clause(*phrases: str) -> str:
    """Build one quoted phrase clause with deduplicated Unicode/ASCII variants."""
    unique_phrases: list[str] = []
    seen_phrases: set[str] = set()
    for phrase in phrases:
        normalized_phrase = " ".join(str(phrase or "").split())
        if not normalized_phrase or normalized_phrase in seen_phrases:
            continue
        seen_phrases.add(normalized_phrase)
        unique_phrases.append(normalized_phrase)

    if not unique_phrases:
        raise ValueError("Expected at least one non-empty phrase variant.")
    if len(unique_phrases) == 1:
        return f'"{unique_phrases[0]}"'
    return "(" + " OR ".join(f'"{phrase}"' for phrase in unique_phrases) + ")"


def build_generic_news_query(full_name: str, commune_name: str) -> str:
    """Build a provider-agnostic search query for API and feed sources.

    API and feed providers like GNews often index both accented and ASCII-only
    spellings of French names and communes. The generic query therefore emits
    both variants when they differ so Tier 3 discovery does not regress behind
    the relaxed GDELT query contract.
    """
    aliases = build_candidate_aliases(full_name)
    candidate_clause = _build_phrase_clause(
        aliases["natural_accented"],
        aliases["natural_deaccented"],
    )
    commune_clause = _build_phrase_clause(commune_name, _deaccent(commune_name))
    return (
        f"{candidate_clause} {commune_clause} "
        "(municipales OR maire OR mairie OR liste)"
    )


def build_candidate_match_terms(case: CandidateQueryCase) -> dict[str, tuple[str, ...]]:
    """Build deterministic local-filter terms for RSS and sitemap entries."""
    aliases = build_candidate_aliases(case.full_name)
    name_parts = parse_candidate_full_name(case.full_name)
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
        "surname_tokens": _normalize_tokens(name_parts.family_name),
        "given_tokens": _normalize_tokens(name_parts.given_name),
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
