"""Shared deterministic normalization for NLP audit features.

The Phase 1 lexicon audit uses exact token and phrase matching. Centralizing
normalization here keeps matching semantics reproducible across count builders,
validators, and tests.
"""

from __future__ import annotations

import re
import unicodedata

import pandas as pd

_APOSTROPHE_OR_HYPHEN_PATTERN = re.compile(r"['’`´\-]+")
_NON_WORD_PATTERN = re.compile(r"[^0-9a-zA-Z_]+")
_REPEATED_WHITESPACE_PATTERN = re.compile(r"\s+")


def normalize_lexicon_text(value: object) -> str:
    """Normalize text for deterministic lexicon matching.

    Args:
        value: Raw text from the NLP input contract or a lexicon term.

    Returns:
        Lowercase ASCII-like text with accents, apostrophes, hyphens,
        punctuation, and repeated whitespace normalized.
    """
    if is_missing_scalar(value):
        return ""

    decomposed_text = unicodedata.normalize("NFKD", str(value))
    ascii_text = "".join(
        character
        for character in decomposed_text
        if not unicodedata.combining(character)
    )
    lowercase_text = ascii_text.lower()
    separated_text = _APOSTROPHE_OR_HYPHEN_PATTERN.sub(" ", lowercase_text)
    word_text = _NON_WORD_PATTERN.sub(" ", separated_text)
    return _REPEATED_WHITESPACE_PATTERN.sub(" ", word_text).strip()


def tokenize_lexicon_text(value: object) -> list[str]:
    """Tokenize text after lexicon normalization.

    Args:
        value: Raw text from the NLP input contract or a lexicon term.

    Returns:
        List of normalized whitespace-delimited tokens.
    """
    normalized_text = normalize_lexicon_text(value)
    if not normalized_text:
        return []
    return normalized_text.split()


def is_missing_scalar(value: object) -> bool:
    """Return whether a scalar value should be treated as missing."""
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def is_null_or_blank(value: object) -> bool:
    """Return whether a required text value is null or blank."""
    if is_missing_scalar(value):
        return True
    return str(value).strip() == ""
