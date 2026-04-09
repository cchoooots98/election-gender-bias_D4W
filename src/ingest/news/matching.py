"""Candidate-resolution helpers for the canonical news corpus."""

from __future__ import annotations

import logging

import pandas as pd

from src.ingest.news.corpus import split_sentences
from src.ingest.news.normalize import stable_md5 as _stable_md5
from src.ingest.news.queries import (
    ELECTION_KEYWORDS,
    build_candidate_aliases,
    normalize_text_for_match,
    parse_candidate_full_name,
)

logger = logging.getLogger(__name__)

# Single authoritative source: queries.py owns ELECTION_KEYWORDS so that
# pre-filter (RSS/sitemap) and body-text resolution use the identical set.
_ELECTION_KEYWORDS = ELECTION_KEYWORDS
_MENTION_COLUMNS = [
    "mention_id",
    "canonical_article_id",
    "leader_id",
    "context_sentences",
    "context_token_count",
    "headline_mention_flag",
    "match_method",
    "match_score",
    "ambiguity_reason",
    "nlp_enrichment_status",
    "sentiment_score",
    "sentiment_label",
    "frame_label",
    "frame_score",
]
_MANUAL_REVIEW_COLUMNS = [
    "canonical_article_id",
    "leader_id",
    "title",
    "outlet_name",
    "published_at",
    "proposed_match_method",
    "match_score",
    "ambiguity_reason",
]
def _build_candidate_profile(candidate_row: dict[str, object]) -> dict[str, object]:
    """Build one deterministic matching profile from a sampled candidate row."""
    full_name = str(candidate_row["full_name"])
    aliases = build_candidate_aliases(full_name)
    name_parts = parse_candidate_full_name(full_name)
    surname_tokens = tuple(
        token
        for token in normalize_text_for_match(name_parts.family_name).split()
        if len(token) >= 3
    )
    given_tokens = tuple(
        token
        for token in normalize_text_for_match(name_parts.given_name).split()
        if len(token) >= 2
    )
    commune_tokens = tuple(
        token
        for token in normalize_text_for_match(
            str(candidate_row["commune_name"])
        ).split()
        if len(token) >= 3
    )
    full_name_variants = tuple(
        sorted(
            {
                normalize_text_for_match(aliases["official"]),
                normalize_text_for_match(aliases["natural_accented"]),
                normalize_text_for_match(aliases["natural_deaccented"]),
                normalize_text_for_match(aliases["normalized"]),
            }
        )
    )
    return {
        "leader_id": str(candidate_row["leader_id"]),
        "full_name": full_name,
        "same_name_candidate_count": int(
            candidate_row.get("same_name_candidate_count") or 0
        ),
        "surname_tokens": surname_tokens,
        "given_tokens": given_tokens,
        "commune_tokens": commune_tokens,
        "full_name_variants": full_name_variants,
    }


def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
    """Return whether any normalized token appears in the normalized text."""
    return any(token in text for token in tokens if token)


def _select_context_sentences(
    article_row: dict[str, object],
    candidate_profile: dict[str, object],
) -> list[str]:
    """Keep only the candidate-relevant 1-3 sentence windows."""
    relevant_sentences = []
    for sentence in split_sentences(str(article_row["body_text"])):
        normalized_sentence = normalize_text_for_match(sentence)
        if any(
            variant and variant in normalized_sentence
            for variant in candidate_profile["full_name_variants"]
        ):
            relevant_sentences.append(sentence.strip())
            continue
        if _contains_any(normalized_sentence, candidate_profile["surname_tokens"]) and (
            _contains_any(normalized_sentence, candidate_profile["commune_tokens"])
            or any(keyword in normalized_sentence for keyword in _ELECTION_KEYWORDS)
        ):
            relevant_sentences.append(sentence.strip())

    if not relevant_sentences and str(article_row["title"]).strip():
        relevant_sentences.append(str(article_row["title"]).strip())
    return relevant_sentences[:3]


def build_fact_mentions(
    fact_article_df: pd.DataFrame,
    sample_leaders_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve canonical articles to sampled candidates.

    Args:
        fact_article_df: Canonical article table.
        sample_leaders_df: Frozen sampled-cohort table.

    Returns:
        Tuple of ``(fact_mention_df, manual_review_df)``.
    """
    if fact_article_df.empty:
        return (
            pd.DataFrame(columns=_MENTION_COLUMNS),
            pd.DataFrame(columns=_MANUAL_REVIEW_COLUMNS),
        )

    candidate_profiles = [
        _build_candidate_profile(candidate_row)
        for candidate_row in sample_leaders_df.to_dict("records")
    ]
    surname_collision_count: dict[str, int] = {}
    for profile in candidate_profiles:
        for surname_token in profile["surname_tokens"]:
            surname_collision_count[surname_token] = (
                surname_collision_count.get(surname_token, 0) + 1
            )

    mention_rows: list[dict[str, object]] = []
    manual_review_rows: list[dict[str, object]] = []

    for article_row in fact_article_df.to_dict("records"):
        title_normalized = normalize_text_for_match(str(article_row["title"]))
        body_normalized = normalize_text_for_match(str(article_row["body_text"]))
        combined_text = f"{title_normalized} {body_normalized}".strip()

        for profile in candidate_profiles:
            title_hit = any(
                variant and variant in title_normalized
                for variant in profile["full_name_variants"]
            )
            full_name_hit = any(
                variant and variant in combined_text
                for variant in profile["full_name_variants"]
            )
            surname_hit = _contains_any(combined_text, profile["surname_tokens"])
            given_hit = _contains_any(combined_text, profile["given_tokens"])
            commune_hit = _contains_any(combined_text, profile["commune_tokens"])
            election_hit = any(
                keyword in combined_text for keyword in _ELECTION_KEYWORDS
            )

            match_method = ""
            match_score = 0.0
            ambiguity_reason = ""

            if full_name_hit:
                match_method = "exact_full_name"
                match_score = 1.0
            elif surname_hit and given_hit:
                match_method = "normalized_alias"
                match_score = 0.9
            elif surname_hit and commune_hit and election_hit:
                match_method = "surname_commune_context"
                match_score = 0.75
            elif surname_hit and commune_hit:
                context_sentences = _select_context_sentences(article_row, profile)
                if context_sentences:
                    match_method = "body_sentence_verification"
                    match_score = 0.7

            if not match_method:
                continue

            surname_ambiguous = any(
                surname_collision_count.get(token, 0) > 1
                for token in profile["surname_tokens"]
            )
            if (
                match_method
                in {"surname_commune_context", "body_sentence_verification"}
                and (surname_ambiguous or profile["same_name_candidate_count"] > 1)
                and not given_hit
            ):
                ambiguity_reason = "surname-only evidence is ambiguous"

            if ambiguity_reason:
                manual_review_rows.append(
                    {
                        "canonical_article_id": article_row["canonical_article_id"],
                        "leader_id": profile["leader_id"],
                        "title": article_row["title"],
                        "outlet_name": article_row["outlet_name"],
                        "published_at": article_row["published_at"],
                        "proposed_match_method": match_method,
                        "match_score": match_score,
                        "ambiguity_reason": ambiguity_reason,
                    }
                )
                continue

            context_sentences = _select_context_sentences(article_row, profile)
            mention_rows.append(
                {
                    "mention_id": _stable_md5(
                        f"{article_row['canonical_article_id']}|{profile['leader_id']}"
                    ),
                    "canonical_article_id": article_row["canonical_article_id"],
                    "leader_id": profile["leader_id"],
                    "context_sentences": " || ".join(context_sentences),
                    "context_token_count": len(
                        normalize_text_for_match(" ".join(context_sentences)).split()
                    ),
                    "headline_mention_flag": bool(title_hit),
                    "match_method": match_method,
                    "match_score": match_score,
                    "ambiguity_reason": None,
                    "nlp_enrichment_status": "pending",
                    "sentiment_score": None,
                    "sentiment_label": None,
                    "frame_label": None,
                    "frame_score": None,
                }
            )

    fact_mention_df = pd.DataFrame(mention_rows, columns=_MENTION_COLUMNS)
    manual_review_df = pd.DataFrame(
        manual_review_rows,
        columns=_MANUAL_REVIEW_COLUMNS,
    )
    if not fact_mention_df.empty:
        fact_mention_df = fact_mention_df.drop_duplicates(
            subset=["canonical_article_id", "leader_id"],
            keep="first",
        ).reset_index(drop=True)
    logger.info(
        "Candidate resolution complete mentions=%d manual_review=%d",
        len(fact_mention_df),
        len(manual_review_df),
    )
    return fact_mention_df, manual_review_df
