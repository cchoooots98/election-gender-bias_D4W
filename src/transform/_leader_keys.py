"""Shared leader-key helpers used across candidate and election-result models.

The project needs one deterministic business-key contract for list leaders.
Centralising it here prevents the classic warehouse bug where two models build
"the same" entity key with slightly different name-normalisation rules.
"""

from __future__ import annotations

import hashlib
import unicodedata

import pandas as pd


def normalize_leader_name(name: str) -> str:
    """Normalise a French leader name for matching and surrogate-keying.

    Args:
        name: Raw name string.

    Returns:
        Uppercase ASCII name with punctuation variants normalised to spaces.
    """
    if not name or not isinstance(name, str):
        return ""

    normalized = name.strip().upper()
    normalized = normalized.replace("-", " ").replace("'", " ").replace("\u2019", " ")
    normalized = (
        unicodedata.normalize("NFD", normalized)
        .encode("ascii", "ignore")
        .decode("ascii")
    )
    return " ".join(normalized.split())


def build_full_name(family_name: str, given_name: str) -> str:
    """Build one display name from family and given names."""
    return f"{family_name or ''} {given_name or ''}".strip()


def build_full_name_columns(candidate_df: pd.DataFrame) -> pd.DataFrame:
    """Ensure full_name and full_name_normalized columns exist.

    Args:
        candidate_df: DataFrame containing candidate/person name columns.

    Returns:
        Copy of the input with full_name and full_name_normalized.

    Raises:
        ValueError: If full_name is absent and the source name components are
            missing or blank.
    """
    result_df = candidate_df.copy()

    if "full_name" not in result_df.columns:
        required_name_columns = {"family_name", "given_name"}
        missing_name_columns = sorted(required_name_columns - set(result_df.columns))
        if missing_name_columns:
            raise ValueError(
                "Cannot build full_name without required columns: "
                f"{missing_name_columns}"
            )

        family_name = result_df["family_name"].astype("string")
        given_name = result_df["given_name"].astype("string")
        blank_component_mask = (
            family_name.isna()
            | given_name.isna()
            | family_name.str.strip().eq("")
            | given_name.str.strip().eq("")
        )
        if blank_component_mask.any():
            invalid_rows = result_df.index[blank_component_mask].tolist()[:10]
            raise ValueError(
                "Cannot build full_name from blank family_name/given_name rows: "
                f"{invalid_rows}"
            )

        result_df["full_name"] = family_name.str.strip() + " " + given_name.str.strip()

    result_df["full_name_normalized"] = result_df["full_name"].apply(
        normalize_leader_name
    )
    return result_df


def generate_leader_id(full_name: str, commune_insee: str) -> str:
    """Generate a deterministic surrogate key for a list leader.

    Args:
        full_name: Official leader full name.
        commune_insee: Five-character commune code.

    Returns:
        32-character lowercase MD5 hex digest.
    """
    raw = f"{full_name}|{commune_insee}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()
