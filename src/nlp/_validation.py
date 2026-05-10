"""Shared DataFrame validation helpers for the NLP package."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from src.nlp.normalization import is_null_or_blank
from src.transform._exceptions import DataQualityError


def require_columns(
    *,
    dataframe: pd.DataFrame,
    required_columns: frozenset[str],
    dataframe_name: str,
) -> None:
    """Raise when a DataFrame is missing required contract columns.

    Args:
        dataframe: DataFrame being validated.
        required_columns: Required column names for the contract.
        dataframe_name: Human-readable source name used in error messages.

    Raises:
        DataQualityError: If any required columns are absent.
    """
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise DataQualityError(
            f"{dataframe_name} missing required columns: {missing_columns}"
        )


def validate_unique_key(
    *,
    dataframe: pd.DataFrame,
    key_columns: tuple[str, ...],
    dataframe_name: str,
) -> None:
    """Raise when a declared key has duplicate rows.

    Args:
        dataframe: DataFrame being validated.
        key_columns: Column tuple defining the expected unique key.
        dataframe_name: Human-readable source name used in error messages.

    Raises:
        DataQualityError: If duplicate key rows are present.
    """
    duplicate_mask = dataframe.duplicated(subset=list(key_columns), keep=False)
    if duplicate_mask.any():
        duplicate_examples = (
            dataframe.loc[duplicate_mask, list(key_columns)]
            .drop_duplicates()
            .head(5)
            .to_dict("records")
        )
        raise DataQualityError(
            f"{dataframe_name} has duplicate key rows for {list(key_columns)}: "
            f"{duplicate_examples}"
        )


def validate_required_identifier_values(
    *,
    dataframe: pd.DataFrame,
    dataframe_name: str,
    identifier_columns: Iterable[str],
) -> None:
    """Raise when required identifier columns contain null or blank values.

    Args:
        dataframe: DataFrame being validated.
        dataframe_name: Human-readable source name used in error messages.
        identifier_columns: Identifier columns that must be present, non-null,
            and non-blank.

    Raises:
        DataQualityError: If any identifier value is null or blank.
    """
    for column_name in identifier_columns:
        invalid_identifier_mask = dataframe[column_name].map(is_null_or_blank)
        if invalid_identifier_mask.any():
            invalid_count = int(invalid_identifier_mask.sum())
            raise DataQualityError(
                f"{dataframe_name} has {invalid_count} null or blank "
                f"{column_name} values"
            )


def coerce_utc_timestamp(value: pd.Timestamp | str | None) -> pd.Timestamp:
    """Return a timezone-aware UTC timestamp.

    Args:
        value: Optional timestamp-like value. ``None`` resolves to the current
            UTC timestamp.

    Returns:
        Timezone-aware pandas timestamp in UTC.
    """
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def pipeline_device_arg(device: str) -> int | str:
    """Convert persisted device metadata to a Hugging Face pipeline argument.

    Args:
        device: Persisted model device value such as ``cpu``, ``cuda``, or
            ``cuda:0``.

    Returns:
        Hugging Face pipeline device argument. ``cpu`` maps to ``-1`` and
        ``cuda`` maps to ``0``.
    """
    normalized_device = device.strip().lower()
    if normalized_device == "cpu":
        return -1
    if normalized_device == "cuda":
        return 0
    if normalized_device.startswith("cuda:"):
        device_suffix = normalized_device.split(":", 1)[1]
        if device_suffix.isdigit():
            return int(device_suffix)
    return device
