"""Shared ingest utilities used by all ingest modules.

This private module (underscore prefix = not public API) centralises the three
operations that every ingest function needs:
  1. Download a file from HTTP with retry-safe semantics and skip-if-unchanged.
  2. Compute an MD5 hash of a local file (streaming, safe for large files).
  3. Build the three bronze provenance columns that CLAUDE.md §6 mandates.

Industry pattern: a shared "_base" or "_utils" module for cross-cutting concerns
prevents copy-paste across ingest modules and ensures provenance columns are
applied consistently — a common code-review finding when they drift apart.
"""

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

# Chunk size for streaming downloads and MD5 computation.
# 8 MB balances memory usage with system-call overhead.
_CHUNK_SIZE_BYTES: int = 8 * 1024 * 1024


def compute_file_md5(file_path: Path) -> str:
    """Compute the MD5 hex digest of a file using streaming reads.

    Streaming (not loading the whole file into memory) is required for the
    137 MB candidates CSV. Loading it entirely would double the RAM footprint.

    Args:
        file_path: Path to the file.

    Returns:
        Lowercase 32-character hex MD5 string.

    Raises:
        FileNotFoundError: If file_path does not exist.
    """
    md5 = hashlib.md5()
    with open(file_path, "rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(_CHUNK_SIZE_BYTES), b""):
            md5.update(chunk)
    return md5.hexdigest()


def download_raw_file(
    url: str,
    dest_path: Path,
    timeout_seconds: int = 120,
) -> tuple[Path, str]:
    """Download a file to dest_path and return (path, md5_hex).

    Skip-if-unchanged: if dest_path already exists, its MD5 is computed first.
    The file is downloaded, and if the MD5 of the new content matches the
    existing file, the new content is discarded and the existing file is kept.
    This avoids re-writing a 137 MB file every pipeline re-run when the
    source has not changed.

    Note: data.gouv.fr uses HTTP redirects for its /api/1/datasets/r/<uuid>
    endpoints. requests follows redirects automatically (allow_redirects=True
    by default), so no special handling is needed.

    Args:
        url: HTTP(S) URL to download.
        dest_path: Absolute path where the raw file will be saved.
        timeout_seconds: requests timeout for the full download. 120 s is
            needed for the 137 MB candidates CSV on a slow connection.

    Returns:
        Tuple of (dest_path, md5_hex_string).

    Raises:
        requests.HTTPError: If the server returns a non-2xx status.
        requests.Timeout: If the request exceeds timeout_seconds.
        OSError: If dest_path's parent directory does not exist.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing MD5 before downloading so we can skip unchanged files.
    existing_md5: str | None = None
    if dest_path.exists():
        existing_md5 = compute_file_md5(dest_path)
        logger.info(
            "Existing file found dest_path=%s md5=%s", dest_path.name, existing_md5
        )

    logger.info("Downloading url=%s → dest=%s", url, dest_path.name)
    response = requests.get(url, stream=True, timeout=timeout_seconds)
    response.raise_for_status()

    # Stream-write to a temporary path, computing MD5 on the fly.
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
    new_md5_obj = hashlib.md5()
    bytes_written = 0

    with open(tmp_path, "wb") as tmp_file:
        for chunk in response.iter_content(chunk_size=_CHUNK_SIZE_BYTES):
            tmp_file.write(chunk)
            new_md5_obj.update(chunk)
            bytes_written += len(chunk)

    new_md5 = new_md5_obj.hexdigest()
    logger.info(
        "Download complete size_mb=%.1f md5=%s", bytes_written / 1_048_576, new_md5
    )

    if new_md5 == existing_md5:
        # Source file is unchanged — discard the download, keep existing.
        tmp_path.unlink()
        logger.info(
            "Source unchanged — keeping existing file dest_path=%s", dest_path.name
        )
    else:
        # New or updated file — replace the existing one atomically.
        tmp_path.replace(dest_path)
        logger.info(
            "File updated dest_path=%s old_md5=%s new_md5=%s",
            dest_path.name,
            existing_md5,
            new_md5,
        )

    return dest_path, new_md5


def build_provenance_columns(
    source_url: str,
    source_hash: str,
) -> dict[str, str]:
    """Return the three bronze provenance columns as a dict.

    CLAUDE.md §6 mandates that every bronze row carries:
      _source_url   — the URL the data was downloaded from
      _ingested_at  — UTC timestamp of the ingestion run
      _source_hash  — MD5 of the raw source file

    Centralising them here ensures every ingest module adds them consistently.
    In pandas, broadcast these as new columns after reading the raw file:

        provenance = build_provenance_columns(url, md5)
        for col, val in provenance.items():
            raw_df[col] = val

    Args:
        source_url: The URL the data was downloaded from.
        source_hash: MD5 hex digest of the downloaded raw file.

    Returns:
        Dict with keys ``_source_url``, ``_ingested_at``, ``_source_hash``.
    """
    return {
        "_source_url": source_url,
        "_ingested_at": datetime.now(UTC).isoformat(),
        "_source_hash": source_hash,
    }
