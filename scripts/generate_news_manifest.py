"""Generate a news_import_manifest.json for a Europresse PDF batch directory.

Run this once after you have finished downloading all PDFs into a cohort folder.
The script scans ``--cohort-dir`` for PDF files, optionally collects PDFs from
additional ``--include-dirs`` directories (without copying them), builds a
NewsImportManifest, and writes ``news_import_manifest.json`` into ``--cohort-dir``.

Each PDF is stored exactly once on disk. Sensitivity-analysis cohorts that
share articles with the primary cohort reference those files by path — no
copying required.

Re-running is safe: the manifest is overwritten in place.

Usage
-----
Primary cohort (36 PDFs, all new)::

    .venv/Scripts/python scripts/generate_news_manifest.py `
        --cohort-dir  data/raw/news/cohort_36 `
        --cohort-id   cohort36 `
        --operator    yyfen `
        --window-start 2025-11-01 `
        --window-end   2026-04-30 `
        --notes "Primary 36-candidate cohort. 18F+18M matched stratified sample."

SA — expanded to 48 (12 new PDFs in cohort_sa_48/, 36 shared from cohort_36/)::

    .venv/Scripts/python scripts/generate_news_manifest.py `
        --cohort-dir   data/raw/news/cohort_sa_48 `
        --cohort-id    sa_48 `
        --operator     yyfen `
        --window-start 2025-11-01 `
        --window-end   2026-04-30 `
        --include-dirs data/raw/news/cohort_36 `
        --notes "SA: expanded cohort to 48 (24F+24M). Includes all 36 primary candidates."

SA — relaxed constraints (some candidates overlap with cohort_36)::

    .venv/Scripts/python scripts/generate_news_manifest.py `
        --cohort-dir   data/raw/news/cohort_sa_relaxed `
        --cohort-id    sa_relaxed `
        --operator     yyfen `
        --window-start 2025-11-01 `
        --window-end   2026-04-30 `
        --include-dirs data/raw/news/cohort_36 `
        --notes "SA: relaxed viability threshold. Overlapping candidates referenced from cohort_36/."

Makefile shortcut::

    make generate-manifest COHORT_DIR=... COHORT_ID=... OPERATOR=yyfen \\
        WINDOW_START=2025-11-01 WINDOW_END=2026-04-30 NOTES="..."

    # With shared PDFs from another cohort:
    make generate-manifest COHORT_DIR=data/raw/news/cohort_sa_48 COHORT_ID=sa_48 \\
        OPERATOR=yyfen INCLUDE_DIRS=data/raw/news/cohort_36 NOTES="SA: expanded to 48."
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

# Allow running from the project root without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingest.news.corpus import write_news_import_manifest
from src.ingest.news.models import NewsImportManifest

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def _build_batch_id(cohort_id: str, exported_at: datetime) -> str:
    """Build a deterministic batch_id from cohort identifier and export date."""
    date_tag = exported_at.strftime("%Y%m%d")
    return f"europresse_{cohort_id}_{date_tag}"


def _collect_pdf_paths(
    cohort_dir: Path,
    include_dirs: list[Path],
) -> list[Path]:
    """Collect PDF paths from cohort_dir and any additional include_dirs.

    PDFs in cohort_dir are listed first (the new files for this cohort).
    PDFs from each include_dir follow in sorted order.
    Duplicate filenames across directories are allowed — each path is unique.

    Args:
        cohort_dir: Primary directory for this cohort's new PDFs.
        include_dirs: Extra directories whose PDFs are shared with this cohort.

    Returns:
        Deduplicated, sorted list of PDF paths.

    Raises:
        FileNotFoundError: If cohort_dir or any include_dir does not exist.
    """
    all_dirs = [cohort_dir] + include_dirs
    seen: set[Path] = set()
    ordered: list[Path] = []

    for directory in all_dirs:
        if not directory.exists():
            raise FileNotFoundError(f"Directory not found: {directory}")
        for pdf_path in sorted(directory.glob("*.pdf")):
            resolved = pdf_path.resolve()
            if resolved not in seen:
                seen.add(resolved)
                ordered.append(pdf_path)

    return ordered


def generate_manifest(
    *,
    cohort_dir: Path,
    cohort_id: str,
    operator: str,
    window_start: date,
    window_end: date,
    notes: str,
    access_level: str,
    exported_at: datetime,
    include_dirs: list[Path] | None = None,
) -> Path:
    """Scan directories for PDF files and write news_import_manifest.json.

    Each PDF is stored once on disk. Sensitivity-analysis cohorts reference
    shared PDFs from other cohort directories via path, not by copying.

    Args:
        cohort_dir: Directory for this cohort's own PDFs (manifest written here).
        cohort_id: Short identifier used in batch_id, e.g. "cohort36", "sa_48".
        operator: Your Europresse username or initials (tracked for audit trail).
        window_start: Analysis window start date (inclusive).
        window_end: Analysis window end date (inclusive).
        notes: Human-readable description of this batch's purpose.
        access_level: Europresse licence description.
        exported_at: Timestamp recorded as the export time.
        include_dirs: Additional directories whose PDFs are included in this
            cohort (referenced by path; never copied). Useful for sensitivity
            analyses that extend the primary cohort.

    Returns:
        Path to the written manifest file.

    Raises:
        FileNotFoundError: If cohort_dir or any include_dir does not exist.
        ValueError: If no PDF files are found across all scanned directories.
    """
    resolved_include_dirs = include_dirs or []
    pdf_paths = _collect_pdf_paths(cohort_dir, resolved_include_dirs)

    if not pdf_paths:
        searched = ", ".join(str(d) for d in [cohort_dir] + resolved_include_dirs)
        raise ValueError(
            f"No PDF files found in: {searched}. "
            "Download Europresse exports before generating the manifest."
        )

    # Log a clear breakdown so the operator can verify correctness.
    own_count = len(list(cohort_dir.glob("*.pdf")))
    shared_count = len(pdf_paths) - own_count
    logger.info(
        "PDF inventory: %d own (in %s)  +  %d shared (from %d include-dir(s))  =  %d total",
        own_count,
        cohort_dir.name,
        shared_count,
        len(resolved_include_dirs),
        len(pdf_paths),
    )
    for pdf_path in pdf_paths:
        origin = (
            cohort_dir.name
            if pdf_path.parent.resolve() == cohort_dir.resolve()
            else pdf_path.parent.name
        )
        logger.info("  [%s] %s", origin, pdf_path.name)

    batch_id = _build_batch_id(cohort_id, exported_at)
    manifest = NewsImportManifest(
        batch_id=batch_id,
        source_system="europresse",
        window_start=window_start,
        window_end=window_end,
        exported_at=exported_at,
        operator=operator,
        access_level=access_level,
        # Paths are stored as-is (relative to project root) so the manifest
        # is portable across machines with the same directory layout.
        file_paths=tuple(str(p) for p in pdf_paths),
        notes=notes,
    )

    manifest_path = cohort_dir / "news_import_manifest.json"
    write_news_import_manifest(manifest, manifest_path)
    logger.info(
        "Manifest written → %s  (batch_id=%s, total_files=%d)",
        manifest_path,
        batch_id,
        len(pdf_paths),
    )
    return manifest_path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate news_import_manifest.json for a Europresse PDF batch.\n"
            "Each PDF is stored once; sensitivity-analysis cohorts reference\n"
            "shared PDFs from other directories via --include-dirs."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--cohort-dir",
        required=True,
        type=Path,
        metavar="PATH",
        help="Directory containing this cohort's own PDFs (manifest written here).",
    )
    parser.add_argument(
        "--cohort-id",
        required=True,
        metavar="ID",
        help='Short identifier used in batch_id, e.g. "cohort36", "sa_48", "sa_relaxed".',
    )
    parser.add_argument(
        "--operator",
        required=True,
        metavar="NAME",
        help="Your username or initials (tracked in audit trail).",
    )
    parser.add_argument(
        "--window-start",
        required=True,
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Analysis window start date (inclusive).",
    )
    parser.add_argument(
        "--window-end",
        required=True,
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        help="Analysis window end date (inclusive).",
    )
    parser.add_argument(
        "--include-dirs",
        nargs="+",
        type=Path,
        default=[],
        metavar="PATH",
        help=(
            "Additional directories whose PDFs are included in this cohort "
            "(referenced by path, never copied). Repeat or space-separate for "
            "multiple directories."
        ),
    )
    parser.add_argument(
        "--notes",
        default="",
        metavar="TEXT",
        help="Human-readable description of this cohort batch.",
    )
    parser.add_argument(
        "--access-level",
        default="restricted subscription export",
        metavar="TEXT",
        help='Europresse licence description (default: "restricted subscription export").',
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        generate_manifest(
            cohort_dir=args.cohort_dir,
            cohort_id=args.cohort_id,
            operator=args.operator,
            window_start=args.window_start,
            window_end=args.window_end,
            notes=args.notes,
            access_level=args.access_level,
            exported_at=datetime.now(UTC),
            include_dirs=args.include_dirs,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
