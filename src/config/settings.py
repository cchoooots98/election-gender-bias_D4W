"""Application settings — single source of truth for all constants and config.

All tuneable values are loaded from the .env file (via python-dotenv) so
that no secrets or environment-specific paths are hard-coded in source code.

Usage in other modules:
    from src.config.settings import WAREHOUSE_PATH, GDELT_MAX_RECORDS

Industry pattern: a central settings module (also called "config" or "env")
is standard in Django, FastAPI, and most data pipeline frameworks. It keeps
configuration in one place and makes the pipeline testable by overriding
os.environ before importing.
"""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env into os.environ before reading any values.
# override=False: real env vars (e.g. CI secrets injected by GitHub Actions)
# take precedence over the .env file — important for reproducible CI runs.
load_dotenv(override=False)

# ── Logging ───────────────────────────────────────────────────────────────────
# Configured once here; every module then calls logging.getLogger(__name__).
# ISO-8601 timestamps make log correlation with external systems straightforward.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

# ── Repository root ───────────────────────────────────────────────────────────
# settings.py lives at src/config/settings.py — two .parents up = project root.
# Using pathlib.Path (not os.path) is the modern Python 3.4+ standard and
# handles Windows/Linux path separators automatically.
PROJECT_ROOT: Path = Path(__file__).resolve().parents[2]

# ── Analysis scope ────────────────────────────────────────────────────────────
ANALYSIS_START_DATE: str = os.getenv("ANALYSIS_START_DATE", "2026-02-01")
ANALYSIS_END_DATE: str = os.getenv("ANALYSIS_END_DATE", "2026-04-30")

# Must be even: the matched-pair analysis requires equal M/F sample sizes.
# Validation is intentionally deferred to the pipeline entry point, not here,
# to keep settings.py free of business logic.
CANDIDATE_SAMPLE_SIZE: int = int(os.getenv("CANDIDATE_SAMPLE_SIZE", "24"))

# ── Paths ─────────────────────────────────────────────────────────────────────
# All paths are resolved relative to PROJECT_ROOT so the project works
# regardless of the current working directory when scripts are invoked.
DATA_DIR: Path = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
RAW_DIR: Path = DATA_DIR / "raw"
BRONZE_DIR: Path = DATA_DIR / "bronze"
SILVER_DIR: Path = DATA_DIR / "silver"
GOLD_DIR: Path = DATA_DIR / "gold"
CACHE_DIR: Path = DATA_DIR / "cache"

# warehouse/ holds the single DuckDB file. Not committed to git (binary + large).
WAREHOUSE_PATH: Path = PROJECT_ROOT / os.getenv(
    "WAREHOUSE_PATH", "warehouse/municipal.duckdb"
)

# ── GDELT ─────────────────────────────────────────────────────────────────────
# GDELT DOC 2.0 API hard cap is 250 records per request; do not raise this.
GDELT_MAX_RECORDS: int = int(os.getenv("GDELT_MAX_RECORDS", "250"))

# Respectful rate limiting: 2 s between requests avoids triggering GDELT's
# anti-scraping protections and is courteous to a free public API.
GDELT_REQUEST_DELAY_SECONDS: float = float(
    os.getenv("GDELT_REQUEST_DELAY_SECONDS", "2")
)

# ── Web scraping ──────────────────────────────────────────────────────────────
SCRAPE_REQUEST_TIMEOUT_SECONDS: int = int(
    os.getenv("SCRAPE_REQUEST_TIMEOUT_SECONDS", "30")
)
SCRAPE_RETRY_MAX_ATTEMPTS: int = int(os.getenv("SCRAPE_RETRY_MAX_ATTEMPTS", "3"))
SCRAPE_RETRY_BACKOFF_SECONDS: float = float(
    os.getenv("SCRAPE_RETRY_BACKOFF_SECONDS", "5")
)

# ── NLP models ────────────────────────────────────────────────────────────────
# Model names are pinned for reproducibility: the same model name may resolve
# to different weights if the HuggingFace Hub maintainer pushes an update.
# Pin to a specific revision (commit hash) in production for full guarantees.
NER_MODEL_NAME: str = os.getenv("NER_MODEL_NAME", "Jean-Baptiste/camembert-ner")
SENTIMENT_MODEL_NAME: str = os.getenv(
    "SENTIMENT_MODEL_NAME", "cmarkea/distilcamembert-base-sentiment"
)
NLI_MODEL_NAME: str = os.getenv(
    "NLI_MODEL_NAME", "BaptisteDoyen/camembert-base-xnli"
)
EMBEDDING_MODEL_NAME: str = os.getenv(
    "EMBEDDING_MODEL_NAME", "dangvantuan/sentence-camembert-base"
)

# ── NLP inference ─────────────────────────────────────────────────────────────
# Batch size: trade-off between GPU memory usage and throughput.
# 32 is a safe default for a 6-8 GB GPU; reduce if OOM errors occur.
NLP_BATCH_SIZE: int = int(os.getenv("NLP_BATCH_SIZE", "32"))

# CamemBERT's maximum context window is 512 tokens; truncation beyond this
# is handled by the tokenizer, but flagged in logs by the NLP modules.
NLP_MAX_TOKEN_LENGTH: int = int(os.getenv("NLP_MAX_TOKEN_LENGTH", "512"))

# ── Data quality (DQ) thresholds ─────────────────────────────────────────────
# DQ = Data Quality. These thresholds determine when a pipeline step fails
# vs. quarantines bad rows to a _rejected sub-table (see CLAUDE.md §6).
#
# Articles shorter than this are likely navigation fragments or paywalled
# stubs, not real article text — quarantine rather than analyse.
DQ_MIN_ARTICLE_TEXT_LENGTH: int = int(
    os.getenv("DQ_MIN_ARTICLE_TEXT_LENGTH", "100")
)

# If more than 5% of rows in a key column are NULL, the silver write fails
# with a DQ error rather than silently propagating bad data to gold.
DQ_MAX_NULL_RATE: float = float(os.getenv("DQ_MAX_NULL_RATE", "0.05"))
