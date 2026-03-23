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
NLI_MODEL_NAME: str = os.getenv("NLI_MODEL_NAME", "BaptisteDoyen/camembert-base-xnli")
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
# vs. quarantines bad rows to a _rejected sub-table with a _rejection_reason column.
#
# Articles shorter than this are likely navigation fragments or paywalled
# stubs, not real article text — quarantine rather than analyse.
DQ_MIN_ARTICLE_TEXT_LENGTH: int = int(os.getenv("DQ_MIN_ARTICLE_TEXT_LENGTH", "100"))

# If more than 5% of rows in a key column are NULL, the silver write fails
# with a DQ error rather than silently propagating bad data to gold.
DQ_MAX_NULL_RATE: float = float(os.getenv("DQ_MAX_NULL_RATE", "0.05"))

# ── Data source URLs ──────────────────────────────────────────────────────────
# All download URLs live here, not scattered across ingest modules.
# If data.gouv.fr updates a resource ID, this is the only file that changes.
# Pattern: 12-factor app config — "store config in the environment".
DATA_SOURCES: dict[str, dict[str, str]] = {
    "candidates_tour1": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/b929c2a4-18ec-4e8b-bc37-2ff346a867cd",
        "description": "Interior Ministry: Tour 1 candidate lists, 2026 municipal elections",
        "format": "csv",
        "raw_filename": "candidates_tour1.csv",
    },
    "seats_population": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/24b13901-052e-4e93-8694-3f071cb8df25",
        "description": "Interior Ministry: Municipal council seats and INSEE population per commune",
        "format": "xlsx",
        "raw_filename": "seats_population.xlsx",
    },
    "rne_incumbents": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/c5026511-0a7f-4d79-9c2f-5c61376d2c0b",
        "description": "Interior Ministry RNE: Current mayors (sortants) as of 2026-02-27",
        "format": "csv",
        "raw_filename": "rne_incumbents.csv",
    },
    "insee_cog_communes": {
        "url": "https://www.insee.fr/fr/statistiques/fichier/8740222/v_commune_2026.csv",
        "description": "INSEE COG 2026: Official commune codes, department codes, region codes",
        "format": "csv",
        "raw_filename": "insee_cog_communes.csv",
    },
    "candidates_tour2": {
        "url": "https://www.data.gouv.fr/api/1/datasets/r/c7e8ced6-3d08-452e-af06-d553634b6d61",
        "description": "Interior Ministry: Tour 2 candidate lists (optional — used for 'advanced to round 2' flag)",
        "format": "csv",
        "raw_filename": "candidates_tour2.csv",
    },
}

# ── City-size stratification thresholds (resident population) ─────────────────
# We use population municipale (resident population) rather than registered
# voters. Rationale: (1) registered voters for 2026 are not yet available as
# structured per-commune data; (2) for stratification the distinction is
# negligible — registered voters track resident population within ~5%.
# Limitation is documented in README.
# Communes < CITY_SIZE_SMALL_THRESHOLD are EXCLUDED from the sample:
# media coverage near these communes is near-zero and GDELT returns 0 results.
CITY_SIZE_LARGE_THRESHOLD: int = 100_000  # ≥ 100k → 'large'
CITY_SIZE_MEDIUM_THRESHOLD: int = 20_000  # 20k–99k → 'medium'
CITY_SIZE_SMALL_THRESHOLD: int = 3_500  # 3.5k–20k → 'small'; < 3500 → 'excluded'

# ── PLM cities (Paris / Lyon / Marseille) — special electoral system ──────────
# The Loi PLM (Loi n°82-1169, 31 December 1982) governs elections in France's
# three largest cities. Unlike every other commune, voters in Paris, Lyon, and
# Marseille elect candidates by arrondissement (sub-district), not by the city
# as a whole. Each arrondissement has its own candidate list (tête de liste),
# and those lists elect an arrondissement council, which in turn sends delegates
# to the city-wide municipal council (conseil municipal), which then elects the
# mayor.
#
# This creates a structural mismatch with the rest of the pipeline:
#   - The COG (Code Officiel Géographique) represents each of the three cities
#     as a single commune (TYPECOM == 'COM') with codes 75056 / 69123 / 13055.
#   - The Interior Ministry candidate file uses arrondissement-level codes
#     (75101–75120 for Paris, 69381–69389 for Lyon, 13201–13216 for Marseille).
#   - A naive LEFT JOIN on commune_insee produces no match for any PLM candidate,
#     silently dropping ~10 million residents from the analysis.
#
# Current treatment (Phase A — main analysis):
#   PLM arrondissement candidates are explicitly excluded before the dim_commune
#   JOIN, with a logged warning. Paris, Lyon, and Marseille are documented as a
#   known limitation in the research output.
#
# Future treatment (Phase B — sensitivity analysis):
#   Use PLM_ARRONDISSEMENT_MAP to re-attach ARM candidates to their parent
#   commune, select one representative tête de liste per party per PLM city,
#   add commune_type = 'plm' column, and include is_plm_city as a regression
#   control variable. See project backlog for implementation plan.
#
# INSEE codes:
#   Paris     (75056): 20 arrondissements, codes 75101–75120
#   Lyon      (69123):  9 arrondissements, codes 69381–69389
#   Marseille (13055): 16 arrondissements, codes 13201–13216
PLM_ARRONDISSEMENT_MAP: dict[str, str] = {
    # Paris arrondissements 75101–75120 → parent commune 75056
    **{f"751{str(i).zfill(2)}": "75056" for i in range(1, 21)},
    # Lyon arrondissements 69381–69389 → parent commune 69123
    **{f"6938{i}": "69123" for i in range(1, 10)},
    # Marseille arrondissements 13201–13216 → parent commune 13055
    **{f"132{str(i).zfill(2)}": "13055" for i in range(1, 17)},
}

# The three parent commune codes — useful for quickly checking whether a
# commune is a PLM city without iterating through PLM_ARRONDISSEMENT_MAP.
PLM_COMMUNE_CODES: frozenset[str] = frozenset({"75056", "69123", "13055"})

# ── Sampling configuration ────────────────────────────────────────────────────
# Target: CANDIDATE_SAMPLE_SIZE = 24 = 12F + 12M, distributed across 3 strata.
# Matched stratified sampling (not simple random) ensures gender balance WITHIN
# each city-size stratum — preventing the confound of "large-city men vs.
# small-city women" that simple random sampling would produce.
SAMPLE_LARGE_TOTAL: int = 4  # 2F + 2M  (large cities ≥ 100k)
SAMPLE_MEDIUM_TOTAL: int = 8  # 4F + 4M  (medium cities 20k–100k)
SAMPLE_SMALL_TOTAL: int = 12  # 6F + 6M  (small cities 3.5k–20k)

# Minimum number of distinct INSEE region codes in the final sample.
# Ensures geographic diversity — avoids a sample dominated by Île-de-France.
SAMPLE_MIN_REGION_COUNT: int = 4

# Reproducible random seed stored here so it appears in the sample manifest.
# Changing this seed changes the sample — document any change in the commit message.
SAMPLING_RANDOM_SEED: int = int(os.getenv("SAMPLING_RANDOM_SEED", "42"))

# ── Incumbent fuzzy-matching ──────────────────────────────────────────────────
# Minimum rapidfuzz token_sort_ratio score (0–100) to declare an RNE match.
# 85 accepts minor spelling/accent variations while rejecting unrelated names.
# All matches below 100 (non-exact) are logged for manual audit.
INCUMBENT_MATCH_THRESHOLD: int = int(os.getenv("INCUMBENT_MATCH_THRESHOLD", "85"))

# ── Political nuance → analytical bloc mapping ────────────────────────────────
# CODENUA codes are the Interior Ministry's official political affiliation codes.
# We collapse them into 5 analytical blocs for the regression model.
# Source: https://www.interieur.gouv.fr/Elections/Les-resultats/Municipales
# Keeping this map in settings.py means adding/renaming a nuance code requires
# changing exactly one file — not hunting through transform logic.
NUANCE_GROUP_MAP: dict[str, str] = {
    # Left bloc
    "SOC": "gauche",
    "DVG": "gauche",
    "RDG": "gauche",
    "COM": "gauche",
    "ECO": "gauche",
    "VEC": "gauche",
    "NUA": "gauche",
    "FI": "gauche",  # La France insoumise (2026 code)
    "EXG": "gauche",  # Extrême gauche (2026 code)
    "UG": "gauche",  # Union gauche (2026 code)
    # Centre
    "REN": "centre",
    "MDM": "centre",
    "UDI": "centre",
    "UVC": "centre",
    "HOR": "centre",  # Horizons — Macron-aligned party (2026 code)
    "UC": "centre",  # Union centre (2026 code)
    # Right bloc
    "LR": "droite",
    "DVD": "droite",
    "DSV": "droite",
    "UD": "droite",  # Union droite (2026 code)
    "UDR": "droite",  # Union droites pour la République (2026 code)
    # Hard-right
    "RN": "extreme_droite",
    "UXD": "extreme_droite",
    "EXD": "extreme_droite",  # Extrême droite (2026 code)
    "REC": "extreme_droite",  # Reconquête! (2026 code)
    # Various / regionalist
    "DVC": "divers",
    "DIV": "divers",
    "REG": "divers",  # Régionaliste (2026 code)
}

# ── Column name maps (defaults — updated after Notebook EDA confirms actuals) ──
# These are the EXPECTED raw column names from each source file, mapped to the
# canonical silver-layer names defined in docs/data-model.md.
# If actual column names differ (discovered in notebooks/02_source_schema_exploration.ipynb),
# update these dicts — the transform modules use them as the sole rename layer.
COG_COLUMN_MAP: dict[str, str] = {
    "COM": "commune_insee",
    "LIBELLE": "commune_name",
    "DEP": "dep_code",
    "REG": "reg_code",
    "TYPECOM": "typecom",  # 'COM', 'ARM', 'COMA' etc — filtered to 'COM' in silver
}

# Column maps confirmed by running notebooks/02_source_schema_exploration.ipynb
# on 2026-03-21. Source: Interior Ministry elections.interieur.gouv.fr + data.gouv.fr.

SEATS_COLUMN_MAP: dict[str, str] = {
    "CODE_DPT": "dep_code",
    "LIB_DPT": "dep_name",
    "CODE_EPCI": "epci_code",
    "LIB_EPCI": "epci_name",
    "CODE_COMMUNE": "commune_insee",  # 5-char string; join key to COG
    "LIB_COMMUNE": "commune_name",
    "CODE_COM_ASSOCIEE": "associated_commune_code",
    "LIB_COM_ASSOCIEE": "associated_commune_name",
    "NBRE_BV": "polling_station_count",
    "POPULATION": "population",  # resident pop — used for city-size bucket
    "INSCRITS": "registered_voters",  # not used in sampling; kept for analysis
    "NBRE_SAP_COM": "seats_municipal",
    "NBRE_SAP_EPCI": "seats_epci",
}

# Used for both Tour 1 (analysis pool) and Tour 2 (advanced_to_tour2 flag only).
# Tour 1 and Tour 2 have identical schemas (17 columns, confirmed by EDA).
CANDIDATES_COLUMN_MAP: dict[str, str] = {
    "Code département": "dep_code",
    "Département": "dep_name",
    "Code circonscription": "commune_insee",  # 5-digit INSEE — confirmed by EDA
    "Circonscription": "commune_name",
    "Numéro de panneau": "list_id",
    "Libellé abrégé de liste": "list_label_short",
    "Libellé de la liste": "list_label",
    "Code nuance de liste": "list_nuance",  # key into NUANCE_GROUP_MAP
    "Nuance de liste": "list_nuance_label",
    "Tête de liste": "is_list_leader",  # 'Oui'/'Non' — authoritative flag
    "Ordre": "position_on_list",  # integer rank on list; 1 = leader
    "Sexe": "gender",  # 'M' or 'F'
    "Nom sur le bulletin de vote": "family_name",
    "Prénom sur le bulletin de vote": "given_name",
    "Nationalité": "nationality",
    "Code personnalité": "personality_code",
    "CC": "candidate_category",  # semantics unknown; preserved for completeness
}

# Source: mun2026-maires-sortants-20260227.csv (3.9 MB) — mayors-only extract.
# No function/role column exists because all rows are mayors (role is implicit).
# No function filter is needed in dim_candidate.py.
RNE_COLUMN_MAP: dict[str, str] = {
    "Code du département": "dep_code",
    "Libellé du département": "dep_name",
    "Code de la collectivité à statut particulier": "special_collectivity_code",
    "Libellé de la collectivité à statut particulier": "special_collectivity_name",
    "Code de la commune": "commune_insee",  # join key for incumbent matching
    "Libellé de la commune": "commune_name",
    "Nom de l'élu": "family_name",
    "Prénom de l'élu": "given_name",
    "Code sexe": "gender",
    "Date de naissance": "birth_date",
    "Code de la catégorie socio-professionnelle": "socio_professional_code",
    "Libellé de la catégorie socio-professionnelle": "socio_professional_label",
    "Date de début du mandat": "mandate_start_date",
    "Date de début de la fonction": "function_start_date",
}
