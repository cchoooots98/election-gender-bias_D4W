# Logical Data Model - Election Gender Bias D4W

> Model type: logical table contracts for the current medallion architecture.
> Physical storage lives in Parquet plus `warehouse/municipal.duckdb`.
> Last updated: 2026-03-23

---

## Overview

The project currently implements a **runnable sampling slice** of the broader
research design:

`Bronze official data -> Silver dimensions -> Gold analytical cohort`

That runnable slice stops at the creation of the 24-person analysis cohort. The
later GDELT, NLP, and regression marts remain part of the planned architecture,
but they are not yet delivered by the executable pipeline in this repository.

The implemented layers are:

| Layer | Status | Current contract |
|---|---|---|
| Bronze | Implemented | Official raw datasets copied with provenance columns |
| Silver | Implemented | `dim_commune`, `dim_candidate_leader`, and quarantine outputs |
| Gold | Implemented for sampling slice | `gold.sample_leaders` plus `sample_manifest.json` |
| Meta | Implemented for sampling slice | `meta.meta_source_snapshot`, `meta.meta_run` |

---

## Design Rationale

### Medallion structure

The Bronze layer preserves raw government files plus provenance metadata
(`_source_url`, `_ingested_at`, `_source_hash`). Silver applies schema mapping,
joins, and DQ checks. Gold contains analysis-ready artifacts rather than raw
operational tables.

This follows an enterprise-friendly rule set:
- Bronze is replayable.
- Silver is validated and idempotent.
- Gold is stable for downstream consumers.
- Meta tables provide auditability and rerun visibility.

### Cohort modeling choice

The 24-person candidate cohort is modeled as a **materialized Gold table**,
`gold.sample_leaders`, not a view. This is intentional.

A view would silently change membership whenever upstream Silver data changed.
For an analytical cohort used to scope article collection and later bias
measurement, that would weaken reproducibility. A materialized table creates a
stable snapshot of who was selected in a given run, while
`data/gold/sample_manifest.json` stores the human-readable audit trail for the
same cohort.

### Sampling design

The project uses a **matched stratified cohort** of 24 list leaders with
city-size quotas `large=4`, `medium=8`, `small=12`, and a 50/50 gender split
inside each stratum.

This is a design for matched comparison, not equal-precision estimation within
each city-size bucket. The non-equal allocation reduces metropolitan
overrepresentation in the eventual media corpus while preserving within-stratum
gender balance.

### Candidate ambiguity feature

`same_name_candidate_count` is part of the Silver candidate dimension rather
than being recomputed ad hoc in Gold. It is derived from the full Tour 1
candidate universe, not only list leaders, because the downstream ambiguity risk
concerns the broader name space that later article matching must navigate.

---

## Bronze Layer

Bronze tables are faithful copies of raw source files with provenance columns
appended.

### `bronze/candidates/candidates_tour1.parquet`

- Grain: one row per Tour 1 candidate
- Source: Interior Ministry candidate list export
- Required provenance columns: `_source_url`, `_ingested_at`, `_source_hash`

### `bronze/candidates/candidates_tour2.parquet`

- Grain: one row per Tour 2 candidate
- Source: Interior Ministry candidate list export
- Status: optional input to the runnable sampling slice
- If unavailable, `advanced_to_tour2` degrades to `NULL` in Silver

### `bronze/geography/cog_communes.parquet`

- Grain: one row per geographic unit from the COG file
- Source: INSEE COG 2026

### `bronze/seats/seats_population.parquet`

- Grain: one row per commune
- Source: Interior Ministry seats/population file

### `bronze/rne/rne_incumbents.parquet`

- Grain: one row per incumbent record in the RNE extract
- Source: Repertoire National des Elus

---

## Silver Layer

Silver tables are cleaned, joined, and DQ-validated outputs. Rows that fail
validation are quarantined to `data/silver/_rejected/` rather than silently
dropped.

### `silver.dim_commune`

- Grain: one row per French commune (`TYPECOM = 'COM'`)
- Sources: `bronze/geography/cog_communes.parquet` left-joined with
  `bronze/seats/seats_population.parquet`
- Role: authoritative geographic reference anchor

| Column | Type | Notes |
|---|---|---|
| `commune_insee` | VARCHAR(5) | Primary key |
| `commune_name` | VARCHAR | Official commune label |
| `dep_code` | VARCHAR | Department code |
| `reg_code` | VARCHAR | Region code |
| `population` | INTEGER | Resident population |
| `seats_municipal` | INTEGER | Municipal council seats |
| `seats_epci` | INTEGER | EPCI seats |
| `city_size_bucket` | VARCHAR | `large` / `medium` / `small` / `excluded` |

Thresholds are controlled in `src/config/settings.py`.

### `silver.dim_candidate_leader`

- Grain: one row per first-round list leader (`tete de liste`) per commune
- Sources: Tour 1 candidates, `silver.dim_commune`, RNE incumbents,
  optional Tour 2 bronze
- Role: candidate dimension used for cohort construction and future analysis

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | Primary key, deterministic MD5 surrogate key |
| `full_name` | VARCHAR | Official candidate name |
| `gender` | VARCHAR(1) | `M` or `F` |
| `commune_insee` | VARCHAR(5) | Foreign key to `silver.dim_commune` |
| `reg_code` | VARCHAR | Denormalized from `dim_commune` |
| `city_size_bucket` | VARCHAR | Denormalized stratification variable |
| `same_name_candidate_count` | INTEGER | Exact normalized-name collision count across all Tour 1 candidates |
| `list_nuance` | VARCHAR | Official political nuance code |
| `nuance_group` | VARCHAR | Simplified bloc grouping |
| `is_incumbent` | BOOLEAN | Fuzzy match against RNE |
| `incumbent_match_score` | FLOAT | `token_sort_ratio` score for auditability |
| `incumbent_match_auditable` | BOOLEAN | Flags non-perfect fuzzy matches |
| `advanced_to_tour2` | BOOLEAN | `TRUE` if list advanced; `NULL` when Tour 2 is unavailable |

Design note:
- `commune_name`, `dep_code`, and `population` are intentionally excluded from
  this table.
- They remain authoritative in `silver.dim_commune` and are joined only when
  needed for audit artifacts such as the sample manifest.

### `silver/_rejected/*`

Quarantine files preserve rows that violate DQ rules.

Current implemented quarantine outputs include:
- `data/silver/_rejected/dim_commune_rejected.parquet`
- `data/silver/_rejected/dim_candidate_leader_rejected.parquet`

---

## Gold Layer

Gold contains stable analysis-facing artifacts.

### `gold.sample_leaders`

- Grain: **one row per selected candidate in the active analytical cohort**
- Source: stratified sampling from `silver.dim_candidate_leader`
- Storage:
  - `data/gold/sample_leaders.parquet`
  - DuckDB table `gold.sample_leaders`
- Current cohort size: 24 rows

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | Primary key within the cohort |
| `full_name` | VARCHAR | Candidate name |
| `gender` | VARCHAR(1) | `M` or `F` |
| `commune_insee` | VARCHAR(5) | Foreign key to `silver.dim_commune` |
| `reg_code` | VARCHAR | Region code |
| `city_size_bucket` | VARCHAR | Sampling stratum |
| `same_name_candidate_count` | INTEGER | Sampling-priority feature |
| `list_nuance` | VARCHAR | Official nuance code |
| `nuance_group` | VARCHAR | Simplified political bloc |
| `is_incumbent` | BOOLEAN | Incumbent flag |
| `incumbent_match_score` | FLOAT | Fuzzy-match audit score |
| `incumbent_match_auditable` | BOOLEAN | Audit flag |
| `advanced_to_tour2` | BOOLEAN | Optional control variable |

Companion audit artifact:
- `data/gold/sample_manifest.json`
- Includes run ID, random seed, per-stratum counts, region coverage, and
  manifest-only commune fields (`commune_name`, `dep_code`, `population`)
  joined from `silver.dim_commune`

Modeling note:
- `gold.sample_leaders` is a **materialized cohort snapshot**, not a view.
- The current implementation stores the active cohort only. Historical cohort
  versioning can be added later by introducing `run_id` into the Gold table or
  a dedicated cohort registry.

### Planned Gold marts

The following Gold outputs remain planned rather than implemented:
- `gold.mart_exposure_metrics`
- `gold.mart_framing_metrics`
- `gold.mart_bias_indicators`
- `gold.mart_regression_results`

---

## Meta Layer

### `meta.meta_source_snapshot`

This table is implemented in `src/observability/run_logger.py` and records
source freshness and provenance.

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | VARCHAR | Primary key |
| `source_key` | VARCHAR | Logical source identifier |
| `source_url` | VARCHAR | Download URL |
| `source_hash` | CHAR(32) | MD5 hash of the raw source file |
| `raw_file_path` | VARCHAR | Local raw file path |
| `row_count` | INTEGER | Rows written to Bronze |
| `fetched_at` | TIMESTAMPTZ | Fetch timestamp |

### `meta.meta_run`

This table is implemented for the runnable sampling slice and records one row
per execution of the sampling pipeline CLI (`run-sampling-pipeline`).

The installable CLI is backed by `src.cli.run_sampling_pipeline`. The legacy
`scripts/run_sampling_pipeline.py` file remains only as a compatibility
wrapper.

| Column | Type | Notes |
|---|---|---|
| `run_id` | VARCHAR | Primary key |
| `flow_name` | VARCHAR | Pipeline name, currently `sampling_pipeline` |
| `start_ts` | TIMESTAMPTZ | Run start time |
| `end_ts` | TIMESTAMPTZ | Run end time |
| `status` | VARCHAR | `success` / `partial` / `failed` |
| `rows_ingested` | INTEGER | Rows materialized by the sampling slice |
| `error_count` | INTEGER | Count of step failures or warnings promoted to run metadata |
| `artifact_paths` | VARCHAR | JSON-encoded ordered list of output artifacts |

---

## Current Relationships

```text
silver.dim_commune (commune_insee PK)
    |
    +--< silver.dim_candidate_leader (commune_insee FK)
             |
             +--< gold.sample_leaders (leader_id PK in active cohort)

meta.meta_source_snapshot records Bronze-source fetch lineage
meta.meta_run records sampling-slice execution lineage
```

---

## Gap Analysis - Code vs Design

| Item | Status | Action needed |
|---|---|---|
| Bronze official-data ingest | Implemented | Keep source schemas aligned with EDA-validated column maps |
| `silver.dim_commune` | Implemented | No immediate action |
| `silver.dim_candidate_leader` | Implemented | No immediate action |
| `gold.sample_leaders` | Implemented | Optional future versioning if multiple cohort runs must be retained |
| `meta.meta_source_snapshot` | Implemented | No immediate action |
| `meta.meta_run` | Implemented for sampling slice | Extend when later pipeline stages are added |
| News ingest and article pipeline | Planned | Add GDELT and extraction modules |
| NLP fact tables and marts | Planned | Add modeling and scoring layers |
