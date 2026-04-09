# Logical Data Model - Election Gender Bias D4W

> Model type: logical table contracts for the current medallion architecture.
> Physical storage lives in Parquet plus `warehouse/municipal.duckdb`.
> Last updated: 2026-04-10

---

## Overview

The project currently implements a **runnable official-data + news-corpus
slice** of the broader research design:

`Bronze official data/news imports -> Silver dimensions/facts -> Gold cohort + marts`

The executable repository now materializes the 36-person viable-candidate
cohort, the source-agnostic news corpus backbone, and the first exposure /
regression audit marts. The transformer-based NLP enrichment stack remains the
main planned extension.

The implemented layers are:

| Layer | Status | Current contract |
|---|---|---|
| Bronze | Implemented | Official raw datasets plus `news_source_record` with provenance and redacted text surrogates |
| Silver | Implemented | `dim_commune`, `fact_election_result`, `dim_candidate_leader`, `fact_article_source`, `fact_article`, `fact_article_discovery`, `fact_mention`, and quarantine outputs |
| Gold | Implemented | `gold.candidate_universe`, `gold.sample_leaders`, `sample_manifest.json`, `mart_exposure_metrics`, `mart_framing_metrics`, `mart_bias_indicators`, `mart_regression_feature_base`, `mart_regression_results` |
| Meta | Implemented | `meta.meta_source_snapshot`, `meta.meta_run`, `meta.meta_news_import_batch` |

---

## Design Rationale

### Medallion structure

The Bronze layer preserves raw government files plus provenance metadata
(`_source_url`, `_ingested_at`, `_source_hash`). Silver applies schema mapping,
entity-level validation, and fact normalization while keeping dimensions and
facts separated. Gold contains consumer-facing marts and wide analytical tables
rather than raw operational tables.

This follows an enterprise-friendly rule set:
- Bronze is replayable.
- Silver is validated and idempotent.
- Gold is stable for downstream consumers.
- Meta tables provide auditability and rerun visibility.

### Cohort modeling choice

The 36-person viable-candidate cohort is modeled as a **materialized Gold table**,
`gold.sample_leaders`, not a view. This is intentional.

A view would silently change membership whenever upstream Silver data changed.
For an analytical cohort used to scope article collection and later bias
measurement, that would weaken reproducibility. A materialized table creates a
stable snapshot of who was selected in a given run, while
`data/gold/sample_manifest.json` stores the human-readable audit trail for the
same cohort.

### Sampling design

The project uses a **matched stratified cohort** of 36 electorally viable list
leaders with city-size quotas `large=6`, `medium=12`, `small=18`, and a 50/50
gender split inside each stratum.

This is a design for matched comparison, not equal-precision estimation within
each city-size bucket. The non-equal allocation reduces metropolitan
overrepresentation in the eventual media corpus while preserving within-stratum
gender balance.

Primary-cohort eligibility is defined from official round-1 results:

- `score_tour1_pct_expressed >= 10`
- or `score_tour1_rank <= 2`

This viability frame narrows the study population to candidates who were more
likely to be part of the local media agenda.

The current cohort also enforces a hard regional concentration cap:
- max `4` candidates from any single region

Within that cap, region diversity remains an adaptive tie-break. The sampler
recomputes that priority after each accepted candidate so that one remaining
uncovered region cannot absorb the rest of a stratum. Department diversity
within a region is treated as a later tie-break, after the sampler has already
applied the bucket x gender political-bloc balancing checks that reduce
gender-bloc confounding risk.

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

### `bronze/results/results_tour1.parquet`

- Grain: one row per commune in the official round-1 results export
- Source: Interior Ministry municipal-results CSV
- Shape: wide repeated-list schema (`Numéro de panneau 1`, `Voix 1`, ...)
- Role: authoritative round-1 vote-share source for downstream normalization

### `bronze/results/results_tour2.parquet`

- Grain: one row per commune in the official round-2 results export
- Source: Interior Ministry municipal-results CSV
- Shape: wide repeated-list schema (`Numéro de panneau 1`, `Voix 1`, ...)
- Status: optional for standalone local runs; when absent, Silver still builds
  round-1 fact rows and leaves round-2 derived fields as `NULL`

### `bronze/geography/cog_communes.parquet`

- Grain: one row per geographic unit from the COG file
- Source: INSEE COG 2026

### `bronze/seats/seats_population.parquet`

- Grain: one row per commune
- Source: Interior Ministry seats/population file

### `bronze/rne/rne_incumbents.parquet`

- Grain: one row per incumbent record in the RNE extract
- Source: Repertoire National des Elus

### `bronze/news_source_record/*`

- Grain: one row per imported news source record before article-source normalization
- Source: restricted exports (for example Europresse) or supplemental provider fetches
- Role: auditable Bronze landing table for the source-agnostic corpus pipeline
- Text contract: persisted artifacts store `raw_body_text_hash` / preview / length
  surrogates instead of the full article body; full text is used transiently in
  memory during the ETL run only

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
- Sources: Tour 1 candidates, RNE incumbents, optional Tour 2 bronze
- Role: candidate dimension used for cohort construction and future analysis

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | Primary key, deterministic MD5 surrogate key |
| `full_name` | VARCHAR | Official candidate name |
| `gender` | VARCHAR(1) | `M` or `F` |
| `commune_insee` | VARCHAR(5) | Foreign key to `silver.dim_commune` |
| `same_name_candidate_count` | INTEGER | Exact normalized-name collision count across all Tour 1 candidates |
| `list_nuance` | VARCHAR | Official political nuance code |
| `nuance_group` | VARCHAR | Simplified bloc grouping |
| `is_incumbent` | BOOLEAN | Nullable fuzzy match against RNE (`NULL` when no commune-level RNE lookup exists) |
| `incumbent_match_score` | FLOAT | `token_sort_ratio` score for auditability |
| `incumbent_match_auditable` | BOOLEAN | Flags non-perfect fuzzy matches |
| `advanced_to_tour2` | BOOLEAN | `TRUE` if list advanced; `NULL` when Tour 2 is unavailable |

Design note:
- `silver.dim_candidate_leader` stays a pure candidate dimension. Geography
  remains authoritative in `silver.dim_commune`, and election performance
  remains authoritative in `silver.fact_election_result`.
- `reg_code` and `city_size_bucket` are intentionally excluded here. They are
  reintroduced only in `gold.candidate_universe`, where the single analytical
  pre-join is materialized for cohort construction and downstream consumers.
- The single consumer-facing pre-join now happens in `gold.candidate_universe`.
  That mart brings together commune attributes, result summary fields, and the
  `is_viable` flag used by the sampling slice.

### `silver.fact_election_result`

- Grain: one row per list leader x commune x election round
- Sources: `bronze/results/results_tour1.parquet`,
  `bronze/results/results_tour2.parquet`, and the candidate bronze files
- Role: normalized official-results fact used to derive stable candidate-facing
  score fields

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | Foreign key to the Tour 1 leader universe |
| `commune_insee` | VARCHAR(5) | Commune code |
| `round_number` | INTEGER | `1` or `2` |
| `list_id` | VARCHAR | Official panel/list number used as the primary bridge key |
| `leader_full_name_official` | VARCHAR | Official leader name from results or official candidate fallback |
| `votes` | INTEGER | Votes won by the list |
| `vote_share_pct_expressed` | FLOAT | Share among valid votes |
| `vote_share_pct_registered` | FLOAT | Share among registered voters |
| `rank_in_commune_round` | INTEGER | Dense rank within commune and round |
| `seats_municipal_won` | INTEGER | Seats won on the municipal council |
| `seats_epci_won` | INTEGER | Seats won on the intercommunal council |
| `list_nuance` | VARCHAR | Official list nuance code |
| `_source_url` | VARCHAR | Bronze provenance |
| `_ingested_at` | VARCHAR | Bronze provenance |
| `_source_hash` | CHAR(32) | Bronze provenance |

Design notes:
- Primary mapping key: `commune_insee + list_id`
- Leader-name normalization is a validation guardrail, not the sole join key
- Round-2 rows are quarantined if the round-2 leader cannot be resolved back to
  the Tour 1 leader universe

### `silver/_rejected/*`

Quarantine files preserve rows that violate DQ rules.

Current implemented quarantine outputs include:
- `data/silver/_rejected/dim_commune_rejected.parquet`
- `data/silver/_rejected/dim_candidate_leader_rejected.parquet`
- `data/silver/_rejected/fact_election_result_rejected.parquet`
- `data/silver/_rejected/fact_article_source_rejected.parquet`
- `data/silver/_rejected/news_import_unsupported.parquet`

### `silver.fact_article_source`

- Grain: one row per normalized article-source record
- Source: `bronze/news_source_record/*`
- Role: canonicalized article-source contract before deduplication
- Text contract: persisted tables keep `body_text_hash`, `body_text_preview`,
  and `body_text_length`; the `body_text` column is a redaction marker rather
  than the original full body text

### `silver.fact_article`

- Grain: one row per canonical article after URL/content deduplication
- Source: `silver.fact_article_source`
- Role: one analytical article denominator used for candidate matching
- Text contract: persisted tables keep only redacted `body_text` plus
  preview/hash/length surrogates; full text is not retained on disk

### `silver.fact_article_discovery`

- Grain: one row per provider discovery hit
- Source: supplemental provider search results merged into the corpus ETL
- Role: provenance/audit bridge between discovery providers and canonical articles

### `silver.fact_mention`

- Grain: one row per canonical article × sampled candidate match
- Source: `silver.fact_article` matched against `gold.sample_leaders`
- Role: anchor table for candidate-level coverage, framing, and future NLP outputs

### `silver.manual_review_candidate_match`

- Grain: one row per ambiguous candidate/article pair withheld from auto-match
- Source: candidate-resolution ambiguity logic in the corpus ETL
- Role: preserves analyst review work instead of silently dropping uncertain matches

---

## Gold Layer

Gold contains stable analysis-facing artifacts.

### `gold.candidate_universe`

- Grain: one row per first-round list leader in the conformed candidate universe
- Source: `silver.dim_candidate_leader` joined once with
  `silver.fact_election_result` (result summary) and `silver.dim_commune`
- Storage:
  - `data/gold/candidate_universe.parquet`
  - DuckDB table `gold.candidate_universe`
- Role: pre-joined consumer mart for sampling and downstream analytical models
- Design note:
  - This is the only place where `silver.dim_candidate_leader`,
    `silver.dim_commune`, and the summarized election-result fact are joined
    into one wide analytical table.
  - `reg_code` and `city_size_bucket` live here rather than in the Silver
    candidate dimension so the warehouse keeps one authoritative geography
    source while still giving downstream consumers a stable modeling-ready base.
  - `is_viable` is computed here once from the round-1 score contract and then
    consumed downstream by the sampling slice instead of being rederived ad hoc.

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | Primary key |
| `full_name` | VARCHAR | Candidate name |
| `gender` | VARCHAR(1) | `M` or `F` |
| `commune_insee` | VARCHAR(5) | Foreign key to `silver.dim_commune` |
| `commune_name` | VARCHAR | Human-readable commune label |
| `dep_code` | VARCHAR | Department code |
| `reg_code` | VARCHAR | Region code |
| `city_size_bucket` | VARCHAR | Modeling-ready stratum |
| `same_name_candidate_count` | INTEGER | Sampling-priority feature |
| `list_nuance` | VARCHAR | Official nuance code |
| `nuance_group` | VARCHAR | Simplified political bloc |
| `is_incumbent` | BOOLEAN | Nullable incumbent flag |
| `incumbent_match_score` | FLOAT | Fuzzy-match audit score |
| `incumbent_match_auditable` | BOOLEAN | Audit flag |
| `advanced_to_tour2` | BOOLEAN | Optional control variable |
| `score_tour1_votes` | INTEGER | Official round-1 vote count |
| `score_tour1_pct_expressed` | FLOAT | Official round-1 vote-share covariate |
| `score_tour1_rank` | INTEGER | Rank within the commune in round 1 |
| `score_tour2_votes` | INTEGER | Official round-2 vote count when applicable |
| `score_tour2_pct_expressed` | FLOAT | Official round-2 vote-share covariate |
| `score_tour2_rank` | INTEGER | Rank within the commune in round 2 |
| `vote_share_band_tour1` | VARCHAR | Stable round-1 competitiveness band |
| `won_final_round` | BOOLEAN | Winner flag based on the final decisive round |
| `is_viable` | BOOLEAN | `TRUE` when `score_tour1_pct_expressed >= 10 OR score_tour1_rank <= 2` |

### `gold.sample_leaders`

- Grain: **one row per selected candidate in the active analytical cohort**
- Source: stratified sampling from `gold.candidate_universe`
- Storage:
  - `data/gold/sample_leaders.parquet`
  - DuckDB table `gold.sample_leaders`
- Current cohort size: 36 rows

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | Primary key within the cohort |
| `full_name` | VARCHAR | Candidate name |
| `gender` | VARCHAR(1) | `M` or `F` |
| `commune_insee` | VARCHAR(5) | Foreign key to `silver.dim_commune` |
| `commune_name` | VARCHAR | Human-readable commune label — joined from `silver.dim_commune` at gold write time; required for GDELT text query construction |
| `dep_code` | VARCHAR | Department code — disambiguates same-name communes (e.g. multiple "Saint-Martin"); joined from `silver.dim_commune` at gold write time |
| `reg_code` | VARCHAR | Region code |
| `city_size_bucket` | VARCHAR | Sampling stratum |
| `same_name_candidate_count` | INTEGER | Sampling-priority feature |
| `list_nuance` | VARCHAR | Official nuance code |
| `nuance_group` | VARCHAR | Simplified political bloc |
| `is_incumbent` | BOOLEAN | Nullable incumbent flag carried into the cohort |
| `incumbent_match_score` | FLOAT | Fuzzy-match audit score |
| `incumbent_match_auditable` | BOOLEAN | Audit flag |
| `advanced_to_tour2` | BOOLEAN | Optional control variable |
| `score_tour1_votes` | INTEGER | Official round-1 vote count |
| `score_tour1_pct_expressed` | FLOAT | Official round-1 vote-share covariate |
| `score_tour1_rank` | INTEGER | Rank within the commune in round 1 |
| `score_tour2_votes` | INTEGER | Official round-2 vote count when applicable |
| `score_tour2_pct_expressed` | FLOAT | Official round-2 vote-share covariate |
| `score_tour2_rank` | INTEGER | Rank within the commune in round 2 |
| `vote_share_band_tour1` | VARCHAR | Stable round-1 competitiveness band |
| `won_final_round` | BOOLEAN | Winner flag based on the final decisive round |

Companion audit artifact:
- `data/gold/sample_manifest.json`
- Includes `sampling_rule_version = v11_metropolitan_36_regioncap4_blocbalance_before_deptdiversity`,
  hard constraints, warning thresholds, triggered warnings, and selection
  priority metadata for cohort auditability.
- Includes run ID, random seed, per-stratum counts, region coverage, and
  per-candidate details. `population` is the only manifest-only field joined
  from `silver.dim_commune` at manifest-write time — `commune_name` and
  `dep_code` are already present in the gold table itself.

Modeling note:
- `gold.sample_leaders` is a **materialized cohort snapshot**, not a view.
- The current implementation stores the active cohort only. Historical cohort
  versioning can be added later by introducing `run_id` into the Gold table or
  a dedicated cohort registry.
- Hard sampling contract:
  - total rows = 36
  - gender split = `18F / 18M`
  - city-size totals = `large=6`, `medium=12`, `small=18`
  - max one sampled candidate per commune
  - max four sampled candidates per region
  - metropolitan-France scope when `EXCLUDE_DOM_TOM=True`
  - round-1 viability: `score_tour1_pct_expressed >= 10 OR score_tour1_rank <= 2`
- Vote share and round advancement remain available as downstream analytical
  variables even though round-1 viability is now part of the primary cohort
  definition.

### `gold.mart_exposure_metrics`

- Grain: one row per sampled leader
- Source: `gold.sample_leaders`, `silver.fact_article`, `silver.fact_mention`,
  `silver.dim_commune`
- Role: leader-level coverage denominator with article counts, headline mentions,
  source diversity, and exposure normalized by commune population

### `gold.mart_framing_metrics`

- Grain: one row per sampled leader × frame label
- Source: `silver.fact_mention`
- Role: current placeholder mart for framing aggregation; rows remain sparse until
  the richer NLP frame scorer is wired into `fact_mention`

### `gold.mart_bias_indicators`

- Grain: one row per gender × exposure metric
- Source: `gold.mart_exposure_metrics`
- Role: quick comparison layer for dashboard-level gender summaries

### `gold.mart_regression_feature_base`

- Grain: one row per sampled leader
- Source: `gold.sample_leaders` joined with `gold.mart_exposure_metrics`
- Role: stable modeling base that materializes the documented regression controls
  (`gender`, `city_size_bucket`, `nuance_group`, `is_incumbent`, `reg_code`)

### `gold.mart_regression_results`

- Grain: one row per model coefficient
- Source: `gold.mart_regression_feature_base`
- Role: persisted Poisson exposure-model audit output
- Status contract: each row carries a `status` field such as `fitted`,
  `fitted_with_warning:*`, or `fit_failed:*` so statistical warnings remain
  visible in downstream audit artifacts

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

This table records one row per execution of an orchestrated pipeline flow such
as `run-sampling-pipeline`, `run-news-benchmark`, or
`run-news-corpus-pipeline`.

The installable CLI is backed by `src.cli.run_sampling_pipeline`. The legacy
`scripts/run_sampling_pipeline.py` file remains only as a compatibility
wrapper.

| Column | Type | Notes |
|---|---|---|
| `run_id` | VARCHAR | Primary key |
| `flow_name` | VARCHAR | Pipeline name such as `sampling_pipeline` or `news_corpus_pipeline` |
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
    +--< gold.candidate_universe (commune_insee FK, leader_id PK)

silver.fact_election_result (leader_id FK, commune_insee FK, round_number)
    |
    +--< gold.candidate_universe (leader_id PK)

silver.dim_candidate_leader (leader_id PK)
    |
    +--< gold.candidate_universe (leader_id PK)
             |
             +--< gold.sample_leaders (leader_id PK in active cohort)
                      |
                      +--< silver.fact_mention (leader_id FK)
                      |
                      +--< gold.mart_exposure_metrics (leader_id PK)
                      |
                      +--< gold.mart_regression_feature_base (leader_id PK)

silver.fact_article (canonical_article_id PK)
    |
    +--< silver.fact_mention (canonical_article_id FK)

meta.meta_source_snapshot records Bronze-source fetch lineage
meta.meta_run records execution lineage across implemented pipelines
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
| `meta.meta_run` | Implemented | Keep execution identity distinct from batch identity |
| News ingest and article pipeline | Implemented | Continue enriching provider coverage and QA diagnostics |
| NLP fact tables and marts | Partially implemented | `fact_mention`/framing scaffolding exists; add transformer enrichments next |
