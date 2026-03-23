# Logical Data Model — Election Gender Bias D4W

> **Model type**: Logical Data Model — table/column/type/PK/FK definitions,
> database-agnostic (not DuckDB DDL). The physical implementation lives in
> `src/transform/` (Parquet files) and `warehouse/municipal.duckdb`.
>
> **Authority**: This file is the single source of truth for table contracts.
> All code modules and tests reference the column names and types defined here.
>
> **Last updated**: 2026-03-23

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Design Rationale](#design-rationale)
3. [Bronze Layer](#bronze-layer)
4. [Silver Layer](#silver-layer)
5. [Gold Layer](#gold-layer)
6. [Meta Layer](#meta-layer)
7. [NLP Processing Pipeline](#nlp-processing-pipeline)
8. [Table Relationships](#table-relationships)
9. [fact_article Status Matrix](#fact_article-status-matrix)
10. [Gap Analysis — Code vs. Design](#gap-analysis--code-vs-design)

---

## Architecture Overview

This project uses the **medallion architecture** (Bronze → Silver → Gold), a
standard pattern in modern data lakes (Databricks, dbt, Snowflake). Think of it
as a kitchen: raw ingredients arrive unchanged (bronze), are cleaned and
validated (silver), then plated as analysis-ready dishes (gold).

| Layer | Purpose | Write rule |
|---|---|---|
| **Bronze** | Faithful raw copies, provenance columns added | Append-only, never modified |
| **Silver** | Cleaned, validated, joined — safe to query | DQ-gated writes; bad rows quarantined to `_rejected/` |
| **Gold** | Aggregated metrics for dashboard and statistics | Recomputed from Silver on each pipeline run |
| **Meta** | Pipeline observability — run logs, source snapshots | Append-only |

---

## Design Rationale

### Layer Architecture

The pipeline adopts the **medallion architecture** (Bronze → Silver → Gold), a
standard pattern in modern data lake platforms (Databricks, dbt, Snowflake). This
structure reflects the distinct guarantees each stage must provide given the
heterogeneous and unreliable nature of the data sources: official government CSV
files updated irregularly, a third-party API with a 3-month rolling data window,
and web scraping subject to paywalls and network failures.

The **Bronze** layer preserves a faithful copy of every ingested source file,
append-only, with provenance columns (`_source_url`, `_ingested_at`, `_source_hash`).
No transformation is applied at this stage. This ensures that any downstream
processing error can be diagnosed and corrected by replaying from Bronze without
re-fetching from the source.

The **Silver** layer enforces the project's data quality contract. Each transform
runs explicit DQ checks before writing; rows that fail (null primary keys, invalid
enumeration values, duplicate keys) are quarantined to `_rejected/` sub-tables with
a `_rejection_reason` column rather than discarded. All writes follow a
delete-then-insert pattern, making every pipeline step safe to re-run without
producing duplicates.

The **Gold** layer contains only pre-aggregated, analysis-ready metrics. All
analytical queries and dashboard reads target this layer exclusively; no Gold
table requires joins to Silver at query time.

---

### Reference Dimensions

**`dim_commune`** is the geographic reference anchor for the entire schema. France
has approximately 35,000 communes; every candidate, article, and exposure metric
is linked to a commune via its 5-character INSEE code. Storing the authoritative
geographic attributes — commune name, department, region, resident population, and
the city-size bucket used for stratified sampling — in a single dedicated table
ensures that boundary definitions (e.g. the population threshold that separates
"large" from "medium" cities) are maintained in one place and propagated consistently
across all downstream tables.

**`dim_candidate_leader`** captures one row per first-round list leader (*tête de
liste*) per commune. Beyond the core identity attributes (name, gender, commune
FK, political affiliation), this table carries two columns denormalised from
`dim_commune`: `city_size_bucket` and `reg_code`. These two columns are direct
inputs to the regression models — `city_size_bucket` is the stratification variable
for matched sampling, and `reg_code` is a regional fixed effect. Carrying them
forward into the candidate dimension eliminates the need for a join on every model
training run. This selective denormalisation is appropriate because
candidate-to-commune assignments are static once the election closes, removing
the update-anomaly risk that normally makes denormalisation inadvisable.

The remaining geographic attributes from `dim_commune` (`commune_name`, `dep_code`,
`population`) are not replicated here. They play no direct role in the regression
or sampling logic and remain available via join when needed for reporting.

---

### Article Collection and Deduplication: `fact_article`

Article discovery proceeds in two decoupled stages. First, GDELT DOC 2.0 API
queries — one per candidate, keyed on name and commune — return URLs with
associated metadata (title, domain, publication timestamp, GDELT tone score).
These results are persisted in the Bronze table `gdelt_results` before any text
extraction is attempted. This decoupling is necessary because GDELT maintains
only a 3-month rolling index: if text extraction fails and the extraction is not
retried promptly, the URL will expire from GDELT's index and cannot be
re-queried. Bronze ensures the URL inventory is preserved independently of
extraction success.

Second, trafilatura attempts to retrieve and extract the body text of each URL.
`fact_article` records the outcome of every such attempt, regardless of result.
The `fetch_status` column classifies each row: `ok` (text successfully extracted),
`fetch_failed` (network error or HTTP 4xx/5xx), `empty_text` (paywalled or
JavaScript-rendered page returning no extractable content), or `too_short` (text
below the minimum length threshold for reliable NLP). This complete record allows
the pipeline to report its own data coverage — the fraction of candidate-linked
URLs that yielded usable text — directly from the table, rather than inferring
coverage from absence of data.

Wire-service duplicates are common in GDELT results: the same article may be
indexed under multiple URLs across different syndication outlets. `fact_article`
addresses this through two columns: `is_duplicate` (set by Sentence-CamemBERT
semantic similarity scoring in the first NLP step) and `canonical_article_id`
(a foreign key pointing to the representative article in each duplicate cluster).
Only rows where `fetch_status = 'ok'` and `is_duplicate = FALSE` proceed to the
NLP pipeline.

---

### NLP Analysis per Candidate Mention: `fact_mention`

The analytical unit of interest for this project is a passage of text in which a
specific candidate is discussed — a *mention*. The grain of `fact_mention` is
therefore one row per (article × candidate) pair, representing all text in a
given article that is relevant to a given candidate. The input to every NLP step
is `context_sentences`: the set of sentences in the article that reference the
candidate, up to the 512-token context limit of the CamemBERT model family.

This table consolidates the outputs of four distinct NLP analyses:

**Candidate matching** columns (`match_confidence`, `match_method`,
`context_token_count`, `context_coverage_ratio`) record how the article was
attributed to the candidate. These are produced by CamemBERT-NER entity recognition
followed by fuzzy name matching against `dim_candidate_leader`.

**Sentiment** columns (`sentiment_score`, `sentiment_label`, `prob_negative`)
are produced by DistilCamemBERT-Sentiment, a 5-class classifier trained on
French-language reviews. The model outputs a probability distribution over five
sentiment levels; `prob_negative` (the sum of the probabilities for the two most
negative classes) serves as the continuous dependent variable in the logistic
regression model.

**Frame classification** columns (`frame_politique`, `frame_vie_privee`,
`frame_apparence`, `frame_scandale`, `frame_personnalite`, `frame_securite`)
are produced by CamemBERT-NLI in zero-shot configuration. Each score is the
entailment probability returned by a separate model call for that frame label;
the six scores are therefore independent and do not sum to 1. A passage of text
may simultaneously score high on multiple frames — for example, a sentence that
addresses both a candidate's policy platform and their physical appearance. This
multi-label independence accurately reflects the structure of media coverage.

**Stereotype word frequency** columns (`stereo_apparence`, `stereo_famille`,
`stereo_emotion`, `stereo_competence`) record the per-thousand-word frequency
of words from four predefined lexicon categories. These aggregated frequencies
serve as control variables in the regression models.

Consolidating all NLP outputs at the (article × candidate) grain in a single table
means that all core analytical queries — average sentiment by gender, frame
distribution by city size, stereotype frequency by political bloc — can be
expressed as direct aggregations over `fact_mention` joined to
`dim_candidate_leader`, without any multi-table NLP result assembly.

---

### Word-Level Stereotype Detail: `fact_stereotype_word_counts`

In addition to the category-level frequency aggregates in `fact_mention`, a
separate long table records the per-word count for every matched lexicon term
within each mention. This word-level detail serves three purposes that the
aggregated columns cannot: it supports word-cloud visualisations of the most
common stereotype-associated terms by gender; it enables lexicon coverage audits
(identifying which words in the lexicon actually appear in the corpus); and it
provides an evidence trail that can be used to validate or challenge the lexicon
composition. The `lexicon_version` column ensures that results remain traceable
when the word list is revised.

---

### Analytical Aggregates: Gold Marts

The four Gold tables are recomputed from Silver on each pipeline run and serve as
the exclusive data source for the Streamlit dashboard and the statistical models.

**`mart_exposure_metrics`** aggregates at the candidate level: total article count,
headline mention count, number of distinct source domains, and exposure rate
normalised by commune population (`exposure_per_10k_pop`). Normalisation by
population is necessary because large-city candidates attract more coverage
regardless of gender; the normalised rate is the appropriate dependent variable
for the Poisson regression on media exposure.

**`mart_framing_metrics`** is structured as a long table — one row per (candidate
× frame) — rather than a wide table with one row per candidate. This structure
maps directly to the bar chart visualisation in the dashboard (grouping by frame
label on the x-axis, by gender on the colour axis) without requiring any client-side
reshaping.

**`mart_bias_indicators`** summarises the gender-level comparison at the metric
level, including bootstrapped 95% confidence intervals. This table is the primary
output of the analysis: each row states whether a given bias metric shows a
statistically meaningful difference between male and female candidates.

**`mart_regression_results`** stores the coefficient vector from each fitted
regression model (Poisson for exposure, logistic for sentiment), together with
standard errors, p-values, and confidence intervals. The `run_id` foreign key
links each result set to the corresponding `meta_run` entry, allowing model outputs
to be traced back to the exact pipeline run, data snapshot, and random seed that
produced them.

---

## Bronze Layer

Bronze tables are exact copies of source data with three provenance columns
appended: `_source_url`, `_ingested_at` (UTC), `_source_hash` (MD5).
They are **append-only** and never modified.

### `bronze/candidates/candidates_tour1.parquet`

- **Source**: Interior Ministry — first-round candidate lists (data.gouv.fr)
- **Grain**: one row per candidate (all list positions)
- **Key columns**: all original columns + `_source_url`, `_ingested_at`, `_source_hash`

### `bronze/candidates/candidates_tour2.parquet`

- **Source**: Interior Ministry — second-round candidate lists
- **Grain**: same schema as Tour 1

### `bronze/geography/cog_communes.parquet`

- **Source**: INSEE COG 2026
- **Grain**: one row per geographic unit (commune / arrondissement / commune associée)

### `bronze/seats/seats_population.parquet`

- **Source**: Interior Ministry — seats per commune + population
- **Grain**: one row per commune

### `bronze/rne/rne_incumbents.parquet`

- **Source**: Interior Ministry RNE (Répertoire National des Élus) — mayors extract
- **Grain**: one row per current mayor (2020–2026 term)
- **Note**: All rows are mayors; no function-column filter needed in Silver transform

### `bronze/articles/gdelt_results.parquet`

- **Source**: GDELT DOC 2.0 API (queried per candidate name)
- **Grain**: one row per GDELT search result (URL + metadata)
- **Why bronze**: GDELT maintains a 3-month rolling window — URLs expire; this table preserves them before `trafilatura` text extraction

| Column | Type | Description |
|---|---|---|
| `url` | VARCHAR | Article URL (input to trafilatura) |
| `title` | VARCHAR | Article title |
| `seendate` | VARCHAR | GDELT first-seen timestamp (raw format) |
| `domain` | VARCHAR | Source domain |
| `language` | VARCHAR | Language code |
| `sourcecountry` | VARCHAR | Source country |
| `gdelt_tone` | FLOAT | GDELT built-in tone score (may be NULL) |
| `query_term` | VARCHAR | Search term used (e.g. "Marie Dupont Lyon") |
| `queried_at` | TIMESTAMP | Query execution time (provenance) |
| `_source_hash` | VARCHAR | MD5 of the batch result |

---

## Silver Layer

Silver tables are cleaned, validated, and analysis-ready. Each Silver write
runs DQ checks; rows failing checks are quarantined to
`data/silver/_rejected/<table_name>_rejected.parquet` with a
`_rejection_reason` column.

### `silver.dim_commune`

- **Grain**: one row per French commune (`TYPECOM = 'COM'`), ~35,000 rows
- **Sources**: `bronze/geography/cog_communes.parquet` LEFT JOIN `bronze/seats/seats_population.parquet`
- **Role**: Complete geographic reference table; FK anchor for `dim_candidate_leader`

| Column | Type | Notes |
|---|---|---|
| `commune_insee` | VARCHAR(5) | **PK** — INSEE commune code |
| `commune_name` | VARCHAR | Official commune name |
| `dep_code` | VARCHAR | Department code |
| `reg_code` | VARCHAR | Region code |
| `population` | INTEGER | Resident population (municipale) |
| `seats_municipal` | INTEGER | Municipal council seats (reserved control variable) |
| `seats_epci` | INTEGER | EPCI seats (reserved control variable) |
| `city_size_bucket` | VARCHAR | `'large'` / `'medium'` / `'small'` / `'excluded'` |

**Thresholds** (from `settings.py`):
- large: population ≥ 100,000
- medium: 20,000 ≤ population < 100,000
- small: 3,500 ≤ population < 20,000
- excluded: population < 3,500 (near-zero GDELT coverage)

---

### `silver.dim_candidate_leader`

- **Grain**: one row per list leader (tête de liste) per commune, first round only
- **Sources**: `bronze/candidates/candidates_tour1.parquet` → filter position=1 → JOIN `dim_commune` → RNE fuzzy match

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | **PK** — MD5(full_name \| commune_insee) |
| `full_name` | VARCHAR | Official name (raw from candidate file) |
| `gender` | VARCHAR(1) | `'M'` or `'F'` |
| `commune_insee` | VARCHAR(5) | **FK** → `dim_commune.commune_insee` |
| `city_size_bucket` | VARCHAR | Denormalised from `dim_commune` — regression stratification variable |
| `reg_code` | VARCHAR | Denormalised from `dim_commune` — regional control variable in regression |
| `list_nuance` | VARCHAR | Official political affiliation code (e.g. `DVG`, `RN`) |
| `nuance_group` | VARCHAR | Simplified bloc: `gauche` / `droite` / `centre` / `extreme_droite` / `divers` |
| `is_incumbent` | BOOLEAN | Current mayor matched via RNE fuzzy name matching |
| `incumbent_match_score` | FLOAT | rapidfuzz `token_sort_ratio` score (0–100) — audit trail |
| `incumbent_match_auditable` | BOOLEAN | `TRUE` when match score < 100 (flags for manual review) |
| `advanced_to_tour2` | BOOLEAN | `TRUE` if list advanced to second round; `NULL` if Tour 2 data not yet ingested |

> **Denormalisation note**: `commune_name`, `dep_code`, and `population` are
> intentionally excluded — they are fully derivable via `JOIN dim_commune`.
> `city_size_bucket` and `reg_code` are kept because they are direct inputs to
> the regression models, and denormalisation is a justified performance trade-off
> for a static, small table.

---

### `silver.fact_article`

- **Grain**: one row per news article URL (all GDELT fetch attempts, including failures)
- **Sources**: `bronze/articles/gdelt_results.parquet` + trafilatura text extraction

| Column | Type | Notes |
|---|---|---|
| `article_id` | CHAR(32) | **PK** — MD5(url) |
| `url` | VARCHAR | Article URL |
| `domain` | VARCHAR | Source domain |
| `source_country` | VARCHAR | Source country |
| `language` | VARCHAR | Language code |
| `published_at` | TIMESTAMP | From GDELT `seendate` |
| `title` | VARCHAR | Article title |
| `text` | VARCHAR | Body text from trafilatura; `NULL` on failure |
| `gdelt_tone` | FLOAT | GDELT built-in tone score |
| `fetch_status` | VARCHAR | `'ok'` / `'fetch_failed'` / `'empty_text'` / `'too_short'` |
| `fetch_error` | VARCHAR | Error message when `fetch_status != 'ok'` |
| `extracted_at` | TIMESTAMP | trafilatura extraction time |
| `content_hash` | CHAR(32) | MD5(text) — change detection on re-fetch |
| `is_duplicate` | BOOLEAN | Result of Sentence-CamemBERT semantic deduplication |
| `canonical_article_id` | CHAR(32) | **FK** → `fact_article.article_id` — points to representative article when `is_duplicate = TRUE` |

> **Write policy**: ALL GDELT URLs are written to this table, including fetch
> failures and duplicates. This follows the "quarantine, not delete" principle
> — failed URLs can be retried; duplicate flags preserve the audit trail.
> Only rows where `fetch_status = 'ok' AND is_duplicate = FALSE` proceed to NLP.

---

### `silver.fact_mention`

- **Grain**: one row per (article × candidate) mention
- **Sources**: NLP pipeline — NER candidate matching + all downstream NLP outputs
- **Role**: Central NLP anchor table. Merges what was originally `bridge_article_candidate` with all NLP result columns.

| Column | Type | Notes |
|---|---|---|
| `mention_id` | CHAR(32) | **PK** — MD5(article_id \| leader_id) |
| `article_id` | CHAR(32) | **FK** → `fact_article.article_id` |
| `leader_id` | CHAR(32) | **FK** → `dim_candidate_leader.leader_id` |
| `match_confidence` | FLOAT | NER matching confidence (0–1) |
| `match_method` | VARCHAR | `'NER'` / `'alias'` / `'rule'` / `'manual'` |
| `context_sentences` | VARCHAR | All relevant sentences up to 512-token CamemBERT limit |
| `context_token_count` | INTEGER | Actual token count used for NLP |
| `context_coverage_ratio` | FLOAT | `context_token_count / article_total_tokens` |
| `sentiment_score` | FLOAT | Weighted average sentiment (1.0–5.0) |
| `sentiment_label` | INTEGER | argmax class (1–5 stars) |
| `prob_negative` | FLOAT | P(1★) + P(2★) — logistic regression dependent variable |
| `frame_politique` | FLOAT | NLI entailment P for policy/governance frame |
| `frame_vie_privee` | FLOAT | NLI entailment P for private life/family frame |
| `frame_apparence` | FLOAT | NLI entailment P for appearance/dress frame |
| `frame_scandale` | FLOAT | NLI entailment P for scandal/controversy frame |
| `frame_personnalite` | FLOAT | NLI entailment P for personality/character frame |
| `frame_securite` | FLOAT | NLI entailment P for security/public order frame |
| `stereo_apparence` | FLOAT | Appearance stereotype word frequency (per 1,000 words) |
| `stereo_famille` | FLOAT | Family/private life stereotype word frequency |
| `stereo_emotion` | FLOAT | Emotional language stereotype word frequency |
| `stereo_competence` | FLOAT | Competence/leadership word frequency |
| `nlp_model_version` | VARCHAR | Concatenated model version string (pinned; audit trail) |
| `scored_at` | TIMESTAMP | NLP inference timestamp |

> **Frame scores note**: The 6 frame scores are **independent probabilities**
> from 6 separate NLI calls (one per frame label). They do NOT sum to 1 — they
> are not a softmax. Each score answers: "does this text entail this frame?"
>
> **Stereotype scores note**: Category-level aggregates stored here. Word-level
> counts stored separately in `fact_stereotype_word_counts`.

---

### `silver.fact_stereotype_word_counts`

- **Grain**: one row per (mention × word) — word-level detail for stereotype analysis
- **Source**: lexicon-based counting on `fact_mention.context_sentences`
- **Complements** `fact_mention.stereo_*` category aggregates — use this table for word clouds, lexicon coverage audits, and discovering high-frequency words

| Column | Type | Notes |
|---|---|---|
| `mention_id` | CHAR(32) | **FK** → `fact_mention.mention_id`; part of composite PK |
| `word` | VARCHAR | Specific word from the lexicon; part of composite PK |
| `category` | VARCHAR | `'apparence'` / `'famille'` / `'emotion'` / `'competence'` |
| `count` | INTEGER | Occurrences in `context_sentences` |
| `per_1k_words` | FLOAT | `count / context_token_count × 1000` |
| `lexicon_version` | VARCHAR | Lexicon version tag — must be updated when word list changes |
| `scored_at` | TIMESTAMP | Scoring timestamp |

---

## Gold Layer

Gold tables are analysis-ready aggregates recomputed from Silver on each
pipeline run. They are the direct inputs to the Streamlit dashboard and
statistical models.

### `gold.mart_exposure_metrics`

- **Grain**: one row per candidate

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | **PK**, FK → `dim_candidate_leader` |
| `gender` | VARCHAR(1) | Denormalised for GROUP BY convenience |
| `article_count` | INTEGER | Total distinct articles mentioning this candidate (deduplicated) |
| `headline_mention_count` | INTEGER | Articles where candidate name appears in title |
| `unique_domain_count` | INTEGER | Distinct media source domains |
| `exposure_per_10k_pop` | FLOAT | `article_count / (population / 10,000)` |
| `analysis_window_days` | INTEGER | Actual days covered in the analysis window |

---

### `gold.mart_framing_metrics`

- **Grain**: one row per (candidate × frame) — long table
- **Note**: Streamlit bar chart reads this directly; avoids client-side reshape

| Column | Type | Notes |
|---|---|---|
| `leader_id` | CHAR(32) | FK → `dim_candidate_leader`; part of composite PK |
| `frame_label` | VARCHAR | Frame name (e.g. `'politique'`, `'apparence'`); part of composite PK |
| `gender` | VARCHAR(1) | Denormalised for GROUP BY convenience |
| `avg_frame_score` | FLOAT | Average NLI entailment score across all articles for this candidate + frame |
| `article_count` | INTEGER | Articles where frame score ≥ threshold |
| `frame_share` | FLOAT | `article_count / candidate_total_articles` |

---

### `gold.mart_bias_indicators`

- **Grain**: one row per (gender × metric)
- **Source**: aggregated from `mart_exposure_metrics` and `mart_framing_metrics` with bootstrapped confidence intervals

| Column | Type | Notes |
|---|---|---|
| `gender` | VARCHAR(1) | `'M'` or `'F'` |
| `metric_name` | VARCHAR | Metric identifier (e.g. `'mean_exposure_per_10k'`, `'frame_apparence_share'`) |
| `metric_value` | FLOAT | Point estimate |
| `conf_int_low` | FLOAT | 95% confidence interval lower bound |
| `conf_int_high` | FLOAT | 95% confidence interval upper bound |

---

### `gold.mart_regression_results`

- **Grain**: one row per (model × variable)
- **Models**: Poisson/NegBin for article count; logistic for sentiment (prob_negative)

| Column | Type | Notes |
|---|---|---|
| `run_id` | VARCHAR | FK → `meta.meta_run.run_id` — links result to the pipeline run |
| `model_name` | VARCHAR | `'poisson_exposure'` / `'logistic_sentiment'` |
| `variable_name` | VARCHAR | `'gender_F'` / `'log_population'` / `'is_incumbent'` / `'reg_code'` / … |
| `coefficient` | FLOAT | Regression coefficient |
| `std_error` | FLOAT | Standard error |
| `p_value` | FLOAT | p-value |
| `conf_int_low` | FLOAT | 95% CI lower bound |
| `conf_int_high` | FLOAT | 95% CI upper bound |

---

## Meta Layer

### `meta.meta_run`

| Column | Type | Notes |
|---|---|---|
| `run_id` | VARCHAR | **PK** |
| `flow_name` | VARCHAR | DAG / task name |
| `start_ts` | TIMESTAMP | Run start time |
| `end_ts` | TIMESTAMP | Run end time |
| `status` | VARCHAR | `'success'` / `'failed'` / `'partial'` |
| `rows_ingested` | INTEGER | Rows written in this run |
| `error_count` | INTEGER | Rows quarantined |
| `artifact_paths` | VARCHAR | JSON-encoded list of output file paths |

### `meta.meta_source_snapshot`

| Column | Type | Notes |
|---|---|---|
| `snapshot_id` | VARCHAR | **PK** |
| `source_key` | VARCHAR | Source identifier (e.g. `'candidates_tour1'`) |
| `source_url` | VARCHAR | Download URL |
| `file_hash` | CHAR(32) | MD5 — change detection |
| `file_size_bytes` | INTEGER | File size in bytes |
| `fetched_at` | TIMESTAMP | Fetch timestamp |

---

## NLP Processing Pipeline

```
bronze/articles/gdelt_results  +  trafilatura fetch
    │
    ▼  Write ALL fetch attempts → silver.fact_article
       (fetch_status: 'ok' / 'fetch_failed' / 'empty_text' / 'too_short')
    │
    ▼  Step 1 — Sentence-CamemBERT Deduplication
       Model: dangvantuan/sentence-camembert-base
       Input: title + first two sentences
       Output: is_duplicate, canonical_article_id  →  update fact_article
    │
    ▼  Filter: is_duplicate = FALSE  AND  fetch_status = 'ok'
    │
    ▼  Step 2 — CamemBERT-NER Candidate Matching
       Model: Jean-Baptiste/camembert-ner
       Input: full article text
       Output: PERSON entities  →  fuzzy-match against dim_candidate_leader
       Write: fact_mention (mention_id, article_id, leader_id, match_*)
    │
    ▼  Step 3 — Context Sentence Extraction  (rule-based, no model)
       Input: NER entity positions + original text
       Output: context_sentences (all relevant text up to 512-token limit)
              context_token_count, context_coverage_ratio
       Write: update fact_mention.context_sentences / token counts
    │
    ├──▶  Step 4 — DistilCamemBERT-Sentiment
    │     Model: cmarkea/distilcamembert-base-sentiment
    │     Input: context_sentences
    │     Output: 5-class softmax → sentiment_score (weighted avg),
    │             sentiment_label (argmax), prob_negative (P(1★)+P(2★))
    │     Write: update fact_mention.sentiment_*
    │
    ├──▶  Step 5 — CamemBERT-NLI × 6 Frame Classification  (zero-shot)
    │     Model: BaptisteDoyen/camembert-base-xnli
    │     Input: context_sentences (6 separate calls per mention)
    │     Output: 6 independent entailment probabilities  (do NOT sum to 1)
    │             frame_politique, frame_vie_privee, frame_apparence,
    │             frame_scandale, frame_personnalite, frame_securite
    │     Write: update fact_mention.frame_*
    │
    └──▶  Step 6 — Lexicon Stereotype Counting  (rule-based, no model)
          Input: context_sentences
          Output: 4 category frequencies (per 1,000 words) + word-level counts
          Write: update fact_mention.stereo_*
                 insert fact_stereotype_word_counts (word-level detail)
```

---

## Table Relationships

```
meta.meta_run ◄─────────────────────────────────────── mart_regression_results.run_id
                                                              │
    dim_commune (PK: commune_insee)                           │
         │                                                    │
         │ FK                                                 │
    dim_candidate_leader (PK: leader_id)                      │
         │                                                    │
         ├──────────────────────────────► mart_exposure_metrics (PK: leader_id)
         ├──────────────────────────────► mart_framing_metrics (FK: leader_id)
         │                                                    │
         │ FK (via fact_mention)                              │
         ▼                                                    │
    fact_mention (PK: mention_id)                             │
         │                                                    │
         │ FK                                                 │
         ▼                                                    │
    fact_article (PK: article_id)                             │
                                                              │
    fact_mention ──────────────────────► fact_stereotype_word_counts (FK: mention_id)
```

---

## `fact_article` Status Matrix

All GDELT URLs are written to `fact_article`, regardless of fetch outcome.

| `fetch_status` | `is_duplicate` | `text` | Proceeds to NLP? | Notes |
|---|---|---|---|---|
| `ok` | `FALSE` | Present | ✅ Yes | Normal article |
| `ok` | `TRUE` | Present | ❌ No | Wire duplicate; `canonical_article_id` points to representative article |
| `fetch_failed` | `NULL` | NULL | ❌ No | Network failure — can be retried |
| `empty_text` | `NULL` | NULL | ❌ No | Paywalled or JS-rendered page |
| `too_short` | `NULL` | Present (short) | ❌ No | Text < 100 chars — DQ quarantine |

---

## Gap Analysis — Code vs. Design

| Item | Status | Action needed |
|---|---|---|
| `silver.dim_commune` | ✅ Implemented | No changes needed |
| `silver.dim_candidate_leader` | ⚠️ Partial | Remove `commune_name`, `dep_code`, `population` from `_OUTPUT_COLUMNS` in `src/transform/dim_candidate.py` |
| `bronze/articles/gdelt_results` | ❌ Missing | Add GDELT bronze ingest in `src/ingest/news.py` |
| `silver.fact_article` | ❌ Missing | Create `src/transform/fact_article.py` |
| `silver.fact_mention` | ❌ Missing | Create `src/transform/fact_mention.py` (consolidates bridge + NLP outputs) |
| `silver.fact_stereotype_word_counts` | ❌ Missing | Create `src/nlp/stereotype_counter.py` |
| Gold mart tables | ❌ Missing | Create `src/metrics/` modules |
| `meta` layer tables | ❌ Missing | Implement `src/observability/run_logger.py` |
