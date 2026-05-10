# Logical Data Model - Election Gender Bias D4W

> Model type: logical table contracts for the current medallion architecture.
> Physical storage lives in Parquet plus `warehouse/municipal.duckdb`.
> Last updated: 2026-05-10

---

## Overview

The project currently implements a **runnable official-data + news-corpus
slice** of the broader research design:

`Bronze official data/news imports -> Silver dimensions/facts -> DuckDB -> dbt Gold marts -> Python regression diagnostics`

The executable repository now materializes the 36-person viable-candidate
cohort, the Europresse-first news corpus backbone, dbt-owned exposure/summary
marts, Python-owned regression/bootstrap diagnostics, and the Phase 0 NLP input
contract builder. Phase 2 generic sentiment, Phase 3 target-aware tone, and
Phase 4 frame scoring are implemented in Silver NLP tables. Gold NLP
activation remains a planned extension.

Active news-analysis window for the implemented corpus slice:
`2025-11-01` to `2026-04-30`.

The implemented layers are:

| Layer | Status | Current contract |
|---|---|---|
| Bronze | Implemented | Official raw datasets plus `news_source_record` and local-only `news_web_fetch` artifacts with provenance |
| Silver | Implemented | `dim_commune`, `fact_election_result`, `dim_candidate_leader`, `fact_article_source`, `fact_article`, `fact_mention`, Phase 0 `fact_mention_nlp_input`, Phase 1 `fact_stereotype_word_counts`, Phase 2/3/4 `fact_mention_nlp_summary`, Phase 4 `fact_mention_frame_score`, and quarantine outputs |
| Gold | Implemented | `gold.candidate_universe`, `gold.sample_leaders`, `sample_manifest.json`, dbt-owned `mart_exposure_metrics`, `mart_framing_metrics`, `mart_bias_indicators`, `mart_regression_feature_base`, `mart_analysis_summary`, Python-owned `mart_regression_results`, `mart_bootstrap_ci`, and NLP tone threshold QA artifacts |
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
- dbt owns SQL-friendly Gold marts so metric definitions are versioned, tested,
  and documented close to the SQL.
- Python owns statistical fitting and bootstrap diagnostics, which are not a
  natural fit for SQL.
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

### News web-enrichment cache

Europresse web-reference articles use a two-stage enrichment contract. The CLI
flag `--enable-web-scrape` means "fetch new uncached URLs"; it does not mean
"use or ignore web data." Existing successful cache rows under
`data/bronze/news_web_fetch/` are always eligible for reuse during a full
refresh, even when the flag is omitted.

This keeps the pipeline reproducible after a successful scrape: a rerun can
rebuild Silver and Gold from local artifacts without visiting the same news URL
again. The cache lives under `data/`, which is git-ignored. It may contain local
full extracted text so candidate matching can be reproduced across runs, while
the committed repository and published Silver/Gold text artifacts continue to
store redacted text markers plus hash/preview/length surrogates.

Single-document Europresse PDFs are also routed through the structured
Europresse parser when the segmenter recovers at least one dated article. Plain
single-article PDFs without Europresse date/header evidence still fall back to
the generic `pdf_text_layer` classification.

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
- Source: restricted Europresse exports registered in `news_import_manifest.json`
- Role: auditable Bronze landing table for the Europresse corpus pipeline
- Text contract: persisted artifacts store `raw_body_text_hash` / preview / length
  surrogates instead of the full article body; full text is used transiently in
  memory during the ETL run only

### `bronze/news_web_fetch/news_web_fetch_cache.parquet`

- Grain: one row per canonical web article URL fetch attempt
- Source: web-reference URLs extracted from Europresse `Read more` links
- Role: local replay cache for web full-text enrichment
- Network contract: cache hits are reused without network access; cache misses
  are fetched only when `--enable-web-scrape` is passed to the CLI
- Local text contract: this artifact is under git-ignored `data/` and may store
  extracted `body_text` for deterministic local reruns; published Silver/Gold
  outputs keep redacted text markers and hash/preview/length metadata

| Column | Type | Notes |
|---|---|---|
| `canonical_url` | VARCHAR | Canonical URL hash key after normalization |
| `source_url` | VARCHAR | Original URL extracted from the PDF |
| `fetch_status` | VARCHAR | `success`, `short_text`, or `failed` |
| `http_status` | INTEGER | HTTP response code when available |
| `body_text` | VARCHAR | Local-only extracted full text when fetch succeeds |
| `body_text_hash` | CHAR(32) | MD5 of extracted text when usable |
| `body_text_preview` | VARCHAR | Short audit preview of extracted text |
| `body_text_length` | INTEGER | Extracted text length in characters |
| `fetched_at` | VARCHAR | UTC fetch timestamp |
| `extractor_name` | VARCHAR | Current extractor: `trafilatura` |
| `extractor_version` | VARCHAR | Installed extractor version |
| `error_type` | VARCHAR | Failure or short-text reason |

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
- Web-enrichment contract:
  - `has_full_text = TRUE` when the row has enough article body text for
    candidate matching and later NLP enrichment
  - `acquisition_method = web_scrape` when a cached or newly fetched web body
    replaces an Europresse web-reference stub
  - `acquisition_method = url_metadata_only` when a URL was preserved but the
    body was unavailable, too short, or paywalled; in this case `body_text` and
    `body_text_hash` are allowed to be `NULL`
  - Rows with `url_metadata_only` remain matchable only through strict evidence
    in title or URL; PDF filenames are not candidate-match evidence

### `silver.fact_article`

- Grain: one row per canonical article after URL/content deduplication
- Source: `silver.fact_article_source`
- Role: one analytical article denominator used for candidate matching
- Text contract: persisted tables keep only redacted `body_text` plus
  preview/hash/length surrogates; full text is not retained on disk
- Text availability: `has_full_text` summarizes whether the representative
  canonical article has a usable full-text body. `url_metadata_only` rows may
  still be retained as article metadata, but they only contribute to exposure
  after strict candidate evidence creates a `silver.fact_mention` row.

| Column | Type | Notes |
|---|---|---|
| `canonical_article_id` | VARCHAR | Primary key for the canonical article denominator |
| `language` | VARCHAR | Article language used by downstream NLP input gating |
| `has_full_text` | BOOLEAN | Whether the canonical record has usable body text for matching and future NLP |

### `silver.fact_mention`

- Grain: one row per canonical article × sampled candidate match
- Source: `silver.fact_article` matched against `gold.sample_leaders`
- Role: anchor table for candidate-level coverage, framing, and future NLP outputs
- Matching contract: full-text articles use title + body + URL evidence.
  Metadata-only articles use title + URL evidence only; the original PDF path or
  candidate-specific PDF filename is never treated as match evidence.

### `silver.fact_mention_nlp_input`

- Grain: one row per `mention_id`
- Source: `silver.fact_mention.context_sentences`
- Owner: Python module `src/nlp/input_contracts.py`
- Status: Phase 0 implemented as a contract builder/materializer. It is not
  wired into the default news-corpus CLI, but it is consumed by the Phase 1
  lexicon audit and Phase 2 generic sentiment baseline.
- Role: model-input boundary for later lexicon, sentiment, tone, and framing
  enrichments.
- Text minimization contract:
  - `input_text` is derived only from mention-level `context_sentences`.
  - `article_language` is joined from `silver.fact_article.language`.
  - Full article body text is never read from this step and never re-persisted
    by this table.
  - Repeated whitespace is collapsed before hashing and word counting.
- DQ contract:
  - `mention_id`, `canonical_article_id`, and `leader_id` are required.
  - `mention_id` must be unique.
  - `input_hash` is populated for every non-empty normalized `input_text`.
  - `skip_reason` is required when `eligible_for_inference = FALSE`.
  - Empty contexts use `skip_reason = 'empty_context'`.
  - Contexts below `NLP_MIN_LEXICON_WORD_COUNT` use
    `skip_reason = 'too_short_for_lexicon'`.
  - Contexts below `NLP_MIN_INFERENCE_WORD_COUNT` but long enough for lexicon
    audit use `skip_reason = 'too_short_for_inference'`.
  - Non-French or unknown article-language rows use
    `skip_reason = 'language_not_french'`.

| Column | Type | Notes |
|---|---|---|
| `mention_id` | VARCHAR | Primary key inherited from `silver.fact_mention` |
| `canonical_article_id` | VARCHAR | Foreign key to `silver.fact_article` |
| `leader_id` | VARCHAR | Foreign key to `gold.sample_leaders` |
| `article_language` | VARCHAR | Language from `silver.fact_article`; French regional subtags are collapsed to `fr`, while non-French language codes preserve region for source-mix audits |
| `input_text` | VARCHAR | Mention context only; nullable for empty skipped rows |
| `input_hash` | CHAR(32) | MD5 of normalized `input_text`; populated for every non-empty input |
| `context_word_count` | INTEGER | Whitespace-delimited word count; not a model tokenizer/BPE count |
| `eligible_for_lexicon` | BOOLEAN | `TRUE` when the context can feed deterministic lexicon audit |
| `eligible_for_inference` | BOOLEAN | `TRUE` when the context can feed Transformer inference |
| `skip_reason` | VARCHAR | Controlled reason for rows not eligible for inference |
| `prepared_at` | TIMESTAMP | UTC timestamp for the contract build |
| `input_contract_version` | VARCHAR | Default `mention_context_v2`; version bumps require full regeneration |

### `silver.fact_mention_nlp_summary`

- Grain: one row per `mention_id`
- Source: `silver.fact_mention_nlp_input`
- Owner: Python modules `src/nlp/sentiment.py` and `src/nlp/nli.py`
- Status: Phase 2 generic sentiment, Phase 3 target-aware tone, and Phase 4
  Silver framing are implemented. Gold framing marts still remain a later
  activation phase.
- Role: compact mention-level NLP output table for model results that do not
  need one row per frame label.
- Model contract:
  - Sentiment uses `cmarkea/distilcamembert-base-sentiment` as a French
    1-5 star baseline. The model card documents Amazon Reviews and Allocine
    training data and 1-5 star labels:
    https://huggingface.co/cmarkea/distilcamembert-base-sentiment
  - `generic_sentiment_score = (expected_star - 3) / 2`, where
    `expected_star = sum(star * probability)` across stars 1 through 5.
  - The score is a generic review-domain sentiment diagnostic, not
    candidate-aware political tone and not a gender-bias conclusion.
  - Target-aware tone uses `cmarkea/distilcamembert-base-nli` as the primary
    French NLI model. Phase 3 builds candidate-specific hypotheses from
    `gold.sample_leaders.full_name` using the exact template
    `Le texte présente {candidate_name} de manière {}.`
  - Low-confidence tone predictions below `NLP_TONE_THRESHOLD` are persisted
    as `unclassified` while retaining the top probability for threshold audits.
  - Framing uses the same primary NLI model in multi-label mode against the
    controlled frame vocabulary. All scorable frame probabilities are stored in
    `silver.fact_mention_frame_score`.
  - Low-confidence frame predictions below `NLP_FRAME_THRESHOLD` are persisted
    as `unclassified` in the summary table. `unclassified` is a fallback state,
    not a model-scored frame row.
- DQ contract:
  - `mention_id` must be unique and must match the current NLP input table.
  - Scored rows require `input_hash`, `generic_sentiment_label`,
    `generic_sentiment_score`, `scored_at`, and `nlp_model_bundle_version`.
  - Failed rows require `error_type`.
  - `generic_sentiment_score` must be between `-1` and `1`.
  - `target_tone_label` must be `favorable`, `unfavorable`, `neutral`, or
    `unclassified`.
  - `target_tone_probability` is populated for scoreable Phase 3 rows and
    remains NULL for skipped or failed rows.
  - `primary_frame_label` must be one of the controlled frame labels or
    `unclassified`.
  - `primary_frame_probability` is populated only when a scorable primary frame
    passes the configured threshold.

| Column | Type | Notes |
|---|---|---|
| `mention_id` | VARCHAR | Primary key and foreign key to `silver.fact_mention_nlp_input` |
| `leader_id` | VARCHAR | Denormalized sampled leader identifier |
| `canonical_article_id` | VARCHAR | Denormalized article identifier for audits |
| `input_hash` | CHAR(32) | Must match the NLP input hash for scored rows |
| `generic_sentiment_label` | VARCHAR | Top 1-5 star label from the baseline sentiment model |
| `generic_sentiment_score` | DOUBLE | Expected-star score mapped to `[-1, 1]` |
| `target_tone_label` | VARCHAR | Target-aware tone label: `favorable`, `unfavorable`, `neutral`, or `unclassified` |
| `target_tone_probability` | DOUBLE | Selected tone probability for scoreable rows; NULL for skipped or failed rows |
| `primary_frame_label` | VARCHAR | Primary frame label or `unclassified` fallback |
| `primary_frame_probability` | DOUBLE | Selected primary frame probability; NULL when `unclassified` |
| `was_truncated_to_max_length` | BOOLEAN | Whether tokenizer input exceeded `NLP_MAX_TOKEN_LENGTH` |
| `nlp_enrichment_status` | VARCHAR | `scored`, `skipped`, or `failed` |
| `nlp_model_bundle_version` | VARCHAR | Deterministic model-bundle hash from `src/nlp/model_bundle.py` |
| `scored_at` | TIMESTAMP | UTC scoring timestamp for scored or failed rows |
| `error_type` | VARCHAR | Required when `nlp_enrichment_status = 'failed'` |

### `silver.fact_mention_frame_score`

- Grain: one row per `mention_id` x scorable `frame_label`
- Source: `silver.fact_mention_nlp_input.input_text`
- Owner: Python module `src/nlp/nli.py`
- Status: Phase 4 implemented as Silver model output. dbt Gold framing
  activation remains a later phase.
- Role: full multi-label NLI frame probabilities for QA, threshold tuning, and
  future Gold marts.
- DQ contract:
  - Unique key is `mention_id`, `frame_label`.
  - `frame_label` excludes `unclassified`; that label is only a summary
    fallback.
  - `frame_probability` must be between `0` and `1`.
  - At most one frame may have `is_primary_frame = TRUE` per mention.
  - Primary frames must pass the configured frame threshold.
  - Every row must match a current NLP input mention.
  - `nlp_model_bundle_version` is required on every row.

| Column | Type | Notes |
|---|---|---|
| `mention_id` | VARCHAR | Foreign key to `silver.fact_mention_nlp_input` |
| `frame_label` | VARCHAR | Controlled frame label, excluding `unclassified` |
| `frame_probability` | DOUBLE | Multi-label NLI probability |
| `is_primary_frame` | BOOLEAN | True for the selected primary frame |
| `passes_threshold` | BOOLEAN | True when probability meets `NLP_FRAME_THRESHOLD` |
| `nli_hypothesis` | VARCHAR | Exact hypothesis string used for the frame |
| `nlp_model_bundle_version` | VARCHAR | Deterministic model-bundle hash |

### `silver.fact_stereotype_word_counts`

- Grain: one row per `mention_id` x `lexicon_category` x `term`
- Source: `silver.fact_mention_nlp_input.input_text`
- Owner: Python module `src/nlp/lexicon.py`
- Status: Phase 1 implemented as a deterministic lexicon audit. It does not
  run Transformer inference and does not activate Gold NLP marts yet.
- Role: deterministic vocabulary audit table for stereotype and framing terms.
- Text minimization contract:
  - Counts are derived only from Phase 0 mention-level `input_text`.
  - Full article body text is never read by this step.
  - Output rows persist normalized terms and counts, not source text snippets.
- DQ contract:
  - Only rows with `eligible_for_lexicon = TRUE` are counted.
  - Zero-count rows are omitted; downstream marts should treat missing rows as
    zero when NLP Gold metrics are activated.
  - `mention_id`, `lexicon_category`, `term`, and `lexicon_version` are required.
  - Unique key is `mention_id`, `lexicon_category`, `term`.
  - `count` must be positive and `count_per_1k_tokens` must be non-negative.
  - `lexicon_version` defaults to `stereotype_terms_v1`.

| Column | Type | Notes |
|---|---|---|
| `mention_id` | VARCHAR | Foreign key to `silver.fact_mention_nlp_input` |
| `lexicon_category` | VARCHAR | Controlled category such as `politique`, `vie_privee`, `apparence`, `scandale`, `personnalite`, or `securite` |
| `term` | VARCHAR | Normalized lexicon term that matched the mention context |
| `count` | INTEGER | Positive match count within the normalized mention context |
| `count_per_1k_tokens` | DOUBLE | `count / normalized_token_count * 1000` |
| `lexicon_version` | VARCHAR | Versioned lexicon identifier persisted on every row |

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
| `commune_name` | VARCHAR | Human-readable commune label — joined from `silver.dim_commune` at gold write time; required for archive search disambiguation |
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
- Owner: dbt model `dbt/models/marts/news/mart_exposure_metrics.sql`
- Source: `gold.sample_leaders`, `silver.fact_article`, `silver.fact_mention`,
  `silver.dim_commune`
- Role: leader-level coverage denominator with article counts, headline mentions,
  source diversity, and exposure normalized by commune population
- Text-availability metrics:
  - `article_count` remains derived from `silver.fact_mention`, so only articles
    with strict candidate evidence count toward exposure
  - `full_text_article_count` counts mentioned articles whose canonical article
    has `has_full_text = TRUE`
  - `metadata_only_article_count` counts mentioned articles whose canonical
    article has `has_full_text = FALSE`
  - `has_full_text` is a leader-level flag showing whether at least one counted
    article has usable full text
- Denominator contract: the mart still keeps one row per sampled leader, so the
  active 36-person cohort denominator is preserved even for zero-coverage
  leaders.
- Provenance contract: `restricted_source_article_count` and
  `supplemental_source_article_count` are source QA/provenance counters. They
  are retained for auditability but excluded from regression predictors.

### `gold.mart_framing_metrics`

- Owner: dbt model `dbt/models/marts/news/mart_framing_metrics.sql`

- Grain: one row per sampled leader × frame label
- Source: `silver.fact_mention`
- Role: NLP pending contract. The current baseline stabilizes the table shape
  with `unclassified` rows only; it does not support framing or dashboard tone
  conclusions until NLP Silver outputs are promoted into Gold marts.

### `gold.mart_bias_indicators`

- Owner: dbt model `dbt/models/marts/news/mart_bias_indicators.sql`

- Grain: one row per gender × exposure metric
- Source: `gold.mart_exposure_metrics`
- Role: quick comparison layer for dashboard-level gender summaries

### `gold.mart_regression_feature_base`

- Owner: dbt model `dbt/models/marts/news/mart_regression_feature_base.sql`

- Grain: one row per sampled leader
- Source: `gold.mart_exposure_metrics`
- Role: stable modeling base consumed by Python statsmodels diagnostics. It
  keeps one row per leader and materializes documented controls (`gender`,
  `city_size_bucket`, `nuance_group`, `is_incumbent`, `reg_code`,
  `won_final_round`).

### `gold.mart_analysis_summary`

- Grain: one row per analysis, dimension, group label, and metric
- Owner: dbt model `dbt/models/marts/news/mart_analysis_summary.sql`
- Source: `gold.mart_exposure_metrics`
- Role: long-form dashboard summary table so Streamlit displays metric results
  rather than recomputing analytical definitions at render time.
- Key contract: `analysis_id` is a unique row-level identifier; related rows
  are grouped by `analysis_section_id` (for example `A1` for exposure
  distribution).

### `gold.mart_regression_results`

- Grain: one row per model coefficient
- Owner: Python module `src.metrics.news.regression`
- Source: `gold.mart_regression_feature_base`
- Role: persisted Poisson and Negative Binomial exposure-model audit output
- Status contract: each row carries a `status` field such as `fitted`,
  `fitted_with_warning:*`, or `fit_failed:*` so statistical warnings remain
  visible in downstream audit artifacts

### `gold.mart_bootstrap_ci`

- Grain: one row per bootstrap coefficient
- Owner: Python module `src.metrics.news.regression`
- Source: `gold.mart_regression_feature_base`
- Role: empirical confidence interval diagnostic for the Negative Binomial
  exposure model. The fixed random seed makes re-runs reproducible.

### `data/gold/news_corpus_qa_report.json`

- Grain: one JSON report per news corpus pipeline run
- Role: lightweight run-level QA artifact for parser mix, language mix,
  rejection rates, candidate coverage, regression status, and web-enrichment
  behavior
- Web-enrichment counters:
  - `web_scrape_queued_count`: candidate URL rows considered for enrichment
  - `web_scrape_cache_hit_count`: queued rows filled from local cache without
    network access
  - `web_scrape_success_count`: queued rows filled by newly fetched web text
  - `url_metadata_only_count`: queued rows retained as metadata-only rows
  - `web_scrape_failure_count`: queued rows whose scrape failed or returned
    unusably short text

### `gold.nlp_tone_threshold_sensitivity`

- Grain: one row per threshold x segment
- Owner: Python module `src.nlp.tone_sensitivity`
- Source: `silver.fact_mention_nlp_summary` joined to
  `gold.sample_leaders.gender`
- Storage:
  - `data/gold/nlp_tone_threshold_sensitivity.parquet`
  - DuckDB table `gold.nlp_tone_threshold_sensitivity`
- Companion report:
  - `data/gold/nlp_tone_sensitivity_report.json`
- Role: model QA artifact for Phase 3 target-aware tone coverage. It audits
  how the classified share of scoreable rows changes when the probability
  threshold varies.
- Scope limitation: this artifact does not reconstruct alternate tone-label
  distributions. The Silver summary persists the final label and top
  probability, not low-confidence raw top labels or full NLI probability
  vectors.

| Column | Type | Notes |
|---|---|---|
| `generated_at` | TIMESTAMP | UTC report generation timestamp |
| `nlp_model_bundle_version` | VARCHAR | Single model-bundle version represented by the source summary |
| `threshold` | DOUBLE | Probability threshold audited |
| `segment_type` | VARCHAR | `overall` or `gender` |
| `segment_value` | VARCHAR | `all`, `F`, or `M` |
| `total_mentions` | INTEGER | Mentions in the segment |
| `scoreable_mentions` | INTEGER | Rows with a persisted top tone probability |
| `not_scoreable_mentions` | INTEGER | Rows skipped or failed before tone probability exists |
| `classified_mentions_at_threshold` | INTEGER | Scoreable rows with `target_tone_probability >= threshold` |
| `low_confidence_mentions_at_threshold` | INTEGER | Scoreable rows below the audited threshold |
| `classified_share_of_scoreable` | DOUBLE | Classified share among scoreable rows |

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
as `run-sampling-pipeline` or `run-news-corpus-pipeline`.

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
                             |
                             +--< gold.mart_regression_results
                             |
                             +--< gold.mart_bootstrap_ci

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
| News ingest and article pipeline | Implemented | Keep the Europresse parser contract and QA checks stable as new exports are added |
| dbt Gold mart layer | Implemented | Keep model schema tests aligned with dashboard and regression contracts |
| NLP fact tables and marts | Partially implemented | Phase 0 `fact_mention_nlp_input`, Phase 1 `fact_stereotype_word_counts`, Phase 2/3/4 `fact_mention_nlp_summary`, Phase 4 `fact_mention_frame_score`, and tone threshold QA artifacts exist; add Gold activation next |
