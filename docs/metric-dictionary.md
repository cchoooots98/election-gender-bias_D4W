# Metric Dictionary — Election Gender Bias D4W

Canonical definitions for all analytical metrics published in the Gold layer.
Consumers: Streamlit dashboard, regression audit, portfolio documentation.

Last updated: 2026-04-13

---

## Conventions

| Field | Meaning |
|---|---|
| **Grain** | The unit of observation for this metric (one row = one X) |
| **Owner** | dbt model or Python module that computes and persists this metric |
| **Formula** | Exact computation, referencing Silver source columns |
| **Null contract** | When the metric may be NULL and what that means |
| **Freshness** | When this metric is rebuilt |

All metrics are recomputed on each full pipeline run. No incremental updates.

---

## Exposure Metrics (`gold.mart_exposure_metrics`)

Grain: **one row per sampled candidate leader** (36 rows total).

### `article_count`

| Field | Value |
|---|---|
| **Definition** | Number of distinct canonical articles in which this leader is mentioned |
| **Formula** | `COUNT(DISTINCT fact_mention.canonical_article_id)` WHERE the leader has a confirmed match |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL. Leaders with zero corpus coverage produce `0`. |
| **Interpretation** | Raw coverage volume. Sensitive to city size and incumbent status. Do not compare across strata without controlling for both. |

### `headline_mention_count`

| Field | Value |
|---|---|
| **Definition** | Number of distinct canonical articles where the candidate appears in the article headline |
| **Formula** | `COUNT(DISTINCT canonical_article_id) WHERE fact_mention.headline_mention_flag = TRUE` |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL (`0` when no headline mentions). |
| **Interpretation** | Headline salience signal. A leader appearing only in body text may be referenced incidentally rather than as the subject. |

### `distinct_source_count`

| Field | Value |
|---|---|
| **Definition** | Number of distinct normalized outlet names across all articles mentioning this candidate |
| **Formula** | `COUNT(DISTINCT fact_article.outlet_name_normalized)` for matched articles |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL. |
| **Interpretation** | Source diversity. A high `article_count` with a low `distinct_source_count` means coverage is concentrated in one outlet — less robust signal than cross-outlet coverage. |

### `exposure_per_10k_population`

| Field | Value |
|---|---|
| **Definition** | Article count normalized by commune resident population |
| **Formula** | `article_count / (population / 10_000)` |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | `0.0` when population is zero (edge case guard). |
| **Interpretation** | **The primary cross-stratum comparison metric.** Raw article counts are confounded by city size because larger communes generate more press volume. Dividing by population creates a comparable rate across the small/medium/large strata. Note: population here is commune resident population (`dim_commune.population`), not registered voters. |
| **Caveat** | Small communes produce high per-capita rates by construction (small denominator). The medium-city stratum provides the most stable within-stratum comparison. |

### `full_text_article_count`

| Field | Value |
|---|---|
| **Definition** | Articles where the canonical record has usable full body text |
| **Formula** | `COUNT(DISTINCT canonical_article_id) WHERE fact_article.has_full_text = TRUE` |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL. |
| **Interpretation** | Data-completeness signal. Only full-text articles can support future NLP enrichment (sentiment, framing). Metadata-only articles contribute to exposure counts but not to NLP outputs. |

### `metadata_only_article_count`

| Field | Value |
|---|---|
| **Definition** | `article_count - full_text_article_count` |
| **Formula** | Derived |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL. |
| **Interpretation** | Articles retained as URL/title metadata only — typically Europresse web-reference stubs where the original page was paywalled or unavailable. These articles count toward exposure only when strict title/URL candidate evidence exists. |

### `restricted_source_article_count`

| Field | Value |
|---|---|
| **Definition** | Articles from rights-restricted local sources (`rights_class = 'restricted_local'`) |
| **Formula** | `COUNT(DISTINCT canonical_article_id) WHERE rights_class = 'restricted_local'` |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL. |
| **Interpretation** | Provenance audit counter. Retained for source-mix QA; excluded from regression predictors. |

---

## Bias Indicators (`gold.mart_bias_indicators`)

Grain: **one row per gender × exposure metric** (compact summary for dashboard).

### `mean_value` / `median_value`

| Field | Value |
|---|---|
| **Definition** | Mean and median of the named `metric_name` across all sampled leaders of this gender |
| **Formula** | Computed from `gold.mart_exposure_metrics` grouped by gender |
| **Owner** | `dbt/models/marts/news/mart_bias_indicators.sql` |
| **Interpretation** | Top-level gender comparison. Because distributions are right-skewed (one large-city incumbent dominates the male corpus), median is often more informative than mean. Interpret alongside stratum-level breakdowns. |

---

## Regression Results (`gold.mart_regression_results`)

Grain: **one row per model × coefficient**.

### `coefficient` (gender_female)

| Field | Value |
|---|---|
| **Definition** | Estimated log-count difference for female vs. male candidates, holding city-size bucket, region fixed effects, political bloc, incumbent status, and election outcome constant |
| **Models** | `poisson_exposure` and `negbinom_exposure` |
| **Formula** | Poisson / Negative Binomial GLM: `log(article_count) = β₀ + β₁·gender_female + Σβᵢ·Xᵢ + log(population)` |
| **Owner** | `src/metrics/news/regression.py → build_mart_regression_results()` |
| **Current estimate** | +0.229 (SE = 0.078, p = 0.003) under Poisson — IRR ≈ 1.26 |
| **Interpretation** | A positive coefficient means female candidacy is associated with more articles after controlling for listed covariates. **Read as a directional audit signal, not a causal claim.** n = 36; results are sensitive to individual high-leverage candidates. |
| **Null contract** | `coefficient` is NULL when `status` starts with `fit_failed:` or `not_fitted_*`. |

### `status`

| Value | Meaning |
|---|---|
| `fitted` | Model converged without warnings |
| `fitted_with_warning:*` | Model converged but statsmodels issued warnings (e.g. `ConvergenceWarning`); coefficients should be treated cautiously |
| `fit_failed:*` | Model could not be fitted (e.g. singular matrix); all coefficient columns are NULL |
| `not_fitted_zero_articles` | No articles in corpus; pipeline ran before news ingest |
| `not_fitted_missing_statsmodels` | statsmodels not installed in this environment |

---

## Bootstrap Confidence Intervals (`gold.mart_bootstrap_ci`)

Grain: **one row per regression variable** (n=2,000 bootstrap resamples, fixed seed 42).

### `ci_lower_95` / `ci_upper_95`

| Field | Value |
|---|---|
| **Definition** | 2.5th and 97.5th percentiles of the bootstrap distribution for this coefficient |
| **Formula** | Empirical percentile bootstrap over n=2,000 Negative Binomial resamples |
| **Owner** | `src/metrics/news/regression.py → build_mart_bootstrap_ci()` |
| **Interpretation** | More conservative interval than the analytical Negative Binomial SE, because it does not assume distributional form. If `ci_excludes_zero = TRUE`, the coefficient is robust to resampling uncertainty at the 95% level. |
| **Null contract** | NULL when fewer than 50 resamples converged (stored as `NaN`). |

### `n_converged`

| Field | Value |
|---|---|
| **Definition** | Number of the 2,000 bootstrap resamples that produced a numerically stable fit |
| **Interpretation** | A convergence rate below ~90% (`n_converged < 1800`) signals numerical instability — treat the CI with additional caution. Causes include sparse region dummies or near-multicollinear predictors. |

---

## Analysis Summary (`gold.mart_analysis_summary`)

Grain: **one row per analysis × dimension × group label × metric name**.

This is a long-format summary table that the Streamlit dashboard reads directly.
It aggregates results from `mart_exposure_metrics` into labeled sections.

### `analysis_section_id`

Grouping key used by the dashboard to display related metrics together.
Example sections: `A1` (exposure distribution), `A2` (stratum breakdown).

### `metric_value`

The computed scalar value for this row. Units depend on `metric_name`:
- `mean_article_count`: articles per leader
- `exposure_per_10k_mean`: articles per 10,000 residents per leader
- `source_diversity_mean`: distinct outlets per leader

---

## NLP Readiness Metrics (`silver.fact_mention_nlp_input`)

Grain: **one row per article x candidate mention**. These are Phase 0 audit
signals, not final bias metrics.

### `context_word_count`

| Field | Value |
|---|---|
| **Definition** | Whitespace-delimited word count after Phase 0 context normalization |
| **Formula** | `len(input_text.split())` after repeated whitespace is collapsed |
| **Owner** | `src/nlp/input_contracts.py` |
| **Null contract** | Never NULL. Empty contexts produce `0`. |
| **Interpretation** | Data-readiness measure for downstream lexicon and Transformer inference. This is not a CamemBERT tokenizer/BPE token count. |

### `eligible_for_lexicon` / `eligible_for_inference`

| Field | Value |
|---|---|
| **Definition** | Boolean gates separating deterministic lexicon audit from higher-context model inference |
| **Formula** | `eligible_for_lexicon = language == 'fr' AND context_word_count >= 3`; `eligible_for_inference = eligible_for_lexicon AND context_word_count >= 12` |
| **Owner** | `src/nlp/input_contracts.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Prevents short snippets from contaminating NLI tone/framing outputs while still allowing deterministic audit counts where safe. |

### `skip_reason`

| Field | Value |
|---|---|
| **Definition** | Controlled reason a row is not eligible for Transformer inference |
| **Accepted values** | `empty_context`, `too_short_for_lexicon`, `too_short_for_inference`, `language_not_french` |
| **Owner** | `src/nlp/input_contracts.py` |
| **Null contract** | NULL only when `eligible_for_inference = TRUE`. |
| **Interpretation** | QA denominator for future NLP coverage reporting by gender, stratum, and source mix. |

---

## Metric Freshness and SLA

| Layer | Rebuilt when | SLA |
|---|---|---|
| Bronze (official data) | `make run-sampling-pipeline` is run | No SLA — static government data; re-ingest only when ministry publishes updates |
| Silver (article corpus) | `make run-news-corpus-pipeline` is run | No SLA — corpus is bounded by the fixed analysis window (Nov 2025 – Apr 2026) |
| Silver (NLP input) | `make run-nlp-input-pipeline` is run | Must pass Phase 0 contract tests before downstream NLP scoring |
| Gold dbt marts | Embedded in `make run-news-corpus-pipeline` via `dbt run` | Must pass all 37 schema tests before the pipeline exits |
| Gold regression | Embedded in news corpus pipeline | Recomputed on each run; `status` column records whether fit succeeded |
| Dashboard | On Streamlit startup — reads Gold Parquet files | Reflects the last complete pipeline run |

There is no streaming or incremental refresh. This is a batch analytical pipeline
over a closed historical window. "Freshness" means the last full pipeline run
completed successfully and `meta.meta_run.status = 'success'`.
