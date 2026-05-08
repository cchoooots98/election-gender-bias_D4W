# Metric Dictionary — Election Gender Bias D4W

Canonical definitions for all analytical metrics published in the Gold layer.
Consumers: Streamlit dashboard, regression audit, portfolio documentation.

Last updated: 2026-05-05

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

## NLP Lexicon Audit Metrics (`silver.fact_stereotype_word_counts`)

Grain: **one row per article x candidate mention x lexicon category x term**.
These are deterministic audit features, not model-inferred tone or framing
labels.

The v1 lexicon is a minimal seed for structural validation. It must be expanded
and reviewed before `count` or `count_per_1k_tokens` are used for statistical
claims about media bias.

### `count`

| Field | Value |
|---|---|
| **Definition** | Number of exact normalized token or phrase matches for one lexicon term in one mention context |
| **Formula** | Sliding-window exact match over `tokenize_lexicon_text(input_text)` |
| **Owner** | `src/nlp/lexicon.py` |
| **Null contract** | Never NULL for emitted rows. Zero-count rows are omitted. |
| **Interpretation** | Reproducible vocabulary signal for auditing media framing and stereotype-associated wording. |

### `count_per_1k_tokens`

| Field | Value |
|---|---|
| **Definition** | Lexicon count normalized by the number of normalized lexicon tokens in the mention context |
| **Formula** | `count / normalized_token_count * 1000` |
| **Owner** | `src/nlp/lexicon.py` |
| **Null contract** | Never NULL for emitted rows. |
| **Interpretation** | Makes counts comparable across short and long context windows. This is a deterministic whitespace-token metric, not a CamemBERT tokenizer/BPE count. Because the v1 lexicon is intentionally sparse, zero counts should be read as "no seed-term match", not as absence of stereotype or framing language. |

---

## NLP Sentiment and Tone Metrics (`silver.fact_mention_nlp_summary`)

Grain: **one row per article x candidate mention**. These are Phase 2 generic
sentiment diagnostics plus Phase 3 target-aware tone audit signals. They do
not activate dashboard-level NLP conclusions until Gold marts consume them.

Model methodology source: `cmarkea/distilcamembert-base-sentiment`, documented
as a French 1-5 star sentiment model trained on Amazon Reviews and Allocine:
https://huggingface.co/cmarkea/distilcamembert-base-sentiment

Tone methodology source: `cmarkea/distilcamembert-base-nli`, used as the
primary French NLI model for candidate-aware tone hypotheses:
https://huggingface.co/cmarkea/distilcamembert-base-nli

### `generic_sentiment_label`

| Field | Value |
|---|---|
| **Definition** | Highest-probability 1-5 star label from the generic sentiment baseline |
| **Accepted values** | `1 star`, `2 stars`, `3 stars`, `4 stars`, `5 stars` |
| **Owner** | `src/nlp/sentiment.py` |
| **Null contract** | NULL when `nlp_enrichment_status` is `skipped` or `failed`. |
| **Interpretation** | Review-domain sentiment signal for QA and baseline comparison only. It is not target-aware political tone. |

### `generic_sentiment_score`

| Field | Value |
|---|---|
| **Definition** | Expected 1-5 star model score mapped to `[-1, 1]` |
| **Formula** | `(expected_star - 3) / 2`, where `expected_star = sum(star * probability)` across stars 1 through 5 |
| **Owner** | `src/nlp/sentiment.py` |
| **Null contract** | NULL when `nlp_enrichment_status` is `skipped` or `failed`. |
| **Interpretation** | Generic negative-to-positive baseline. Do not use it as the primary bias conclusion because the model is not conditioned on the candidate target. |

### `target_tone_label`

| Field | Value |
|---|---|
| **Definition** | Candidate-aware tone selected from NLI probabilities for the mention context |
| **Accepted values** | `favorable`, `unfavorable`, `neutral`, `unclassified` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL. Skipped, failed, or low-confidence rows use `unclassified`. |
| **Interpretation** | Target-aware political tone audit signal. This is more appropriate than generic sentiment for candidate coverage because the hypothesis explicitly names the candidate. It remains a Silver model output until Gold marts and dashboard panels are activated. |

### `target_tone_probability`

| Field | Value |
|---|---|
| **Definition** | Probability for the selected tone label, or the top probability when the row is below the confidence threshold |
| **Formula** | Highest NLI probability across `favorable`, `unfavorable`, and `neutral`; labels below `NLP_TONE_THRESHOLD` are persisted as `unclassified` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | NULL for skipped or failed rows; populated for scoreable Phase 3 rows. |
| **Interpretation** | Confidence and threshold-sensitivity audit field. A low probability attached to `unclassified` means the model saw no confident tone, not that the mention had neutral tone. |

### `nlp_enrichment_status`

| Field | Value |
|---|---|
| **Definition** | Row-level Phase 2 scoring status |
| **Accepted values** | `scored`, `skipped`, `failed` |
| **Owner** | `src/nlp/sentiment.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Coverage denominator for model QA. `skipped` means the Phase 0 input was not eligible for inference; `failed` means model scoring was requested but failed for the row. |

### `nlp_model_bundle_version`

| Field | Value |
|---|---|
| **Definition** | Deterministic short hash of model names, immutable Hugging Face commit revisions, thresholds, runtime dimensions, and hypothesis template version |
| **Owner** | `src/nlp/model_bundle.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Model provenance identifier. A changed bundle version means sentiment outputs were produced under different runtime metadata and should not be mixed without audit. Mutable revisions such as `main`, `master`, or `latest` are rejected by the model-bundle contract. |

---

## NLP Tone Threshold Sensitivity (`gold.nlp_tone_threshold_sensitivity`)

Grain: **one row per threshold x segment**. Segment is either overall
(`segment_type = 'overall'`, `segment_value = 'all'`) or gender
(`segment_type = 'gender'`, `segment_value IN ('F', 'M')`).

This is a model QA artifact, not a dashboard metric. It supports sensitivity
analysis for `NLP_TONE_THRESHOLD` by showing how candidate-aware tone coverage
changes across a fixed threshold grid.

### `threshold`

| Field | Value |
|---|---|
| **Definition** | Candidate-aware tone probability cutoff being audited |
| **Owner** | `src/nlp/tone_sensitivity.py` |
| **Null contract** | Never NULL. Values are validated to be unique and within `[0, 1]`. |
| **Interpretation** | Lower thresholds increase coverage but may accept weaker model confidence; higher thresholds reduce false-confidence risk but leave more rows unclassified. |

### `classified_mentions_at_threshold`

| Field | Value |
|---|---|
| **Definition** | Number of scoreable mention rows whose persisted top tone probability is at least `threshold` |
| **Formula** | `COUNT(*) WHERE target_tone_probability >= threshold` among scoreable rows |
| **Owner** | `src/nlp/tone_sensitivity.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Coverage count at the audited threshold. It should be compared with `scoreable_mentions`, not total corpus rows, because skipped rows never called the model. |

### `classified_share_of_scoreable`

| Field | Value |
|---|---|
| **Definition** | Share of scoreable mention rows classified at the audited threshold |
| **Formula** | `classified_mentions_at_threshold / scoreable_mentions` |
| **Owner** | `src/nlp/tone_sensitivity.py` |
| **Null contract** | NULL only when a segment has zero scoreable rows. |
| **Interpretation** | Primary sensitivity metric. Compare the female and male segment rows at the same threshold to see whether model coverage is balanced by gender. |

Companion JSON artifact:
- `data/gold/nlp_tone_sensitivity_report.json`
- Includes current persisted label distribution, probability bins by gender,
  female-minus-male coverage gap by threshold, and the explicit limitation that
  alternate label distributions are not reconstructed from Silver.

---

## Metric Freshness and SLA

| Layer | Rebuilt when | SLA |
|---|---|---|
| Bronze (official data) | `make run-sampling-pipeline` is run | No SLA — static government data; re-ingest only when ministry publishes updates |
| Silver (article corpus) | `make run-news-corpus-pipeline` is run | No SLA — corpus is bounded by the fixed analysis window (Nov 2025 – Apr 2026) |
| Silver (NLP input) | `make run-nlp-input-pipeline` is run | Must pass Phase 0 contract tests before downstream NLP scoring |
| Silver (NLP lexicon audit) | `make run-nlp-lexicon-pipeline` is run | Must pass Phase 1 contract tests before downstream NLP Gold activation |
| Silver (NLP sentiment baseline) | `make run-nlp-sentiment-pipeline` is run | Requires optional Transformer dependencies and must pass Phase 2 contract tests |
| Silver (NLP target-aware tone) | `make run-nlp-tone-pipeline` is run | Requires existing Phase 2 summary rows and optional Transformer dependencies |
| Gold NLP tone sensitivity QA | `make run-nlp-tone-sensitivity-pipeline` is run | Must read a tone-enriched Phase 3 summary; does not load Transformer models |
| Gold dbt marts | Embedded in `make run-news-corpus-pipeline` via `dbt run` | Must pass all 37 schema tests before the pipeline exits |
| Gold regression | Embedded in news corpus pipeline | Recomputed on each run; `status` column records whether fit succeeded |
| Dashboard | On Streamlit startup — reads Gold Parquet files | Reflects the last complete pipeline run |

There is no streaming or incremental refresh. This is a batch analytical pipeline
over a closed historical window. "Freshness" means the last full pipeline run
completed successfully and `meta.meta_run.status = 'success'`.
