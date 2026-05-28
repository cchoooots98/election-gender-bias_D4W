# Metric Dictionary — Election Gender Bias D4W

Canonical definitions for analytical metrics published in the Gold layer and
deterministic dashboard reports derived from those metrics.
Consumers: Streamlit dashboard, regression audit, portfolio documentation.

Last updated: 2026-05-27

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
| **Interpretation** | Data-completeness signal. Only full-text articles can support downstream NLP enrichment (sentiment, framing). Metadata-only articles contribute to exposure counts but not to NLP outputs. |

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

### `supplemental_source_article_count`

| Field | Value |
|---|---|
| **Definition** | Articles whose acquisition-method provenance includes supplemental sourcing |
| **Formula** | `COUNT(DISTINCT canonical_article_id) WHERE acquisition_methods LIKE '%supplemental%'` |
| **Owner** | `dbt/models/marts/news/mart_exposure_metrics.sql` |
| **Null contract** | Never NULL. |
| **Interpretation** | Provenance audit counter for manually supplied or non-primary source records. Retained for source-mix QA; excluded from regression predictors. |

---

## Bias Indicators (`gold.mart_bias_indicators`)

Grain: **one row per gender x exposure or NLP audit metric** (compact summary
for dashboard).

### Exposure Metrics

| Field | Value |
|---|---|
| **Definition** | Mean or total of the named exposure `metric_name` across all sampled leaders of this gender |
| **Formula** | Computed from `gold.mart_exposure_metrics` grouped by gender |
| **Owner** | `dbt/models/marts/news/mart_bias_indicators.sql` |
| **Interpretation** | Top-level gender comparison. Because distributions are right-skewed (one large-city incumbent dominates the male corpus), median is often more informative than mean. Interpret alongside stratum-level breakdowns. |

### NLP Audit Metrics

| Metric | Formula | Interpretation |
|---|---|---|
| `nlp_inference_coverage_rate` | Mean leader-level share of NLP summary rows with `nlp_enrichment_status = 'scored'` | Coverage denominator that must be read before tone or framing comparisons |
| `mean_unfavorable_tone_share` | Mean leader-level share of scoreable tone rows labeled `unfavorable` at the 0.60 threshold | Candidate-aware NLI tone diagnostic. A value of `0.0` means no row crossed the threshold; it does not prove negative coverage is absent. |
| `mean_policy_frame_share` | Mean leader-level share of primary-frame-classified mentions with primary frame `politique`; `unclassified` rows are excluded from this denominator | Policy/governance framing signal |
| `mean_scandal_frame_share` | Mean leader-level share of primary-frame-classified mentions with primary frame `scandale`; `unclassified` rows are excluded from this denominator | Scandal/conflict framing signal. Compare with volume-weighted primary-frame chart because high-coverage leaders can dominate raw mention counts. |
| `mean_appearance_private_life_frame_share` | Mean leader-level share of primary-frame-classified mentions with primary frame `apparence` or `vie_privee`; `unclassified` rows are excluded from this denominator | Appearance/private-life framing signal |
| `generic_sentiment_coverage_rate` | Mean leader-level share of NLP summary rows with non-null generic sentiment label and score | Baseline sentiment coverage diagnostic; generic sentiment is not candidate-aware tone |
| `mean_generic_sentiment_score` | Mean leader-level generic sentiment score on the `[-1, 1]` expected-star scale | Baseline polarity diagnostic only; do not interpret as gendered treatment without context review |
| `mean_stereotype_count_per_1k_tokens` | Per-leader average over lexicon-eligible mentions only; non-eligible mentions are excluded from both numerator and denominator | Sparse deterministic lexicon audit feature |

Frame-share metrics currently assign `0.0` to leaders with no
primary-frame-classified mentions before taking the gender mean. This keeps
zero-coverage leader rows visible in the dashboard; interpret low means with
the NLP inference and frame-classification coverage metrics.

---

## Outlier Sensitivity Report (`build_outlier_sensitivity_report`)

Grain: **one row per sensitivity scenario**.

This report is computed at dashboard render time from
`gold.mart_exposure_metrics`. It is not a persisted Gold table; it is a
deterministic robustness view over the Gold exposure mart.

| Field | Value |
|---|---|
| **Owner** | `src/metrics/news/outlier_sensitivity.py -> build_outlier_sensitivity_report()` |
| **Source** | `gold.mart_exposure_metrics` |
| **Primary metric** | `article_count` by `gender` |
| **Null contract** | Empty exposure input returns an empty report schema. Missing, null, non-numeric, or negative exposure values raise before rendering. |
| **Interpretation** | Robustness check for high-leverage candidates. The panel asks whether the headline gender gap survives when the top exposure rows are removed, capped, or replaced by medians. |

### Scenarios

| `scenario_id` | Formula | Interpretation |
|---|---|---|
| `all` | Mean `article_count` by gender across the full cohort | Headline arithmetic mean; most sensitive to extreme leaders |
| `drop_top_overall` | Mean after removing the single highest-`article_count` leader across the full cohort | Tests whether one leader drives the entire gender gap |
| `drop_top_each_gender` | Mean after removing the highest-`article_count` leader within each gender | Symmetric top-tail check that preserves gender comparability |
| `winsorized_mean` | Mean after capping `article_count` at the cohort p95 threshold | Reduces leverage without deleting observations; one shared cap is used for both genders |
| `median` | Median `article_count` by gender across the full cohort | Typical-candidate comparison; robust to tail concentration |

### Gender comparison columns

| Column | Definition |
|---|---|
| `f_value` / `m_value` | Scenario-specific exposure value for female and male leaders |
| `female_minus_male` | `f_value - m_value`; positive means the female segment is higher |
| `female_to_male_ratio` | `f_value / m_value`; NULL/NaN when the male denominator is zero or absent |
| `f_n` / `m_n` | Number of female and male leaders included after the scenario filter |

---

## Regression Results (`gold.mart_regression_results`)

Grain: **one row per model × coefficient**.

### `coefficient` (gender_female)

| Field | Value |
|---|---|
| **Definition** | Estimated log-count difference for female vs. male candidates under the model's documented control set |
| **Models** | `negbinom_exposure` is the primary model; `poisson_exposure` is overdispersion diagnostic; `negbinom_exposure_full_controls` is sensitivity; `negbinom_exposure_placebo` is falsification |
| **Formula** | Primary Negative Binomial GLM: `log(E[article_count]) = beta_0 + beta_1 * gender_female + beta_2 * is_incumbent + offset(log(population))` |
| **Owner** | `src/metrics/news/regression.py → build_mart_regression_results()` |
| **Interpretation** | Population is an exposure offset, so its coefficient is fixed at 1.0 rather than estimated as a covariate. The primary model is intentionally low-dimensional for n = 36. City-size bucket, political bloc, election outcome, and region fixed effects are sensitivity or appendix controls, not the headline model. **Read as an audit signal, not a causal claim.** Results are sensitive to individual high-leverage candidates. |
| **Null contract** | `coefficient` is NULL when `status` starts with `fit_failed:` or `not_fitted_*`. |

### Governance columns

| Column | Definition |
|---|---|
| `model_role` | `primary`, `diagnostic`, `sensitivity`, or `placebo`; dashboards should privilege `primary` and label the others explicitly |
| `q_value` | Benjamini-Hochberg false-discovery-rate adjusted value computed from fitted p-values in the persisted result set; dashboard tables display values below `0.001` in scientific notation |
| `parameter_count` | Number of parameters estimated for the row's model; this makes high-dimensional sensitivity models visibly different from the primary model |
| `excluded_missing_control_count` | Number of rows excluded before fitting because a required primary control, currently `is_incumbent`, was unknown |
| `inference_status` | Machine-readable governance status such as `inconclusive`, `publishable_signal`, `diagnostic_only`, `placebo_check`, or `not_fitted` |
| `is_publishable` | Boolean flag for coefficient rows with a publishable adjusted signal; diagnostic, placebo, intercept, and inconclusive rows are `false` |

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
| **Interpretation** | A convergence rate below ~90% (`n_converged < 1800`) signals numerical instability; treat the CI with additional caution. Causes include sparse controls, high-leverage candidates, or near-multicollinear predictors. |

---

## Analysis Summary (`gold.mart_analysis_summary`)

Grain: **one row per analysis × dimension × group label × metric name**.

This is a long-format summary table that the Streamlit dashboard reads directly.
It aggregates results from `mart_exposure_metrics` and NLP rows from
`mart_bias_indicators` into labeled sections.

### `analysis_section_id`

Grouping key used by the dashboard to display related metrics together.
Example sections: `A1` (exposure distribution), `A2` (stratum breakdown), and
`A5` (NLP audit signals).

### `metric_value`

The computed scalar value for this row. Units depend on `metric_name`:
- `mean_article_count`: articles per leader
- `exposure_per_10k_mean`: articles per 10,000 residents per leader
- `source_diversity_mean`: distinct outlets per leader

---

## Gold Framing Metrics (`gold.mart_framing_metrics`)

Grain: **one row per sampled leader x frame label**.

### `mention_count`

| Field | Value |
|---|---|
| **Definition** | Distinct mention count assigned to the leader and frame label |
| **Formula** | For scorable labels, `COUNT(DISTINCT mention_id)` where `silver.fact_mention_frame_score.passes_threshold = TRUE`; for `unclassified`, skipped, failed, or below-threshold summary rows |
| **Owner** | `dbt/models/marts/news/mart_framing_metrics.sql` |
| **Null contract** | Never NULL. Leaders with no matching mentions produce `0`. |
| **Interpretation** | Multi-label frame diagnostic. One mention can pass multiple frame thresholds, so counts can sum above the mention denominator. The dashboard uses `gold.mart_primary_frame_metrics` for the main gender comparison. When NLP Silver outputs are unavailable, all mentions remain in the unclassified fallback. |

### `mean_frame_score`

| Field | Value |
|---|---|
| **Definition** | Mean frame probability for the leader and frame label |
| **Formula** | `AVG(frame_probability)` from `silver.fact_mention_frame_score`; `0.0` for `unclassified` fallback rows |
| **Owner** | `dbt/models/marts/news/mart_framing_metrics.sql` |
| **Interpretation** | Probability audit companion to `mention_count`; it is not a calibrated prevalence estimate. |

---

## Primary Frame Metrics (`gold.mart_primary_frame_metrics`)

Grain: **one row per sampled leader x primary frame label**.

### `mention_count`

| Field | Value |
|---|---|
| **Definition** | Distinct mention count assigned to the selected `primary_frame_label`, or `unclassified` when no primary frame is available |
| **Formula** | `COUNT(DISTINCT mention_id)` grouped by `silver.fact_mention_nlp_summary.primary_frame_label`, with blank or NULL labels mapped to `unclassified` |
| **Owner** | `dbt/models/marts/news/mart_primary_frame_metrics.sql` |
| **Null contract** | Never NULL. The mart contains one row for every sampled leader x seven frame labels. |
| **Interpretation** | Main dashboard frame distribution. Each mention contributes to at most one frame, so totals reconcile with the mention denominator and with frame-share metrics in `gold.mart_bias_indicators`. |

### `mean_primary_frame_score`

| Field | Value |
|---|---|
| **Definition** | Mean primary-frame probability for the leader and frame label |
| **Formula** | `AVG(primary_frame_probability)` from `silver.fact_mention_nlp_summary`; `0.0` for `unclassified` fallback rows |
| **Owner** | `dbt/models/marts/news/mart_primary_frame_metrics.sql` |
| **Interpretation** | Probability companion for the primary frame. It is a model-score diagnostic, not an adjudicated prevalence estimate. |

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
| **Interpretation** | QA denominator for NLP coverage reporting by gender, stratum, and source mix. |

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

## Trait Lexicon Counts (`silver.fact_trait_word_counts`)

Grain: **one row per mention x trait category x tier x term**.

This table is a deterministic, two-tier trait audit over
`silver.fact_mention_nlp_input.input_text`. It does not read or persist full
article body text.

### `trait_category`

| Field | Value |
|---|---|
| **Definition** | Controlled category for a matched candidate-description trait |
| **Accepted values** | `political_work`, `leadership_competence`, `personality`, `family_private_life`, `appearance_body`, `romance_relationship`, `scandal_conflict`, `security_order` |
| **Owner** | `src/nlp/trait_lexicon.py` |
| **Interpretation** | Explains which kind of candidate description the matched term represents. Categories are project-specific because general French resources such as FEEL, Lexique, and LIWC do not directly encode this political gender-bias taxonomy. |

### `trait_tier`

| Field | Value |
|---|---|
| **Definition** | Precision/coverage tier for the matched term |
| **Accepted values** | `core`, `exploratory` |
| **Interpretation** | `core` terms prioritize precision and support main dashboard interpretation. `exploratory` terms increase coverage and are discovery signals that require context review before strong claims. |

### `count` / `count_per_1k_tokens`

| Field | Value |
|---|---|
| **Definition** | Exact normalized term or phrase count, plus a per-1,000-token rate within the mention context |
| **Formula** | Sliding-window exact match over `tokenize_lexicon_text(input_text)`; rate = `count / normalized_token_count * 1000` |
| **Null contract** | Never NULL for emitted rows. Zero-count rows are omitted. |
| **Interpretation** | Reproducible trait vocabulary signal. It is not a complete semantic classifier and should be read alongside NLI frame outputs and QA samples. |

---

## Trait Metrics (`gold.mart_trait_metrics`)

Grain: **one row per scenario x trait tier x gender x trait category**.

### Core metrics

| Metric | Formula | Interpretation |
|---|---|---|
| `mention_count` | Count of mention contexts in the segment after scenario exclusions | Denominator for coverage |
| `hit_mentions` | Distinct mentions with at least one matched trait term | Coverage numerator |
| `term_hits` | Sum of matched trait term counts | Raw vocabulary volume |
| `hits_per_1k_context_words` | `term_hits / context_word_count * 1000` | Primary normalized comparison metric |
| `coverage_rate` | `hit_mentions / mention_count` | Share of mentions containing at least one term in the category |
| `evidence_level` | `chart_ready` when `hit_mentions >= 30`; `sparse_evidence` when `10 <= hit_mentions < 30`; `table_only` when `< 10` | Dashboard guardrail against overinterpreting sparse categories |

### Scenarios

| Scenario | Definition |
|---|---|
| `all` | Full sampled cohort |
| `drop_top_overall` | Removes the leader with the largest `article_count` |
| `drop_top_each_gender` | Removes the largest-exposure male leader and largest-exposure female leader |

Companion Gold artifacts:
- `gold.mart_trait_top_terms`: ranked terms by scenario, tier, gender, and category.
- `gold.mart_trait_candidate_metrics`: candidate-level trait counts for drilldown.
- `gold.mart_trait_qa_samples`: representative mention-context excerpts for human QA.

---

## NLP Sentiment and Tone Metrics (`silver.fact_mention_nlp_summary`)

Grain: **one row per article x candidate mention**. These are Phase 2 generic
sentiment diagnostics plus Phase 3 target-aware tone audit signals. Phase 6
Gold marts consume these outputs as descriptive dashboard audit signals.

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
| **Interpretation** | Target-aware political tone audit signal. This is more appropriate than generic sentiment for candidate coverage because the hypothesis explicitly names the candidate. Gold marts aggregate it only with explicit coverage denominators and caveats. |

### `target_tone_probability`

| Field | Value |
|---|---|
| **Definition** | Probability for the selected tone label, or the top probability when the row is below the confidence threshold |
| **Formula** | Highest NLI probability across `favorable`, `unfavorable`, and `neutral`; labels below `NLP_TONE_THRESHOLD` are persisted as `unclassified` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | NULL for skipped or failed rows; populated for scoreable Phase 3 rows. |
| **Interpretation** | Confidence and threshold-sensitivity audit field. A low probability attached to `unclassified` means the model saw no confident tone, not that the mention had neutral tone. |

### `primary_frame_label`

| Field | Value |
|---|---|
| **Definition** | Highest-probability frame label accepted for the mention context |
| **Accepted values** | `politique`, `vie_privee`, `apparence`, `scandale`, `personnalite`, `securite`, `unclassified` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL. Skipped, failed, or low-confidence rows use `unclassified`. |
| **Interpretation** | Compact summary field for the selected Phase 4 frame. Use the full `silver.fact_mention_frame_score` table for threshold tuning and multi-label QA because this field stores only the selected primary frame. |

### `primary_frame_probability`

| Field | Value |
|---|---|
| **Definition** | Probability for the selected primary frame |
| **Formula** | Highest NLI probability across scorable frame labels when that probability meets the configured threshold for that frame: `NLP_FRAME_THRESHOLDS[frame_label]`, defaulting to `NLP_FRAME_THRESHOLD` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | NULL for `unclassified`, skipped, or failed rows. |
| **Interpretation** | Confidence field for the summary primary frame. A NULL value means no frame passed the configured threshold, not that the mention has no topic. |

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
| **Interpretation** | Model provenance identifier. A changed bundle version means NLP outputs were produced under different runtime metadata and should not be mixed without audit. The bundle includes per-frame thresholds, local runtime dimensions, immutable Hugging Face revisions, and hypothesis-template version. Mutable revisions such as `main`, `master`, or `latest` are rejected by the model-bundle contract. |

---

## NLP Framing Metrics (`silver.fact_mention_frame_score`)

Grain: **one row per article x candidate mention x scorable frame label**.
These are Phase 4 Silver model outputs for QA, threshold tuning, and future
Gold mart activation. The `unclassified` label is not emitted in this table; it
is only a fallback value in `silver.fact_mention_nlp_summary`.

Model methodology source: `cmarkea/distilcamembert-base-nli`, used as the
primary French NLI model for multi-label frame hypotheses:
https://huggingface.co/cmarkea/distilcamembert-base-nli

### `frame_label`

| Field | Value |
|---|---|
| **Definition** | Controlled frame label scored for the mention context |
| **Accepted values** | `politique`, `vie_privee`, `apparence`, `scandale`, `personnalite`, `securite` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Frame vocabulary member being scored. `unclassified` is intentionally excluded because it is a fallback state, not a model hypothesis. |

### `frame_probability`

| Field | Value |
|---|---|
| **Definition** | Multi-label NLI probability for `frame_label` on the mention context |
| **Formula** | Hugging Face zero-shot classification score for the exact `nli_hypothesis` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL; validated to be within `[0, 1]`. |
| **Interpretation** | Model confidence for one frame. Multiple frames may pass threshold for the same mention, so use `is_primary_frame` when a single selected frame is required. |

### `is_primary_frame`

| Field | Value |
|---|---|
| **Definition** | Boolean flag for the highest-probability frame that passed that label's configured threshold |
| **Formula** | `TRUE` for the selected primary frame, otherwise `FALSE`; at most one row per mention may be `TRUE` |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Reconciles the long frame-score table to `primary_frame_label` in the summary table. If no frame passes threshold, all rows for the mention are `FALSE`. |

### `passes_threshold`

| Field | Value |
|---|---|
| **Definition** | Whether `frame_probability >= NLP_FRAME_THRESHOLDS[frame_label]`, falling back to `NLP_FRAME_THRESHOLD` when no per-label override is configured |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Threshold audit flag. It should not be treated as a mutually exclusive classification because the framing scorer runs in multi-label mode. |

### `nli_hypothesis`

| Field | Value |
|---|---|
| **Definition** | Exact French hypothesis string sent to the NLI model for this frame |
| **Owner** | `src/nlp/nli.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Model-input lineage field. Persisting the hypothesis makes frame scores auditable when hypothesis templates or frame definitions change. |

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

## Unified NLP QA Report (`data/gold/nlp_qa_report.json`)

Grain: **one JSON report per Phase 5 QA run**.

This is a model-governance artifact, not a dashboard metric. It summarizes
coverage, failures, threshold sensitivity, and model provenance across Phase
0-4 NLP outputs for Gold NLP mart and dashboard interpretation.

In Phase 5, `scoreable` means a mention has persisted model output for the
audited task. It is distinct from the Phase 0 `eligible_for_inference` gate,
which counts rows allowed to call Transformer models.

### `input_coverage.total_mentions`

| Field | Value |
|---|---|
| **Definition** | Number of rows in `silver.fact_mention_nlp_input` |
| **Owner** | `src/nlp/qa.py` |
| **Null contract** | Never NULL. |
| **Interpretation** | Denominator for all mention-level NLP QA counters. |

### `input_coverage.eligible_for_inference_mentions`

| Field | Value |
|---|---|
| **Definition** | Number of Phase 0 input rows eligible for Transformer inference |
| **Formula** | `COUNT(*) WHERE eligible_for_inference = TRUE` |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Measures how much of the mention corpus can support sentiment, tone, and framing model outputs. |

### `output_coverage.tone.classified_share_of_scoreable`

| Field | Value |
|---|---|
| **Definition** | Share of scoreable tone rows with a non-`unclassified` persisted label |
| **Formula** | `classified_mentions / scoreable_mentions` from `silver.fact_mention_nlp_summary` |
| **Owner** | `src/nlp/qa.py` |
| **Null contract** | NULL only when no rows have `target_tone_probability`. |
| **Interpretation** | Operational model-coverage signal, not model confidence. Low coverage means many scoreable mention contexts do not exceed the configured probability threshold. |

### `output_coverage.framing.primary_frame_share_of_frame_scored`

| Field | Value |
|---|---|
| **Definition** | Share of frame-scored mentions with a selected primary frame |
| **Formula** | `mentions_with_primary_frame / frame_scored_mentions` |
| **Owner** | `src/nlp/qa.py` |
| **Null contract** | NULL only when no frame-score rows exist. |
| **Interpretation** | Frame acceptance coverage at the current per-frame threshold policy. This is QA, not a bias conclusion. |

### `threshold_sensitivity`

| Field | Value |
|---|---|
| **Definition** | Tone and framing coverage recomputed over the Phase 5 threshold grid `(0.40, 0.50, 0.60, 0.70, 0.80)` |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Shows how sensitive classified coverage is to threshold choice without rerunning models. Frame sensitivity uses each mention's maximum persisted frame probability. |

### `backup_model_agreement.status`

| Field | Value |
|---|---|
| **Definition** | Whether precomputed backup-model outputs were supplied for agreement comparison |
| **Accepted values** | `not_available`, `available` |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | `not_available` is the default for Phase 5 Core QA. The pipeline does not run backup inference; it only compares precomputed backup summaries when explicitly provided. |

### `backup_model_agreement.backup_scored_mentions`

| Field | Value |
|---|---|
| **Definition** | Number of mention contexts actually scored by the backup NLI model |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Governance sample size for backup-model review. This is the count to read before agreement rates. |

### `backup_model_agreement.backup_summary_joined_mentions`

| Field | Value |
|---|---|
| **Definition** | Number of rows joining the full primary summary and backup-shaped summary artifacts |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Lineage diagnostic only. It is not the denominator for agreement rates; use `tone_compared_mentions` and `frame_compared_mentions` for the actual comparison denominators. |

### `hypothesis_examples`

| Field | Value |
|---|---|
| **Definition** | Exact example NLI hypothesis strings for target-aware tone and every controlled frame label |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Governance field for model-input auditability. Reviewers can inspect what the NLI model was asked to judge without opening Silver frame-score rows. |

### `backup_model_agreement.tone_cohens_kappa`

| Field | Value |
|---|---|
| **Definition** | Cohen's kappa between the primary NLI model and the backup NLI model on the deterministic backup sample for target-aware tone |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Agreement diagnostic only. The backup model is not ground truth; low kappa signals model-family sensitivity that requires review before publication. |

### `backup_model_agreement.frame_cohens_kappa`

| Field | Value |
|---|---|
| **Definition** | Cohen's kappa between the primary NLI model and the backup NLI model on the deterministic backup sample for primary frame labels |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Agreement diagnostic only. It should be read with agreement rates and the frame-specific hypothesis examples. |

### `blessed_bundle_comparison`

| Field | Value |
|---|---|
| **Definition** | Optional comparison between the observed NLP bundle and `BLESSED_NLP_MODEL_BUNDLE_VERSION` |
| **Owner** | `src/nlp/qa.py` |
| **Interpretation** | Deployment governance field. When a blessed bundle is configured and does not match the observed run bundle, dashboards and CLI artifact checks surface a warning. |

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
| Silver (NLP framing) | `make run-nlp-framing-pipeline` is run | Requires current NLP summary rows and optional Transformer dependencies |
| Gold NLP tone sensitivity QA | `make run-nlp-tone-sensitivity-pipeline` is run | Must read a tone-enriched Phase 3 summary; does not load Transformer models |
| Gold unified NLP QA | `make run-nlp-qa-pipeline` is run | Must read Phase 0-4 NLP artifacts; does not load Transformer models |
| Gold dbt marts | Embedded in `make run-news-corpus-pipeline` via `dbt run`; rerun after NLP pipelines for Phase 6 activation | Must pass dbt schema tests before the pipeline exits |
| Gold regression | Embedded in news corpus pipeline | Recomputed on each run; `status` column records whether fit succeeded |
| Dashboard | On Streamlit startup — reads Gold Parquet files | Reflects the last complete pipeline run |

There is no streaming or incremental refresh. This is a batch analytical pipeline
over a closed historical window. "Freshness" means the last full pipeline run
completed successfully and `meta.meta_run.status = 'success'`.
