# Limitations - Election Gender Bias D4W

> Last updated: 2026-05-27

This document records interpretation limits that must remain visible when the
dashboard or README is used for portfolio review.

## Scope

- The 36-candidate cohort uses stratified sampling with gender quota, not
  1:1 statistical matching or a national census of all municipal-election
  candidates.
- News coverage comes from Europresse exports during `2025-11-01` to
  `2026-04-30`; pure-digital outlets, broadcast transcripts, and social media
  are out of scope.
- Persisted artifacts use mention contexts and derived metrics, not full
  article bodies. QA snippets are hidden by default in the dashboard unless
  `SHOW_QA_SAMPLES=true`.

## Regression

- The raw exposure gap is dominated by one large-city incumbent with roughly
  1.3k articles.
- The Poisson model is overdispersed in current governed runs
  (`dispersion_ratio` well above the threshold of 5) and is treated as
  diagnostic only.
- The Negative Binomial model and bootstrap confidence interval do not detect
  an adjusted gender effect at n = 36; the study remains underpowered for
  moderate effects.
- All regression outputs are observational associations, not causal estimates.

## NLP Review Status

- Native French review is pending.
- Trait lexicon matches are deterministic exact matches; they do not adjudicate
  sarcasm, negation, quotation, or whether the term describes the candidate
  rather than a surrounding event.
- The `core` trait tier is intended for dashboard and README interpretation.
  The `exploratory` tier is a discovery aid and requires context review before
  strong claims.
- The NLI tone and frame models are descriptive audit signals. Calibration on
  French municipal-election coverage has not been independently validated.
- The NLI models were trained outside this exact municipal-election setting and
  may encode gendered associations of their own. Findings about media bias
  should be cross-checked against alternative model families before
  publication.
- Tone and frame scoring see mention contexts, not full article narrative arcs.
  Negative paragraphs that refer to a candidate through pronouns or role
  references may be missed when the candidate name is absent from that context
  window.
- `mean_unfavorable_tone_share = 0.0` means no mention crossed the configured
  0.60 threshold for unfavorable tone in this run; it does not prove negative
  coverage is absent. Lower-confidence unfavorable top labels below 0.60 are
  persisted as `unclassified` and should be reviewed through tone-threshold
  sensitivity diagnostics.

## Planned Audit

- Review 50 representative contexts per `trait_category x tier`.
- Record reviewer role, review date, agreement notes, and any lexicon changes
  in this document before using the NLP layer for stronger conclusions.

## Deployment

- The Streamlit app is designed for local and containerized review.
- Production exposure requires an external access-control layer such as a
  reverse proxy with OIDC; no in-app authentication is implemented.
- Future production deployments should store artifact bundles in S3 or GCS and
  mount versioned Gold outputs into the dashboard container.
