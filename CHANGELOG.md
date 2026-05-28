# Changelog

All notable governance, cohort, and model-bundle changes are recorded here.

## 2026-05-28

- Added enterprise NLP governance controls: per-frame thresholds, backup-model
  agreement sampling, hypothesis examples, blessed-bundle comparison, and a
  project-side NLI model card.
- Refactored the primary exposure regression to a low-dimensional Negative
  Binomial model with a population offset; moved high-dimensional controls to
  sensitivity output and added placebo-label checks.
- Updated dashboard review flow to use continuous Q numbering, concise lede
  text, formatted metadata tables, hidden QA snippets by default, and a
  headline comparison between volume-weighted and leader-mean scandal framing.
- Clarified the cohort method as stratified sampling with gender quota rather
  than statistical matching.

## Governance Fields To Record Per Release

| Field | Value |
|---|---|
| Cohort rule version | Record from `sample_manifest.json` |
| Sample run_id | Record from `sample_manifest.json` |
| NLP model bundle version | Record from `nlp_qa_report.json` |
| Blessed NLP model bundle version | Record from deployment configuration |
| Dashboard artifact run | Record from the blessed Gold artifact path |
