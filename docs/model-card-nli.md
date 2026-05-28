# Project Model Card - NLI Tone and Framing

> Last updated: 2026-05-27

This project-side model card documents how Natural Language Inference (NLI)
models are used inside the municipal-election media audit. It complements the
upstream Hugging Face model cards and focuses on this repository's application
domain, controls, and limitations.

## Intended Use

- Score French municipal-election mention contexts for candidate-aware tone:
  `favorable`, `unfavorable`, `neutral`, or `unclassified`.
- Score the same mention contexts against controlled frame labels:
  `politique`, `scandale`, `personnalite`, `apparence`, `vie_privee`, and
  `securite`.
- Produce descriptive audit signals for aggregate dashboard review, not
  individual candidate judgments.

## Out-of-Scope Use

- Causal claims about gender bias in media coverage.
- Automated moderation, legal, employment, or reputational decisions about
  candidates or journalists.
- Individual-level scoring outside the project cohort and analysis window.
- Publication without representative French-language review or model-family
  sensitivity checks.

## Model Bundle

The primary NLI model and backup NLI model are pinned by immutable Hugging Face
commit revisions in `src/config/settings.py`. The project computes a
deterministic `nlp_model_bundle_version` from model names, revisions,
thresholds, runtime dimensions, and hypothesis-template version.

Optional deployment controls:

- `NLP_MODEL_CACHE_DIR`: local model cache path for air-gapped or mirrored
  deployments.
- `BLESSED_NLP_MODEL_BUNDLE_VERSION`: expected production bundle hash for
  dashboard and QA comparison.
- `NLP_FRAME_THRESHOLDS`: JSON map of per-frame thresholds. Defaults keep all
  frames at `0.60` until labeled calibration evidence supports changes.

## Hypothesis Policy

Tone uses candidate-aware hypotheses built from the sampled leader name. Frame
scoring uses one auditable French hypothesis per controlled frame label. The
exact hypothesis examples are written to `data/gold/nlp_qa_report.json` and
displayed in the dashboard under "Model Bundle and Hypotheses".

## Threshold Policy

The current default threshold is `0.60` for tone and all frame labels. This is a
governance default, not a validated optimum. Threshold changes require a
labeled French municipal-election review set and must update the model bundle
version. Per-frame threshold support exists to allow future calibration without
changing the scoring schema.

## Validation Status

Target-domain French review is pending. Current automated controls include:

- Input eligibility and failure coverage in `nlp_qa_report.json`.
- Zero-unfavorable and low-coverage sanity warnings.
- Threshold sensitivity over the configured QA grid.
- Optional backup-model agreement on a deterministic 100-mention sample,
  including agreement rates and Cohen's kappa.

The backup model is a governance diagnostic, not ground truth.

## Known Limitations

- Mention-context scoring can miss article-level negative paragraphs that refer
  to a candidate only through pronouns, office titles, or role references.
- The primary and backup models were not trained specifically on French
  municipal-election reporting.
- The models may encode gendered associations from their own training data.
  Project findings about media bias must therefore be cross-checked against
  alternative model families before publication.
- `unclassified` means no label crossed the configured threshold. It does not
  prove the absence of tone or frame content.
- `mean_unfavorable_tone_share = 0.0` is a calibration warning condition, not
  evidence that negative coverage is absent.

## Data Minimization

Persisted public artifacts keep derived features, hashes, lengths, model
scores, and limited review snippets rather than full article bodies. Dashboard
QA context excerpts are hidden by default and require `SHOW_QA_SAMPLES=true` in
a controlled local environment.
