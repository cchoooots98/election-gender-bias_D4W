# Runbook — Election Gender Bias D4W

Operational reference for running, monitoring, and recovering the pipeline.
Audience: anyone with repository access who needs to reproduce or debug a run.

---

## Contents

1. [Pipeline Inventory](#1-pipeline-inventory)
2. [Prerequisites and Environment](#2-prerequisites-and-environment)
3. [Running the Pipeline](#3-running-the-pipeline)
4. [Freshness Checks](#4-freshness-checks)
5. [Failure Triage](#5-failure-triage)
6. [Rollback Procedures](#6-rollback-procedures)
7. [Data Quality Monitoring](#7-data-quality-monitoring)
8. [Known Limitations](#8-known-limitations)

---

## 1. Pipeline Inventory

The repository implements six operational entry points. The first five are
sequentially dependent; dbt build is also embedded in the news corpus pipeline.

| Pipeline | Entry point | Produces | Typical runtime |
|---|---|---|---|
| **Sampling pipeline** | `make run-sampling-pipeline` | Bronze official data → Silver dims/facts → `gold.candidate_universe` → `gold.sample_leaders` + `sample_manifest.json` | ~2 min (download + DuckDB writes) |
| **News corpus pipeline** | `make run-news-corpus-pipeline` | `news_source_record` bronze → Silver article/mention tables → dbt Gold marts → Python regression diagnostics → `news_corpus_qa_report.json` | ~5–30 min depending on corpus size and web-scrape flag |
| **NLP input pipeline** | `make run-nlp-input-pipeline` | Materializes `silver.fact_mention_nlp_input` from existing `fact_mention` and `fact_article` Silver outputs | <30 sec |
| **NLP lexicon pipeline** | `make run-nlp-lexicon-pipeline` | Materializes `silver.fact_stereotype_word_counts` from Phase 0 NLP input rows and the packaged versioned lexicon | <30 sec |
| **NLP sentiment pipeline** | `make run-nlp-sentiment-pipeline` | Materializes `silver.fact_mention_nlp_summary` from Phase 0 NLP input rows and the optional Transformer sentiment model | Depends on CPU/GPU and model cache |
| **dbt build** (embedded in news corpus) | `make dbt-build` | Refreshes all five Gold dbt mart tables and runs 37 schema tests | ~30 sec |

**Dependency contract**: the news corpus pipeline reads `gold.sample_leaders` as its candidate scope.
The NLP input pipeline reads the Silver article and mention tables produced by the news corpus pipeline.
The NLP lexicon pipeline reads the Phase 0 NLP input table.
The NLP sentiment pipeline also reads the Phase 0 NLP input table and requires
the optional future NLP dependency set.
Always run sampling, then news corpus, then NLP input, then NLP lexicon and
sentiment.

---

## 2. Prerequisites and Environment

### Python environment

```bash
# Create and activate virtual environment (first time only)
python -m venv .venv
source .venv/bin/activate          # macOS / Linux
.\.venv\Scripts\activate            # Windows PowerShell

# Install pinned dependencies
pip install -r requirements.txt
pip install -e . --no-build-isolation
```

### Environment configuration

```bash
cp .env.example .env
# Edit .env — key values:
# ANALYSIS_START_DATE=2025-11-01
# ANALYSIS_END_DATE=2026-04-30
# CANDIDATE_SAMPLE_SIZE=36
# SAMPLING_RANDOM_SEED=42
```

Changing `SAMPLING_RANDOM_SEED` changes the 36-candidate cohort selection.
Document any seed change in the commit message.

### Europresse corpus manifest

The news corpus pipeline reads a manifest file that lists local Europresse
export paths. Before running:

```bash
# Check the manifest exists and paths are valid
python -c "
import json, pathlib
manifest = json.load(open('data/raw/news_import_manifest.json'))
for entry in manifest['imports']:
    p = pathlib.Path(entry['file_path'])
    print(p, 'OK' if p.exists() else 'MISSING')
"
```

If paths are missing, re-export from Europresse and update `data/raw/news_import_manifest.json`.

---

## 3. Running the Pipeline

### Full end-to-end run

```bash
# Step 1: ingest official data, build cohort
make run-sampling-pipeline

# Step 2 (option A): corpus pipeline without new web scraping
make run-news-corpus-pipeline

# Step 2 (option B): corpus pipeline with new web scraping enabled
python -m src.cli.run_news_corpus_pipeline --enable-web-scrape

# Step 3 (optional): build the Phase 0 NLP input contract
make run-nlp-input-pipeline

# Step 4 (optional): build the Phase 1 deterministic lexicon audit
make run-nlp-lexicon-pipeline

# Step 5 (optional): build the Phase 2 generic sentiment baseline
pip install -r requirements-future.in
make run-nlp-sentiment-pipeline
```

### Verification after each step

After the sampling pipeline:
```bash
python -c "
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
row_count = conn.execute('SELECT COUNT(*) FROM gold.sample_leaders').fetchone()[0]
gender = conn.execute(
    'SELECT gender, COUNT(*) FROM gold.sample_leaders GROUP BY gender'
).fetchall()
print(f'sample_leaders rows: {row_count}')
print(f'gender split: {gender}')
conn.close()
"
# Expected: row_count = 36, gender = [('F', 18), ('M', 18)] in some order
```

After the news corpus pipeline:
```bash
python -c "
import json
report = json.load(open('data/gold/news_corpus_qa_report.json'))
print('QA report:', json.dumps(report, indent=2))
"
```

After the NLP input pipeline:
```bash
python -c "
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
summary = conn.execute(\"\"\"
    SELECT
        COUNT(*) AS rows,
        SUM(CASE WHEN eligible_for_lexicon THEN 1 ELSE 0 END) AS lexicon_rows,
        SUM(CASE WHEN eligible_for_inference THEN 1 ELSE 0 END) AS inference_rows
    FROM silver.fact_mention_nlp_input
\"\"\").fetchone()
print(f'nlp_input rows: {summary[0]}, lexicon: {summary[1]}, inference: {summary[2]}')
conn.close()
"
```

After the NLP lexicon pipeline:
```bash
python -c "
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
summary = conn.execute(\"\"\"
    SELECT
        COUNT(*) AS rows,
        COUNT(DISTINCT mention_id) AS mentions_with_terms,
        COUNT(DISTINCT lexicon_category) AS categories_with_terms,
        SUM(count) AS total_term_count
    FROM silver.fact_stereotype_word_counts
\"\"\").fetchone()
print(
    f'stereotype rows: {summary[0]}, mentions: {summary[1]}, '
    f'categories: {summary[2]}, total terms: {summary[3]}'
)
conn.close()
"
```

After the NLP sentiment pipeline:
```bash
python -c "
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
summary = conn.execute(\"\"\"
    SELECT
        COUNT(*) AS rows,
        SUM(CASE WHEN nlp_enrichment_status = 'scored' THEN 1 ELSE 0 END) AS scored,
        SUM(CASE WHEN nlp_enrichment_status = 'skipped' THEN 1 ELSE 0 END) AS skipped,
        SUM(CASE WHEN nlp_enrichment_status = 'failed' THEN 1 ELSE 0 END) AS failed
    FROM silver.fact_mention_nlp_summary
\"\"\").fetchone()
print(
    f'nlp_summary rows: {summary[0]}, scored: {summary[1]}, '
    f'skipped: {summary[2]}, failed: {summary[3]}'
)
conn.close()
"
```

After dbt build:
```bash
make dbt-build
# Expect: 5 models pass, 37 tests pass, 0 errors
```

NLP dbt tests are planned when `fact_mention_frame_score` and NLP summary
fields feed Gold marts. Phase 0, Phase 1, and Phase 2 are covered by Python
contract tests because they do not yet feed dbt marts.

---

## 4. Freshness Checks

### Check when official data was last ingested

```python
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
snapshots = conn.execute("""
    SELECT source_key, source_hash[:8] AS hash_prefix,
           row_count, fetched_at
    FROM meta.meta_source_snapshot
    ORDER BY fetched_at DESC
    LIMIT 20
""").fetchdf()
print(snapshots.to_string(index=False))
conn.close()
```

Expected sources (one snapshot per ingest run each):
`candidates_tour1`, `candidates_tour2`, `results_tour1`, `results_tour2`,
`seats_population`, `rne_incumbents`, `insee_cog_communes`.

### Detect if a source file changed between runs

```python
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
changes = conn.execute("""
    SELECT source_key,
           COUNT(DISTINCT source_hash) AS distinct_versions,
           MIN(fetched_at)             AS first_seen,
           MAX(fetched_at)             AS last_seen
    FROM meta.meta_source_snapshot
    GROUP BY source_key
    HAVING COUNT(DISTINCT source_hash) > 1
""").fetchdf()
print("Sources with hash changes (re-published by ministry):")
print(changes.to_string(index=False))
conn.close()
```

A `distinct_versions > 1` means the ministry updated that file after initial
publication. Re-run the sampling pipeline if `candidates_tour1` or
`results_tour1` changed, as the cohort and metrics depend on those.

### Check pipeline run history

```python
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
runs = conn.execute("""
    SELECT flow_name, status, rows_ingested, error_count,
           start_ts, end_ts
    FROM meta.meta_run
    ORDER BY start_ts DESC
    LIMIT 10
""").fetchdf()
print(runs.to_string(index=False))
conn.close()
```

A run with `status = 'failed'` or `error_count > 0` should be investigated
before trusting downstream marts.

---

## 5. Failure Triage

### Triage checklist

1. **Read the log output** — every step uses `logging.INFO` / `logging.WARNING`.
   The module path in the log line (`src.ingest.candidates`, `src.transform.dim_commune`, etc.)
   identifies the exact failing stage.

2. **Check `meta.meta_run`** for the last run's `status` and `error_count`.

3. **Check rejection tables** under `data/silver/_rejected/`:
   - `dim_commune_rejected.parquet` — communes that failed geography validation
   - `dim_candidate_leader_rejected.parquet` — candidates that failed DQ checks
   - `fact_election_result_rejected.parquet` — result rows that couldn't be joined
   - `fact_article_source_rejected.parquet` — article records that failed parsing DQ
   - `news_import_unsupported.parquet` — Europresse files with unrecognised format

4. **Inspect rejection reasons**:
   ```python
   import pandas as pd
   rejected = pd.read_parquet('data/silver/_rejected/dim_candidate_leader_rejected.parquet')
   print(rejected['_rejection_reason'].value_counts())
   ```

---

### Common failure scenarios

#### A. `gold.sample_leaders` has wrong row count

**Symptom**: row count ≠ 36 or gender split ≠ 18F/18M.

**Triage**:
```python
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
viable = conn.execute(
    "SELECT city_size_bucket, gender, COUNT(*) FROM gold.candidate_universe "
    "WHERE is_viable = TRUE GROUP BY 1, 2"
).fetchdf()
print(viable)
conn.close()
```
If any stratum has fewer viable candidates than the quota (`large=3F/3M,
medium=6F/6M, small=9F/9M`), the sampling constraints cannot be satisfied
and the pipeline raises `SamplingError`.

**Fix**: Check if `SAMPLE_MIN_VOTE_SHARE_PCT_TOUR1` or city-size thresholds
in `.env` are too restrictive for the current results data. Check also whether
results files loaded correctly (non-zero row counts in `meta_source_snapshot`).

---

#### B. Bronze ingest fails with HTTP error

**Symptom**: `requests.HTTPError` or `requests.Timeout` in log for a
`data.gouv.fr` or `insee.fr` URL.

**Triage**: The URLs in `src/config/settings.py → DATA_SOURCES` may have
changed. Check the official ministry portal for updated resource IDs.

**Fix**:
1. Find the new URL from `data.gouv.fr`.
2. Update `DATA_SOURCES[source_key]['url']` in `src/config/settings.py`.
3. Re-run the sampling pipeline. The new hash will appear in `meta_source_snapshot`.

---

#### C. dbt tests fail after a re-run

**Symptom**: `make dbt-build` exits non-zero with test failures.

**Triage**: Run `make dbt-build` and read the failing test names.
Most schema tests (`not_null_*`, `unique_*`, `accepted_values_*`) point to
a specific column in a specific mart.

```bash
# Run only tests, not models, for faster iteration
.venv/bin/dbt test --profiles-dir dbt --project-dir dbt
```

The most common cause is that `gold.sample_leaders` was rebuilt with different
membership (e.g. seed change), which changes cohort-level row counts in
`mart_exposure_metrics`. The test `row_count_equals_mart_exposure_*` asserts
exactly 36 rows.

**Fix**: If the cohort changed intentionally, update the expected row count
in `dbt/models/marts/news/schema.yml`. If the change was unintentional,
restore the previous `.env` seed and re-run the sampling pipeline.

---

#### D. News corpus pipeline produces zero candidate mentions

**Symptom**: `fact_mention` is empty; `news_corpus_qa_report.json` shows
`candidate_coverage = 0`.

**Triage**:
1. Check that `gold.sample_leaders` is populated (step A above).
2. Confirm `news_import_manifest.json` points to valid Europresse export files.
3. Check `data/silver/_rejected/fact_article_source_rejected.parquet` for
   high rejection rates.
4. Inspect the QA report's `parser_mix` key — if all records show `unsupported`,
   the Europresse export format may have changed.

---

#### E. Regression pipeline produces `fit_failed` status

**Symptom**: `mart_regression_results` rows have `status = 'fit_failed:*'`.

**Triage**: This is expected when `article_count` is all-zero (zero-corpus run
before Europresse import). Check `mart_exposure_metrics` article count sums:

```python
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')
totals = conn.execute(
    "SELECT SUM(article_count), SUM(full_text_article_count) "
    "FROM gold.mart_exposure_metrics"
).fetchone()
print(f"Total articles: {totals[0]}, with full text: {totals[1]}")
conn.close()
```

If zero, the news corpus pipeline has not been run or completed successfully.

---

## 6. Rollback Procedures

### Philosophy

Bronze is the recovery baseline. Every pipeline step is idempotent:
Silver and Gold use delete-then-insert, so a re-run always replaces the
previous output with the current inputs. There is no in-place mutation.

### Rollback to previous Bronze state

Bronze Parquet files are full-file overwrites. The previous state is not
retained in the repository (`data/` is git-ignored). If a previous version
of a government source file is needed, retrieve it from the ministry's
versioned download history or from the Wayback Machine.

**Practical rollback**: if a re-ingest produced a worse result, restore the
previous `.env` configuration, then re-run the pipeline from scratch. The
deterministic sampling seed ensures the 36-candidate cohort is reproducible.

### Rollback dbt Gold marts

All dbt models are fully refreshed on `make dbt-build`. To roll back to a
previous mart state:

1. Reset `gold.sample_leaders` to the desired cohort (re-run sampling pipeline
   with the previous seed).
2. Run `make dbt-build` — all five marts rebuild from the restored cohort.

### Rollback the cohort snapshot

`gold.sample_leaders` is a materialized snapshot, not a view. Changing the
random seed and re-running the sampling pipeline fully replaces it.
The `sample_manifest.json` in `data/gold/` records the exact seed, rule
version, and per-candidate selection metadata for every run — this is the
audit trail.

```bash
# Re-run with original seed to reproduce the published cohort
SAMPLING_RANDOM_SEED=42 make run-sampling-pipeline
```

### Clearing the warehouse for a full clean rebuild

```bash
# Delete the DuckDB file and all derived Parquet outputs
rm warehouse/municipal.duckdb
rm -rf data/bronze data/silver data/gold

# Then run both pipelines in order
make run-sampling-pipeline
make run-news-corpus-pipeline
```

The web-scrape cache in `data/bronze/news_web_fetch/` can optionally be
preserved to avoid re-fetching URLs that were already successfully scraped.

---

## 7. Data Quality Monitoring

### Automated DQ thresholds (set in `.env`)

| Threshold | Key | Default | Effect if exceeded |
|---|---|---|---|
| Minimum article text length | `DQ_MIN_ARTICLE_TEXT_LENGTH` | 100 chars | Row quarantined to `fact_article_source_rejected.parquet` |
| Maximum null rate in key columns | `DQ_MAX_NULL_RATE` | 5% | Silver write fails with `DataQualityError` |

### Manual DQ checks after a run

```python
import duckdb
conn = duckdb.connect('warehouse/municipal.duckdb')

# 1. Confirm cohort completeness
coverage = conn.execute("""
    SELECT
        COUNT(DISTINCT m.leader_id) AS leaders_with_mentions,
        COUNT(*) AS total_leaders
    FROM gold.sample_leaders AS s
    LEFT JOIN silver.fact_mention AS m ON s.leader_id = m.leader_id
""").fetchone()
print(f"Leaders with ≥1 mention: {coverage[0]} / {coverage[1]}")

# 2. Check exposure mart
exposure = conn.execute("""
    SELECT gender,
           ROUND(AVG(article_count), 1)              AS avg_articles,
           ROUND(AVG(exposure_per_10k_population), 1) AS avg_per_10k,
           COUNT(*)                                   AS n
    FROM gold.mart_exposure_metrics
    GROUP BY gender
""").fetchdf()
print(exposure)

# 3. Check regression result status
reg_status = conn.execute("""
    SELECT model_name, status, COUNT(*) AS coefficient_rows
    FROM gold.mart_regression_results
    GROUP BY model_name, status
""").fetchdf()
print(reg_status)

conn.close()
```

### Rejection rate monitoring

A sudden spike in rejection rates often indicates a source-file format change
(the ministry sometimes changes column names between election cycles). Compare
rejection counts with `accepted_record_count` in `meta.meta_news_import_batch`.

---

## 8. Known Limitations

| Limitation | Operational impact |
|---|---|
| **Single-node DuckDB** | No concurrent writers. Running two pipeline instances simultaneously will corrupt the warehouse file. Serialize all runs. |
| **No incremental news ingest** | The news corpus pipeline rebuilds all Silver/Gold tables from the full Europresse manifest on each run. Adding new exports re-processes the full history. |
| **Web-scrape cache is local-only** | `data/bronze/news_web_fetch/` is git-ignored. A fresh clone has no cache; add `--enable-web-scrape` to rebuild it, which requires network access to news sites. |
| **PLM cities excluded** | Paris, Lyon, Marseille arrondissement candidates are excluded from the analytical cohort. This is documented as a known scope limitation in README. |
| **Target-aware tone and framing not implemented** | Phase 0 materializes `silver.fact_mention_nlp_input`, Phase 1 materializes deterministic stereotype lexicon counts, and Phase 2 materializes generic sentiment baseline outputs. Candidate-aware NLI tone, framing scores, and Gold NLP marts remain planned. `mart_framing_metrics` contains only `unclassified` rows until frame-score Silver outputs and dbt tests are added. |
| **Seed lexicon is sparse** | The Phase 1 lexicon is a minimal structural-validation seed. Expand and review the vocabulary before interpreting lexicon rates as statistical media-bias evidence. |
