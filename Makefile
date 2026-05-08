# Makefile - common development commands for election-gender-bias_D4W.
#
# Usage: make <target>   (for example: make lint, make test)
# Run  : make            to see available targets (default: help)
#
# Every target is pinned to the project virtual environment so local PATH
# differences cannot silently swap in the wrong interpreter or tool version.

ifeq ($(OS),Windows_NT)
VENV_BIN := .venv/Scripts
PYTHON := $(VENV_BIN)/python.exe
DBT_EXE := $(VENV_BIN)/dbt.exe
else
VENV_BIN := .venv/bin
PYTHON := $(VENV_BIN)/python
DBT_EXE := $(VENV_BIN)/dbt
endif

PIP := $(PYTHON) -m pip
PIP_COMPILE := $(PYTHON) -m piptools compile
PYTEST := $(PYTHON) -m pytest
RUFF := $(PYTHON) -m ruff
BLACK := $(PYTHON) -m black
JUPYTER := $(PYTHON) -m jupyterlab
STREAMLIT := $(PYTHON) -m streamlit
DBT := $(DBT_EXE)

.PHONY: help lint format format-check test test-coverage install compile \
        notebook dashboard dbt-run dbt-test dbt-build run-sampling-pipeline \
        run-news-corpus-pipeline run-nlp-input-pipeline \
        run-nlp-lexicon-pipeline run-nlp-sentiment-pipeline \
        run-nlp-tone-pipeline run-nlp-tone-sensitivity-pipeline \
        run-news-corpus-sa48 run-news-corpus-sa-relaxed generate-manifest

help:
	@echo ""
	@echo "  Election Gender Bias D4W - available make targets"
	@echo ""
	@echo "  Virtual environment"
	@echo "    all targets use $(PYTHON)"
	@echo ""
	@echo "  Code quality"
	@echo "    lint           ruff check src/ tests/ scripts/"
	@echo "    format         black src/ tests/ scripts/ notebooks/"
	@echo "    format-check   black --check (CI mode, no writes)"
	@echo ""
	@echo "  Tests"
	@echo "    test           pytest tests/ -v --tb=short"
	@echo "    test-coverage  pytest with coverage report"
	@echo ""
	@echo "  Dependencies"
	@echo "    install        pip install deps + editable package CLI"
	@echo "    compile        pip-compile requirements.in -o requirements.txt"
	@echo ""
	@echo "  Development"
	@echo "    notebook       jupyter lab"
	@echo "    dashboard      streamlit run src/dashboard/app.py"
	@echo "    dbt-run        dbt run --select marts.news"
	@echo "    dbt-test       dbt test --select marts.news"
	@echo "    dbt-build      dbt build --select marts.news"
	@echo "    run-sampling-pipeline      python -m src.cli.run_sampling_pipeline"
	@echo "    run-news-corpus-pipeline   primary cohort (cohort_36, default sample_leaders)"
	@echo "    run-nlp-input-pipeline     materialize silver.fact_mention_nlp_input"
	@echo "    run-nlp-lexicon-pipeline   materialize silver.fact_stereotype_word_counts"
	@echo "    run-nlp-sentiment-pipeline materialize silver.fact_mention_nlp_summary"
	@echo "    run-nlp-tone-pipeline      enrich fact_mention_nlp_summary with target-aware tone"
	@echo "    run-nlp-tone-sensitivity-pipeline audit tone coverage across thresholds"
	@echo "    run-news-corpus-sa48       sensitivity analysis: expanded cohort (48 candidates)"
	@echo "    run-news-corpus-sa-relaxed sensitivity analysis: relaxed sampling constraints"
	@echo "    generate-manifest          scan cohort dir, write news_import_manifest.json"
	@echo "      e.g.: make generate-manifest COHORT_DIR=data/raw/news/cohort_36 COHORT_ID=cohort36 OPERATOR=yyfen WINDOW_START=2025-11-01 WINDOW_END=2026-04-30 NOTES=..."
	@echo ""

lint:
	$(RUFF) check src/ tests/ scripts/

format:
	$(BLACK) src/ tests/ scripts/ notebooks/

format-check:
	$(BLACK) --check src/ tests/ scripts/

test:
	$(PYTEST) tests/ -v --tb=short

test-coverage:
	$(PYTEST) tests/ --cov=src --cov-report=term-missing

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e . --no-build-isolation

compile:
	$(PIP_COMPILE) requirements.in -o requirements.txt --strip-extras

notebook:
	$(JUPYTER) lab

dashboard:
	$(STREAMLIT) run src/dashboard/app.py

dbt-run:
	$(DBT) run --project-dir dbt --profiles-dir dbt --select marts.news

dbt-test:
	$(DBT) test --project-dir dbt --profiles-dir dbt --select marts.news

dbt-build:
	$(DBT) build --project-dir dbt --profiles-dir dbt --select marts.news

run-sampling-pipeline:
	$(PYTHON) -m src.cli.run_sampling_pipeline

run-news-corpus-pipeline:
	$(PYTHON) -m src.cli.run_news_corpus_pipeline

run-nlp-input-pipeline:
	$(PYTHON) -m src.cli.run_nlp_input_pipeline

run-nlp-lexicon-pipeline:
	$(PYTHON) -m src.cli.run_nlp_lexicon_pipeline

run-nlp-sentiment-pipeline:
	$(PYTHON) -m src.cli.run_nlp_sentiment_pipeline

run-nlp-tone-pipeline:
	$(PYTHON) -m src.cli.run_nlp_tone_pipeline

run-nlp-tone-sensitivity-pipeline:
	$(PYTHON) -m src.cli.run_nlp_tone_sensitivity_pipeline

# Sensitivity analysis: expanded cohort (48 candidates, 24F + 24M).
# Requires: data/raw/news/cohort_sa_48/news_import_manifest.json
#           data/gold/sample_leaders_sa48.parquet
run-news-corpus-sa48:
	$(PYTHON) -m src.cli.run_news_corpus_pipeline \
		--manifest-path data/raw/news/cohort_sa_48/news_import_manifest.json \
		--sample-leaders-path data/gold/sample_leaders_sa48.parquet

# Sensitivity analysis: relaxed sampling constraints.
# Requires: data/raw/news/cohort_sa_relaxed/news_import_manifest.json
#           data/gold/sample_leaders_sa_relaxed.parquet
run-news-corpus-sa-relaxed:
	$(PYTHON) -m src.cli.run_news_corpus_pipeline \
		--manifest-path data/raw/news/cohort_sa_relaxed/news_import_manifest.json \
		--sample-leaders-path data/gold/sample_leaders_sa_relaxed.parquet

# Generate news_import_manifest.json by scanning a cohort directory for PDFs.
# Required variables: COHORT_DIR, COHORT_ID, OPERATOR, WINDOW_START, WINDOW_END
# Optional variable:  NOTES (default empty)
# Example:
#   make generate-manifest \
#     COHORT_DIR=data/raw/news/cohort_36 COHORT_ID=cohort36 OPERATOR=yyfen \
#     WINDOW_START=2025-11-01 WINDOW_END=2026-04-30 NOTES="Primary cohort."
COHORT_DIR   ?=
COHORT_ID    ?=
OPERATOR     ?=
WINDOW_START ?= 2025-11-01
WINDOW_END   ?= 2026-04-30
NOTES        ?=
# Space-separated list of extra dirs whose PDFs are included by reference (no copy).
# Example: INCLUDE_DIRS=data/raw/news/cohort_36
INCLUDE_DIRS ?=
generate-manifest:
	$(PYTHON) scripts/generate_news_manifest.py \
		--cohort-dir "$(COHORT_DIR)" \
		--cohort-id "$(COHORT_ID)" \
		--operator "$(OPERATOR)" \
		--window-start "$(WINDOW_START)" \
		--window-end "$(WINDOW_END)" \
		--notes "$(NOTES)" \
		$(if $(INCLUDE_DIRS),--include-dirs $(INCLUDE_DIRS),)
