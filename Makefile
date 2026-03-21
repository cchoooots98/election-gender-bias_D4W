# Makefile — common development commands for election-gender-bias_D4W.
#
# Usage: make <target>   (e.g. make lint, make test)
# Run  : make            to see available targets (default: help)
#
# Why a Makefile: standardises commands so every contributor (and CI) runs
# exactly the same tool invocations. "make lint" is the same on every machine.

.PHONY: help lint format format-check test test-coverage install compile \
        notebook dashboard \
        dbt-run dbt-test dbt-docs

# ── Default target ────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  Election Gender Bias D4W — available make targets"
	@echo ""
	@echo "  Code quality"
	@echo "    lint           ruff check src/ tests/"
	@echo "    format         black src/ tests/ notebooks/"
	@echo "    format-check   black --check (CI mode, no writes)"
	@echo ""
	@echo "  Tests"
	@echo "    test           pytest tests/ -v --tb=short"
	@echo "    test-coverage  pytest with coverage report"
	@echo ""
	@echo "  Dependencies"
	@echo "    install        pip install -r requirements.txt"
	@echo "    compile        pip-compile requirements.in -o requirements.txt"
	@echo ""
	@echo "  Development"
	@echo "    notebook       jupyter lab"
	@echo "    dashboard      streamlit run src/dashboard/app.py"
	@echo ""
	@echo "  dbt"
	@echo "    dbt-run        dbt run"
	@echo "    dbt-test       dbt test"
	@echo "    dbt-docs       dbt docs generate && dbt docs serve"
	@echo ""

# ── Code quality ──────────────────────────────────────────────────────────────
lint:
	ruff check src/ tests/

# Formats src/, tests/, and notebooks/ — notebooks/ excluded from lint
# because notebooks contain non-standard code patterns intentionally.
format:
	black src/ tests/ notebooks/

# CI mode: exits non-zero if any file would be reformatted (no writes).
# Used in ci.yml to block PRs with unformatted code.
format-check:
	black --check src/ tests/

# ── Tests ─────────────────────────────────────────────────────────────────────
test:
	pytest tests/ -v --tb=short

# --cov-report=term-missing shows which lines are not covered — actionable output.
test-coverage:
	pytest tests/ --cov=src --cov-report=term-missing

# ── Dependencies ──────────────────────────────────────────────────────────────
# Install exact pinned versions — guarantees reproducibility.
install:
	pip install -r requirements.txt

# Regenerate the lockfile after editing requirements.in.
# --strip-extras: omit extras markers for cleaner output.
compile:
	pip-compile requirements.in -o requirements.txt --strip-extras

# ── Development ───────────────────────────────────────────────────────────────
notebook:
	jupyter lab

dashboard:
	streamlit run src/dashboard/app.py

# ── dbt ───────────────────────────────────────────────────────────────────────
dbt-run:
	dbt run

dbt-test:
	dbt test

# Generates HTML docs then opens them in the browser at localhost:8080.
dbt-docs:
	dbt docs generate && dbt docs serve
