param(
    [Parameter(Position = 0)]
    [string]$Command = "help"
)

$ErrorActionPreference = "Stop"

# Canonical local entrypoint for Windows contributors. This script always uses
# the project virtual environment so PATH order cannot accidentally swap in a
# different interpreter or toolchain than the one the repository expects.
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Dbt = Join-Path $RepoRoot ".venv\Scripts\dbt.exe"

if (-not (Test-Path $Python)) {
    throw "Missing .venv interpreter at $Python. Create or rebuild .venv before running development commands."
}

function Invoke-ProjectCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Executable,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments
    )

    & $Executable @Arguments
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Set-Location $RepoRoot

switch ($Command) {
    "help" {
        Write-Host ""
        Write-Host "Election Gender Bias D4W - Windows development commands"
        Write-Host ""
        Write-Host "All commands use $Python"
        Write-Host ""
        Write-Host "  lint"
        Write-Host "  format"
        Write-Host "  format-check"
        Write-Host "  test"
        Write-Host "  test-coverage"
        Write-Host "  install"
        Write-Host "  compile"
        Write-Host "  notebook"
        Write-Host "  dashboard"
        Write-Host "  dbt-run"
        Write-Host "  dbt-test"
        Write-Host "  dbt-build"
        Write-Host "  run-sampling-pipeline"
        Write-Host "  run-news-corpus-pipeline"
        Write-Host "  run-nlp-input-pipeline"
        Write-Host "  run-nlp-lexicon-pipeline"
        Write-Host "  run-nlp-sentiment-pipeline"
        Write-Host "  run-nlp-tone-pipeline"
        Write-Host "  run-nlp-framing-pipeline"
        Write-Host "  run-nlp-backup-agreement-pipeline"
        Write-Host "  run-nlp-tone-sensitivity-pipeline"
        Write-Host "  run-nlp-qa-pipeline"
        Write-Host "  verify-nlp-lexicon"
        Write-Host "  verify-dashboard-artifacts"
    }
    "lint" {
        Invoke-ProjectCommand $Python @("-m", "ruff", "check", "src/", "tests/", "scripts/")
    }
    "format" {
        Invoke-ProjectCommand $Python @("-m", "black", "src/", "tests/", "scripts/", "notebooks/")
    }
    "format-check" {
        Invoke-ProjectCommand $Python @("-m", "black", "--check", "src/", "tests/", "scripts/")
    }
    "test" {
        Invoke-ProjectCommand $Python @("-m", "pytest", "tests/", "-v", "--tb=short")
    }
    "test-coverage" {
        Invoke-ProjectCommand $Python @("-m", "pytest", "tests/", "--cov=src", "--cov-report=term-missing")
    }
    "install" {
        Invoke-ProjectCommand $Python @("-m", "pip", "install", "-r", "requirements.txt")
        Invoke-ProjectCommand $Python @("-m", "pip", "install", "-e", ".", "--no-build-isolation")
    }
    "compile" {
        Invoke-ProjectCommand $Python @("-m", "piptools", "compile", "requirements.in", "-o", "requirements.txt", "--strip-extras")
    }
    "notebook" {
        Invoke-ProjectCommand $Python @("-m", "jupyterlab", "lab")
    }
    "dashboard" {
        Invoke-ProjectCommand $Python @("-m", "streamlit", "run", "src/dashboard/app.py")
    }
    "dbt-run" {
        Invoke-ProjectCommand $Dbt @("run", "--project-dir", "dbt", "--profiles-dir", "dbt", "--select", "marts.news")
    }
    "dbt-test" {
        Invoke-ProjectCommand $Dbt @("test", "--project-dir", "dbt", "--profiles-dir", "dbt", "--select", "marts.news")
    }
    "dbt-build" {
        Invoke-ProjectCommand $Dbt @("build", "--project-dir", "dbt", "--profiles-dir", "dbt", "--select", "marts.news")
    }
    "run-sampling-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_sampling_pipeline")
    }
    "run-news-corpus-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_news_corpus_pipeline")
    }
    "run-nlp-input-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_input_pipeline")
    }
    "run-nlp-lexicon-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_lexicon_pipeline")
    }
    "run-nlp-sentiment-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_sentiment_pipeline")
    }
    "run-nlp-tone-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_tone_pipeline")
    }
    "run-nlp-framing-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_framing_pipeline")
    }
    "run-nlp-backup-agreement-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_backup_agreement_pipeline")
    }
    "run-nlp-tone-sensitivity-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_tone_sensitivity_pipeline")
    }
    "run-nlp-qa-pipeline" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.run_nlp_qa_pipeline")
    }
    "verify-nlp-lexicon" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.verify_nlp_lexicon")
    }
    "verify-dashboard-artifacts" {
        Invoke-ProjectCommand $Python @("-m", "src.cli.verify_dashboard_artifacts")
    }
    default {
        throw "Unknown command '$Command'. Run '.\scripts\dev.ps1 help' for the supported command list."
    }
}

