"""Tests for the installable sampling pipeline CLI entrypoint."""

from types import SimpleNamespace

from src.cli import run_sampling_pipeline as cli_module


def test_cli_main_returns_zero_on_success(monkeypatch):
    """Happy path: CLI should return exit code 0 after a successful run."""

    def _stub_run_sampling_pipeline():
        return SimpleNamespace(
            status="success",
            run_id="run-123",
            artifact_paths=["data/gold/sample_leaders.parquet"],
        )

    monkeypatch.setattr(
        cli_module,
        "run_sampling_pipeline",
        _stub_run_sampling_pipeline,
    )

    assert cli_module.main() == 0


def test_cli_main_returns_one_on_failure(monkeypatch):
    """Error path: CLI should convert pipeline exceptions into exit code 1."""

    def _raise_failure():
        raise ValueError("pipeline failed")

    monkeypatch.setattr(
        cli_module,
        "run_sampling_pipeline",
        _raise_failure,
    )

    assert cli_module.main() == 1


def test_legacy_script_wrapper_reuses_cli_main():
    """Compatibility: the legacy scripts/ wrapper should not duplicate CLI logic."""
    from scripts import run_sampling_pipeline as script_wrapper

    assert script_wrapper.main is cli_module.main
