"""Compatibility wrapper for the sampling pipeline CLI."""

from __future__ import annotations

from src.cli.run_sampling_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
