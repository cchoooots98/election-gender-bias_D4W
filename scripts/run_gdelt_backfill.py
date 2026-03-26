"""Legacy script wrapper for the installable GDELT backfill CLI."""

from src.cli.run_gdelt_backfill import main

if __name__ == "__main__":
    raise SystemExit(main())
