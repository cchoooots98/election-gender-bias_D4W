"""Compatibility wrapper for the installable news corpus CLI."""

from src.cli.run_news_corpus_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
