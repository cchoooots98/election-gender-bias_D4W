"""Tests for the enterprise news corpus backbone."""

from __future__ import annotations

import json
import warnings
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import run_news_corpus_pipeline as script_wrapper
from src.cli import run_news_corpus_pipeline as cli_module
from src.ingest.news import corpus as corpus_module
from src.ingest.news import corpus_pipeline as corpus_pipeline_module
from src.ingest.news.corpus import (
    _coerce_optional_str,
    _extract_europresse_declared_document_count,
    _is_europresse_format,
    _parse_timestamp,
    _segment_europresse_articles,
    build_fact_article,
    build_fact_article_source,
    enrich_article_sources_with_web_cache,
    inspect_import_batch,
    load_news_import_manifest,
    parse_import_batch,
    write_news_import_manifest,
    write_news_web_fetch_cache,
)
from src.ingest.news.corpus_pipeline import run_news_corpus_etl
from src.ingest.news.corpus_storage import write_duckdb_table
from src.ingest.news.matching import build_fact_mentions
from src.ingest.news.models import (
    ImportBatchFile,
    ImportBatchInspection,
    NewsImportManifest,
)
from src.ingest.news.normalize import canonicalize_url, sanitize_request_url
from src.metrics.news import regression as regression_module
from src.metrics.news.dbt_runner import read_duckdb_table
from src.metrics.news.quality import run_news_corpus_quality_checks
from src.metrics.news.regression import (
    build_mart_bootstrap_ci,
    build_mart_regression_results,
)
from src.orchestration import news_corpus_pipeline as orchestration_module
from src.transform._exceptions import DataQualityError

_FIXTURE_DIR = Path(__file__).with_name("fixtures")


def _load_fixture_text(file_name: str) -> str:
    return (_FIXTURE_DIR / file_name).read_text(encoding="utf-8")


def _valid_body(candidate_name: str, commune_name: str) -> str:
    return (
        f"{candidate_name} conduit la campagne des municipales a {commune_name}. "
        "Le candidat detaille son programme pour la mairie, la gouvernance locale, "
        "les finances publiques et le conseil municipal dans une interview complete."
    )


def _make_bronze_row(
    source_record_id: str,
    *,
    title: str,
    body_text: str,
    published_at: str,
    outlet: str,
    article_url: str = "",
) -> dict[str, object]:
    return {
        "batch_id": "batch-001",
        "source_system": "europresse",
        "source_record_id": source_record_id,
        "local_record_key": source_record_id,
        "source_record_hash": f"hash-{source_record_id}",
        "source_native_payload": "{}",
        "raw_title": title,
        "raw_body_text": body_text,
        "raw_published_at": published_at,
        "raw_outlet": outlet,
        "raw_article_url": article_url,
        "raw_author": "Reporter",
        "raw_language": "fr",
        "raw_file_path": f"data/raw/{source_record_id}.csv",
        "raw_file_type": "csv",
        "import_classification": "table_export",
        "parser_name": "parse_table_export",
        "parser_version": "news_corpus_v1",
        "rights_class": "restricted_local",
        "_ingested_at": datetime(2026, 4, 7, tzinfo=UTC).isoformat(),
    }


def _write_stub_dbt_news_marts(duckdb_path: Path) -> None:
    """Test double for dbt-owned marts when pytest cannot spawn dbt on Windows."""
    sample_df = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="sample_leaders",
    )
    dim_commune_df = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="silver",
        table_name="dim_commune",
    )
    fact_article_df = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="silver",
        table_name="fact_article",
    )
    fact_mention_df = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="silver",
        table_name="fact_mention",
    )

    for optional_column in ("is_incumbent", "won_final_round"):
        if optional_column not in sample_df.columns:
            sample_df[optional_column] = False

    exposure_columns = [
        "leader_id",
        "gender",
        "commune_insee",
        "city_size_bucket",
        "reg_code",
        "nuance_group",
        "is_incumbent",
        "won_final_round",
    ]
    exposure_df = sample_df[exposure_columns].merge(
        dim_commune_df[["commune_insee", "population"]],
        on="commune_insee",
        how="left",
        validate="many_to_one",
    )
    if fact_mention_df.empty:
        aggregated_df = pd.DataFrame(columns=["leader_id"])
    else:
        article_fields = [
            "canonical_article_id",
            "outlet_name_normalized",
            "rights_class",
            "acquisition_methods",
            "has_full_text",
        ]
        mention_article_df = fact_mention_df.merge(
            fact_article_df[article_fields],
            on="canonical_article_id",
            how="left",
            validate="many_to_one",
        )
        mention_article_df["restricted_flag"] = (
            mention_article_df["rights_class"].fillna("") == "restricted_local"
        )
        mention_article_df["supplemental_flag"] = (
            mention_article_df["acquisition_methods"]
            .fillna("")
            .str.contains("supplemental")
        )
        mention_article_df["full_text_flag"] = (
            mention_article_df["has_full_text"].astype("boolean").fillna(False)
        )
        aggregated_df = (
            mention_article_df.groupby("leader_id", dropna=False)
            .agg(
                article_count=("canonical_article_id", "nunique"),
                headline_mention_count=("headline_mention_flag", "sum"),
                distinct_source_count=("outlet_name_normalized", "nunique"),
                restricted_source_article_count=("restricted_flag", "sum"),
                supplemental_source_article_count=("supplemental_flag", "sum"),
                full_text_article_count=("full_text_flag", "sum"),
            )
            .reset_index()
        )

    exposure_df = exposure_df.merge(
        aggregated_df,
        on="leader_id",
        how="left",
        validate="one_to_one",
    )
    count_columns = [
        "article_count",
        "headline_mention_count",
        "distinct_source_count",
        "restricted_source_article_count",
        "supplemental_source_article_count",
        "full_text_article_count",
    ]
    for count_column in count_columns:
        if count_column not in exposure_df.columns:
            exposure_df[count_column] = 0
    exposure_df[count_columns] = exposure_df[count_columns].fillna(0).astype(int)
    exposure_df["metadata_only_article_count"] = (
        exposure_df["article_count"] - exposure_df["full_text_article_count"]
    )
    exposure_df["has_full_text"] = exposure_df["full_text_article_count"] > 0
    exposure_df["exposure_per_10k_population"] = (
        exposure_df["article_count"] / (exposure_df["population"] / 10_000)
    ).fillna(0.0)

    regression_feature_df = exposure_df.copy()
    regression_feature_df["gender_female"] = (
        regression_feature_df["gender"] == "F"
    ).astype(int)
    regression_feature_df["is_incumbent"] = (
        regression_feature_df["is_incumbent"]
        .astype("boolean")
        .fillna(False)
        .astype(int)
    )
    regression_feature_df["won_final_round"] = (
        regression_feature_df["won_final_round"]
        .astype("boolean")
        .fillna(False)
        .astype(int)
    )
    regression_feature_df = regression_feature_df[
        [
            "leader_id",
            "gender",
            "gender_female",
            "commune_insee",
            "city_size_bucket",
            "reg_code",
            "nuance_group",
            "is_incumbent",
            "won_final_round",
            "population",
            "article_count",
            "headline_mention_count",
            "distinct_source_count",
            "restricted_source_article_count",
            "supplemental_source_article_count",
            "exposure_per_10k_population",
        ]
    ]

    frame_labels = [
        "politique",
        "vie_privee",
        "apparence",
        "scandale",
        "personnalite",
        "securite",
        "unclassified",
    ]
    framing_df = pd.DataFrame(
        [
            {
                "leader_id": leader_id,
                "frame_label": frame_label,
                "mention_count": 0,
                "mean_frame_score": 0.0,
            }
            for leader_id in sample_df["leader_id"]
            for frame_label in frame_labels
        ]
    )
    bias_df = (
        exposure_df.groupby("gender", dropna=False)
        .agg(metric_value=("article_count", "mean"))
        .reset_index()
        .assign(metric_name="mean_article_count")
    )[["gender", "metric_name", "metric_value"]]
    analysis_df = pd.DataFrame(
        columns=[
            "analysis_id",
            "analysis_section_id",
            "analysis_name",
            "dimension",
            "group_label",
            "metric_name",
            "metric_value",
            "note",
        ]
    )

    for dataframe, table_name in [
        (exposure_df, "mart_exposure_metrics"),
        (framing_df, "mart_framing_metrics"),
        (bias_df, "mart_bias_indicators"),
        (regression_feature_df, "mart_regression_feature_base"),
        (analysis_df, "mart_analysis_summary"),
    ]:
        write_duckdb_table(
            dataframe=dataframe,
            schema_name="gold",
            table_name=table_name,
            duckdb_path=duckdb_path,
        )


def _make_quality_check_inputs(
    *,
    accepted_count: int = 1,
    rejected_count: int = 0,
    regression_results_df: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Build a minimal valid quality-check input bundle."""
    fact_article_source_df = pd.DataFrame(
        [
            {
                "article_source_id": f"source-{row_number}",
                "title_normalized": f"title-{row_number}",
                "published_at_normalized": "2026-03-01",
                "outlet_name_normalized": "local-paper",
                "language": "fr",
                "body_text_hash": f"hash-{row_number}",
                "has_full_text": True,
            }
            for row_number in range(accepted_count)
        ]
    )
    return {
        "sample_leaders_df": pd.DataFrame([{"leader_id": "leader-001"}]),
        "fact_article_source_df": fact_article_source_df,
        "fact_article_source_rejected_df": pd.DataFrame(
            [
                {
                    "article_source_id": f"rejected-{row_number}",
                    "_rejection_reason": "invalid_language",
                }
                for row_number in range(rejected_count)
            ]
        ),
        "fact_article_df": pd.DataFrame([{"canonical_article_id": "article-001"}]),
        "fact_mention_df": pd.DataFrame(
            [
                {
                    "canonical_article_id": "article-001",
                    "leader_id": "leader-001",
                }
            ]
        ),
        "mart_exposure_metrics_df": pd.DataFrame(
            [{"leader_id": "leader-001", "article_count": 1}]
        ),
        "mart_regression_results_df": (
            regression_results_df
            if regression_results_df is not None
            else pd.DataFrame([{"status": "fitted"}])
        ),
    }


def test_coerce_optional_str_returns_empty_for_container_values():
    """Regression: pandas null checks must not raise on list or dict inputs."""
    assert _coerce_optional_str([1, 2, 3]) == ""
    assert _coerce_optional_str({"api_key": "secret"}) == ""


def test_news_quality_checks_use_configured_rejected_ratio_threshold():
    """Regression: rejected-ratio DQ should honor DQ_MAX_NULL_RATE, not 25%."""
    quality_inputs = _make_quality_check_inputs(
        accepted_count=18,
        rejected_count=1,
    )

    with pytest.raises(DataQualityError, match="rejected ratio"):
        run_news_corpus_quality_checks(**quality_inputs)


def test_news_quality_checks_require_regression_status_column():
    """Regression: missing regression status must be a contract failure."""
    quality_inputs = _make_quality_check_inputs(
        regression_results_df=pd.DataFrame([{"model_name": "poisson_exposure"}])
    )

    with pytest.raises(DataQualityError, match="status"):
        run_news_corpus_quality_checks(**quality_inputs)


def test_canonicalize_url_removes_sensitive_query_params():
    """Regression: canonical URLs must not retain credentials or tracking noise."""
    url = (
        "https://www.example.com/article/?utm_source=newsletter"
        "&api_key=secret&token=hidden&keep=1"
    )

    assert canonicalize_url(url) == "https://example.com/article?keep=1"


def test_sanitize_request_url_removes_sensitive_query_params():
    """Regression: persisted request metadata must redact credential-like params."""
    url = "https://example.com/article?utm_source=newsletter&api_key=secret&keep=1"

    assert (
        sanitize_request_url(url)
        == "https://example.com/article?utm_source=newsletter&keep=1"
    )


def test_news_import_manifest_roundtrip(tmp_path):
    """Happy path: import manifests should round-trip cleanly through JSON."""
    manifest = NewsImportManifest(
        batch_id="batch-001",
        source_system="europresse",
        window_start=date(2026, 2, 1),
        window_end=date(2026, 4, 7),
        exported_at=datetime(2026, 4, 7, 21, 0, tzinfo=UTC),
        operator="tester",
        access_level="restricted subscription export",
        file_paths=(str(tmp_path / "export.csv"),),
        notes="manual export",
    )
    manifest_path = tmp_path / "news_import_manifest.json"
    write_news_import_manifest(manifest, manifest_path)

    loaded_manifest = load_news_import_manifest(manifest_path)

    assert loaded_manifest == manifest


def test_inspect_import_batch_classifies_supported_files(tmp_path):
    """Import routing must separate table, document, text-layer PDF, and unsupported files."""
    csv_path = tmp_path / "export.csv"
    csv_path.write_text("title,body,date\nA,B,2026-03-01\n", encoding="utf-8")
    html_path = tmp_path / "article.html"
    html_path.write_text(
        "<html><head><title>Example</title></head><body>Bonjour</body></html>",
        encoding="utf-8",
    )
    pdf_path = tmp_path / "article.pdf"
    pdf_path.write_bytes(
        b"%PDF-1.4\n1 0 obj << /Type /Catalog /Font << /F1 2 0 R >> >> endobj\n"
        b"2 0 obj << /Length 40 >> stream\nBT /F1 12 Tf (Bonjour la mairie) Tj ET\nendstream\nendobj\n"
    )
    unsupported_path = tmp_path / "article.bin"
    unsupported_path.write_bytes(b"binary")

    manifest = NewsImportManifest(
        batch_id="batch-001",
        source_system="europresse",
        window_start=date(2026, 2, 1),
        window_end=date(2026, 4, 7),
        exported_at=datetime(2026, 4, 7, 21, 0, tzinfo=UTC),
        operator="tester",
        access_level="restricted subscription export",
        file_paths=(
            str(csv_path),
            str(html_path),
            str(pdf_path),
            str(unsupported_path),
        ),
        notes="manual export",
    )

    inspection = inspect_import_batch(manifest)
    classifications = {
        Path(file.path).name: file.classification for file in inspection.files
    }

    assert classifications["export.csv"] == "table_export"
    assert classifications["article.html"] == "document_export"
    assert classifications["article.pdf"] == "pdf_text_layer"
    assert classifications["article.bin"] == "unsupported"


def test_build_fact_article_source_normalizes_and_rejects_bad_rows():
    """DQ path: invalid article rows should be quarantined without breaking valid rows."""
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                f"valid-{index}",
                title=f"DUPONT Alice article {index}",
                body_text=_valid_body("DUPONT Alice", "Commune 1"),
                published_at="2026-03-15",
                outlet="Le Parisien",
            )
            for index in range(20)
        ]
        + [
            _make_bronze_row(
                "invalid-1",
                title="",
                body_text="short",
                published_at="not-a-date",
                outlet="Le Parisien",
            )
        ]
    )

    accepted_df, rejected_df = build_fact_article_source(
        bronze_df,
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 30),
    )

    assert len(accepted_df) == 20
    assert len(rejected_df) == 1
    assert accepted_df["language"].eq("fr").all()
    assert "published_at unparseable" in rejected_df.loc[0, "_rejection_reason"]


def test_build_fact_article_source_rejects_articles_outside_analysis_window():
    """Regression: articles published outside the analysis window must be quarantined.

    Previously window_start/window_end were metadata-only fields with no DQ enforcement.
    This test ensures out-of-window rows land in the rejected table with an informative reason.

    Note: 40 in-window + 2 out-of-window keeps the test below the configured
    DQ reject-rate threshold while still proving both boundary violations are
    quarantined.
    """
    window_start = date(2025, 11, 1)
    window_end = date(2026, 4, 30)
    in_window_body = _valid_body("DUPONT Alice", "Lyon")
    out_before_body = _valid_body("MARTIN Jean", "Paris")
    out_after_body = _valid_body("BERNARD Claire", "Marseille")

    in_window_rows = [
        _make_bronze_row(
            f"in-window-{index}",
            title=f"DUPONT Alice article {index}",
            body_text=in_window_body,
            published_at="2026-01-15",  # inside [2025-11-01, 2026-04-30]
            outlet="Le Monde",
        )
        for index in range(40)
    ]
    out_of_window_rows = [
        _make_bronze_row(
            "before-window-1",
            title="MARTIN Jean article ancien",
            body_text=out_before_body,
            published_at="2025-10-31",  # one day before window_start
            outlet="Le Figaro",
        ),
        _make_bronze_row(
            "after-window-1",
            title="BERNARD Claire article futur",
            body_text=out_after_body,
            published_at="2026-05-01",  # one day after window_end
            outlet="Libération",
        ),
    ]
    bronze_df = pd.DataFrame(in_window_rows + out_of_window_rows)

    accepted_df, rejected_df = build_fact_article_source(
        bronze_df,
        window_start=window_start,
        window_end=window_end,
    )

    assert len(accepted_df) == 40, "Only in-window articles should be accepted"
    assert len(rejected_df) == 2, "Both out-of-window articles must be quarantined"
    rejection_reasons = rejected_df["_rejection_reason"].tolist()
    assert all("outside analysis window" in reason for reason in rejection_reasons)
    assert any("2025-10-31" in reason for reason in rejection_reasons)
    assert any("2026-05-01" in reason for reason in rejection_reasons)


def test_build_fact_article_source_requires_complete_analysis_window():
    """DQ contract: callers must not silently disable the publication window."""
    with pytest.raises(ValueError, match="window_start and window_end"):
        build_fact_article_source(
            pd.DataFrame(),
            window_start=None,
            window_end=date(2026, 4, 30),
        )


def test_run_news_corpus_etl_fails_fast_on_missing_manifest_window(
    monkeypatch, tmp_path
):
    """DQ contract: manifest analysis windows are required for production ETL."""
    monkeypatch.setattr(
        corpus_pipeline_module,
        "load_news_import_manifest",
        lambda _: SimpleNamespace(
            batch_id="batch-missing-window",
            source_system="europresse",
            window_start=None,
            window_end=date(2026, 4, 30),
        ),
    )

    with pytest.raises(ValueError, match="window_start and window_end"):
        run_news_corpus_etl(
            import_manifest_path=tmp_path / "news_import_manifest.json",
            sample_leaders_path=tmp_path / "sample_leaders.parquet",
            dim_commune_path=tmp_path / "dim_commune.parquet",
        )


def test_enrich_article_sources_reuses_cached_web_body_without_network(
    tmp_path,
    monkeypatch,
):
    """Regression: cached web extractions should survive offline full refreshes."""
    article_url = (
        "https://actu.fr/ile-de-france/ferrieres-en-brie_77181/" "article_63956385.html"
    )
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                "web-stub-1",
                title="A Ferrieres-en-Brie, trois candidates sont en lice",
                body_text=(
                    "Par Julia Gualtieri Publié le 11 mars 2026 à 8h00... "
                    f"Read more {article_url} This document contains links to Web-sites."
                ),
                published_at="11 mars 2026",
                outlet="Actu.fr (site web réf.) - Actu",
                article_url=article_url,
            )
        ]
    )
    accepted_df, rejected_df = build_fact_article_source(
        bronze_df,
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 30),
    )
    cache_path = tmp_path / "news_web_fetch_cache.parquet"
    cached_body = _valid_body("FAYSSE-HASSAN Catherine", "Ferrieres-en-Brie")
    write_news_web_fetch_cache(
        pd.DataFrame(
            [
                {
                    "canonical_url": accepted_df.loc[0, "canonical_url"],
                    "source_url": article_url,
                    "fetch_status": "success",
                    "http_status": 200,
                    "body_text": cached_body,
                    "body_text_hash": "cached-body-hash",
                    "body_text_preview": cached_body[:120],
                    "body_text_length": len(cached_body),
                    "fetched_at": datetime(2026, 4, 11, tzinfo=UTC).isoformat(),
                    "extractor_name": "trafilatura",
                    "extractor_version": "test",
                    "error_type": None,
                }
            ]
        ),
        cache_path,
    )
    monkeypatch.setattr(
        "src.ingest.news.corpus._fetch_web_article",
        lambda *_: pytest.fail("network fetch should not run on cache hit"),
    )

    enriched_df, report, cache_written = enrich_article_sources_with_web_cache(
        accepted_df,
        cache_path=cache_path,
        enable_web_scrape=False,
    )

    assert rejected_df.empty
    assert bool(accepted_df.loc[0, "has_full_text"]) is False
    assert bool(enriched_df.loc[0, "has_full_text"]) is True
    assert enriched_df.loc[0, "body_text"] == cached_body
    assert enriched_df.loc[0, "acquisition_method"] == "web_scrape"
    assert report["web_scrape_cache_hit_count"] == 1
    assert cache_written is False


def test_enrich_article_sources_fetches_uncached_urls_when_enabled(
    tmp_path,
    monkeypatch,
):
    """Happy path: opt-in scraping should fill web-reference stubs and write cache."""
    article_url = "https://example.org/article.html"
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                "web-stub-2",
                title="FAYSSE-HASSAN Catherine présente sa liste",
                body_text=(
                    "Read more https://example.org/article.html "
                    "This document contains links to Web-sites."
                ),
                published_at="11 mars 2026",
                outlet="Actu.fr (site web réf.) - Actu",
                article_url=article_url,
            )
        ]
    )
    accepted_df, _ = build_fact_article_source(bronze_df)
    scraped_body = _valid_body("FAYSSE-HASSAN Catherine", "Ferrieres-en-Brie")

    def fake_fetch(canonical_url: str, source_url: str) -> dict[str, object]:
        return {
            "canonical_url": canonical_url,
            "source_url": source_url,
            "fetch_status": "success",
            "http_status": 200,
            "body_text": scraped_body,
            "body_text_hash": "scraped-body-hash",
            "body_text_preview": scraped_body[:120],
            "body_text_length": len(scraped_body),
            "fetched_at": datetime(2026, 4, 11, tzinfo=UTC).isoformat(),
            "extractor_name": "trafilatura",
            "extractor_version": "test",
            "error_type": None,
        }

    monkeypatch.setattr("src.ingest.news.corpus._fetch_web_article", fake_fetch)

    enriched_df, report, cache_written = enrich_article_sources_with_web_cache(
        accepted_df,
        cache_path=tmp_path / "news_web_fetch_cache.parquet",
        enable_web_scrape=True,
    )

    assert bool(enriched_df.loc[0, "has_full_text"]) is True
    assert enriched_df.loc[0, "body_text"] == scraped_body
    assert report["web_scrape_success_count"] == 1
    assert cache_written is True


def test_enrich_article_sources_degrades_failed_scrapes_to_metadata_only(
    tmp_path,
    monkeypatch,
):
    """Error path: scrape failures should preserve metadata without crashing."""
    article_url = "https://example.org/paywalled.html"
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                "web-stub-3",
                title="Article municipal",
                body_text=(
                    "Read more https://example.org/paywalled.html "
                    "This document contains links to Web-sites."
                ),
                published_at="11 mars 2026",
                outlet="Actu.fr (site web réf.) - Actu",
                article_url=article_url,
            )
        ]
    )
    accepted_df, _ = build_fact_article_source(bronze_df)

    def fake_fetch(canonical_url: str, source_url: str) -> dict[str, object]:
        return {
            "canonical_url": canonical_url,
            "source_url": source_url,
            "fetch_status": "short_text",
            "http_status": 200,
            "body_text": "paywall",
            "body_text_hash": "short-body-hash",
            "body_text_preview": "paywall",
            "body_text_length": 7,
            "fetched_at": datetime(2026, 4, 11, tzinfo=UTC).isoformat(),
            "extractor_name": "trafilatura",
            "extractor_version": "test",
            "error_type": "body_text_too_short",
        }

    monkeypatch.setattr("src.ingest.news.corpus._fetch_web_article", fake_fetch)

    enriched_df, report, cache_written = enrich_article_sources_with_web_cache(
        accepted_df,
        cache_path=tmp_path / "news_web_fetch_cache.parquet",
        enable_web_scrape=True,
    )

    assert bool(enriched_df.loc[0, "has_full_text"]) is False
    assert pd.isna(enriched_df.loc[0, "body_text"])
    assert pd.isna(enriched_df.loc[0, "body_text_hash"])
    assert enriched_df.loc[0, "acquisition_method"] == "url_metadata_only"
    assert report["web_scrape_failure_count"] == 1
    assert report["url_metadata_only_count"] == 1
    assert cache_written is True


def test_build_fact_article_merges_url_and_url_less_records_via_hybrid_content():
    """Regression: duplicate article rows should merge across archive surfaces."""
    article_body = _valid_body("DUPONT Alice", "Commune 1")
    source_df = pd.DataFrame(
        [
            {
                "article_source_id": "src-1",
                "batch_id": "batch-001",
                "source_system": "europresse",
                "source_record_id": "src-1",
                "source_record_hash": "hash-1",
                "title": "DUPONT Alice conduit la campagne",
                "title_normalized": "dupont alice conduit la campagne",
                "body_text": article_body,
                "body_text_hash": "body-hash-1",
                "has_full_text": True,
                "published_at_normalized": pd.Timestamp("2026-03-15T08:00:00Z"),
                "published_date": "2026-03-15",
                "outlet_name": "Le Parisien",
                "outlet_name_normalized": "le parisien",
                "article_url": None,
                "canonical_url": None,
                "author": None,
                "language": "fr",
                "acquisition_method": "restricted_export",
                "parser_status": "parsed",
                "rights_class": "restricted_local",
                "raw_file_path": "a.csv",
                "raw_file_type": "csv",
                "import_classification": "table_export",
                "parser_name": "parse_table_export",
                "parser_version": "news_corpus_v1",
                "source_native_payload": "{}",
                "_ingested_at": datetime(2026, 4, 7, tzinfo=UTC).isoformat(),
            },
            {
                "article_source_id": "src-2",
                "batch_id": "batch-001",
                "source_system": "news_archive",
                "source_record_id": "src-2",
                "source_record_hash": "hash-2",
                "title": "DUPONT Alice conduit la campagne",
                "title_normalized": "dupont alice conduit la campagne",
                "body_text": article_body,
                "body_text_hash": "body-hash-1",
                "has_full_text": True,
                "published_at_normalized": pd.Timestamp("2026-03-15T08:00:00Z"),
                "published_date": "2026-03-15",
                "outlet_name": "Le Parisien",
                "outlet_name_normalized": "le parisien",
                "article_url": "https://www.leparisien.fr/article-123?utm_source=rss",
                "canonical_url": "https://leparisien.fr/article-123",
                "author": None,
                "language": "fr",
                "acquisition_method": "document_archive",
                "parser_status": "success",
                "rights_class": "restricted_local",
                "raw_file_path": "archive.html",
                "raw_file_type": "html",
                "import_classification": "document_export",
                "parser_name": "parse_document_export",
                "parser_version": "news_corpus_v1",
                "source_native_payload": "{}",
                "_ingested_at": datetime(2026, 4, 7, tzinfo=UTC).isoformat(),
            },
        ]
    )

    fact_article_df = build_fact_article(source_df)

    assert len(fact_article_df) == 1
    assert fact_article_df.loc[0, "dedup_method"] == "hybrid_url_content"
    assert fact_article_df.loc[0, "source_record_count"] == 2


def test_build_fact_mentions_routes_ambiguous_surname_matches_to_manual_review():
    """Regression: surname-only matches with ambiguity should never auto-pass."""
    fact_article_df = pd.DataFrame(
        [
            {
                "canonical_article_id": "article-001",
                "title": "Municipales a Rennes : Martin detaille son programme",
                "body_text": _valid_body("Martin", "Rennes"),
                "outlet_name": "Le Monde",
                "published_at": pd.Timestamp("2026-03-12T08:00:00Z"),
            }
        ]
    )
    sample_leaders_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "full_name": "MARTIN Alice",
                "commune_name": "Rennes",
                "same_name_candidate_count": 2,
            },
            {
                "leader_id": "leader-002",
                "full_name": "MARTIN Bruno",
                "commune_name": "Rennes",
                "same_name_candidate_count": 2,
            },
        ]
    )

    fact_mention_df, manual_review_df = build_fact_mentions(
        fact_article_df,
        sample_leaders_df,
    )

    assert fact_mention_df.empty
    assert len(manual_review_df) == 2
    assert (
        manual_review_df["ambiguity_reason"]
        .eq("surname-only evidence is ambiguous")
        .all()
    )


def test_build_fact_mentions_matches_compound_given_names_without_manual_review():
    """Regression: candidate matching must keep multi-token given names intact."""
    fact_article_df = pd.DataFrame(
        [
            {
                "canonical_article_id": "article-002",
                "title": "Jean Claude Dupont detaille son programme a Lyon",
                "body_text": _valid_body("Jean Claude Dupont", "Lyon"),
                "outlet_name": "Le Monde",
                "published_at": pd.Timestamp("2026-03-13T08:00:00Z"),
            }
        ]
    )
    sample_leaders_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-dupont",
                "full_name": "DUPONT Jean Claude",
                "commune_name": "Lyon",
                "same_name_candidate_count": 1,
            }
        ]
    )

    fact_mention_df, manual_review_df = build_fact_mentions(
        fact_article_df,
        sample_leaders_df,
    )

    assert manual_review_df.empty
    assert len(fact_mention_df) == 1
    assert fact_mention_df.loc[0, "match_method"] == "exact_full_name"
    assert bool(fact_mention_df.loc[0, "headline_mention_flag"]) is True


def test_build_fact_mentions_ignores_metadata_only_candidate_filename_evidence():
    """Regression: candidate-named PDFs must not create evidence by file path."""
    fact_article_df = pd.DataFrame(
        [
            {
                "canonical_article_id": "article-metadata-only",
                "title": "Le conseil municipal adopte son budget local",
                "body_text": None,
                "canonical_url": "https://example.org/conseil-municipal-budget.html",
                "representative_url": "https://example.org/conseil-municipal-budget.html",
                "raw_file_path": "data/raw/news/FAYSSE-HASSAN_Catherine.pdf",
                "outlet_name": "Actu.fr",
                "published_at": pd.Timestamp("2026-03-13T08:00:00Z"),
            }
        ]
    )
    sample_leaders_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-faysse",
                "full_name": "FAYSSE-HASSAN Catherine",
                "commune_name": "Ferrieres-en-Brie",
                "same_name_candidate_count": 1,
            }
        ]
    )

    fact_mention_df, manual_review_df = build_fact_mentions(
        fact_article_df,
        sample_leaders_df,
    )

    assert fact_mention_df.empty
    assert manual_review_df.empty


def test_mart_exposure_contract_splits_full_text_and_metadata_only_counts(tmp_path):
    """Regression: exposure mart keeps denominator and text availability."""
    sample_leaders_df = pd.DataFrame(
        [
            {
                "leader_id": f"leader-{index:03d}",
                "gender": "F" if index % 2 == 0 else "M",
                "commune_insee": f"{index:05d}",
                "city_size_bucket": "small",
                "reg_code": "11",
                "nuance_group": "gauche",
                "is_incumbent": False,
                "won_final_round": False,
            }
            for index in range(1, 37)
        ]
    )
    dim_commune_df = pd.DataFrame(
        {
            "commune_insee": [f"{index:05d}" for index in range(1, 37)],
            "population": [10_000 for _ in range(1, 37)],
        }
    )
    fact_article_df = pd.DataFrame(
        [
            {
                "canonical_article_id": "article-full-text",
                "outlet_name_normalized": "actu fr",
                "rights_class": "restricted_local",
                "acquisition_methods": "web_scrape",
                "has_full_text": True,
            },
            {
                "canonical_article_id": "article-metadata-only",
                "outlet_name_normalized": "actu fr",
                "rights_class": "restricted_local",
                "acquisition_methods": "url_metadata_only",
                "has_full_text": False,
            },
        ]
    )
    fact_mention_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "canonical_article_id": "article-full-text",
                "mention_id": "mention-full-text",
                "headline_mention_flag": True,
                "frame_label": None,
                "frame_score": None,
            },
            {
                "leader_id": "leader-001",
                "canonical_article_id": "article-metadata-only",
                "mention_id": "mention-metadata-only",
                "headline_mention_flag": False,
                "frame_label": None,
                "frame_score": None,
            },
        ]
    )

    duckdb_path = tmp_path / "warehouse.duckdb"
    write_duckdb_table(
        dataframe=sample_leaders_df,
        schema_name="gold",
        table_name="sample_leaders",
        duckdb_path=duckdb_path,
    )
    write_duckdb_table(
        dataframe=dim_commune_df,
        schema_name="silver",
        table_name="dim_commune",
        duckdb_path=duckdb_path,
    )
    write_duckdb_table(
        dataframe=fact_article_df,
        schema_name="silver",
        table_name="fact_article",
        duckdb_path=duckdb_path,
    )
    write_duckdb_table(
        dataframe=fact_mention_df,
        schema_name="silver",
        table_name="fact_mention",
        duckdb_path=duckdb_path,
    )
    _write_stub_dbt_news_marts(duckdb_path)

    exposure_df = read_duckdb_table(
        duckdb_path=duckdb_path,
        schema_name="gold",
        table_name="mart_exposure_metrics",
    )
    covered_row = exposure_df.loc[exposure_df["leader_id"] == "leader-001"].iloc[0]
    uncovered_rows = exposure_df.loc[exposure_df["leader_id"] != "leader-001"]

    assert len(exposure_df) == 36
    assert covered_row["article_count"] == 2
    assert covered_row["full_text_article_count"] == 1
    assert covered_row["metadata_only_article_count"] == 1
    assert bool(covered_row["has_full_text"]) is True
    assert uncovered_rows["article_count"].sum() == 0


def test_run_news_corpus_etl_builds_24_row_exposure_mart(monkeypatch, tmp_path):
    """Integration: the main pipeline must preserve the full 24-candidate denominator."""
    dbt_calls = []

    def _recording_stub_dbt(*, duckdb_path: Path, **kwargs):
        dbt_calls.append(str(duckdb_path))
        _write_stub_dbt_news_marts(duckdb_path)

    monkeypatch.setattr(
        corpus_pipeline_module,
        "run_dbt_news_marts",
        _recording_stub_dbt,
    )
    sample_leaders_df = pd.DataFrame(
        [
            {
                "leader_id": f"leader-{index:03d}",
                "full_name": (
                    f"CANDIDAT{index} Alice"
                    if index % 2 == 0
                    else f"CANDIDAT{index} Bruno"
                ),
                "gender": "F" if index % 2 == 0 else "M",
                "commune_insee": f"{index:05d}",
                "commune_name": f"Commune {index}",
                "city_size_bucket": "small",
                "reg_code": "11",
                "nuance_group": "centre",
                "is_incumbent": False,
                "same_name_candidate_count": 1,
            }
            for index in range(1, 25)
        ]
    )
    dim_commune_df = pd.DataFrame(
        {
            "commune_insee": [f"{index:05d}" for index in range(1, 25)],
            "population": [10_000 + index for index in range(1, 25)],
        }
    )
    sample_path = tmp_path / "gold" / "sample_leaders.parquet"
    dim_commune_path = tmp_path / "silver" / "dim_commune.parquet"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    dim_commune_path.parent.mkdir(parents=True, exist_ok=True)
    sample_leaders_df.to_parquet(sample_path, index=False)
    dim_commune_df.to_parquet(dim_commune_path, index=False)

    article_csv = tmp_path / "europresse_export.csv"
    article_csv.write_text(
        "Title,Full Text,Date de publication,Journal,Url\n"
        f'"CANDIDAT1 Bruno detaille sa campagne","{_valid_body("CANDIDAT1 Bruno", "Commune 1")}",2026-03-15,"Le Parisien",\n'
        f'"CANDIDAT2 Alice detaille sa campagne","{_valid_body("CANDIDAT2 Alice", "Commune 2")}",2026-03-16,"Le Monde",\n',
        encoding="utf-8",
    )
    manifest = NewsImportManifest(
        batch_id="batch-002",
        source_system="europresse",
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 1),
        exported_at=datetime(2026, 4, 7, 21, 0, tzinfo=UTC),
        operator="tester",
        access_level="restricted subscription export",
        file_paths=(str(article_csv),),
        notes="manual export",
    )
    manifest_path = tmp_path / "raw" / "news" / "news_import_manifest.json"
    write_news_import_manifest(manifest, manifest_path)

    result = run_news_corpus_etl(
        import_manifest_path=manifest_path,
        sample_leaders_path=sample_path,
        dim_commune_path=dim_commune_path,
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
        bootstrap_resamples=60,
    )

    exposure_df = pd.read_parquet(tmp_path / "gold" / "mart_exposure_metrics.parquet")
    qa_report = json.loads(
        (tmp_path / "gold" / "news_corpus_qa_report.json").read_text(encoding="utf-8")
    )

    assert result.row_counts["fact_article"] == 2
    assert dbt_calls == [str(tmp_path / "warehouse.duckdb")]
    assert len(exposure_df) == 24
    assert exposure_df["article_count"].sum() == 2
    assert qa_report["qa"]["zero_coverage_leader_count"] == 22


def test_build_mart_regression_results_includes_bloc_and_region_controls():
    """Regression: published model controls must be present in the fitted design."""
    exposure_rows = []
    leader_index = 1
    for city_size_bucket in ("small", "medium", "large"):
        for nuance_group in ("gauche", "centre"):
            for reg_code, article_count in (
                ("11", 1 + leader_index),
                ("84", 2 + leader_index),
            ):
                leader_id = f"leader-{leader_index:03d}"
                commune_insee = f"{leader_index:05d}"
                gender = "F" if leader_index % 2 == 0 else "M"
                exposure_rows.append(
                    {
                        "leader_id": leader_id,
                        "gender": gender,
                        "commune_insee": commune_insee,
                        "city_size_bucket": city_size_bucket,
                        "reg_code": reg_code,
                        "nuance_group": nuance_group,
                        "is_incumbent": leader_index % 3 == 0,
                        "won_final_round": leader_index % 4 == 0,
                        "population": 10_000 + leader_index,
                        "article_count": article_count,
                        "headline_mention_count": article_count,
                        "distinct_source_count": 1 + (leader_index % 2),
                        "restricted_source_article_count": leader_index % 2,
                        "supplemental_source_article_count": (leader_index + 1) % 2,
                        "exposure_per_10k_population": float(article_count),
                    }
                )
                leader_index += 1

    regression_feature_base_df = pd.DataFrame(exposure_rows)
    regression_feature_base_df["gender_female"] = (
        regression_feature_base_df["gender"] == "F"
    ).astype(int)
    regression_feature_base_df["is_incumbent"] = regression_feature_base_df[
        "is_incumbent"
    ].astype(int)
    regression_feature_base_df["won_final_round"] = regression_feature_base_df[
        "won_final_round"
    ].astype(int)

    regression_results_df = build_mart_regression_results(regression_feature_base_df)

    variable_names = set(regression_results_df["variable_name"].astype(str))
    assert any(name.startswith("nuance_group_") for name in variable_names)
    assert any(name.startswith("reg_code_") for name in variable_names)
    assert "won_final_round" in variable_names


def test_build_regression_design_matrix_keeps_stable_column_order():
    """Regression: model coefficient order should be deterministic for auditability."""
    modeling_df = pd.DataFrame(
        [
            {
                "gender_female": 1,
                "is_incumbent": 0,
                "won_final_round": 1,
                "city_size_bucket": "small",
                "restricted_source_article_count": 2,
                "supplemental_source_article_count": 0,
                "nuance_group": "centre",
                "reg_code": "11",
            },
            {
                "gender_female": 0,
                "is_incumbent": 1,
                "won_final_round": 0,
                "city_size_bucket": "medium",
                "restricted_source_article_count": 0,
                "supplemental_source_article_count": 1,
                "nuance_group": "gauche",
                "reg_code": "84",
            },
            {
                "gender_female": 1,
                "is_incumbent": 0,
                "won_final_round": 0,
                "city_size_bucket": "large",
                "restricted_source_article_count": 1,
                "supplemental_source_article_count": 1,
                "nuance_group": "centre",
                "reg_code": "11",
            },
        ]
    )

    design_matrix_df = regression_module._build_regression_design_matrix(modeling_df)

    assert design_matrix_df.columns.tolist() == [
        "const",
        "gender_female",
        "is_incumbent",
        "won_final_round",
        "bucket_large",
        "bucket_medium",
        "nuance_group_gauche",
        "reg_code_84",
    ]


def test_build_regression_design_matrix_excludes_source_provenance_counts():
    """Regression: source provenance counters are not causal predictors."""
    modeling_df = pd.DataFrame(
        [
            {
                "gender_female": 1,
                "is_incumbent": 0,
                "won_final_round": 1,
                "city_size_bucket": "small",
                "restricted_source_article_count": 2,
                "supplemental_source_article_count": 1,
                "nuance_group": "centre",
                "reg_code": "11",
            }
        ]
    )

    design_matrix_df = regression_module._build_regression_design_matrix(modeling_df)

    assert "restricted_source_article_count" not in design_matrix_df.columns
    assert "supplemental_source_article_count" not in design_matrix_df.columns


def test_build_mart_regression_results_marks_fit_warnings(monkeypatch):
    """Regression: statsmodel warnings must be surfaced in mart_regression_results.

    Both Poisson and NegativeBinomial are mocked with the same FakeGLM so that
    warnings emitted during fit() are captured in both model's status strings.
    """

    class FakeFitResult:
        params = pd.Series({"const": 0.1, "gender_female": 0.2})
        bse = pd.Series({"const": 0.01, "gender_female": 0.02})
        pvalues = pd.Series({"const": 0.5, "gender_female": 0.04})

    class FakeGLM:
        def __init__(self, *args, **kwargs):
            pass

        def fit(self):
            warnings.warn("separation", RuntimeWarning, stacklevel=2)
            return FakeFitResult()

    fake_statsmodels = SimpleNamespace(
        GLM=FakeGLM,
        families=SimpleNamespace(
            Poisson=lambda: object(),
            NegativeBinomial=lambda: object(),
        ),
    )
    monkeypatch.setattr(regression_module, "sm", fake_statsmodels)

    regression_feature_base_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "gender": "F",
                "gender_female": 1,
                "commune_insee": "01001",
                "city_size_bucket": "small",
                "reg_code": "11",
                "nuance_group": "gauche",
                "is_incumbent": 0,
                "won_final_round": 0,
                "population": 10_000,
                "article_count": 3,
                "headline_mention_count": 3,
                "distinct_source_count": 2,
                "restricted_source_article_count": 1,
                "supplemental_source_article_count": 0,
                "exposure_per_10k_population": 3.0,
            },
            {
                "leader_id": "leader-002",
                "gender": "M",
                "gender_female": 0,
                "commune_insee": "01002",
                "city_size_bucket": "medium",
                "reg_code": "84",
                "nuance_group": "centre",
                "is_incumbent": 1,
                "won_final_round": 1,
                "population": 12_000,
                "article_count": 2,
                "headline_mention_count": 2,
                "distinct_source_count": 1,
                "restricted_source_article_count": 0,
                "supplemental_source_article_count": 1,
                "exposure_per_10k_population": 1.7,
            },
        ]
    )

    regression_results_df = build_mart_regression_results(regression_feature_base_df)

    # Both models are present and both must carry the warning status.
    assert set(regression_results_df["model_name"].unique()) == {
        "poisson_exposure",
        "negbinom_exposure",
    }
    for model_name, model_rows_df in regression_results_df.groupby("model_name"):
        assert (
            model_rows_df["status"] == "fitted_with_warning:RuntimeWarning"
        ).all(), f"{model_name} rows did not carry the expected warning status"


def _make_feature_base(n_candidates: int = 20) -> pd.DataFrame:
    """Build a minimal regression feature base for bootstrap tests.

    Uses enough candidates (20) and enough region diversity (3 regions with
    3+ members each) that sparse-region collapsing does not eliminate all
    region dummies, while staying small enough for fast test runs.
    """
    rows = []
    regions = ["11", "84", "76"]  # 3 regions, each gets ~6-7 candidates
    nuances = ["gauche", "droite", "divers"]
    for i in range(n_candidates):
        gender = "F" if i % 2 == 0 else "M"
        rows.append(
            {
                "leader_id": f"leader-{i:03d}",
                "gender": gender,
                "gender_female": 1 if gender == "F" else 0,
                "commune_insee": f"{i:05d}",
                "city_size_bucket": ["small", "medium", "large"][i % 3],
                "reg_code": regions[i % 3],
                "nuance_group": nuances[i % 3],
                "is_incumbent": i % 4 == 0,
                "won_final_round": i % 5 == 0,
                "population": 10_000 + i * 1_000,
                "article_count": max(1, (i % 7) * 10 + 5),
                "headline_mention_count": i % 5,
                "distinct_source_count": 1 + i % 3,
                "restricted_source_article_count": i % 2,
                "supplemental_source_article_count": 0,
                "exposure_per_10k_population": float((i % 7) * 10 + 5),
            }
        )
    return pd.DataFrame(rows)


def test_build_mart_bootstrap_ci_returns_expected_schema():
    """Bootstrap CI: output must contain all documented columns with correct types."""
    features_df = _make_feature_base()
    ci_df = build_mart_bootstrap_ci(features_df, n_bootstrap=50, random_seed=0)

    assert not ci_df.empty, "Bootstrap CI must return at least one row"
    assert set(ci_df.columns) >= {
        "variable_name",
        "n_bootstrap",
        "n_converged",
        "observed_coef",
        "ci_lower_95",
        "ci_upper_95",
        "ci_lower_90",
        "ci_upper_90",
        "bootstrap_std",
        "ci_excludes_zero",
        "fitted_at",
    }
    assert "gender_female" in ci_df["variable_name"].values
    assert ci_df["n_bootstrap"].iloc[0] == 50
    assert ci_df["ci_excludes_zero"].dtype == bool


def test_build_mart_bootstrap_ci_ci_bounds_ordered():
    """Bootstrap CI: lower bound must be <= observed coefficient <= upper bound (when CI is valid)."""
    features_df = _make_feature_base()
    ci_df = build_mart_bootstrap_ci(features_df, n_bootstrap=100, random_seed=1)

    valid_rows = ci_df.dropna(subset=["ci_lower_95", "ci_upper_95"])
    assert not valid_rows.empty, "At least some rows should have valid CIs"
    assert (
        valid_rows["ci_lower_95"] <= valid_rows["ci_upper_95"]
    ).all(), "ci_lower_95 must be <= ci_upper_95 for all rows"
    assert (
        valid_rows["ci_lower_90"] <= valid_rows["ci_upper_90"]
    ).all(), "ci_lower_90 must be <= ci_upper_90 for all rows"
    # 90% CI must be strictly contained within 95% CI
    assert (
        valid_rows["ci_lower_90"] >= valid_rows["ci_lower_95"]
    ).all(), "90% lower bound must be >= 95% lower bound"


def test_build_mart_bootstrap_ci_reproducible_with_same_seed():
    """Bootstrap CI: identical seed must produce bit-for-bit identical CIs (reproducibility contract)."""
    features_df = _make_feature_base()
    ci_a = build_mart_bootstrap_ci(features_df, n_bootstrap=50, random_seed=99)
    ci_b = build_mart_bootstrap_ci(features_df, n_bootstrap=50, random_seed=99)

    pd.testing.assert_frame_equal(
        ci_a.drop(columns=["fitted_at"]),
        ci_b.drop(columns=["fitted_at"]),
        check_exact=True,
    )


def test_build_mart_bootstrap_ci_empty_input_returns_empty():
    """Bootstrap CI: empty feature base must return an empty DataFrame without error."""
    ci_df = build_mart_bootstrap_ci(pd.DataFrame(), n_bootstrap=10, random_seed=0)
    assert ci_df.empty


def test_run_news_corpus_etl_persists_redacted_text_artifacts(monkeypatch, tmp_path):
    """Regression: persisted Parquet artifacts must not retain full article text."""
    monkeypatch.setattr(
        corpus_pipeline_module,
        "run_dbt_news_marts",
        lambda *, duckdb_path, **kwargs: _write_stub_dbt_news_marts(duckdb_path),
    )
    sample_leaders_df = pd.DataFrame(
        [
            {
                "leader_id": "leader-001",
                "full_name": "DUPONT Alice",
                "gender": "F",
                "commune_insee": "00001",
                "commune_name": "Commune 1",
                "city_size_bucket": "small",
                "reg_code": "11",
                "nuance_group": "centre",
                "is_incumbent": False,
                "same_name_candidate_count": 1,
            }
        ]
    )
    dim_commune_df = pd.DataFrame({"commune_insee": ["00001"], "population": [10_000]})
    sample_path = tmp_path / "gold" / "sample_leaders.parquet"
    dim_commune_path = tmp_path / "silver" / "dim_commune.parquet"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    dim_commune_path.parent.mkdir(parents=True, exist_ok=True)
    sample_leaders_df.to_parquet(sample_path, index=False)
    dim_commune_df.to_parquet(dim_commune_path, index=False)

    article_body = _valid_body("DUPONT Alice", "Commune 1")
    article_csv = tmp_path / "europresse_export.csv"
    article_csv.write_text(
        "Title,Full Text,Date de publication,Journal,Url\n"
        f'"DUPONT Alice detaille sa campagne","{article_body}",2026-03-15,"Le Parisien",\n',
        encoding="utf-8",
    )
    manifest = NewsImportManifest(
        batch_id="batch-redact",
        source_system="europresse",
        window_start=date(2026, 2, 1),
        window_end=date(2026, 4, 7),
        exported_at=datetime(2026, 4, 7, 21, 0, tzinfo=UTC),
        operator="tester",
        access_level="restricted subscription export",
        file_paths=(str(article_csv),),
        notes="manual export",
    )
    manifest_path = tmp_path / "raw" / "news" / "news_import_manifest.json"
    write_news_import_manifest(manifest, manifest_path)

    run_news_corpus_etl(
        import_manifest_path=manifest_path,
        sample_leaders_path=sample_path,
        dim_commune_path=dim_commune_path,
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
        bootstrap_resamples=60,
    )

    bronze_df = pd.read_parquet(
        tmp_path
        / "bronze"
        / "news_source_record"
        / "batch_id=batch-redact"
        / "source_system=europresse"
        / "news_source_record.parquet"
    )
    source_df = pd.read_parquet(tmp_path / "silver" / "fact_article_source.parquet")
    article_df = pd.read_parquet(tmp_path / "silver" / "fact_article.parquet")

    assert (
        bronze_df.loc[0, "raw_body_text"]
        == corpus_pipeline_module._REDACTED_TEXT_MARKER
    )
    assert source_df.loc[0, "body_text"] == corpus_pipeline_module._REDACTED_TEXT_MARKER
    assert (
        article_df.loc[0, "body_text"] == corpus_pipeline_module._REDACTED_TEXT_MARKER
    )
    assert source_df.loc[0, "body_text_preview"] in article_body
    assert article_df.loc[0, "body_text_preview"] in article_body
    assert bronze_df.loc[0, "raw_body_text"] != article_body


def test_news_corpus_cli_main_returns_zero_on_success(monkeypatch):
    """Happy path: CLI should return exit code 0 after a successful run."""
    monkeypatch.setattr(
        cli_module,
        "run_news_corpus_pipeline",
        lambda **kwargs: SimpleNamespace(
            status="success",
            batch_id="batch-001",
            artifact_paths=["data/gold/mart_exposure_metrics.parquet"],
        ),
    )

    # Pass argv=[] so argparse reads our empty list instead of pytest's sys.argv.
    assert cli_module.main(argv=[]) == 0


def test_news_corpus_cli_main_returns_one_on_failure(monkeypatch):
    """Error path: CLI should convert pipeline exceptions into exit code 1."""
    monkeypatch.setattr(
        cli_module,
        "run_news_corpus_pipeline",
        lambda **kwargs: (_ for _ in ()).throw(ValueError("pipeline failed")),
    )

    assert cli_module.main(argv=[]) == 1


def test_news_corpus_legacy_script_wrapper_reuses_cli_main():
    """Compatibility: the legacy scripts wrapper should not duplicate CLI logic."""
    assert script_wrapper.main is cli_module.main


def test_news_corpus_orchestration_logs_meta_batch(monkeypatch, tmp_path):
    """Happy path: orchestration should record both pipeline run and batch observability."""
    logged_runs = []
    logged_batches = []
    manifest = NewsImportManifest(
        batch_id="batch-003",
        source_system="europresse",
        window_start=date(2026, 2, 1),
        window_end=date(2026, 4, 7),
        exported_at=datetime(2026, 4, 7, 21, 0, tzinfo=UTC),
        operator="tester",
        access_level="restricted subscription export",
        file_paths=(str(tmp_path / "export.csv"),),
        notes="manual export",
    )
    manifest_path = tmp_path / "news_import_manifest.json"
    write_news_import_manifest(manifest, manifest_path)
    qa_path = tmp_path / "qa.json"
    qa_path.write_text(
        json.dumps({"parser_mix": {"table_export": 1}, "language_mix": {"fr": 2}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        orchestration_module,
        "run_news_corpus_etl",
        lambda **kwargs: SimpleNamespace(
            run_id="etl-run-003",
            batch_id="batch-003",
            status="success",
            error_count=0,
            row_counts={
                "fact_article": 2,
                "fact_article_source": 2,
                "fact_article_source_rejected": 0,
            },
            artifact_paths=("a.parquet",),
            qa_report_path=str(qa_path),
        ),
    )
    monkeypatch.setattr(
        orchestration_module,
        "log_pipeline_run",
        lambda **kwargs: logged_runs.append(kwargs),
    )
    monkeypatch.setattr(
        orchestration_module,
        "log_news_import_batch",
        lambda **kwargs: logged_batches.append(kwargs),
    )

    result = orchestration_module.run_news_corpus_pipeline(
        import_manifest_path=manifest_path,
        sample_leaders_path=tmp_path / "sample.parquet",
        dim_commune_path=tmp_path / "commune.parquet",
        bronze_dir=tmp_path / "bronze",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
    )

    assert result.status == "success"
    assert logged_batches[0]["batch_id"] == "batch-003"
    assert logged_runs[0]["flow_name"] == "news_corpus_pipeline"
    assert logged_runs[0]["run_id"] == "etl-run-003"


# ---------------------------------------------------------------------------
# _is_europresse_format
# ---------------------------------------------------------------------------

_EUROPRESSE_ONE_ARTICLE = (
    "Le Progrès\n• p. 12\n• 320 words\nPage lyon18\nSome title\nBody text here."
)
_EUROPRESSE_TWO_ARTICLES = (
    "Outlet A\n• p. 5\n• 120 words\nPage a1\nFirst title\nFirst body.\n"
    "This document is destined for the exclusive use of...\n"
    "Outlet B\n• p. 8\n• 250 words\nPage b2\nSecond title\nSecond body."
)
_EUROPRESSE_SINGLE_DOCUMENT = (
    "Saved documents\n"
    "1 document\n"
    "Vendredi 21 novembre 2025\n"
    "Midi Libre\n"
    "â€¢ 139 words\n"
    "Â© 2025 Midi Libre. Tous droits rÃ©servÃ©s.\n"
    "Myriam Bui-Xuan veut faire vivre Â« le coeur du village Â»\n"
    "Une erreur s'est glissÃ©e dans notre article du 7 novembre dernier "
    "consacrÃ© Ã  la candidature aux municipales prochaines de Myriam Bui-Xuan. "
    "Sa liste veut faire vivre le coeur du village Ã  Clapiers.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
)
_EUROPRESSE_SITE_WEB_REF_ARTICLE = (
    "11 mars 2026\n"
    "Actu.fr (site web rÃ©f.) - Actu\n"
    "â€¢ 855 words\n"
    "A FerriÃ¨res-en-Brie, trois candidates sont en lice aux Ã©lections municipales "
    "2026 : que proposent-elles ?\n"
    "Julia Gualtieri\n"
    "Par Julia Gualtieri PubliÃ© le 11 mars 2026 Ã  8h00...\n"
    "Read more\n"
    "\n"
    "https://actu.fr/ile-de-france/ferrieres-en-\n"
    "\n"
    "brie_77181/a-ferrieres-en-brie-trois-ca\n"
    "\n"
    "ndidates-sont-en-lice-aux-elections-mu\n"
    "\n"
    "nicipales-2026-que-proposent-elles_63956385.html\n"
    "This document contains links to Web-sites that are not hosted by CEDROM-SNi.\n"
)
_EUROPRESSE_ENGLISH_DATE_ARTICLE = (
    "Saved documents\n"
    "Thursday, January 15, 2026\n"
    "Paris-Normandie (site web)\n"
    "â€¢ 260 words\n"
    "Lecoq candidat à la mairie, Pirouelle vers le record, Gorgelin de retour à "
    "Soquence... Le point actu à 20 h\n"
    "Jean-Paul Lecoq candidat de la gauche à la mairie du Havre explique son "
    "programme municipal, sa stratégie de campagne, le conseil municipal et les "
    "enjeux locaux dans une interview complète publiée par la rédaction locale.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
)
_NON_EUROPRESSE_TEXT = "Some plain PDF text with no word-count bullets."


def test_is_europresse_format_returns_true_for_multi_article_pdf():
    """Happy path: two or more word-count anchors → Europresse batch detected."""
    assert _is_europresse_format(_EUROPRESSE_TWO_ARTICLES) is True


def test_is_europresse_format_returns_true_for_single_dated_europresse_document():
    """Regression: one-document Europresse PDFs still use the structured parser."""
    assert _is_europresse_format(_EUROPRESSE_SINGLE_DOCUMENT) is True


def test_is_europresse_format_returns_false_for_single_article():
    """Boundary: exactly one word-count anchor is NOT enough — could be any PDF."""
    assert _is_europresse_format(_EUROPRESSE_ONE_ARTICLE) is False


def test_is_europresse_format_returns_false_for_plain_text():
    """Boundary: text with no Europresse bullets must not be misclassified."""
    assert _is_europresse_format(_NON_EUROPRESSE_TEXT) is False


def test_is_europresse_format_returns_false_for_empty_string():
    """Error: empty string must not raise and must return False."""
    assert _is_europresse_format("") is False


# ---------------------------------------------------------------------------
# _segment_europresse_articles
# ---------------------------------------------------------------------------

_MINIMAL_EUROPRESSE_BATCH = (
    "Le Progrès\n"
    "12 janvier 2026\n"
    "Le Progrès\n"
    "• p. 4\n"
    "• 80 words\n"
    "Page lyon5\n"
    "Un titre d'article\n"
    "Corps du premier article avec suffisamment de texte.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
    "L'Internaute\n"
    "15 janvier 2026\n"
    "L'Internaute\n"
    "• p. 8\n"
    "• 150 words\n"
    "Page b3\n"
    "Deuxième titre\n"
    "Corps du deuxième article ici.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
)
_LEGACY_REALISTIC_EUROPRESSE_BATCH = (
    "Dimanche 9 novembre 2025\n"
    "Le Progrès (Lyon)\n"
    "• p. LYON23\n"
    "• 480 words\n"
    "Page\n"
    "lyoe18\n"
    "Page lyon23\n"
    "Villeurbanne\n"
    "« Un sacré foutoir » : Les Républicains cherchent un\n"
    "candidat pour les municipales\n"
    "\n"
    "Olivier Philippe\n"
    "\n"
    "L’ancien député Marc Fraysse est pour l’instant le seul candidat autodéclaré "
    "de la droite à Villeurbanne, mais son investiture reste incertaine malgré une "
    "campagne municipale déjà bien engagée.\n"
    "Villeurbanne. Photo Olivier Philippe\n"
    ".\n"
    "Le candidat poursuit sa campagne avec une réunion publique et plusieurs "
    "propositions détaillées pour la mairie et la gouvernance locale.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
    "Jeudi 4 février 2026\n"
    "Le Progrès (Lyon)\n"
    "• p. LYON24\n"
    "• 220 words\n"
    "Page lyon24\n"
    "Municipales 2026 : six points chauds à surveiller\n"
    "dans le Rhône\n"
    "\n"
    "STEPHANE FRACHET\n"
    "\n"
    "Les alliances locales évoluent rapidement dans la métropole et les équipes "
    "de campagne ajustent leurs stratégies au fil des investitures, des sondages "
    "et des négociations entre partis avant le scrutin municipal.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
)
_REALISTIC_EUROPRESSE_BATCH = _load_fixture_text("europresse_batch_synthetic.txt")


def test_parse_timestamp_parses_french_literal_dates():
    """Regression: French month names from Europresse exports must parse to UTC."""
    assert _parse_timestamp("Jeudi 4 février 2026") == pd.Timestamp(
        "2026-02-04T00:00:00Z"
    )
    assert _parse_timestamp("9 novembre 2025") == pd.Timestamp("2025-11-09T00:00:00Z")
    assert _parse_timestamp("Jeudi 4 février 2026") == pd.Timestamp(
        "2026-02-04T00:00:00Z"
    )


def test_parse_timestamp_parses_english_europresse_ui_dates():
    """Regression: English Europresse UI dates should not reject French articles."""
    assert _parse_timestamp("January 15, 2026") == pd.Timestamp("2026-01-15T00:00:00Z")
    assert _parse_timestamp("Thursday, January 15, 2026") == pd.Timestamp(
        "2026-01-15T00:00:00Z"
    )


def test_segment_europresse_articles_returns_correct_count():
    """Happy path: two anchors in the batch must yield exactly two article dicts."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    assert len(articles) == 2


def test_segment_europresse_articles_isolates_single_article_parse_errors(monkeypatch):
    """Regression: one malformed Europresse segment should not drop sibling articles."""
    original_splitter = corpus_module._extract_europresse_title_and_body

    def fake_splitter(effective_lines: list[str]) -> tuple[str, str]:
        if effective_lines and effective_lines[0].startswith("Un titre"):
            raise ValueError("bad header")
        return original_splitter(effective_lines)

    monkeypatch.setattr(
        corpus_module,
        "_extract_europresse_title_and_body",
        fake_splitter,
    )

    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)

    assert len(articles) == 2
    assert articles[0]["parse_error"] == "ValueError"
    assert articles[0]["title"] == ""
    assert articles[1]["title"].startswith("Deux")


def test_segment_europresse_articles_extracts_date_and_outlet():
    """Happy path: publication date and outlet name must be parsed from article headers."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    first = articles[0]
    assert first["published_at"] == "12 janvier 2026"
    assert first["outlet"] == "Le Progrès"


def test_segment_europresse_articles_extracts_english_header_date():
    """Regression: English UI dates should be valid Europresse publication dates."""
    articles = _segment_europresse_articles(_EUROPRESSE_ENGLISH_DATE_ARTICLE)

    assert len(articles) == 1
    assert articles[0]["published_at"] == "January 15, 2026"
    assert articles[0]["outlet"] == "Paris-Normandie (site web)"
    assert "Lecoq candidat à la mairie" in articles[0]["title"]


def test_segment_europresse_articles_extracts_single_document_pdf():
    """Regression: one-document Europresse PDFs must parse as one article."""
    articles = _segment_europresse_articles(_EUROPRESSE_SINGLE_DOCUMENT)

    assert len(articles) == 1
    assert articles[0]["published_at"] == "21 novembre 2025"
    assert articles[0]["outlet"] == "Midi Libre"
    assert "Myriam Bui-Xuan" in articles[0]["title"]
    assert "Clapiers" in articles[0]["body_text"]


def test_segment_europresse_articles_extracts_wrapped_read_more_url():
    """Regression: wrapped Europresse web-reference URLs should survive parsing."""
    articles = _segment_europresse_articles(_EUROPRESSE_SITE_WEB_REF_ARTICLE)

    assert len(articles) == 1
    assert articles[0]["article_url"] == (
        "https://actu.fr/ile-de-france/ferrieres-en-brie_77181/"
        "a-ferrieres-en-brie-trois-candidates-sont-en-lice-aux-elections-"
        "municipales-2026-que-proposent-elles_63956385.html"
    )


def test_segment_europresse_articles_extracts_title_and_body():
    """Happy path: post-anchor content must be split into title and body."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    first = articles[0]
    assert first["title"] == "Un titre d'article"
    assert "Corps du premier article" in first["body_text"]


def test_segment_europresse_articles_skips_page_ref_lines():
    """Regression: 'Page lyon5' page-reference lines must not appear in the title."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    for article in articles:
        assert not article["title"].lower().startswith("page")


def test_segment_europresse_articles_declared_word_count_is_numeric_string():
    """Boundary: declared_word_count must be the raw digit string captured by the regex."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    assert articles[0]["declared_word_count"] == "80"
    assert articles[1]["declared_word_count"] == "150"


def test_segment_europresse_articles_article_index_is_zero_based():
    """Boundary: article_index must be '0', '1', … (str) matching list position."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    assert articles[0]["article_index"] == "0"
    assert articles[1]["article_index"] == "1"


def test_segment_europresse_articles_returns_empty_for_non_europresse_text():
    """Error: text without any word-count anchor must return an empty list."""
    assert _segment_europresse_articles(_NON_EUROPRESSE_TEXT) == []


def test_segment_europresse_articles_body_ends_before_footer():
    """Regression: boilerplate footer must not leak into any article body."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    footer_fragment = "destined for the exclusive use"
    for article in articles:
        assert footer_fragment not in article["body_text"]


def test_segment_europresse_articles_stitches_titles_and_removes_layout_noise():
    """Regression: wrapped titles, bylines, captions, and page refs must be cleaned."""
    articles = _segment_europresse_articles(_REALISTIC_EUROPRESSE_BATCH)

    assert articles[0]["title"] == (
        "Villeurbanne « Un sacré foutoir » : Les Républicains cherchent un "
        "candidat pour les municipales"
    )
    assert "Olivier Philippe" not in articles[0]["body_text"]
    assert "Photo Olivier Philippe" not in articles[0]["body_text"]
    assert "Page lyon23" not in articles[0]["title"]
    assert "Page lyon23" not in articles[0]["body_text"]

    assert articles[1]["title"] == (
        "Municipales 2026 : six points chauds à surveiller dans le Rhône"
    )
    assert "STEPHANE FRACHET" not in articles[1]["body_text"]


# Fixtures for two-line outlet-name regression tests.
# Europresse prints some outlet names across two lines:
#   (a) dash-continuation: "L'intern@ute (site web) -\nL'Internaute"
#   (b) unclosed-paren:    "France 3 Régions (site web\nréf.) - France 3 Regions"
_SPLIT_OUTLET_DASH_BATCH = (
    "17 mars 2026\n"
    "L'intern@ute (site web) -\n"
    "L'Internaute\n"
    "• p. 1\n"
    "• 450 words\n"
    "Résultat de l'élection municipale 2026 à Villeurbanne\n"
    "Cédric Van Styvendael face à Jean-Paul Bret au second tour.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
    "23 mars 2026\n"
    "Le Progrès (Lyon)\n"
    "• p. 2\n"
    "• 300 words\n"
    "Deuxième article de remplissage.\n"
    "Corps du deuxième article ici.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
)
_SPLIT_OUTLET_PAREN_BATCH = (
    "23 mars 2026\n"
    "France 3 Régions (site web\n"
    "réf.) - France 3 Regions\n"
    "• p. 1\n"
    "• 277 words\n"
    "RÉSULTAT DÉFINITIF à Villeurbanne\n"
    "Le maire socialiste sortant réélu.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
    "23 mars 2026\n"
    "Le Progrès (Lyon)\n"
    "• p. 2\n"
    "• 300 words\n"
    "Deuxième article de remplissage.\n"
    "Corps du deuxième article ici.\n"
    "This document is destined for the exclusive use of the subscriber.\n"
)


def test_segment_europresse_articles_joins_dash_split_outlet_name():
    """Regression: outlet names split across two lines with a trailing dash must be joined."""
    articles = _segment_europresse_articles(_SPLIT_OUTLET_DASH_BATCH)
    assert articles[0]["outlet"] == "L'intern@ute (site web) - L'Internaute"


def test_segment_europresse_articles_joins_unclosed_paren_outlet_name():
    """Regression: outlet names with an unclosed parenthesis must consume the next line."""
    articles = _segment_europresse_articles(_SPLIT_OUTLET_PAREN_BATCH)
    assert (
        articles[0]["outlet"] == "France 3 Régions (site web réf.) - France 3 Regions"
    )


def test_extract_europresse_declared_document_count_reads_cover_page_summary():
    """Regression: parser should read the cover-page article count."""
    assert _extract_europresse_declared_document_count(_REALISTIC_EUROPRESSE_BATCH) == 2


def test_parse_import_batch_fails_fast_on_declared_document_count_mismatch(
    tmp_path, monkeypatch
):
    """DQ contract: cover-page count mismatches must raise before bronze write."""
    pdf_path = tmp_path / "batch.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 placeholder")
    manifest = NewsImportManifest(
        batch_id="batch-001",
        source_system="europresse",
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 1),
        exported_at=datetime(2026, 4, 10, 12, 0, tzinfo=UTC),
        operator="tester",
        access_level="restricted subscription export",
        file_paths=(str(pdf_path),),
        notes="synthetic fixture",
    )
    inspection = ImportBatchInspection(
        batch_id=manifest.batch_id,
        source_system=manifest.source_system,
        files=(
            ImportBatchFile(
                path=str(pdf_path),
                classification="pdf_europresse_batch",
                file_type="pdf",
                has_text_layer=True,
            ),
        ),
        parser_mix={"pdf_europresse_batch": 1},
    )
    mismatched_fixture = _REALISTIC_EUROPRESSE_BATCH.replace(
        "2 documents",
        "3 documents",
        1,
    )
    monkeypatch.setattr(
        "src.ingest.news.corpus._extract_pdf_text",
        lambda _: mismatched_fixture,
    )

    with pytest.raises(DataQualityError, match="declared 3 documents but segmented 2"):
        parse_import_batch(manifest, inspection)


def test_build_fact_article_source_accepts_french_dated_europresse_batch_rows():
    """Regression: cleaned Europresse rows with French dates must survive silver DQ."""
    segmented_articles = _segment_europresse_articles(_REALISTIC_EUROPRESSE_BATCH)
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                f"europresse-{index}",
                title=article["title"],
                body_text=article["body_text"],
                published_at=article["published_at"],
                outlet=article["outlet"],
            )
            for index, article in enumerate(segmented_articles)
        ]
    )

    accepted_df, rejected_df = build_fact_article_source(
        bronze_df,
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 30),
    )

    assert len(accepted_df) == 2
    assert rejected_df.empty
    assert accepted_df["published_date"].tolist() == ["2025-11-09", "2026-02-04"]


def test_build_fact_article_source_accepts_single_document_europresse_rows():
    """Regression: one-document Europresse rows should not be rejected for date parsing."""
    segmented_articles = _segment_europresse_articles(_EUROPRESSE_SINGLE_DOCUMENT)
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                "bui-xuan-single",
                title=segmented_articles[0]["title"],
                body_text=segmented_articles[0]["body_text"],
                published_at=segmented_articles[0]["published_at"],
                outlet=segmented_articles[0]["outlet"],
            )
        ]
    )

    accepted_df, rejected_df = build_fact_article_source(
        bronze_df,
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 30),
    )

    assert len(accepted_df) == 1
    assert rejected_df.empty
    assert accepted_df.loc[0, "published_date"] == "2025-11-21"
    assert bool(accepted_df.loc[0, "has_full_text"]) is True


def test_build_fact_article_source_accepts_english_europresse_header_dates():
    """Regression: English header dates must not reject French Europresse articles."""
    segmented_articles = _segment_europresse_articles(_EUROPRESSE_ENGLISH_DATE_ARTICLE)
    bronze_df = pd.DataFrame(
        [
            _make_bronze_row(
                "lecoq-english-date",
                title=segmented_articles[0]["title"],
                body_text=segmented_articles[0]["body_text"],
                published_at=segmented_articles[0]["published_at"],
                outlet=segmented_articles[0]["outlet"],
            )
        ]
    )

    accepted_df, rejected_df = build_fact_article_source(
        bronze_df,
        window_start=date(2025, 11, 1),
        window_end=date(2026, 4, 30),
    )

    assert len(accepted_df) == 1
    assert rejected_df.empty
    assert accepted_df.loc[0, "published_date"] == "2026-01-15"
    assert bool(accepted_df.loc[0, "has_full_text"]) is True
