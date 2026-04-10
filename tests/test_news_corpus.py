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
from src.ingest.news import corpus_pipeline as corpus_pipeline_module
from src.ingest.news import marts as marts_module
from src.ingest.news.corpus import (
    _extract_europresse_declared_document_count,
    _is_europresse_format,
    _parse_timestamp,
    _segment_europresse_articles,
    build_fact_article,
    build_fact_article_source,
    inspect_import_batch,
    load_news_import_manifest,
    parse_import_batch,
    write_news_import_manifest,
)
from src.ingest.news.corpus_pipeline import run_news_corpus_etl
from src.ingest.news.marts import (
    build_mart_regression_feature_base,
    build_mart_regression_results,
)
from src.ingest.news.matching import build_fact_mentions
from src.ingest.news.models import (
    ImportBatchFile,
    ImportBatchInspection,
    NewsImportManifest,
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
            for index in range(4)
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

    accepted_df, rejected_df = build_fact_article_source(bronze_df)

    assert len(accepted_df) == 4
    assert len(rejected_df) == 1
    assert accepted_df["language"].eq("fr").all()
    assert "published_at unparseable" in rejected_df.loc[0, "_rejection_reason"]


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


def test_run_news_corpus_etl_builds_24_row_exposure_mart(tmp_path):
    """Integration: the main pipeline must preserve the full 24-candidate denominator."""
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
    )

    exposure_df = pd.read_parquet(tmp_path / "gold" / "mart_exposure_metrics.parquet")
    qa_report = json.loads(
        (tmp_path / "gold" / "news_corpus_qa_report.json").read_text(encoding="utf-8")
    )

    assert result.row_counts["fact_article"] == 2
    assert len(exposure_df) == 24
    assert exposure_df["article_count"].sum() == 2
    assert qa_report["qa"]["zero_coverage_leader_count"] == 22


def test_build_mart_regression_results_includes_bloc_and_region_controls():
    """Regression: published model controls must be present in the fitted design."""
    sample_rows = []
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
                sample_rows.append(
                    {
                        "leader_id": leader_id,
                        "gender": gender,
                        "commune_insee": commune_insee,
                        "city_size_bucket": city_size_bucket,
                        "reg_code": reg_code,
                        "nuance_group": nuance_group,
                        "is_incumbent": leader_index % 3 == 0,
                        "won_final_round": leader_index % 4 == 0,
                    }
                )
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

    regression_feature_base_df = build_mart_regression_feature_base(
        pd.DataFrame(sample_rows),
        pd.DataFrame(exposure_rows),
    )

    regression_results_df = build_mart_regression_results(regression_feature_base_df)

    variable_names = set(regression_results_df["variable_name"].astype(str))
    assert any(name.startswith("nuance_group_") for name in variable_names)
    assert any(name.startswith("reg_code_") for name in variable_names)
    assert "won_final_round" in variable_names


def test_build_mart_regression_results_marks_fit_warnings(monkeypatch):
    """Regression: statsmodel warnings must be surfaced in mart_regression_results."""

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
        families=SimpleNamespace(Poisson=lambda: object()),
    )
    monkeypatch.setattr(marts_module, "sm", fake_statsmodels)

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

    assert regression_results_df["status"].nunique() == 1
    assert (
        regression_results_df["status"].iloc[0] == "fitted_with_warning:RuntimeWarning"
    )


def test_run_news_corpus_etl_persists_redacted_text_artifacts(tmp_path):
    """Regression: persisted Parquet artifacts must not retain full article text."""
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
_NON_EUROPRESSE_TEXT = "Some plain PDF text with no word-count bullets."


def test_is_europresse_format_returns_true_for_multi_article_pdf():
    """Happy path: two or more word-count anchors → Europresse batch detected."""
    assert _is_europresse_format(_EUROPRESSE_TWO_ARTICLES) is True


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


def test_segment_europresse_articles_returns_correct_count():
    """Happy path: two anchors in the batch must yield exactly two article dicts."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    assert len(articles) == 2


def test_segment_europresse_articles_extracts_date_and_outlet():
    """Happy path: publication date and outlet name must be parsed from article headers."""
    articles = _segment_europresse_articles(_MINIMAL_EUROPRESSE_BATCH)
    first = articles[0]
    assert first["published_at"] == "12 janvier 2026"
    assert first["outlet"] == "Le Progrès"


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
    assert articles[0]["outlet"] == "France 3 Régions (site web réf.) - France 3 Regions"


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

    accepted_df, rejected_df = build_fact_article_source(bronze_df)

    assert len(accepted_df) == 2
    assert rejected_df.empty
    assert accepted_df["published_date"].tolist() == ["2025-11-09", "2026-02-04"]
