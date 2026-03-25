"""Tests for the hybrid news benchmark and provider contracts."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import requests

from src.cli import run_news_benchmark as cli_module
from src.config import news_outlets
from src.ingest.news import benchmark, pipeline, queries, storage
from src.ingest.news.models import (
    ArticleFetchResult,
    BenchmarkRunResult,
    CandidateQueryCase,
    OutletDiagnostic,
    ProviderQueryResult,
    RawDocument,
    SearchHit,
)
from src.ingest.news.normalize import build_fact_article_frames
from src.ingest.news.providers import curated, gdelt, gnews
from src.orchestration import news_benchmark_pipeline


class MockResponse:
    """Minimal response double for hermetic HTTP tests."""

    def __init__(
        self,
        *,
        status_code: int = 200,
        url: str = "https://example.test/request",
        json_payload: dict | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self.url = url
        self._json_payload = json_payload or {}
        self.text = text

    def raise_for_status(self) -> None:
        """Raise ``requests.HTTPError`` for non-success statuses."""
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self) -> dict:
        """Return the mocked JSON body."""
        return self._json_payload


def _candidate_case() -> CandidateQueryCase:
    """Build a reusable candidate query case."""
    return CandidateQueryCase(
        leader_id="leader-001",
        full_name="PRÉCIGOUT Sandrine",
        commune_name="Terres-de-Haute-Charente",
        dep_code="16",
        city_size_bucket="small",
        window_start=date(2026, 3, 9),
        window_end=date(2026, 3, 22),
    )


def _search_hit(
    *,
    provider: str = "gdelt",
    provider_tier: str = "tier2_gdelt",
    outlet_key: str = "lemonde.fr",
    article_url: str = "https://www.lemonde.fr/article?id=1&utm_source=newsletter",
    raw_payload_path: str = "raw-doc-1",
    query_text: str = '"Sandrine Précigout" "Terres-de-Haute-Charente"',
) -> SearchHit:
    """Build a reusable search hit."""
    return SearchHit(
        provider=provider,
        provider_tier=provider_tier,
        outlet_key=outlet_key,
        article_url=article_url,
        title="Sandrine Precigout launches campaign",
        published_at=datetime(2026, 3, 10, 8, 30, tzinfo=UTC),
        domain=outlet_key,
        language="fr",
        raw_payload_path=raw_payload_path,
        query_text=query_text,
        query_strategy="precise",
        leader_id="leader-001",
        full_name="PRÉCIGOUT Sandrine",
        commune_name="Terres-de-Haute-Charente",
        dep_code="16",
        city_size_bucket="small",
    )


def test_wave_1_curated_catalog_separates_inventory_from_active_trial_set():
    """Config contract: wave-1 inventory is broader than the currently runnable set.

    After the 2026-03-26 source probe:
    - france3-regions.franceinfo.fr and ici.fr: deactivated (html_index_urls only —
      curated connector does not handle HTML index pages; these rely on GDELT Tier 2).
    - actu.fr: downgraded to browser_required (http_403 bot detection added).
    - bfmtv.com (wave_2), mediapart.fr, marsactu.fr, rue89strasbourg.com,
      lyoncapitale.fr (wave_3): activated after probe confirmed RSS/sitemap access.
    """
    # All wave-1 outlets must stay in the inventory.
    assert "leparisien.fr" in news_outlets.WAVE_1_OUTLET_KEYS
    assert "actu.fr" in news_outlets.WAVE_1_OUTLET_KEYS
    assert "france3-regions.franceinfo.fr" in news_outlets.WAVE_1_OUTLET_KEYS
    assert "ici.fr" in news_outlets.WAVE_1_OUTLET_KEYS
    assert "ouest-france.fr" in news_outlets.WAVE_1_OUTLET_KEYS
    assert "sudouest.fr" in news_outlets.WAVE_1_OUTLET_KEYS

    # Active set after 2026-03-26 probe: 9 outlets with confirmed RSS/sitemap access.
    assert "leparisien.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "lemonde.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "lefigaro.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "franceinfo.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "bfmtv.com" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "mediapart.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "marsactu.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "rue89strasbourg.com" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "lyoncapitale.fr" in news_outlets.ACTIVE_CURATED_OUTLET_KEYS

    # Deactivated after probe: html_index_only outlets depend on GDELT (Tier 2).
    assert (
        "france3-regions.franceinfo.fr" not in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    )
    assert "ici.fr" not in news_outlets.ACTIVE_CURATED_OUTLET_KEYS

    # Blocked by bot detection or missing feeds — inventory only.
    assert "actu.fr" not in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "ouest-france.fr" not in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
    assert "sudouest.fr" not in news_outlets.ACTIVE_CURATED_OUTLET_KEYS

    # Catalog invariant: ACTIVE_CURATED_OUTLET_KEYS must be exactly the set of outlets
    # with curated_trial_enabled=True. Catches accidental activation of blocked outlets.
    for outlet_key, outlet_config in news_outlets.OUTLET_CATALOG.items():
        if outlet_config.curated_trial_enabled:
            assert (
                outlet_key in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
            ), f"{outlet_key} has curated_trial_enabled=True but is missing from ACTIVE_CURATED_OUTLET_KEYS"
        else:
            assert (
                outlet_key not in news_outlets.ACTIVE_CURATED_OUTLET_KEYS
            ), f"{outlet_key} has curated_trial_enabled=False but was found in ACTIVE_CURATED_OUTLET_KEYS"


def test_build_candidate_aliases_normalizes_accents_and_hyphens():
    """Happy path: aliases should preserve natural French forms and add normalized recall."""
    aliases = queries.build_candidate_aliases("YAÏCH Daisy")

    assert aliases["official"] == "YAÏCH Daisy"
    assert aliases["natural_accented"] == "Daisy Yaïch"
    assert aliases["natural_deaccented"] == "Daisy Yaich"
    assert aliases["normalized"] == "Daisy Yaich"


def test_build_gdelt_query_raises_on_invalid_mode():
    """Error path: unsupported query modes must fail fast."""
    with pytest.raises(ValueError, match="Unknown query mode"):
        queries.build_gdelt_query(
            full_name="BAGUET Pierre-Christophe",
            commune_name="Boulogne-Billancourt",
            mode="wide",
        )


def test_fetch_gdelt_window_returns_rate_limited_on_http_429(monkeypatch):
    """Regression: HTTP 429 must map to ``rate_limited``, not to fake zero coverage."""
    candidate_case = _candidate_case()

    monkeypatch.setattr(gdelt, "SCRAPE_RETRY_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(gdelt.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        gdelt.requests,
        "get",
        lambda *args, **kwargs: MockResponse(
            status_code=429,
            url="https://api.gdeltproject.org/api/v2/doc/doc?query=test",
        ),
    )

    result = gdelt._fetch_gdelt_window(
        candidate_case=candidate_case,
        query_text='"Pierre-Christophe Baguet" AND "Boulogne-Billancourt"',
        query_strategy="precise",
        window_start=datetime(2026, 3, 9, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 3, 9, 23, 59, 59, tzinfo=UTC),
    )

    assert result.status == "rate_limited"
    assert result.error_type == "http_429"
    assert result.hits == ()


def test_fetch_gdelt_window_returns_error_on_connection_reset(monkeypatch):
    """Regression: transport-level connection resets must not crash the benchmark run."""
    candidate_case = _candidate_case()

    monkeypatch.setattr(gdelt, "SCRAPE_RETRY_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(gdelt.time, "sleep", lambda _: None)

    def _raise_connection_error(*args, **kwargs):
        raise requests.ConnectionError("socket reset")

    monkeypatch.setattr(gdelt.requests, "get", _raise_connection_error)

    result = gdelt._fetch_gdelt_window(
        candidate_case=candidate_case,
        query_text='"Pierre-Christophe Baguet" AND "Boulogne-Billancourt"',
        query_strategy="precise",
        window_start=datetime(2026, 3, 9, 0, 0, tzinfo=UTC),
        window_end=datetime(2026, 3, 9, 23, 59, 59, tzinfo=UTC),
    )

    assert result.status == "error"
    assert result.error_type == "connection_error"


def test_search_gnews_candidate_maps_success_response_to_hits(monkeypatch):
    """Happy path: GNews responses should map to the shared ``SearchHit`` contract."""
    candidate_case = _candidate_case()
    response_payload = {
        "articles": [
            {
                "url": "https://www.leparisien.fr/article-123",
                "title": "Sandrine Precigout candidate profile",
                "publishedAt": "2026-03-10T12:00:00Z",
            }
        ]
    }

    monkeypatch.setattr(gnews, "GNEWS_API_KEY", "test-key")
    monkeypatch.setattr(
        gnews.requests,
        "get",
        lambda *args, **kwargs: MockResponse(
            url="https://gnews.io/api/v4/search?q=precigout",
            json_payload=response_payload,
        ),
    )

    result = gnews.search_gnews_candidate(candidate_case)

    assert result.status == "success_hits"
    assert len(result.hits) == 1
    assert result.hits[0].provider == "gnews"
    assert result.hits[0].outlet_key == "leparisien.fr"
    assert result.hits[0].published_at == datetime(2026, 3, 10, 12, 0, tzinfo=UTC)


def test_search_curated_outlets_skips_parse_errors_instead_of_crashing(monkeypatch):
    """Regression: malformed connector payloads should degrade to warnings, not abort the run."""
    candidate_case = _candidate_case()

    monkeypatch.setattr(
        curated,
        "ACTIVE_CURATED_OUTLET_KEYS",
        ("lemonde.fr",),
    )
    monkeypatch.setattr(
        curated,
        "_fetch_xml_document",
        lambda url: ("<!doctype html><html>not xml</html>", url, None),
    )

    result = curated.search_curated_outlets(candidate_case)

    assert result.status in {"success_zero", "error"}
    assert result.warning_count == 2
    assert len(result.outlet_diagnostics) == 1
    assert result.outlet_diagnostics[0].documents_fetched == 2
    assert result.outlet_diagnostics[0].status == "fetched"
    assert result.outlet_diagnostics[0].connector_capability == "realtime_only"
    assert result.outlet_diagnostics[0].error_types == ("parse_error",)


def test_search_curated_outlets_marks_blocked_outlet_diagnostic(monkeypatch):
    """Regression: blocked connectors should surface a per-outlet diagnostic row."""
    candidate_case = CandidateQueryCase(
        leader_id="leader-duclos",
        full_name="DUCLOS Erell",
        commune_name="Rennes",
        dep_code="35",
        city_size_bucket="large",
        window_start=date(2026, 3, 9),
        window_end=date(2026, 3, 22),
    )

    monkeypatch.setattr(curated, "ACTIVE_CURATED_OUTLET_KEYS", ("actu.fr",))
    monkeypatch.setattr(
        curated,
        "_fetch_xml_document",
        lambda url: (None, url, "http_403"),
    )

    result = curated.search_curated_outlets(candidate_case)

    assert result.status == "error"
    assert result.error_type == "all_connectors_failed"
    assert len(result.outlet_diagnostics) == 1
    assert result.outlet_diagnostics[0].outlet_key == "actu.fr"
    # actu.fr was downgraded to browser_required after 2026-03-26 probe (http_403 bot detection).
    assert result.outlet_diagnostics[0].connector_capability == "browser_required"
    assert result.outlet_diagnostics[0].status == "blocked"
    assert result.outlet_diagnostics[0].error_types == ("http_403",)


def test_build_fact_article_frames_deduplicates_canonical_urls_but_keeps_discoveries():
    """Regression: one canonical article should keep multiple discovery provenance rows."""
    fact_article_df, discovery_df = build_fact_article_frames(
        [
            _search_hit(
                provider="curated",
                provider_tier="tier1_curated",
                article_url="https://www.lemonde.fr/article?id=1&utm_source=rss",
            ),
            _search_hit(
                provider="gdelt",
                provider_tier="tier2_gdelt",
                article_url="https://lemonde.fr/article?id=1&utm_medium=api",
                raw_payload_path="raw-doc-2",
            ),
        ]
    )

    assert len(fact_article_df) == 1
    assert len(discovery_df) == 2
    assert discovery_df["canonical_url"].nunique() == 1


def test_persist_provider_query_result_logs_snapshot_and_writes_parquet(
    tmp_path,
    monkeypatch,
):
    """Happy path: raw payloads and bronze hits should both be materialized."""
    candidate_case = _candidate_case()
    provider_result = ProviderQueryResult(
        provider="gnews",
        provider_tier="tier3_api",
        status="success_hits",
        hits=(
            _search_hit(
                provider="gnews",
                provider_tier="tier3_api",
                outlet_key="leparisien.fr",
                article_url="https://www.leparisien.fr/article-123",
            ),
        ),
        raw_documents=(
            RawDocument(
                raw_document_key="raw-doc-1",
                source_url="https://gnews.io/api/v4/search?q=precigout",
                payload={
                    "articles": [{"url": "https://www.leparisien.fr/article-123"}]
                },
                row_count=1,
                partition_date="2026-03-09",
            ),
        ),
    )
    logged_snapshots: list[dict[str, object]] = []

    def _capture_snapshot(**kwargs):
        logged_snapshots.append(kwargs)
        return "snapshot-001"

    monkeypatch.setattr(storage, "log_source_snapshot", _capture_snapshot)

    updated_result, artifact_paths = storage.persist_provider_query_result(
        candidate_case=candidate_case,
        provider_result=provider_result,
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        duckdb_path=tmp_path / "warehouse.duckdb",
    )

    assert len(artifact_paths) == 2
    assert len(logged_snapshots) == 1
    assert Path(updated_result.hits[0].raw_payload_path).exists()

    bronze_df = pd.read_parquet(artifact_paths[-1])
    assert bronze_df.loc[0, "provider"] == "gnews"
    assert bronze_df.loc[0, "_query_status"] == "success_hits"


def test_run_news_benchmark_writes_expected_artifacts(tmp_path):
    """Integration slice: benchmark runner should emit results, summary, and provider logs."""
    manifest_path = tmp_path / "news_source_benchmark_cases.yaml"
    manifest_path.write_text(
        """
benchmark_window:
  start_date: "2026-03-09"
  end_date: "2026-03-22"
curated_outlets:
  - lemonde.fr
candidates:
  - leader_id: "leader-001"
    full_name: "PRÉCIGOUT Sandrine"
    commune_name: "Terres-de-Haute-Charente"
    dep_code: "16"
    city_size_bucket: "small"
manual_truth_articles:
  - benchmark_case_id: "truth-001"
    leader_id: "leader-001"
    expected_outlet_key: "lemonde.fr"
    manual_article_url: "https://www.lemonde.fr/article?id=1"
    manual_title: "Sandrine Precigout launches campaign"
    manual_published_at: "2026-03-10"
        """.strip(),
        encoding="utf-8",
    )

    provider_result = ProviderQueryResult(
        provider="gdelt",
        provider_tier="tier2_gdelt",
        status="success_hits",
        hits=(
            _search_hit(
                article_url="https://www.lemonde.fr/article?id=1",
                outlet_key="lemonde.fr",
            ),
        ),
        outlet_diagnostics=(
            OutletDiagnostic(
                outlet_key="lemonde.fr",
                display_name="Le Monde",
                connector_capability="realtime_only",
                status="success_hits",
                documents_fetched=1,
                documents_parsed=1,
                entry_count=1,
                window_entry_count=1,
                hit_count=1,
            ),
        ),
    )

    def _provider_runner_resolver(provider_name: str):
        assert provider_name == "gdelt"
        return lambda candidate_case: provider_result

    def _provider_result_persister(candidate_case, result, **kwargs):
        del candidate_case, kwargs
        return result, ()

    benchmark_result = benchmark.run_news_benchmark(
        benchmark_manifest_path=manifest_path,
        provider_order=("gdelt",),
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        gold_dir=tmp_path / "gold",
        duckdb_path=tmp_path / "warehouse.duckdb",
        provider_runner_resolver=_provider_runner_resolver,
        provider_result_persister=_provider_result_persister,
        article_fetcher=lambda url: ArticleFetchResult(
            canonical_url=url,
            fetch_status="success",
            body_text="x" * 200,
        ),
    )

    summary_payload = json.loads(
        (tmp_path / "gold" / "news_source_benchmark_summary.json").read_text(
            encoding="utf-8"
        )
    )

    assert benchmark_result.status == "success"
    assert benchmark_result.results_row_count == 1
    assert summary_payload["summary_metrics"]["recall"] == 1.0
    assert (
        tmp_path / "gold" / "news_source_benchmark_provider_queries.parquet"
    ).exists()
    assert (
        tmp_path / "gold" / "news_source_benchmark_outlet_diagnostics.parquet"
    ).exists()


def test_run_news_ingest_consumes_manifest_and_marks_partial_on_provider_error(
    tmp_path,
    monkeypatch,
):
    """Boundary: one provider error should yield ``partial`` without dropping successful hits."""
    manifest_path = tmp_path / "sample_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "leader_id": "leader-001",
                        "full_name": "PRÉCIGOUT Sandrine",
                        "commune_name": "Terres-de-Haute-Charente",
                        "dep_code": "16",
                        "city_size_bucket": "small",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    provider_results = {
        "curated": ProviderQueryResult(
            provider="curated",
            provider_tier="tier1_curated",
            status="success_hits",
            hits=(_search_hit(provider="curated", provider_tier="tier1_curated"),),
        ),
        "gdelt": ProviderQueryResult(
            provider="gdelt",
            provider_tier="tier2_gdelt",
            status="error",
            error_type="timeout",
        ),
    }

    monkeypatch.setattr(
        pipeline,
        "get_provider_runner",
        lambda provider_name: lambda candidate_case: provider_results[provider_name],
    )
    monkeypatch.setattr(
        pipeline,
        "persist_provider_query_result",
        lambda candidate_case, provider_result, **kwargs: (provider_result, ()),
    )

    result = pipeline.run_news_ingest(
        sample_manifest_path=manifest_path,
        provider_order=("curated", "gdelt"),
        raw_dir=tmp_path / "raw",
        bronze_dir=tmp_path / "bronze",
        duckdb_path=tmp_path / "warehouse.duckdb",
    )

    assert result.status == "partial"
    assert result.query_count == 2
    assert result.hit_count == 1
    assert result.error_count == 1


def test_news_benchmark_pipeline_logs_meta_run_and_returns_runner_result(
    tmp_path,
    monkeypatch,
):
    """Happy path: orchestration wrapper should reuse the benchmark run_id in meta_run."""
    logged_runs: list[dict[str, object]] = []

    monkeypatch.setattr(
        news_benchmark_pipeline,
        "run_news_benchmark",
        lambda **kwargs: BenchmarkRunResult(
            run_id=kwargs["pipeline_run_id"],
            status="partial",
            error_count=1,
            results_row_count=7,
            artifact_paths=(
                str(tmp_path / "gold" / "news_source_benchmark_results.parquet"),
            ),
            summary_metrics={"provider_availability_rate": 0.5},
        ),
    )
    monkeypatch.setattr(
        news_benchmark_pipeline,
        "log_pipeline_run",
        lambda **kwargs: logged_runs.append(kwargs),
    )

    result = news_benchmark_pipeline.run_news_benchmark_pipeline(
        benchmark_manifest_path=tmp_path / "manifest.yaml",
        gold_dir=tmp_path / "gold",
        bronze_dir=tmp_path / "bronze",
        raw_dir=tmp_path / "raw",
        duckdb_path=tmp_path / "warehouse.duckdb",
    )

    assert result.status == "partial"
    assert logged_runs[0]["flow_name"] == "news_benchmark_pipeline"
    assert logged_runs[0]["run_id"] == result.run_id
    assert logged_runs[0]["rows_ingested"] == 7


def test_news_benchmark_cli_main_returns_zero_on_success(monkeypatch):
    """Happy path: CLI should convert successful runs into exit code 0."""
    monkeypatch.setattr(
        cli_module,
        "run_news_benchmark_pipeline",
        lambda: SimpleNamespace(
            status="success",
            run_id="run-123",
            artifact_paths=["data/gold/news_source_benchmark_results.parquet"],
        ),
    )

    assert cli_module.main() == 0


def test_news_benchmark_cli_main_returns_one_on_failure(monkeypatch):
    """Error path: CLI should convert raised exceptions into exit code 1."""
    monkeypatch.setattr(
        cli_module,
        "run_news_benchmark_pipeline",
        lambda: (_ for _ in ()).throw(ValueError("benchmark failed")),
    )

    assert cli_module.main() == 1


def test_news_benchmark_script_wrapper_reuses_cli_main():
    """Compatibility: the legacy script wrapper should point to the installable CLI."""
    from scripts import run_news_benchmark as script_wrapper

    assert script_wrapper.main is cli_module.main


def test_entry_needs_candidate_verification_keeps_city_result_roundups_in_recall():
    """Boundary: commune-level result pages should trigger body verification before rejection."""
    candidate_case = CandidateQueryCase(
        leader_id="leader-yaich",
        full_name="YAÏCH Daisy",
        commune_name="Cergy",
        dep_code="95",
        city_size_bucket="medium",
        window_start=date(2026, 3, 9),
        window_end=date(2026, 3, 22),
    )

    roundup_text = (
        "https://actu.fr/ile-de-france/cergy_95127/municipales-2026-a-cergy-"
        "le-maire-ps-sortant-jean-paul-jeandon-lemporte-de-185-voix"
    )
    verified_body = "Daisy Yaich obtient 26,97 % des voix a Cergy lors du second tour."

    assert queries.entry_matches_candidate(roundup_text, candidate_case) is False
    assert (
        queries.entry_needs_candidate_verification(roundup_text, candidate_case) is True
    )
    assert queries.entry_matches_candidate(verified_body, candidate_case) is True


def test_search_curated_outlets_expands_sitemap_index_and_verifies_article_body(
    monkeypatch,
):
    """Integration: sitemap indexes should recurse into child sitemaps and verify candidate mentions in article bodies.

    Uses marsactu.fr (historical_capable, sitemap confirmed accessible in 2026-03-26 probe)
    as the representative historical outlet. actu.fr was downgraded to browser_required
    and is no longer a valid test fixture for the historical sitemap path.
    """
    candidate_case = CandidateQueryCase(
        leader_id="leader-yaich",
        full_name="YAÏCH Daisy",
        commune_name="Marseille",
        dep_code="13",
        city_size_bucket="large",
        window_start=date(2026, 3, 9),
        window_end=date(2026, 3, 22),
    )
    # URL deliberately does not contain the candidate name so the connector
    # must fetch and verify the article body — exercising the body_verify path.
    article_url = (
        "https://marsactu.fr/municipales-2026-marseille-"
        "le-maire-sortant-remporte-les-elections_64036900.html"
    )
    sitemap_index_xml = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap>
            <loc>https://marsactu.fr/sitemaps/marseille-2026.xml</loc>
          </sitemap>
        </sitemapindex>
    """.strip()
    child_sitemap_xml = f"""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>{article_url}</loc>
            <lastmod>2026-03-22T20:00:00Z</lastmod>
          </url>
        </urlset>
    """.strip()
    article_html = """
        <html><body>
          <article>
            <p>Jean-Paul Jeandon l'emporte de 185 voix.</p>
            <p>Daisy Yaïch recueille 26,97 % des suffrages au second tour.</p>
          </article>
        </body></html>
    """.strip()

    monkeypatch.setattr(curated, "ACTIVE_CURATED_OUTLET_KEYS", ("marsactu.fr",))

    # Empty RSS feed — connector always iterates rss_urls before sitemap_urls.
    _empty_rss = '<?xml version="1.0"?><rss version="2.0"><channel><title>Marsactu</title></channel></rss>'

    def _fetch_document(url: str):
        if url == "https://marsactu.fr/feed":
            return _empty_rss, url, None
        if url == "https://marsactu.fr/sitemap_index.xml":
            return sitemap_index_xml, url, None
        if url == "https://marsactu.fr/sitemaps/marseille-2026.xml":
            return child_sitemap_xml, url, None
        if url == article_url:
            return article_html, url, None
        raise AssertionError(f"Unexpected URL requested: {url}")

    monkeypatch.setattr(curated, "_fetch_xml_document", _fetch_document)

    result = curated.search_curated_outlets(candidate_case)

    assert result.status == "success_hits"
    assert len(result.hits) == 1
    assert result.hits[0].outlet_key == "marsactu.fr"
    assert result.hits[0].article_url == article_url
    assert result.hits[0].query_strategy == "rss_sitemap_body_verify"
    # 3 raw documents: empty RSS feed + sitemap_index + child sitemap
    # (marsactu.fr has both rss_urls and sitemap_urls, unlike actu.fr which was sitemap-only)
    assert len(result.raw_documents) == 3
    assert len(result.outlet_diagnostics) == 1
    assert result.outlet_diagnostics[0].status == "success_hits"
    assert result.outlet_diagnostics[0].connector_capability == "historical_capable"
    assert result.outlet_diagnostics[0].documents_fetched == 3
    assert result.outlet_diagnostics[0].documents_parsed == 3
    assert result.outlet_diagnostics[0].entry_count == 1
    assert result.outlet_diagnostics[0].window_entry_count == 1
    assert result.outlet_diagnostics[0].hit_count == 1
