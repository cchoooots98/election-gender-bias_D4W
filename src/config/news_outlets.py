"""Curated French news outlet catalog for Tier 1 discovery.

This module separates three concerns that had been mixed together before:
1. The committed outlet inventory we want to evaluate over time.
2. The current active curated trial set we actually run in code.
3. Lightweight policy metadata that helps explain why a source is active now.

## Probe results (2026-03-26)

Probed all 26 outlets via RSS and sitemap. Key findings:

accessible_rss:
  lemonde.fr, lefigaro.fr, franceinfo.fr, leparisien.fr,
  bfmtv.com, mediapart.fr, lyoncapitale.fr,
  marsactu.fr (+ sitemap, historical since 2015),
  rue89strasbourg.com (+ sitemap, historical since 2012)

html_index_only (not probed — curated connector does not yet handle HTML index):
  france3-regions.franceinfo.fr, ici.fr

blocked (http_403 / http_404 / http_406 / http_503 / connection_error):
  actu.fr (was historical_capable, now 403 — bot detection added),
  ouest-france.fr, sudouest.fr, francebleu.fr, lavoixdunord.fr,
  laprovence.com, letelegramme.fr, lanouvellerepublique.fr,
  nicematin.com, varmatin.com, paris-normandie.fr,
  lcp.fr, publicsenat.fr, 94.citoyens.com, madeinmarseille.net
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OutletConfig:
    """Connector configuration and trial-policy metadata for one outlet."""

    outlet_key: str
    display_name: str
    connector_capability: str
    recommended_role: str
    benchmark_wave: str
    curated_trial_enabled: bool = False
    rss_urls: tuple[str, ...] = ()
    sitemap_urls: tuple[str, ...] = ()
    html_index_urls: tuple[str, ...] = ()


OUTLET_CATALOG: dict[str, OutletConfig] = {
    # ── Wave 1: national outlets ─────────────────────────────────────────────
    "lemonde.fr": OutletConfig(
        outlet_key="lemonde.fr",
        display_name="Le Monde",
        connector_capability="realtime_only",
        recommended_role="benchmark_first",
        benchmark_wave="wave_1",
        curated_trial_enabled=True,
        rss_urls=(
            "https://www.lemonde.fr/rss/une.xml",
            "https://www.lemonde.fr/france/rss_full.xml",
        ),
    ),
    "lefigaro.fr": OutletConfig(
        outlet_key="lefigaro.fr",
        display_name="Le Figaro",
        connector_capability="realtime_only",
        recommended_role="benchmark_first",
        benchmark_wave="wave_1",
        curated_trial_enabled=True,
        rss_urls=("https://www.lefigaro.fr/rss/figaro_actualites.xml",),
    ),
    "franceinfo.fr": OutletConfig(
        outlet_key="franceinfo.fr",
        display_name="Franceinfo",
        connector_capability="realtime_only",
        recommended_role="production_candidate",
        benchmark_wave="wave_1",
        curated_trial_enabled=True,
        rss_urls=("https://www.francetvinfo.fr/titres.rss",),
    ),
    "france3-regions.franceinfo.fr": OutletConfig(
        outlet_key="france3-regions.franceinfo.fr",
        display_name="France 3 Regions",
        # html_index_urls only — curated connector does not yet handle HTML index pages.
        # Contributes zero hits via curated layer until RSS/sitemap is identified.
        # GDELT indexes france3-regions content and covers it via Tier 2.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_1",
        curated_trial_enabled=False,
        html_index_urls=(
            "https://france3-regions.francetvinfo.fr/paris-ile-de-france/",
            "https://france3-regions.francetvinfo.fr/bretagne/",
            "https://france3-regions.francetvinfo.fr/hauts-de-france/",
            "https://france3-regions.francetvinfo.fr/nouvelle-aquitaine/",
        ),
    ),
    "leparisien.fr": OutletConfig(
        outlet_key="leparisien.fr",
        display_name="Le Parisien",
        connector_capability="realtime_only",
        recommended_role="production_candidate",
        benchmark_wave="wave_1",
        curated_trial_enabled=True,
        rss_urls=("https://feeds.leparisien.fr/leparisien/rss/politique",),
    ),
    "ouest-france.fr": OutletConfig(
        outlet_key="ouest-france.fr",
        display_name="Ouest-France",
        # Probe: http_404 on RSS URL — feed moved or removed.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_1",
        curated_trial_enabled=False,
        rss_urls=("https://www.ouest-france.fr/rss/une.xml",),
    ),
    "sudouest.fr": OutletConfig(
        outlet_key="sudouest.fr",
        display_name="Sud Ouest",
        # Probe: http_403 — bot detection blocks unauthenticated requests.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_1",
        curated_trial_enabled=False,
        rss_urls=("https://www.sudouest.fr/arc/outboundfeeds/rss/",),
    ),
    "ici.fr": OutletConfig(
        outlet_key="ici.fr",
        display_name="ici.fr",
        # html_index_urls only — same limitation as france3-regions.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_1",
        curated_trial_enabled=False,
        html_index_urls=(
            "https://www.ici.fr/107-1",
            "https://www.ici.fr/armorique",
            "https://www.ici.fr/nord",
            "https://www.ici.fr/poitou",
            "https://www.ici.fr/la-rochelle",
        ),
    ),
    "actu.fr": OutletConfig(
        outlet_key="actu.fr",
        display_name="Actu.fr",
        # Probe: http_403 on sitemap — bot detection added since last benchmark run.
        # Previously historical_capable; truth articles from this source will rely on GDELT.
        connector_capability="browser_required",
        recommended_role="benchmark_first",
        benchmark_wave="wave_1",
        curated_trial_enabled=False,
        sitemap_urls=("https://actu.fr/sitemap_index.xml",),
    ),
    # ── Wave 2: national / regional mix ──────────────────────────────────────
    "bfmtv.com": OutletConfig(
        outlet_key="bfmtv.com",
        display_name="BFM TV",
        # Probe: RSS accessible, 30 realtime entries.
        connector_capability="realtime_only",
        recommended_role="benchmark_first",
        benchmark_wave="wave_2",
        curated_trial_enabled=True,
        rss_urls=("https://www.bfmtv.com/rss/news-24-7/",),
    ),
    "lavoixdunord.fr": OutletConfig(
        outlet_key="lavoixdunord.fr",
        display_name="La Voix du Nord",
        # Probe: http_403 — consistent with earlier browser_required tag.
        connector_capability="browser_required",
        recommended_role="truth_only",
        benchmark_wave="wave_3",
        curated_trial_enabled=False,
        rss_urls=("https://www.lavoixdunord.fr/arc/outboundfeeds/rss/",),
    ),
    "francebleu.fr": OutletConfig(
        outlet_key="francebleu.fr",
        display_name="France Bleu",
        # Probe: xml_parse_error — feed is malformed or not standard RSS/Atom.
        connector_capability="browser_required",
        recommended_role="benchmark_first",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.francebleu.fr/rss",),
    ),
    "laprovence.com": OutletConfig(
        outlet_key="laprovence.com",
        display_name="La Provence",
        # Probe: http_503 — server unavailable or rate-blocked.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.laprovence.com/arc/outboundfeeds/rss/",),
        sitemap_urls=("https://www.laprovence.com/arc/outboundfeeds/sitemap/",),
    ),
    "letelegramme.fr": OutletConfig(
        outlet_key="letelegramme.fr",
        display_name="Le Télégramme",
        # Probe: http_404 — RSS and sitemap URLs have moved.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.letelegramme.fr/rss/titres.rss",),
        sitemap_urls=("https://www.letelegramme.fr/sitemap.xml",),
    ),
    "lanouvellerepublique.fr": OutletConfig(
        outlet_key="lanouvellerepublique.fr",
        display_name="La Nouvelle République",
        # Probe: http_403 — Arc Publishing bot detection.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.lanouvellerepublique.fr/arc/outboundfeeds/rss/",),
        sitemap_urls=(
            "https://www.lanouvellerepublique.fr/arc/outboundfeeds/sitemap/",
        ),
    ),
    "nicematin.com": OutletConfig(
        outlet_key="nicematin.com",
        display_name="Nice-Matin",
        # Probe: http_406 — Arc Publishing requires specific Accept headers.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.nicematin.com/arc/outboundfeeds/rss/",),
        sitemap_urls=("https://www.nicematin.com/arc/outboundfeeds/sitemap/",),
    ),
    "paris-normandie.fr": OutletConfig(
        outlet_key="paris-normandie.fr",
        display_name="Paris-Normandie",
        # Probe: http_403 — bot detection.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.paris-normandie.fr/feed",),
    ),
    "varmatin.com": OutletConfig(
        outlet_key="varmatin.com",
        display_name="Var-Matin",
        # Probe: http_406 — Arc Publishing requires specific Accept headers.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_2",
        curated_trial_enabled=False,
        rss_urls=("https://www.varmatin.com/arc/outboundfeeds/rss/",),
        sitemap_urls=("https://www.varmatin.com/arc/outboundfeeds/sitemap/",),
    ),
    # ── Wave 3: specialist and local sources ─────────────────────────────────
    "mediapart.fr": OutletConfig(
        outlet_key="mediapart.fr",
        display_name="Mediapart",
        # Probe: RSS accessible, 10 entries, realtime. Subscriber articles are paywalled
        # but headlines and metadata are accessible via the public RSS feed.
        connector_capability="realtime_only",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=True,
        rss_urls=("https://www.mediapart.fr/articles/feed",),
    ),
    "lcp.fr": OutletConfig(
        outlet_key="lcp.fr",
        display_name="LCP",
        # Probe: http_404 — RSS URL not found.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=False,
        rss_urls=("https://www.lcp.fr/rss",),
    ),
    "publicsenat.fr": OutletConfig(
        outlet_key="publicsenat.fr",
        display_name="Public Sénat",
        # Probe: http_404 — RSS URL not found.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=False,
        rss_urls=("https://www.publicsenat.fr/rss/toutes-les-actualites.rss",),
    ),
    "94.citoyens.com": OutletConfig(
        outlet_key="94.citoyens.com",
        display_name="94 Citoyens",
        # Probe: connection_error — domain unreachable.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=False,
        rss_urls=("https://94.citoyens.com/feed",),
        sitemap_urls=("https://94.citoyens.com/sitemap_index.xml",),
    ),
    "rue89strasbourg.com": OutletConfig(
        outlet_key="rue89strasbourg.com",
        display_name="Rue89 Strasbourg",
        # Probe: RSS + sitemap both accessible. Sitemap has ~3000 entries dating
        # to 2012 — deep historical coverage for Strasbourg area.
        connector_capability="historical_capable",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=True,
        rss_urls=("https://www.rue89strasbourg.com/feed",),
        sitemap_urls=("https://www.rue89strasbourg.com/sitemap_index.xml",),
    ),
    "marsactu.fr": OutletConfig(
        outlet_key="marsactu.fr",
        display_name="Marsactu",
        # Probe: RSS + sitemap both accessible. Sitemap has ~3000 entries dating
        # to 2015 — deep historical coverage for Marseille area.
        connector_capability="historical_capable",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=True,
        rss_urls=("https://marsactu.fr/feed",),
        sitemap_urls=("https://marsactu.fr/sitemap_index.xml",),
    ),
    "lyoncapitale.fr": OutletConfig(
        outlet_key="lyoncapitale.fr",
        display_name="Lyon Capitale",
        # Probe: RSS accessible, 22 realtime entries. Lyon area coverage.
        connector_capability="realtime_only",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=True,
        rss_urls=("https://www.lyoncapitale.fr/rss/",),
    ),
    "madeinmarseille.net": OutletConfig(
        outlet_key="madeinmarseille.net",
        display_name="Made in Marseille",
        # Probe: connection_error — domain unreachable.
        connector_capability="browser_required",
        recommended_role="production_candidate",
        benchmark_wave="wave_3",
        curated_trial_enabled=False,
        rss_urls=("https://www.madeinmarseille.net/feed",),
        sitemap_urls=("https://www.madeinmarseille.net/sitemap_index.xml",),
    ),
}

CATALOG_OUTLET_KEYS: tuple[str, ...] = tuple(OUTLET_CATALOG)
WAVE_1_OUTLET_KEYS: tuple[str, ...] = tuple(
    outlet_key
    for outlet_key, outlet_config in OUTLET_CATALOG.items()
    if outlet_config.benchmark_wave == "wave_1"
)
ACTIVE_CURATED_OUTLET_KEYS: tuple[str, ...] = tuple(
    outlet_key
    for outlet_key, outlet_config in OUTLET_CATALOG.items()
    if outlet_config.curated_trial_enabled
)
