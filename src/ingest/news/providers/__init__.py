"""Provider registry for hybrid news discovery."""

from __future__ import annotations

from collections.abc import Callable

from src.ingest.news.models import CandidateQueryCase, ProviderQueryResult
from src.ingest.news.providers.curated import search_curated_outlets
from src.ingest.news.providers.gdelt import search_gdelt_candidate
from src.ingest.news.providers.gnews import search_gnews_candidate

ProviderRunner = Callable[[CandidateQueryCase], ProviderQueryResult]

_PROVIDER_REGISTRY: dict[str, ProviderRunner] = {
    "curated": search_curated_outlets,
    "gdelt": search_gdelt_candidate,
    "gnews": search_gnews_candidate,
}


def get_provider_runner(provider_name: str) -> ProviderRunner:
    """Return the adapter function for a configured provider name."""
    try:
        return _PROVIDER_REGISTRY[provider_name]
    except KeyError as exc:
        raise ValueError(f"Unknown news provider: {provider_name!r}") from exc


def get_registered_provider_names() -> tuple[str, ...]:
    """Return the provider names supported by the package."""
    return tuple(_PROVIDER_REGISTRY)
