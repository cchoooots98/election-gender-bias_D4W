"""Streamlit dashboard entrypoint.

Streamlit is launched against ``src/dashboard/app.py`` in local and container
deployments. The implementation lives in ``src.dashboard.application``; this
thin compatibility boundary re-exports existing helper names so legacy tests
and dashboard imports keep working while the app is refactored into modules.
"""

from __future__ import annotations

from src.dashboard import application as _application

globals().update(
    {
        name: getattr(_application, name)
        for name in dir(_application)
        if not name.startswith("__")
    }
)


if __name__ == "__main__":
    _application.main()
