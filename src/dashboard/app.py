"""Streamlit dashboard for the current election-gender-bias analytical artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from src.config.settings import GOLD_DIR

_APP_TITLE = "French Municipal Media Bias Monitor"
_APP_SUBTITLE = (
    "Enterprise-style audit view over the sampled cohort, exposure metrics, and "
    "regression diagnostics."
)


def _load_parquet_if_exists(path: Path) -> pd.DataFrame:
    """Load a Parquet artifact when available, otherwise return an empty DataFrame."""
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _load_json_if_exists(path: Path) -> dict[str, Any]:
    """Load a JSON artifact when available, otherwise return an empty dict."""
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_dashboard_payload(gold_dir: Path = GOLD_DIR) -> dict[str, Any]:
    """Load the dashboard's persisted analytical artifacts from the gold layer."""
    sample_path = gold_dir / "sample_leaders.parquet"
    exposure_path = gold_dir / "mart_exposure_metrics.parquet"
    regression_path = gold_dir / "mart_regression_results.parquet"
    manifest_path = gold_dir / "sample_manifest.json"
    qa_report_path = gold_dir / "news_corpus_qa_report.json"

    artifact_paths = {
        "sample_leaders": sample_path,
        "mart_exposure_metrics": exposure_path,
        "mart_regression_results": regression_path,
        "sample_manifest": manifest_path,
        "news_corpus_qa_report": qa_report_path,
    }
    missing_artifacts = [
        artifact_name
        for artifact_name, artifact_path in artifact_paths.items()
        if not artifact_path.exists()
    ]

    return {
        "sample_df": _load_parquet_if_exists(sample_path),
        "exposure_df": _load_parquet_if_exists(exposure_path),
        "regression_df": _load_parquet_if_exists(regression_path),
        "manifest": _load_json_if_exists(manifest_path),
        "qa_report": _load_json_if_exists(qa_report_path),
        "missing_artifacts": missing_artifacts,
    }


def build_overview_metrics(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Build the headline metrics rendered at the top of the dashboard."""
    sample_df: pd.DataFrame = payload["sample_df"]
    exposure_df: pd.DataFrame = payload["exposure_df"]
    regression_df: pd.DataFrame = payload["regression_df"]
    manifest: dict[str, Any] = payload["manifest"]
    qa_report: dict[str, Any] = payload["qa_report"]

    triggered_warning_count = len(manifest.get("triggered_warnings", []))
    zero_coverage_count = qa_report.get("qa", {}).get("zero_coverage_leader_count", 0)
    regression_statuses = regression_df.get("status", pd.Series(dtype=str))
    regression_issue_count = int(
        regression_statuses.astype(str).str.contains("warning|failed", case=False).sum()
    )

    return [
        {
            "label": "Sampled Leaders",
            "value": str(len(sample_df)),
            "help": "Frozen analytical cohort size materialized in gold.sample_leaders.",
        },
        {
            "label": "Covered Leaders",
            "value": str(
                int((exposure_df.get("article_count", pd.Series(dtype=int)) > 0).sum())
            ),
            "help": "Candidates with at least one canonical article in the corpus.",
        },
        {
            "label": "Sampling Warnings",
            "value": str(triggered_warning_count),
            "help": "Soft-constraint diagnostics persisted in sample_manifest.json.",
        },
        {
            "label": "Regression Issues",
            "value": str(regression_issue_count),
            "help": "Model fits marked with warnings or failures in mart_regression_results.",
        },
        {
            "label": "Zero Coverage",
            "value": str(zero_coverage_count),
            "help": "Leaders that remained in the denominator but received no matched coverage.",
        },
    ]


def _render_header() -> None:
    """Render the dashboard header and lightweight branded styling."""
    st.set_page_config(page_title=_APP_TITLE, layout="wide")
    st.markdown(
        """
        <style>
        :root {
            --bg: #f4f1ea;
            --panel: #fffaf2;
            --ink: #16202a;
            --accent: #8d3c1f;
            --accent-soft: #ead8c9;
            --line: #d6cabb;
        }
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(141, 60, 31, 0.14), transparent 32%),
                linear-gradient(180deg, var(--bg) 0%, #f8f6f1 100%);
            color: var(--ink);
        }
        .hero {
            padding: 1.4rem 1.6rem;
            border: 1px solid var(--line);
            border-radius: 18px;
            background: linear-gradient(135deg, rgba(255, 250, 242, 0.98), rgba(245, 235, 224, 0.98));
            box-shadow: 0 16px 40px rgba(22, 32, 42, 0.08);
            margin-bottom: 1rem;
        }
        .hero h1 {
            margin: 0;
            font-size: 2.1rem;
            letter-spacing: -0.02em;
        }
        .hero p {
            margin: 0.4rem 0 0;
            color: rgba(22, 32, 42, 0.76);
        }
        .metric-card {
            border: 1px solid var(--line);
            border-radius: 16px;
            padding: 0.9rem 1rem;
            background: rgba(255, 250, 242, 0.94);
            min-height: 118px;
        }
        .metric-label {
            font-size: 0.78rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: rgba(22, 32, 42, 0.58);
        }
        .metric-value {
            font-size: 2rem;
            font-weight: 700;
            color: var(--accent);
            margin-top: 0.25rem;
        }
        .metric-help {
            font-size: 0.88rem;
            color: rgba(22, 32, 42, 0.72);
            margin-top: 0.35rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <section class="hero">
            <h1>{_APP_TITLE}</h1>
            <p>{_APP_SUBTITLE}</p>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_dashboard(gold_dir: Path = GOLD_DIR) -> None:
    """Render the Streamlit dashboard from persisted project artifacts."""
    _render_header()
    payload = load_dashboard_payload(gold_dir)

    if payload["missing_artifacts"]:
        st.warning(
            "Some analytical artifacts are missing: "
            + ", ".join(payload["missing_artifacts"])
        )

    metric_columns = st.columns(5)
    for column, metric in zip(
        metric_columns, build_overview_metrics(payload), strict=False
    ):
        column.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-label">{metric["label"]}</div>
                <div class="metric-value">{metric["value"]}</div>
                <div class="metric-help">{metric["help"]}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    sample_df: pd.DataFrame = payload["sample_df"]
    exposure_df: pd.DataFrame = payload["exposure_df"]
    regression_df: pd.DataFrame = payload["regression_df"]
    manifest: dict[str, Any] = payload["manifest"]

    left_column, right_column = st.columns((1.35, 1.0))

    with left_column:
        st.subheader("Cohort Snapshot")
        if sample_df.empty:
            st.info("Run the sampling pipeline to materialize gold.sample_leaders.")
        else:
            st.dataframe(
                sample_df[
                    [
                        "full_name",
                        "gender",
                        "city_size_bucket",
                        "commune_name",
                        "reg_code",
                        "nuance_group",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Exposure Metrics")
        if exposure_df.empty:
            st.info(
                "Run the news corpus pipeline to materialize mart_exposure_metrics."
            )
        else:
            exposure_view = exposure_df[
                [
                    "leader_id",
                    "article_count",
                    "headline_mention_count",
                    "distinct_source_count",
                    "exposure_per_10k_population",
                ]
            ].sort_values(["article_count", "distinct_source_count"], ascending=False)
            st.dataframe(exposure_view, use_container_width=True, hide_index=True)

    with right_column:
        st.subheader("Sampling Diagnostics")
        triggered_warnings = manifest.get("triggered_warnings", [])
        if not triggered_warnings:
            st.success("No sampling warnings recorded in the current manifest.")
        else:
            st.dataframe(pd.DataFrame(triggered_warnings), use_container_width=True)

        st.subheader("Regression Status")
        if regression_df.empty:
            st.info(
                "Run the news corpus pipeline to materialize mart_regression_results."
            )
        else:
            regression_view = regression_df[
                [
                    "model_name",
                    "variable_name",
                    "coefficient",
                    "p_value",
                    "status",
                ]
            ]
            st.dataframe(regression_view, use_container_width=True, hide_index=True)


def main() -> None:
    """CLI-style entrypoint for ``streamlit run src/dashboard/app.py``."""
    render_dashboard()


if __name__ == "__main__":
    main()
