"""Narrative Streamlit dashboard for the news exposure audit."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.config.settings import GOLD_DIR

_APP_TITLE = "Gender And Media Visibility"
_F_COLOR = "#5b2a7b"
_M_COLOR = "#2fa7a0"
_ACCENT = "#5b2a7b"
_LAVENDER = "#b89af0"
_PINK = "#c93678"
_GENDER_PALETTE = {"F": _F_COLOR, "M": _M_COLOR}
_REQUIRED_ARTIFACTS = {
    "sample_leaders": "sample_leaders.parquet",
    "mart_exposure_metrics": "mart_exposure_metrics.parquet",
    "mart_regression_results": "mart_regression_results.parquet",
    "mart_bootstrap_ci": "mart_bootstrap_ci.parquet",
    "mart_analysis_summary": "mart_analysis_summary.parquet",
    "sample_manifest": "sample_manifest.json",
    "news_corpus_qa_report": "news_corpus_qa_report.json",
}


def _require_columns(
    dataframe: pd.DataFrame,
    *,
    dataframe_name: str,
    required_columns: set[str],
) -> None:
    """Raise when a loaded artifact violates the dashboard schema contract."""
    missing_columns = sorted(required_columns - set(dataframe.columns))
    if missing_columns:
        raise KeyError(
            f"{dataframe_name} missing required columns: " + ", ".join(missing_columns)
        )


def _load_parquet(path: Path) -> pd.DataFrame:
    """Load a Parquet artifact if it exists."""
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def _load_json(path: Path) -> dict[str, Any]:
    """Load a JSON artifact if it exists."""
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def load_dashboard_payload(gold_dir: Path = GOLD_DIR) -> dict[str, Any]:
    """Load all persisted gold-layer artifacts needed by the dashboard."""
    missing_artifacts = [
        artifact_name
        for artifact_name, file_name in _REQUIRED_ARTIFACTS.items()
        if not (gold_dir / file_name).exists()
    ]
    return {
        "sample_df": _load_parquet(gold_dir / "sample_leaders.parquet"),
        "exposure_df": _load_parquet(gold_dir / "mart_exposure_metrics.parquet"),
        "regression_df": _load_parquet(gold_dir / "mart_regression_results.parquet"),
        "bootstrap_df": _load_parquet(gold_dir / "mart_bootstrap_ci.parquet"),
        "analysis_df": _load_parquet(gold_dir / "mart_analysis_summary.parquet"),
        "manifest": _load_json(gold_dir / "sample_manifest.json"),
        "qa_report": _load_json(gold_dir / "news_corpus_qa_report.json"),
        "missing_artifacts": missing_artifacts,
    }


def build_overview_metrics(payload: dict[str, Any]) -> list[dict[str, str]]:
    """Build high-level counters for the trust section of the dashboard."""
    sample_df: pd.DataFrame = payload["sample_df"]
    exposure_df: pd.DataFrame = payload["exposure_df"]
    regression_df: pd.DataFrame = payload["regression_df"]
    manifest: dict[str, Any] = payload["manifest"]
    qa_report: dict[str, Any] = payload["qa_report"]

    qa = qa_report.get("qa", {})
    if exposure_df.empty:
        covered_count = 0
    else:
        _require_columns(
            exposure_df,
            dataframe_name="mart_exposure_metrics",
            required_columns={"article_count"},
        )
        covered_count = int((exposure_df["article_count"] > 0).sum())

    if regression_df.empty:
        regression_issue_count = 0
    else:
        _require_columns(
            regression_df,
            dataframe_name="mart_regression_results",
            required_columns={"status"},
        )
        regression_issue_count = int(
            regression_df["status"]
            .astype(str)
            .str.contains("warning|failed", case=False)
            .sum()
        )

    return [
        {
            "label": "Sampled Leaders",
            "value": str(len(sample_df)),
            "help": "Frozen analytical cohort materialized in gold.sample_leaders.",
            "tone": "purple",
        },
        {
            "label": "Covered Leaders",
            "value": str(covered_count),
            "help": "Candidates with at least one canonical article.",
            "tone": "teal",
        },
        {
            "label": "Sampling Warnings",
            "value": str(len(manifest.get("triggered_warnings", []))),
            "help": "Soft-constraint diagnostics from sample_manifest.json.",
            "tone": "lavender",
        },
        {
            "label": "Regression Issues",
            "value": str(regression_issue_count),
            "help": "Model rows marked with warnings or failures.",
            "tone": "purple",
        },
        {
            "label": "Zero Coverage",
            "value": str(qa.get("zero_coverage_leader_count", 0)),
            "help": "Leaders with no matched article coverage.",
            "tone": "teal",
        },
    ]


def _apply_page_config() -> None:
    """Apply Streamlit page settings and dashboard CSS."""
    st.set_page_config(page_title=_APP_TITLE, layout="wide")
    st.markdown(
        """
        <style>
        :root {
            --bg:#f7f5f1;
            --panel:#ffffff;
            --ink:#30313a;
            --muted:#6f6f78;
            --line:#e5e1da;
            --purple:#5b2a7b;
            --purple-soft:#b89af0;
            --teal:#2fa7a0;
            --pink:#c93678;
            --section-line:#eee9f7;
        }
        .stApp {
            background:#f7f5f1;
            color:var(--ink);
        }
        .block-container {
            max-width:1180px;
            padding-top:1.25rem;
            padding-bottom:3rem;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color:var(--ink);
            letter-spacing:0;
        }
        p, div, span {
            letter-spacing:0;
        }
        .hero {
            padding:2rem 2.5rem;
            border-radius:0;
            background:
                linear-gradient(100deg, rgba(35,35,42,.96), rgba(91,42,123,.96) 55%, rgba(105,39,189,.94)),
                radial-gradient(circle at 76% 20%, rgba(184,154,240,.25), transparent 26%);
            color:#ffffff;
            margin-bottom:1.6rem;
            min-height:210px;
            display:grid;
            grid-template-columns:1fr auto;
            gap:1.25rem;
            align-items:center;
        }
        .hero h1 {
            margin:.4rem 0 0;
            font-family: Arial, sans-serif;
            font-size:2.35rem;
            font-weight:400;
            letter-spacing:.06em;
            text-transform:uppercase;
            color:#ffffff;
        }
        .hero p {
            margin:.75rem 0 0;
            color:#cfe6dc;
            font-size:1.25rem;
        }
        .hero-badge {
            border:1px solid rgba(255,255,255,.45);
            padding:1.2rem 1.4rem;
            min-width:180px;
            text-align:center;
            font-family:Arial, sans-serif;
            text-transform:uppercase;
            color:#ffffff;
        }
        .hero-badge strong {
            display:block;
            font-size:2.2rem;
            font-weight:400;
            color:#ffffff;
        }
        .eyebrow {
            font-size:.72rem;
            text-transform:uppercase;
            color:#ffffff;
            font-weight:700;
            letter-spacing:.16em;
        }
        .lede {
            margin:0 0 2.25rem;
            max-width:1080px;
            font-family:Georgia, "Times New Roman", serif;
            font-size:1.2rem;
            line-height:1.38;
            color:var(--ink);
        }
        .lede strong {
            color:var(--ink);
        }
        .kpi-card {
            border:0;
            border-radius:0;
            padding:1rem .8rem .9rem;
            background:#ffffff;
            min-height:124px;
            box-shadow:0 1px 0 rgba(48,49,58,.04);
            border-top:5px solid var(--purple);
        }
        .kpi-card.teal { border-top-color:var(--teal); }
        .kpi-card.lavender { border-top-color:var(--purple-soft); }
        .kpi-card.pink { border-top-color:var(--pink); }
        .kpi-card.teal .kpi-value { color:var(--teal); }
        .kpi-card.lavender .kpi-value { color:var(--purple-soft); }
        .kpi-card.pink .kpi-value { color:var(--pink); }
        .kpi-card.purple .kpi-value {
            color:var(--purple);
        }
        .kpi-label {
            font-size:.76rem;
            color:var(--muted);
            font-family:Georgia, "Times New Roman", serif;
        }
        .kpi-value {
            margin-top:.25rem;
            font-size:1.9rem;
            font-family:Georgia, "Times New Roman", serif;
            font-weight:400;
            color:var(--purple);
        }
        .kpi-help {
            margin-top:.3rem;
            color:var(--muted);
            font-size:.82rem;
        }
        .callout {
            padding:.1rem 0 .1rem 1rem;
            border-left:4px solid var(--section-line);
            border-radius:0;
            background:transparent;
            color:var(--ink);
            margin:.6rem 0 1.1rem;
            max-width:1040px;
            font-family:Georgia, "Times New Roman", serif;
            font-size:1.05rem;
            line-height:1.38;
        }
        .section-note {
            color:var(--muted);
            font-size:.9rem;
            margin-bottom:.6rem;
        }
        .element-container:has(.js-plotly-plot) {
            background:#ffffff;
        }
        hr {
            border-color:#eee9f7;
            margin:2rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _kpi_card(metric: dict[str, str]) -> str:
    """Render one KPI card as HTML."""
    tone = metric.get("tone", "purple")
    return (
        f'<div class="kpi-card {tone}">'
        f'<div class="kpi-label">{metric["label"]}</div>'
        f'<div class="kpi-value">{metric["value"]}</div>'
        f'<div class="kpi-help">{metric["help"]}</div>'
        "</div>"
    )


def _callout(text: str) -> None:
    """Render a short narrative callout."""
    st.markdown(f'<div class="callout">{text}</div>', unsafe_allow_html=True)


def _plotly_defaults(fig: go.Figure, height: int = 340) -> go.Figure:
    """Apply one visual system to all Plotly figures."""
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=36, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#ffffff",
        colorway=[_F_COLOR, _M_COLOR, _LAVENDER, _PINK],
        font=dict(family="Arial, sans-serif", color="#30313a", size=12),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(gridcolor="#e9e6df", linecolor="#d9d3ca")
    fig.update_yaxes(gridcolor="#e9e6df", linecolor="#d9d3ca")
    return fig


def _candidate_labels(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> pd.DataFrame:
    """Attach readable candidate labels when sample metadata is available."""
    labeled_df = exposure_df.copy()
    label_columns = [
        column
        for column in ["leader_id", "full_name", "commune_name"]
        if column in sample_df.columns
    ]
    if {"leader_id", "full_name"}.issubset(label_columns):
        labeled_df = labeled_df.merge(
            sample_df[label_columns].drop_duplicates("leader_id"),
            on="leader_id",
            how="left",
            validate="one_to_one",
        )
    if "full_name" in labeled_df.columns:
        label_series = labeled_df["full_name"]
    else:
        label_series = labeled_df["leader_id"]
    labeled_df["candidate_label"] = label_series.fillna(labeled_df["leader_id"])
    if "commune_name" in labeled_df.columns:
        labeled_df["candidate_label"] = (
            labeled_df["candidate_label"].astype(str)
            + " - "
            + labeled_df["commune_name"].fillna("").astype(str)
        ).str.rstrip(" -")
    return labeled_df


def _render_hero() -> None:
    """Render the page title and scope statement."""
    st.markdown(
        f"""
        <section class="hero">
            <div>
                <div class="eyebrow">French municipal elections 2026</div>
                <h1>{_APP_TITLE}</h1>
                <p>Local press exposure audit</p>
            </div>
            <div class="hero-badge">
                Data pipeline
                <strong>dbt</strong>
                Gold marts
            </div>
        </section>
        <section class="lede">
            <p>
                <strong>Media visibility is not the same as political parity.</strong>
                This dashboard asks whether women and men in the sampled municipal
                cohort receive comparable local news attention.
            </p>
            <p>
                The current release is an exposure analysis, not a completed NLP
                framing or tone analysis. Python builds the corpus, dbt builds the
                SQL-friendly Gold marts, and Python fits the regression diagnostics.
            </p>
        </section>
        """,
        unsafe_allow_html=True,
    )


def _render_panel0_quality(payload: dict[str, Any]) -> None:
    """Panel 0: data quality and coverage."""
    st.subheader("Can we trust the corpus?")
    _callout(
        "First check whether the corpus is usable: accepted sources, rejected "
        "sources, canonical articles, candidate mentions, and cohort coverage."
    )
    metric_columns = st.columns(5)
    for column, metric in zip(
        metric_columns,
        build_overview_metrics(payload),
        strict=True,
    ):
        with column:
            st.markdown(_kpi_card(metric), unsafe_allow_html=True)

    qa = payload["qa_report"].get("qa", {})
    coverage_count = qa.get("coverage_row_count", 0)
    zero_count = qa.get("zero_coverage_leader_count", 0)
    accepted = qa.get("accepted_article_source_count", 0)
    rejected = qa.get("rejected_article_source_count", 0)
    canonical = qa.get("canonical_article_count", 0)
    mentions = qa.get("mention_count", 0)
    st.caption(
        "Accepted sources: "
        f"{accepted:,} | Rejected sources: {rejected:,} | Canonical articles: "
        f"{canonical:,} | Candidate mentions: {mentions:,} | Coverage: "
        f"{coverage_count - zero_count}/{coverage_count}"
    )


def _render_panel1_headline_finding(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> None:
    """Panel 1: headline finding and mean-vs-median story."""
    st.subheader("Who receives coverage?")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    summary_df = (
        exposure_df.groupby("gender", dropna=False)["article_count"]
        .agg(["mean", "median", "max"])
        .reset_index()
    )
    _callout(
        "Use median and distribution shape before interpreting the mean. News "
        "coverage is highly concentrated, so a single high-exposure outlier can "
        "pull the male mean upward while the typical candidate gap stays smaller."
    )

    col_table, col_chart = st.columns((0.7, 1.3))
    with col_table:
        st.dataframe(summary_df, hide_index=True, use_container_width=True)
    with col_chart:
        fig = px.box(
            exposure_df,
            x="gender",
            y="article_count",
            color="gender",
            points="all",
            color_discrete_map=_GENDER_PALETTE,
            labels={"article_count": "Articles", "gender": "Gender"},
        )
        st.plotly_chart(_plotly_defaults(fig, height=360), use_container_width=True)

    with st.expander("Candidate-level exposure table"):
        labeled_df = _candidate_labels(exposure_df, sample_df)
        display_columns = [
            column
            for column in [
                "candidate_label",
                "gender",
                "city_size_bucket",
                "article_count",
                "headline_mention_count",
                "distinct_source_count",
            ]
            if column in labeled_df.columns
        ]
        st.dataframe(
            labeled_df[display_columns].sort_values("article_count", ascending=False),
            hide_index=True,
            use_container_width=True,
        )


def _render_panel2_gap_sources(
    exposure_df: pd.DataFrame,
    sample_df: pd.DataFrame,
) -> None:
    """Panel 2: where the exposure gap comes from."""
    st.subheader("Where does the gap come from?")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    _callout(
        "Split the gap by city size and candidate ranking. This separates broad "
        "gender patterns from local-market and outlier effects."
    )
    city_df = (
        exposure_df.groupby(["city_size_bucket", "gender"], dropna=False)
        .agg(mean_articles=("article_count", "mean"))
        .reset_index()
    )
    col_city, col_rank = st.columns((1, 1.1))
    with col_city:
        fig = px.bar(
            city_df,
            x="city_size_bucket",
            y="mean_articles",
            color="gender",
            barmode="group",
            color_discrete_map=_GENDER_PALETTE,
            labels={
                "city_size_bucket": "City size",
                "mean_articles": "Mean articles",
            },
        )
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
    with col_rank:
        ranked_df = _candidate_labels(exposure_df, sample_df).nlargest(
            12,
            "article_count",
        )
        fig = px.bar(
            ranked_df.sort_values("article_count"),
            x="article_count",
            y="candidate_label",
            color="gender",
            orientation="h",
            color_discrete_map=_GENDER_PALETTE,
            labels={"article_count": "Articles", "candidate_label": ""},
        )
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)


def _render_panel3_visibility_quality(exposure_df: pd.DataFrame) -> None:
    """Panel 3: distinct sources and headline visibility."""
    st.subheader("Is visibility broad or repetitive?")
    if exposure_df.empty:
        st.info("Run the news corpus pipeline to generate exposure metrics.")
        return

    _callout(
        "Article count alone can hide whether attention is broad or repetitive. "
        "Distinct source count and zero-headline rate measure visibility quality."
    )
    visibility_df = exposure_df.copy()
    visibility_df["headline_rate"] = visibility_df["headline_mention_count"].where(
        visibility_df["article_count"] > 0,
        0,
    ) / visibility_df["article_count"].where(visibility_df["article_count"] > 0, 1)
    zero_headline_df = (
        visibility_df.assign(
            zero_headline=(visibility_df["headline_mention_count"] == 0).astype(int)
        )
        .groupby("gender", dropna=False)
        .agg(zero_headline_rate=("zero_headline", "mean"))
        .reset_index()
    )

    col_source, col_headline = st.columns(2)
    with col_source:
        fig = px.scatter(
            visibility_df,
            x="article_count",
            y="distinct_source_count",
            color="gender",
            size="headline_mention_count",
            color_discrete_map=_GENDER_PALETTE,
            labels={
                "article_count": "Articles",
                "distinct_source_count": "Distinct sources",
            },
        )
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)
    with col_headline:
        fig = px.bar(
            zero_headline_df,
            x="gender",
            y="zero_headline_rate",
            color="gender",
            color_discrete_map=_GENDER_PALETTE,
            labels={
                "zero_headline_rate": "Zero-headline rate",
                "gender": "Gender",
            },
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(_plotly_defaults(fig), use_container_width=True)


def _render_panel4_model_diagnostics(
    regression_df: pd.DataFrame,
    bootstrap_df: pd.DataFrame,
) -> None:
    """Panel 4: Poisson, Negative Binomial, and bootstrap diagnostics."""
    st.subheader("How robust is the model signal?")
    if regression_df.empty:
        st.info("Run the news corpus pipeline to generate regression diagnostics.")
        return

    _callout(
        "The model question is narrow: does gender predict article count after "
        "controlling for city size, region, political bloc, incumbency, and final "
        "round status? Source provenance counts are intentionally excluded."
    )

    gender_rows = regression_df[
        regression_df["variable_name"] == "gender_female"
    ].copy()
    if not gender_rows.empty:
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=gender_rows["coefficient"],
                y=gender_rows["model_name"],
                mode="markers",
                marker=dict(color=_ACCENT, size=11),
                error_x=dict(
                    type="data",
                    array=1.96 * gender_rows["std_error"].fillna(0),
                    visible=True,
                ),
                name="Parametric estimate",
            )
        )
        if not bootstrap_df.empty:
            bootstrap_gender = bootstrap_df[
                bootstrap_df["variable_name"] == "gender_female"
            ]
            if not bootstrap_gender.empty:
                row = bootstrap_gender.iloc[0]
                fig.add_trace(
                    go.Scatter(
                        x=[row["observed_coef"]],
                        y=["bootstrap_negbinom"],
                        mode="markers",
                        marker=dict(color=_PINK, size=11),
                        error_x=dict(
                            type="data",
                            symmetric=False,
                            array=[row["ci_upper_95"] - row["observed_coef"]],
                            arrayminus=[row["observed_coef"] - row["ci_lower_95"]],
                            visible=True,
                        ),
                        name="Bootstrap 95% CI",
                    )
                )
        fig.add_vline(x=0, line_dash="dash", line_color="#9ca3af")
        fig.update_layout(xaxis_title="Coefficient on log scale", yaxis_title="")
        st.plotly_chart(_plotly_defaults(fig, height=330), use_container_width=True)

    dispersion_df = regression_df[
        regression_df["variable_name"] == "_dispersion_ratio"
    ][["model_name", "coefficient", "status"]]
    if not dispersion_df.empty:
        st.caption("Dispersion ratio greater than 1 indicates overdispersion.")
        st.dataframe(dispersion_df, hide_index=True, use_container_width=True)

    with st.expander("Full coefficient table"):
        st.dataframe(regression_df, hide_index=True, use_container_width=True)


def _render_panel5_future_nlp() -> None:
    """Panel 5: future NLP contract."""
    st.subheader("What remains out of scope?")
    _callout(
        "Framing and tone are planned next-phase NLP outputs. The current "
        "mart_framing_metrics table is a stable contract with an unclassified "
        "baseline only. Full text is used transiently during local processing; "
        "persisted public artifacts are redacted and hash-based."
    )
    st.markdown(
        """
        Planned additions:

        - NER (Named Entity Recognition) to anchor candidate mentions.
        - NLI (Natural Language Inference) to classify frames.
        - Sentiment scoring for tone, with model versions pinned for reproducibility.
        - A separate outcome model only after a formal `mart_electoral_outcome_model`
          exists in the data model.
        """
    )


def render_dashboard(gold_dir: Path = GOLD_DIR) -> None:
    """Render the complete narrative dashboard."""
    _apply_page_config()
    _render_hero()
    payload = load_dashboard_payload(gold_dir)

    missing_artifacts = payload["missing_artifacts"]
    if missing_artifacts:
        st.warning(
            "Missing dashboard artifacts: "
            + ", ".join(missing_artifacts)
            + ". Run the news corpus pipeline before interpreting the dashboard."
        )

    _render_panel0_quality(payload)
    st.divider()
    _render_panel1_headline_finding(payload["exposure_df"], payload["sample_df"])
    st.divider()
    _render_panel2_gap_sources(payload["exposure_df"], payload["sample_df"])
    st.divider()
    _render_panel3_visibility_quality(payload["exposure_df"])
    st.divider()
    _render_panel4_model_diagnostics(payload["regression_df"], payload["bootstrap_df"])
    st.divider()
    _render_panel5_future_nlp()


def main() -> None:
    """Entrypoint for ``streamlit run src/dashboard/app.py``."""
    render_dashboard()


if __name__ == "__main__":
    main()
