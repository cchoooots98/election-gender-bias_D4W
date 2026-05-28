"""Smoke tests for public portfolio documentation."""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_public_docs_have_current_limitations_and_methodology_links():
    """Regression: dashboard-linked methodology documents must exist."""
    readme_text = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    assert (PROJECT_ROOT / "docs" / "limitations.md").exists()
    assert (PROJECT_ROOT / "docs" / "deployment.md").exists()
    assert (PROJECT_ROOT / "docs" / "model-card-nli.md").exists()
    assert "docs/architecture.md" in readme_text
    assert "docs/metric-dictionary.md" in readme_text
    assert "docs/limitations.md" in readme_text
    assert "docs/model-card-nli.md" in readme_text
    assert "docs/deployment.md" in readme_text


def test_public_docs_have_last_updated_banner():
    """Regression: public docs should expose artifact recency for reviewers."""
    doc_paths = [
        PROJECT_ROOT / "README.md",
        PROJECT_ROOT / "docs" / "architecture.md",
        PROJECT_ROOT / "docs" / "data-model.md",
        PROJECT_ROOT / "docs" / "metric-dictionary.md",
        PROJECT_ROOT / "docs" / "runbook.md",
        PROJECT_ROOT / "docs" / "limitations.md",
        PROJECT_ROOT / "docs" / "model-card-nli.md",
        PROJECT_ROOT / "docs" / "deployment.md",
    ]

    for doc_path in doc_paths:
        assert re.search(
            r"Last updated: \d{4}-\d{2}-\d{2}",
            doc_path.read_text(encoding="utf-8"),
        )


def test_public_docs_do_not_show_stale_fact_mention_wide_nlp_fields():
    """Regression: NLP outputs should be documented as normalized facts."""
    stale_field_names = [
        "sentiment_score",
        "prob_negative",
        "frame_politique",
        "frame_scandale",
        "stereo_apparence",
        "stereo_competence",
    ]
    architecture_text = (PROJECT_ROOT / "docs" / "architecture.md").read_text(
        encoding="utf-8"
    )
    data_model_text = (PROJECT_ROOT / "docs" / "data-model.md").read_text(
        encoding="utf-8"
    )
    fact_mention_sections = [
        architecture_text.split("FACT_MENTION {", maxsplit=1)[1].split(
            "}",
            maxsplit=1,
        )[0],
    ]
    if "FACT_MENTION {" in data_model_text:
        fact_mention_sections.append(
            data_model_text.split("FACT_MENTION {", maxsplit=1)[1].split(
                "}",
                maxsplit=1,
            )[0]
        )

    for section in fact_mention_sections:
        for field_name in stale_field_names:
            assert field_name not in section


def test_limitations_avoid_hard_coded_dispersion_ratio():
    """Regression: public limitations should not drift from latest regression artifacts."""
    limitations_text = (PROJECT_ROOT / "docs" / "limitations.md").read_text(
        encoding="utf-8"
    )

    assert "dispersion_ratio = " not in limitations_text
    assert "threshold of 5" in limitations_text


def test_public_findings_use_approximate_or_governed_numbers():
    """Regression: README should not hard-code brittle artifact-level decimals."""
    readme_text = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    stale_exact_values = [
        "27.4",
        "29.8",
        "32.7",
        "27.5",
        "22.1",
        "9.6",
        "2.3x",
        "475 articles",
        "604 articles",
    ]
    for stale_value in stale_exact_values:
        assert stale_value not in readme_text


def test_dbt_sources_define_nlp_input_mention_relationship():
    """Regression: dbt must catch orphan NLP input rows before dashboard publish."""
    sources_text = (
        PROJECT_ROOT / "dbt" / "models" / "marts" / "news" / "sources.yml"
    ).read_text(encoding="utf-8")

    assert "fact_mention_nlp_input" in sources_text
    assert "relationships:" in sources_text
    assert "to: source('silver', 'fact_mention')" in sources_text
    assert "field: mention_id" in sources_text


def test_dashboard_source_avoids_dead_review_sections_and_raw_json():
    """Regression: stakeholder dashboard should not keep dead panels or raw JSON dumps."""
    dashboard_source = (
        PROJECT_ROOT / "src" / "dashboard" / "application.py"
    ).read_text(encoding="utf-8")

    assert "_render_panel2_outlier_sensitivity" not in dashboard_source
    assert "Exposure Robustness Appendix" not in dashboard_source
    assert "st.json(" not in dashboard_source
    assert "q-value (BH)" not in dashboard_source
