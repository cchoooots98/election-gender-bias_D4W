# French Municipal Elections 2026 — Gender Bias in Media Coverage

A data analysis portfolio project examining whether French media coverage of the 2026 municipal elections (*élections municipales*) shows systematic differences in how male and female candidates are reported on.

---

## Research Questions

- Do French news outlets cover male and female list leaders at different rates during the 2026 municipal election cycle?
- When coverage does exist, does the framing and tone differ by candidate gender?
- Are observed differences associated with factors such as city size, political affiliation, or incumbent status?

---

## Context

France held its municipal elections on **15 and 22 March 2026**. With over 900,000 candidates across more than 50,000 lists, these elections are one of the largest democratic exercises in the country. Since 2026, all communes — including those under 1,000 inhabitants — are required to use party-list balloting with strict gender-alternation rules, making this a particularly relevant cycle for studying gender representation in both politics and media.

This project focuses on **list leaders** (*têtes de liste*) as the unit of analysis, keeping the scope tractable while capturing the candidates most likely to receive media attention.

---

## Analysis Scope

| Dimension | Detail |
|---|---|
| Analysis window | 1 February – 30 April 2026 |
| Candidates | 10–30 list leaders, balanced 50/50 male/female |
| Sampling | Stratified by city size, geographic region, political affiliation, and incumbent status |
| Language | French-language sources |
| Geography | France (metropolitan and overseas) |

The sample is intentionally small and stratified rather than exhaustive, to allow for deeper per-candidate analysis while remaining reproducible and auditable.

---

## Data Sources

All primary data comes from official French public open data, used under their respective open licences. Sources include:

- **Candidate and list data** — French Interior Ministry (*Ministère de l'Intérieur*) via [data.gouv.fr](https://www.data.gouv.fr), covering first-round candidate lists for the 2026 municipal elections. Data may be updated following legal challenges or corrections; version metadata is tracked.
- **Geographic reference data** — INSEE Code Officiel Géographique (COG) 2026, providing commune/département/région codes and labels used as join keys.
- **Seat and population data** — Interior Ministry dataset on council seat counts and population figures per commune, used for normalising exposure metrics.
- **Incumbent labels** — Interior Ministry RNE (*Répertoire National des Élus*) data on current mayors and councillors, used to flag incumbent candidates.
- **News data** — French-language news articles covering the election period, collected in compliance with applicable access rules and data minimisation principles. Only derived features and limited contextual excerpts are retained; full article text is not stored or published.

---

## Key Metrics (planned)

The analysis is organised around three layers:

1. **Exposure** — article counts, headline mentions, and number of distinct media sources per candidate, normalised by commune population.
2. **Tone and framing** — sentiment signals and topical frame distribution (e.g. policy/governance vs. appearance/private life) for sentences associated with each candidate.
3. **Bias indicators** — gender-level comparisons of framing distributions and stereotype-associated vocabulary frequency, with regression models controlling for city size, political bloc, incumbent status, and region.

---

## Ethical and Legal Notes

- All official datasets are published under open licences; sources and version timestamps are recorded.
- News article collection follows the principle of **data minimisation**: only fields required for the analysis are retained.
- No full article texts are stored, redistributed, or displayed publicly.
- French TDM (text and data mining) rules and CNIL recommendations are respected.

---

## Status

> **In progress.** Bronze ingest layer and Silver dimension tables are implemented. NLP pipeline and Gold mart tables are forthcoming.

---

## Pipeline Architecture

This project uses the **medallion architecture** (Bronze → Silver → Gold),
the standard pattern in modern data engineering (Databricks, dbt, Snowflake).

| Layer | Purpose | Key Tables |
|---|---|---|
| **Bronze** | Faithful raw copies, append-only | `gdelt_results`, `candidates_tour1/2`, `cog_communes`, `seats_population`, `rne_incumbents` |
| **Silver** | Cleaned, validated, analysis-ready | `dim_commune`, `dim_candidate_leader`, `fact_article`, `fact_mention`, `fact_stereotype_word_counts` |
| **Gold** | Aggregated metrics for dashboard | `mart_exposure_metrics`, `mart_framing_metrics`, `mart_bias_indicators`, `mart_regression_results` |
| **Meta** | Pipeline observability | `meta_run`, `meta_source_snapshot` |

The central fact table is **`fact_mention`** (grain: one article × one candidate).
All NLP outputs — sentiment scores, frame classifications, stereotype word counts — are anchored to this grain.

Full logical data model: [`docs/data-model.md`](docs/data-model.md)

### End-to-End Pipeline

```mermaid
flowchart LR
    subgraph SRC["Data Sources"]
        A1["data.gouv.fr\nCandidates · RNE · Seats"]
        A2["INSEE COG 2026"]
        A3["GDELT DOC 2.0 API"]
        A4["News websites\ntrafilatura"]
    end

    subgraph BRZ["🥉 Bronze  (raw copies, append-only)"]
        B1["candidates_tour1/2\nseats_population · rne_incumbents"]
        B2["cog_communes"]
        B3["gdelt_results"]
    end

    subgraph SLV["🥈 Silver  (cleaned · validated · joined)"]
        subgraph DIMS["Dimensions"]
            D1["dim_commune"]
            D2["dim_candidate_leader"]
        end
        subgraph FACTS["Facts"]
            F1["fact_article"]
            F2["fact_mention ★"]
            F3["fact_stereotype\n_word_counts"]
        end
    end

    subgraph NLP["🤖 NLP Pipeline"]
        N1["① Dedup\nSentence-CamemBERT"]
        N2["② NER\nCamemBERT-NER"]
        N3["③ Context\nExtraction"]
        N4["④ Sentiment\nDistilCamemBERT"]
        N5["⑤ Frames\nCamemBERT-NLI × 6"]
        N6["⑥ Stereotype\nLexicon counts"]
        N1 --> N2 --> N3 --> N4 & N5 & N6
    end

    subgraph GLD["🥇 Gold  (analysis-ready aggregates)"]
        G1["mart_exposure_metrics"]
        G2["mart_framing_metrics"]
        G3["mart_bias_indicators"]
        G4["mart_regression_results"]
    end

    A1 --> B1
    A2 --> B2
    A3 --> B3
    A4 --> F1

    B1 & B2 --> D1 & D2
    B3 --> F1

    F1 --> NLP
    NLP --> F2 & F3

    D2 & F2 --> G1 & G2 & G3 & G4
    G1 & G2 & G3 & G4 --> DASH["📊 Streamlit Dashboard"]
```

### Silver Layer — Entity Relationships

```mermaid
erDiagram
    DIM_COMMUNE {
        varchar commune_insee PK
        varchar commune_name
        integer population
        varchar city_size_bucket
    }

    DIM_CANDIDATE_LEADER {
        char    leader_id PK
        varchar gender
        varchar commune_insee FK
        varchar city_size_bucket
        varchar reg_code
        boolean is_incumbent
        boolean advanced_to_tour2
    }

    FACT_ARTICLE {
        char    article_id PK
        varchar url
        varchar fetch_status
        boolean is_duplicate
        char    canonical_article_id FK
    }

    FACT_MENTION {
        char  mention_id PK
        char  article_id FK
        char  leader_id FK
        float sentiment_score
        float prob_negative
        float frame_politique
        float frame_apparence
        float stereo_famille
        float stereo_competence
    }

    FACT_STEREOTYPE_WORD_COUNTS {
        char    mention_id FK
        varchar word
        varchar category
        float   per_1k_words
    }

    DIM_COMMUNE ||--o{ DIM_CANDIDATE_LEADER : "commune_insee"
    DIM_CANDIDATE_LEADER ||--o{ FACT_MENTION : "leader_id"
    FACT_ARTICLE ||--o{ FACT_MENTION : "article_id"
    FACT_MENTION ||--o{ FACT_STEREOTYPE_WORD_COUNTS : "mention_id"
```

---

## Project Structure

```
election-gender-bias_D4W/
  README.md
  docs/               # Project documentation (data model, architecture)
  data/               # Data files (not committed to git)
  src/                # Source code
  tests/              # Tests
```

---

## Motivation

This project is being developed as a portfolio piece demonstrating end-to-end data work: from ingesting and modelling structured official data, to collecting and analysing unstructured text, to producing interpretable quantitative findings. The 2026 municipal elections provide a timely, well-defined, and publicly documented empirical setting.
