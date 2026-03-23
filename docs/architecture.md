# Pipeline Architecture — Election Gender Bias D4W

> Full logical data model: [`docs/data-model.md`](data-model.md)

---

## End-to-End Data Pipeline

This diagram shows the complete data flow from ingestion through NLP to the
Streamlit dashboard. It answers: *"Can you design a production-grade DE pipeline?"*

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

---

## Silver Layer — Entity Relationship Diagram

This diagram shows the primary-key / foreign-key relationships between Silver
tables. It answers: *"Do you understand dimensional modelling and fact/dimension
table design?"*

```mermaid
erDiagram
    DIM_COMMUNE {
        varchar commune_insee PK
        varchar commune_name
        varchar dep_code
        varchar reg_code
        integer population
        integer seats_municipal
        integer seats_epci
        varchar city_size_bucket
    }

    DIM_CANDIDATE_LEADER {
        char    leader_id PK
        varchar full_name
        varchar gender
        varchar commune_insee FK
        varchar city_size_bucket
        varchar reg_code
        varchar list_nuance
        varchar nuance_group
        boolean is_incumbent
        float   incumbent_match_score
        boolean advanced_to_tour2
    }

    FACT_ARTICLE {
        char      article_id PK
        varchar   url
        varchar   domain
        timestamp published_at
        varchar   text
        varchar   fetch_status
        boolean   is_duplicate
        char      canonical_article_id FK
    }

    FACT_MENTION {
        char    mention_id PK
        char    article_id FK
        char    leader_id FK
        varchar context_sentences
        integer context_token_count
        float   sentiment_score
        float   prob_negative
        float   frame_politique
        float   frame_vie_privee
        float   frame_apparence
        float   frame_scandale
        float   frame_personnalite
        float   frame_securite
        float   stereo_apparence
        float   stereo_famille
        float   stereo_emotion
        float   stereo_competence
    }

    FACT_STEREOTYPE_WORD_COUNTS {
        char    mention_id FK
        varchar word
        varchar category
        integer count
        float   per_1k_words
    }

    DIM_COMMUNE ||--o{ DIM_CANDIDATE_LEADER : "commune_insee"
    DIM_CANDIDATE_LEADER ||--o{ FACT_MENTION : "leader_id"
    FACT_ARTICLE ||--o{ FACT_MENTION : "article_id"
    FACT_ARTICLE ||--o| FACT_ARTICLE : "canonical_article_id (self-ref)"
    FACT_MENTION ||--o{ FACT_STEREOTYPE_WORD_COUNTS : "mention_id"
```

---

## Technology Stack

| Component | Tool | Industry Analogue |
|---|---|---|
| Warehouse | DuckDB (single file) | Snowflake / BigQuery (local) |
| File format | Parquet (Snappy compressed) | Delta Lake / ORC |
| Orchestration | Apache Airflow | Prefect, Dagster |
| SQL transforms | dbt-duckdb | dbt-snowflake, dbt-bigquery |
| French NLP | CamemBERT family (HuggingFace) | BERT (English equivalent) |
| Text extraction | trafilatura | Scrapy, newspaper3k |
| Dashboard | Streamlit | Tableau, Looker |
| CI/CD | GitHub Actions | Jenkins, CircleCI |

