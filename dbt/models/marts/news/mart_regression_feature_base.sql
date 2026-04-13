-- =============================================================================
-- Table: gold.mart_regression_feature_base
-- Grain: One row per sampled candidate leader.
-- Source: gold.mart_exposure_metrics
-- Purpose: Stable modeling base consumed by Python statsmodels diagnostics.
-- =============================================================================

select
    leader_id,
    gender,
    case when gender = 'F' then 1 else 0 end as gender_female,
    commune_insee,
    city_size_bucket,
    reg_code,
    nuance_group,
    case when is_incumbent then 1 else 0 end as is_incumbent,
    case when won_final_round then 1 else 0 end as won_final_round,
    population,
    article_count,
    headline_mention_count,
    distinct_source_count,
    restricted_source_article_count,
    supplemental_source_article_count,
    exposure_per_10k_population
from {{ ref('mart_exposure_metrics') }}
