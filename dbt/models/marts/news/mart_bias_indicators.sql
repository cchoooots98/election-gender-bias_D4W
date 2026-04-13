-- =============================================================================
-- Table: gold.mart_bias_indicators
-- Grain: One row per gender per aggregate metric.
-- Source: gold.mart_exposure_metrics
-- Purpose: Compact exposure-focused bias summary for dashboard KPIs.
-- =============================================================================

with exposure as (
    select * from {{ ref('mart_exposure_metrics') }}
),

gender_metrics as (
    select
        gender,
        'mean_article_count' as metric_name,
        avg(article_count)::double as metric_value
    from exposure
    group by gender

    union all

    select
        gender,
        'mean_distinct_source_count' as metric_name,
        avg(distinct_source_count)::double as metric_value
    from exposure
    group by gender

    union all

    select
        gender,
        'mean_exposure_per_10k_population' as metric_name,
        avg(exposure_per_10k_population)::double as metric_value
    from exposure
    group by gender

    union all

    select
        gender,
        'total_headline_mention_count' as metric_name,
        sum(headline_mention_count)::double as metric_value
    from exposure
    group by gender
)

select * from gender_metrics
