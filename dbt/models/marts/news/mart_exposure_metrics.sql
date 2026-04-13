-- =============================================================================
-- Table: gold.mart_exposure_metrics
-- Grain: One row per sampled candidate leader.
-- Source: gold.sample_leaders, silver.dim_commune, silver.fact_article,
--         silver.fact_mention
-- Purpose: Leader-level exposure counts and source provenance metrics.
-- =============================================================================

with sample_leaders as (
    select
        leader_id,
        gender,
        commune_insee,
        city_size_bucket,
        reg_code,
        nuance_group,
        coalesce(cast(is_incumbent as boolean), false) as is_incumbent,
        coalesce(cast(won_final_round as boolean), false) as won_final_round
    from {{ source('gold', 'sample_leaders') }}
),

population as (
    select
        commune_insee,
        population
    from {{ source('silver', 'dim_commune') }}
),

mention_article as (
    select
        mention.leader_id,
        mention.canonical_article_id,
        coalesce(cast(mention.headline_mention_flag as boolean), false)
            as headline_mention_flag,
        article.outlet_name_normalized,
        coalesce(article.rights_class, '') as rights_class,
        coalesce(article.acquisition_methods, '') as acquisition_methods,
        coalesce(cast(article.has_full_text as boolean), false) as has_full_text
    from {{ source('silver', 'fact_mention') }} as mention
    left join {{ source('silver', 'fact_article') }} as article
        on mention.canonical_article_id = article.canonical_article_id
),

aggregated_mentions as (
    select
        leader_id,
        count(distinct canonical_article_id) as article_count,
        count(
            distinct case
                when headline_mention_flag then canonical_article_id
            end
        ) as headline_mention_count,
        count(distinct outlet_name_normalized) as distinct_source_count,
        count(
            distinct case
                when rights_class = 'restricted_local' then canonical_article_id
            end
        ) as restricted_source_article_count,
        count(
            distinct case
                when acquisition_methods like '%supplemental%'
                    then canonical_article_id
            end
        ) as supplemental_source_article_count,
        count(
            distinct case
                when has_full_text then canonical_article_id
            end
        ) as full_text_article_count
    from mention_article
    group by leader_id
),

final as (
    select
        sample.leader_id,
        sample.gender,
        sample.commune_insee,
        sample.city_size_bucket,
        sample.reg_code,
        sample.nuance_group,
        sample.is_incumbent,
        sample.won_final_round,
        population.population,
        coalesce(aggregated.article_count, 0) as article_count,
        coalesce(aggregated.headline_mention_count, 0) as headline_mention_count,
        coalesce(aggregated.distinct_source_count, 0) as distinct_source_count,
        coalesce(aggregated.restricted_source_article_count, 0)
            as restricted_source_article_count,
        coalesce(aggregated.supplemental_source_article_count, 0)
            as supplemental_source_article_count,
        coalesce(aggregated.full_text_article_count, 0) as full_text_article_count,
        coalesce(aggregated.article_count, 0)
            - coalesce(aggregated.full_text_article_count, 0)
            as metadata_only_article_count,
        coalesce(aggregated.full_text_article_count, 0) > 0 as has_full_text,
        case
            when population.population > 0
                then coalesce(aggregated.article_count, 0)
                    / (population.population / 10000.0)
            else 0.0
        end as exposure_per_10k_population
    from sample_leaders as sample
    left join population
        on sample.commune_insee = population.commune_insee
    left join aggregated_mentions as aggregated
        on sample.leader_id = aggregated.leader_id
)

select * from final
