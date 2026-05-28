-- =============================================================================
-- Table: gold.mart_frame_article_drilldown
-- Grain: One row per candidate leader x primary frame x canonical article.
-- Source: silver.fact_mention_nlp_summary, silver.fact_article,
--         gold.sample_leaders
-- Purpose: Auditable article drilldown for high-volume frame findings.
-- =============================================================================

{% set has_nlp_summary_source = relation_exists('silver', 'fact_mention_nlp_summary') %}
{% set has_summary_article_key =
    column_exists('silver', 'fact_mention_nlp_summary', 'canonical_article_id')
%}
{% set has_primary_frame_probability =
    column_exists('silver', 'fact_mention_nlp_summary', 'primary_frame_probability')
%}

with frame_mentions as (
    {% if has_nlp_summary_source and has_summary_article_key %}
    select
        summary.leader_id,
        summary.canonical_article_id,
        summary.primary_frame_label as frame_label,
        {% if has_primary_frame_probability %}
        max(summary.primary_frame_probability)::double as max_frame_probability,
        {% else %}
        0.0 as max_frame_probability,
        {% endif %}
        count(*) as mention_count
    from {{ source('silver', 'fact_mention_nlp_summary') }} as summary
    where summary.primary_frame_label in (
        'politique',
        'vie_privee',
        'apparence',
        'scandale',
        'personnalite',
        'securite'
    )
    group by
        summary.leader_id,
        summary.canonical_article_id,
        summary.primary_frame_label
    {% else %}
    select
        cast(null as varchar) as leader_id,
        cast(null as varchar) as canonical_article_id,
        cast(null as varchar) as frame_label,
        cast(null as double) as max_frame_probability,
        cast(0 as bigint) as mention_count
    where false
    {% endif %}
),

final as (
    select
        frame_mentions.leader_id,
        leaders.full_name,
        leaders.gender,
        leaders.commune_name,
        frame_mentions.frame_label,
        frame_mentions.canonical_article_id,
        article.published_date,
        article.outlet_name_normalized,
        article.title,
        frame_mentions.mention_count,
        frame_mentions.max_frame_probability
    from frame_mentions
    inner join {{ source('gold', 'sample_leaders') }} as leaders
        on frame_mentions.leader_id = leaders.leader_id
    inner join {{ source('silver', 'fact_article') }} as article
        on frame_mentions.canonical_article_id = article.canonical_article_id
)

select * from final
