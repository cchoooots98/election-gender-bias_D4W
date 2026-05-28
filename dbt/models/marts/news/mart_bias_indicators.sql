-- =============================================================================
-- Table: gold.mart_bias_indicators
-- Grain: One row per gender per aggregate metric.
-- Source: gold.mart_exposure_metrics and optional Silver NLP outputs
-- Purpose: Compact gender-level bias indicators for dashboard KPI blocks.
-- =============================================================================

{% set has_nlp_bias_sources =
    relation_exists('silver', 'fact_mention_nlp_input')
    and relation_exists('silver', 'fact_mention_nlp_summary')
    and relation_exists('silver', 'fact_stereotype_word_counts')
%}

with exposure as (
    select * from {{ ref('mart_exposure_metrics') }}
),

exposure_metrics as (
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
),

{% if has_nlp_bias_sources %}

nlp_summary_by_leader as (
    select
        leaders.leader_id,
        leaders.gender,
        count(summary.mention_id) as total_nlp_mentions,
        count(
            case when summary.nlp_enrichment_status = 'scored' then 1 end
        ) as scored_mentions,
        count(
            case when summary.target_tone_probability is not null then 1 end
        ) as tone_scoreable_mentions,
        count(
            case when summary.target_tone_label = 'unfavorable' then 1 end
        ) as unfavorable_tone_mentions,
        count(
            case
                when summary.primary_frame_label in (
                    'personnalite',
                    'scandale',
                    'politique',
                    'apparence',
                    'securite',
                    'vie_privee'
                )
                    then 1
            end
        ) as primary_frame_classified_mentions,
        count(
            case when summary.primary_frame_label = 'politique' then 1 end
        ) as policy_frame_mentions,
        count(
            case when summary.primary_frame_label = 'scandale' then 1 end
        ) as scandal_frame_mentions,
        count(
            case
                when summary.primary_frame_label in ('apparence', 'vie_privee')
                    then 1
            end
        ) as appearance_private_life_frame_mentions,
        count(
            case
                when summary.generic_sentiment_label is not null
                    and summary.generic_sentiment_score is not null
                    then 1
            end
        ) as generic_sentiment_mentions,
        avg(
            case
                when summary.generic_sentiment_score is not null
                    then summary.generic_sentiment_score::double
            end
        ) as mean_generic_sentiment_score
    from {{ source('gold', 'sample_leaders') }} as leaders
    left join {{ source('silver', 'fact_mention_nlp_summary') }} as summary
        on leaders.leader_id = summary.leader_id
    group by leaders.leader_id, leaders.gender
),

-- Denominator: lexicon-eligible mentions only.
stereotype_by_mention as (
    select
        nlp_input.leader_id,
        nlp_input.mention_id,
        coalesce(sum(word_counts.count_per_1k_tokens), 0.0)
            as stereotype_count_per_1k_tokens
    from {{ source('silver', 'fact_mention_nlp_input') }} as nlp_input
    left join {{ source('silver', 'fact_stereotype_word_counts') }} as word_counts
        on nlp_input.mention_id = word_counts.mention_id
    where cast(nlp_input.eligible_for_lexicon as boolean)
    group by nlp_input.leader_id, nlp_input.mention_id
),

stereotype_by_leader as (
    select
        leaders.leader_id,
        leaders.gender,
        coalesce(
            avg(stereotype_by_mention.stereotype_count_per_1k_tokens),
            0.0
        ) as stereotype_count_per_1k_tokens
    from {{ source('gold', 'sample_leaders') }} as leaders
    left join stereotype_by_mention
        on leaders.leader_id = stereotype_by_mention.leader_id
    group by leaders.leader_id, leaders.gender
),

leader_nlp_metrics as (
    select
        summary.leader_id,
        summary.gender,
        case
            when summary.total_nlp_mentions > 0
                then summary.scored_mentions::double / summary.total_nlp_mentions
            else 0.0
        end as nlp_inference_coverage_rate,
        case
            when summary.tone_scoreable_mentions > 0
                then summary.unfavorable_tone_mentions::double
                    / summary.tone_scoreable_mentions
            else 0.0
        end as unfavorable_tone_share,
        case
            when summary.primary_frame_classified_mentions > 0
                then summary.policy_frame_mentions::double
                    / summary.primary_frame_classified_mentions
            else 0.0
        end as policy_frame_share,
        case
            when summary.primary_frame_classified_mentions > 0
                then summary.scandal_frame_mentions::double
                    / summary.primary_frame_classified_mentions
            else 0.0
        end as scandal_frame_share,
        case
            when summary.primary_frame_classified_mentions > 0
                then summary.appearance_private_life_frame_mentions::double
                    / summary.primary_frame_classified_mentions
            else 0.0
        end as appearance_private_life_frame_share,
        case
            when summary.total_nlp_mentions > 0
                then summary.generic_sentiment_mentions::double
                    / summary.total_nlp_mentions
            else 0.0
        end as generic_sentiment_coverage_rate,
        coalesce(summary.mean_generic_sentiment_score, 0.0)
            as mean_generic_sentiment_score,
        stereotype.stereotype_count_per_1k_tokens
    from nlp_summary_by_leader as summary
    inner join stereotype_by_leader as stereotype
        on summary.leader_id = stereotype.leader_id
),

nlp_metrics as (
    select
        gender,
        'nlp_inference_coverage_rate' as metric_name,
        avg(nlp_inference_coverage_rate)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'mean_unfavorable_tone_share' as metric_name,
        avg(unfavorable_tone_share)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'mean_policy_frame_share' as metric_name,
        avg(policy_frame_share)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'mean_scandal_frame_share' as metric_name,
        avg(scandal_frame_share)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'mean_appearance_private_life_frame_share' as metric_name,
        avg(appearance_private_life_frame_share)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'generic_sentiment_coverage_rate' as metric_name,
        avg(generic_sentiment_coverage_rate)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'mean_generic_sentiment_score' as metric_name,
        avg(mean_generic_sentiment_score)::double as metric_value
    from leader_nlp_metrics
    group by gender

    union all

    select
        gender,
        'mean_stereotype_count_per_1k_tokens' as metric_name,
        avg(stereotype_count_per_1k_tokens)::double as metric_value
    from leader_nlp_metrics
    group by gender
),

{% endif %}

final as (
    select * from exposure_metrics
    {% if has_nlp_bias_sources %}
    union all
    select * from nlp_metrics
    {% endif %}
)

select * from final
