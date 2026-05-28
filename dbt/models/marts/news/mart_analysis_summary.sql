-- =============================================================================
-- Table: gold.mart_analysis_summary
-- Grain: One row per analysis, dimension, group, and metric.
-- Source: gold.mart_exposure_metrics
-- Purpose: Long-form dashboard summary so Streamlit does not recalculate
--          business metrics at render time.
-- =============================================================================

with exposure as (
    select
        *,
        case
            when article_count > 0
                then headline_mention_count::double / article_count
        end as headline_rate,
        article_count::double / greatest(distinct_source_count, 1)
            as articles_per_source,
        case when article_count < 10 then 1 else 0 end as low_exposure,
        case when headline_mention_count = 0 then 1 else 0 end as zero_headline
    from {{ ref('mart_exposure_metrics') }}
),

ranked_exposure as (
    select
        gender,
        article_count::double as article_count,
        row_number() over (
            partition by gender
            order by article_count
        ) as row_number,
        count(*) over (partition by gender) as row_count,
        sum(article_count) over (partition by gender) as total_articles
    from exposure
),

gini_by_gender as (
    select
        gender,
        case
            when max(total_articles) = 0 then null
            else
                (2.0 * sum(row_number * article_count)
                    / (max(row_count) * max(total_articles)))
                - ((max(row_count) + 1.0) / max(row_count))
        end as gini
    from ranked_exposure
    group by gender
),

bias_indicators as (
    select * from {{ ref('mart_bias_indicators') }}
),

analysis_rows as (
    select
        'A1' as analysis_id,
        'Exposure Distribution' as analysis_name,
        'gender' as dimension,
        gender as group_label,
        'p10' as metric_name,
        quantile_cont(article_count, 0.10)::double as metric_value,
        '' as note
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'p25',
        quantile_cont(article_count, 0.25)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'median',
        quantile_cont(article_count, 0.50)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'p75',
        quantile_cont(article_count, 0.75)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'p90',
        quantile_cont(article_count, 0.90)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'mean',
        avg(article_count)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'max',
        max(article_count)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'low_exposure_rate',
        avg(low_exposure)::double,
        'Share of candidates with fewer than 10 articles'
    from exposure
    group by gender

    union all

    select
        'A1',
        'Exposure Distribution',
        'gender',
        gender,
        'gini',
        gini,
        '0=equal, 1=concentrated; higher means one candidate dominates'
    from gini_by_gender

    union all

    select
        'A2',
        'Source Diversity',
        city_size_bucket,
        gender || ' - ' || city_size_bucket,
        'mean_distinct_sources',
        avg(distinct_source_count)::double,
        ''
    from exposure
    group by city_size_bucket, gender

    union all

    select
        'A2',
        'Source Diversity',
        city_size_bucket,
        gender || ' - ' || city_size_bucket,
        'median_distinct_sources',
        median(distinct_source_count)::double,
        ''
    from exposure
    group by city_size_bucket, gender

    union all

    select
        'A2',
        'Source Diversity',
        city_size_bucket,
        gender || ' - ' || city_size_bucket,
        'mean_articles_per_source',
        avg(articles_per_source)::double,
        'Lower values indicate coverage spread across more outlets'
    from exposure
    group by city_size_bucket, gender

    union all

    select
        'A2',
        'Source Diversity',
        'overall',
        gender,
        'mean_distinct_sources',
        avg(distinct_source_count)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A2',
        'Source Diversity',
        'overall',
        gender,
        'median_distinct_sources',
        median(distinct_source_count)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A3',
        'Headline Prominence',
        'gender',
        gender,
        'mean_headline_rate',
        avg(headline_rate)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A3',
        'Headline Prominence',
        'gender',
        gender,
        'median_headline_rate',
        median(headline_rate)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A3',
        'Headline Prominence',
        'gender',
        gender,
        'zero_headline_rate',
        avg(zero_headline)::double,
        'Share of candidates never appearing in a headline'
    from exposure
    group by gender

    union all

    select
        'A3',
        'Headline Prominence',
        city_size_bucket,
        gender || ' - ' || city_size_bucket,
        'mean_headline_rate',
        avg(headline_rate)::double,
        ''
    from exposure
    group by city_size_bucket, gender

    union all

    select
        'A4',
        'Electoral Outcomes',
        'gender',
        gender,
        'win_rate',
        avg(case when won_final_round then 1 else 0 end)::double,
        'Share winning the final round'
    from exposure
    group by gender

    union all

    select
        'A4',
        'Electoral Outcomes',
        'gender',
        gender,
        'n_winners',
        sum(case when won_final_round then 1 else 0 end)::double,
        ''
    from exposure
    group by gender

    union all

    select
        'A4',
        'Electoral Outcomes',
        case when won_final_round then 'winner' else 'non_winner' end,
        gender || ' - '
            || case when won_final_round then 'winner' else 'non_winner' end,
        'mean_article_count',
        avg(article_count)::double,
        ''
    from exposure
    group by won_final_round, gender

    union all

    select
        'A4',
        'Electoral Outcomes',
        case when won_final_round then 'winner' else 'non_winner' end,
        gender || ' - '
            || case when won_final_round then 'winner' else 'non_winner' end,
        'median_article_count',
        median(article_count)::double,
        ''
    from exposure
    group by won_final_round, gender

    union all

    select
        'A4',
        'Electoral Outcomes',
        city_size_bucket,
        gender || ' - ' || city_size_bucket,
        'win_rate',
        avg(case when won_final_round then 1 else 0 end)::double,
        ''
    from exposure
    group by city_size_bucket, gender
),

nlp_analysis_rows as (
    select
        'A5' as analysis_id,
        'NLP Audit Signals' as analysis_name,
        'gender' as dimension,
        gender as group_label,
        metric_name,
        metric_value,
        case
            when metric_name = 'nlp_inference_coverage_rate'
                then 'Coverage denominator for scoreable NLP outputs by gender'
            when metric_name = 'mean_unfavorable_tone_share'
                then 'Candidate-aware NLI tone share; descriptive audit signal only'
            when metric_name = 'mean_policy_frame_share'
                then 'Primary politique frame share among primary-frame-classified contexts'
            when metric_name = 'mean_scandal_frame_share'
                then 'Primary scandale frame share among primary-frame-classified contexts'
            when metric_name = 'mean_appearance_private_life_frame_share'
                then 'Primary apparence or vie_privee frame share among primary-frame-classified contexts'
            when metric_name = 'generic_sentiment_coverage_rate'
                then 'Generic sentiment baseline coverage; not candidate-aware tone'
            when metric_name = 'mean_generic_sentiment_score'
                then 'Generic sentiment baseline score; not candidate-aware tone'
            when metric_name = 'mean_stereotype_count_per_1k_tokens'
                then 'Seed lexicon count rate from mention contexts; sparse audit feature'
            else ''
        end as note
    from bias_indicators
    where metric_name in (
        'nlp_inference_coverage_rate',
        'mean_unfavorable_tone_share',
        'mean_policy_frame_share',
        'mean_scandal_frame_share',
        'mean_appearance_private_life_frame_share',
        'generic_sentiment_coverage_rate',
        'mean_generic_sentiment_score',
        'mean_stereotype_count_per_1k_tokens'
    )
),

final_analysis_rows as (
    select * from analysis_rows
    union all
    select * from nlp_analysis_rows
)

select
    analysis_id || '|' || coalesce(dimension, '') || '|'
    || coalesce(group_label, '') || '|' || metric_name as analysis_id,
    analysis_id as analysis_section_id,
    analysis_name,
    dimension,
    group_label,
    metric_name,
    metric_value,
    note
from final_analysis_rows
