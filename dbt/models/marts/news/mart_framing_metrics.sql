-- =============================================================================
-- Table: gold.mart_framing_metrics
-- Grain: One row per sampled candidate leader per frame label.
-- Source: gold.sample_leaders, silver.fact_mention,
--         optional silver.fact_mention_nlp_summary,
--         optional silver.fact_mention_frame_score
-- Purpose: Consumer-facing frame metrics with a backward-compatible
--          unclassified fallback before NLP Silver outputs exist.
-- =============================================================================

{% set has_nlp_framing_sources =
    relation_exists('silver', 'fact_mention_nlp_summary')
    and relation_exists('silver', 'fact_mention_frame_score')
%}

with frame_labels as (
    select * from (
        values
            ('politique'),
            ('vie_privee'),
            ('apparence'),
            ('scandale'),
            ('personnalite'),
            ('securite'),
            ('unclassified')
    ) as frame_labels(frame_label)
),

leader_frames as (
    select
        leaders.leader_id,
        frame_labels.frame_label
    from {{ source('gold', 'sample_leaders') }} as leaders
    cross join frame_labels
),

-- Note: zero-coverage leaders get unclassified rows via the final left join
-- to leader_frames.
{% if has_nlp_framing_sources %}

scored_frame_metrics as (
    select
        nlp_summary.leader_id,
        frame_score.frame_label,
        count(
            distinct case
                when cast(frame_score.passes_threshold as boolean)
                    then frame_score.mention_id
            end
        ) as mention_count,
        coalesce(avg(cast(frame_score.frame_probability as double)), 0.0)
            as mean_frame_score
    from {{ source('silver', 'fact_mention_frame_score') }} as frame_score
    inner join {{ source('silver', 'fact_mention_nlp_summary') }} as nlp_summary
        on frame_score.mention_id = nlp_summary.mention_id
    group by nlp_summary.leader_id, frame_score.frame_label
),

unclassified_frame_metrics as (
    select
        leader_id,
        'unclassified' as frame_label,
        count(distinct mention_id) as mention_count,
        0.0 as mean_frame_score
    from {{ source('silver', 'fact_mention_nlp_summary') }}
    where nlp_enrichment_status in ('skipped', 'failed')
        or coalesce(primary_frame_label, 'unclassified') = 'unclassified'
    group by leader_id
),

aggregated_mentions as (
    select * from scored_frame_metrics
    union all
    select * from unclassified_frame_metrics
)

{% else %}

aggregated_mentions as (
    select
        leader_id,
        'unclassified' as frame_label,
        count(distinct mention_id) as mention_count,
        0.0 as mean_frame_score
    from {{ source('silver', 'fact_mention') }}
    group by leader_id
)

{% endif %}

select
    leader_frames.leader_id,
    leader_frames.frame_label,
    coalesce(aggregated_mentions.mention_count, 0) as mention_count,
    coalesce(aggregated_mentions.mean_frame_score, 0.0) as mean_frame_score
from leader_frames
left join aggregated_mentions
    on leader_frames.leader_id = aggregated_mentions.leader_id
    and leader_frames.frame_label = aggregated_mentions.frame_label
