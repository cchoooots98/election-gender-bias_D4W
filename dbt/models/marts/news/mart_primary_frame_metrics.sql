-- =============================================================================
-- Table: gold.mart_primary_frame_metrics
-- Grain: One row per sampled candidate leader per primary frame label.
-- Source: gold.sample_leaders, silver.fact_mention,
--         optional silver.fact_mention_nlp_summary
-- Purpose: Consumer-facing primary-frame counts for gender comparisons. Unlike
--          mart_framing_metrics, each mention contributes to only one frame.
-- =============================================================================

{% set has_nlp_summary_source = relation_exists('silver', 'fact_mention_nlp_summary') %}
{% set has_primary_frame_probability =
    column_exists('silver', 'fact_mention_nlp_summary', 'primary_frame_probability')
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

{% if has_nlp_summary_source %}

primary_frame_mentions as (
    select
        leader_id,
        coalesce(nullif(primary_frame_label, ''), 'unclassified') as frame_label,
        count(distinct mention_id) as mention_count,
        {% if has_primary_frame_probability %}
        avg(coalesce(cast(primary_frame_probability as double), 0.0))
            as mean_primary_frame_score
        {% else %}
        0.0 as mean_primary_frame_score
        {% endif %}
    from {{ source('silver', 'fact_mention_nlp_summary') }}
    group by
        leader_id,
        coalesce(nullif(primary_frame_label, ''), 'unclassified')
)

{% else %}

primary_frame_mentions as (
    select
        leader_id,
        'unclassified' as frame_label,
        count(distinct mention_id) as mention_count,
        0.0 as mean_primary_frame_score
    from {{ source('silver', 'fact_mention') }}
    group by leader_id
)

{% endif %}

select
    leader_frames.leader_id,
    leader_frames.frame_label,
    coalesce(primary_frame_mentions.mention_count, 0) as mention_count,
    coalesce(primary_frame_mentions.mean_primary_frame_score, 0.0)
        as mean_primary_frame_score
from leader_frames
left join primary_frame_mentions
    on leader_frames.leader_id = primary_frame_mentions.leader_id
    and leader_frames.frame_label = primary_frame_mentions.frame_label
