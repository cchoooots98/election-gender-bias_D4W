-- =============================================================================
-- Table: gold.mart_framing_metrics
-- Grain: One row per sampled candidate leader per frame label.
-- Source: gold.sample_leaders, silver.fact_mention
-- Purpose: NLP pending contract. Current baseline only stabilizes the shape
--          with unclassified mention counts; it does not claim tone/framing.
-- =============================================================================

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

mentions as (
    select
        leader_id,
        coalesce(cast(frame_label as varchar), 'unclassified') as frame_label,
        mention_id,
        frame_score
    from {{ source('silver', 'fact_mention') }}
),

aggregated_mentions as (
    select
        leader_id,
        frame_label,
        count(mention_id) as mention_count,
        coalesce(avg(cast(frame_score as double)), 0.0) as mean_frame_score
    from mentions
    group by leader_id, frame_label
)

select
    leader_frames.leader_id,
    leader_frames.frame_label,
    coalesce(aggregated_mentions.mention_count, 0) as mention_count,
    coalesce(aggregated_mentions.mean_frame_score, 0.0) as mean_frame_score
from leader_frames
left join aggregated_mentions
    on leader_frames.leader_id = aggregated_mentions.leader_id
    and leader_frames.frame_label = aggregated_mentions.frame_label
