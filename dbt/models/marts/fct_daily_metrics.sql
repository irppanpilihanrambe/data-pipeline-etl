-- models/marts/fct_daily_metrics.sql
-- Fact table: daily aggregated event metrics per user

with events as (
    select * from {{ ref('stg_events') }}
),

daily as (
    select
        date_trunc('day', event_at)   as event_date,
        user_id,
        count(*)                       as total_events,
        count(case when event_name = 'purchase' then 1 end) as purchases,
        sum(amount)                    as total_revenue,
        count(distinct event_name)     as unique_event_types
    from events
    group by 1, 2
)

select * from daily
