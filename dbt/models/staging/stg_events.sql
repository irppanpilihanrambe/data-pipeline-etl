-- models/staging/stg_events.sql
-- Staging layer: clean types, rename columns, basic filters

with source as (
    select * from {{ source('raw', 'events') }}
),

renamed as (
    select
        user_id,
        lower(trim(event))          as event_name,
        cast(timestamp as timestamp) as event_at,
        coalesce(amount, 0.0)       as amount,
        page,
        source                      as traffic_source,
        current_timestamp           as _loaded_at
    from source
    where user_id is not null
)

select * from renamed
