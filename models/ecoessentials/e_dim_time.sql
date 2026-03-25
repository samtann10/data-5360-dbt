{{ config(
   materialized = 'table',
   schema = 'dw_eco_essentials'
)}}


-- with cte_time as (
--     select 
--         seq4() % 24 as hour,
--         (seq4() / 24) % 60 as minute,
--         (seq4() / (24*60)) % 60 as second
--     from table(generator(rowcount => 86400))
-- )

-- select
--     {{ dbt_utils.generate_surrogate_key(['hour', 'minute', 'second']) }} as time_key,
--     hour,
--     minute,
--     second,
--     CONCAT(hour, ':', minute, ':', second) as time_string
-- from cte_time

with cte as (
    select 
        extract(hour from eventtimestamp) as hour,
        extract(minute from eventtimestamp) as minute,
        extract(second from eventtimestamp) as second
    from {{ source('eco_emails_landing', 'MARKETINGEMAILS') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['hour', 'minute', 'second']) }} as time_key,
    hour,
    minute,
    second,
    CONCAT(hour, ':', minute, ':', second) as time_string
from cte