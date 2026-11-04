-- dbt/models/marts/mart_delay_by_stop.sql
select
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    mode,
    count(*)                  as observations,
    round(avg(delay_min), 2)  as avg_delay_min
from {{ ref('int_stop_delays_flagged') }}
group by all
having count(*) >= 50