-- dbt/models/staging/stg_stop_delays.sql
select
    start_date,
    trip_id,
    stop_sequence,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    route_id,
    route_short_name,
    route_long_name,
    mode,
    delay_s,
    delay_s / 60.0               as delay_min,
    event_local_ts,
    to_date(event_local_ts)      as service_day,
    hour(event_local_ts)         as hour_of_day,
    dayofweek(event_local_ts)    as day_of_week   -- 1 = domenica, 7 = sabato
from `workspace`.`silver`.`stop_delays`
where route_short_name is not null