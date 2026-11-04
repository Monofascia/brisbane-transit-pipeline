-- dbt/models/marts/mart_punctuality_by_mode.sql
select
    mode,
    count(*)                                                     as observations,
    round(avg(delay_min), 2)                                     as avg_delay_min,
    round(avg(case when not is_late_5min and not is_early then 1.0 else 0.0 end), 3) as share_on_time
from `workspace`.`dbt`.`int_stop_delays_flagged`
group by all