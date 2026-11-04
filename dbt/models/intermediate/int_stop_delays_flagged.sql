select
    *,
    day_of_week between 2 and 6
      and (hour_of_day between 7 and 9 or hour_of_day between 16 and 18) as is_peak,
    delay_s > 300  as is_late_5min,
    delay_s < -60  as is_early
from {{ ref('stg_stop_delays') }}