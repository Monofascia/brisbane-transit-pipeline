





with validation_errors as (

    select
        start_date, trip_id, stop_sequence
    from `workspace`.`dbt`.`stg_stop_delays`
    group by start_date, trip_id, stop_sequence
    having count(*) > 1

)

select *
from validation_errors


