
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        start_date, trip_id, stop_sequence
    from `workspace`.`dbt`.`stg_stop_delays`
    group by start_date, trip_id, stop_sequence
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test