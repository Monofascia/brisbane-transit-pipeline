
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select delay_s
from `workspace`.`dbt`.`stg_stop_delays`
where delay_s is null



  
  
      
    ) dbt_internal_test