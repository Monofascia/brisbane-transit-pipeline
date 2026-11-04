

with meet_condition as(
  select *
  from `workspace`.`dbt`.`stg_stop_delays`
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not delay_s >= -3600
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not delay_s <= 7200
)

select *
from validation_errors

