
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  -- Sanity check: events that are expected to occur during the analytics window
-- should not fall outside that window.
--
-- We intentionally EXCLUDE 'install' because some users may have created accounts
-- before the analytics window and we still keep those installs for longitudinal context.

with bounds as (
  select
    timestamp '2024-01-01 00:00:00' as start_dt,
    timestamp '2024-06-30 23:59:59' as end_dt
),
scoped as (
  select *
  from "carton_caps_20260203T224751Z"."main"."stg_events"
  where event_type != 'install'
),
out_of_window as (
  select count(*) as cnt
  from scoped, bounds
  where event_at < start_dt or event_at > end_dt
),
total as (
  select count(*) as cnt from scoped
)
select 1
from out_of_window, total
where (out_of_window.cnt * 1.0 / nullif(total.cnt, 0)) > 0.001
  
  
      
    ) dbt_internal_test