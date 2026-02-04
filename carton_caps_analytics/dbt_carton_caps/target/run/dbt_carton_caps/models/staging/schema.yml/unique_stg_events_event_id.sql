
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    event_id as unique_field,
    count(*) as n_records

from "carton_caps_20260203T224751Z"."main"."stg_events"
where event_id is not null
group by event_id
having count(*) > 1



  
  
      
    ) dbt_internal_test