
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    purchase_id as unique_field,
    count(*) as n_records

from "carton_caps_20260203T224751Z"."main"."stg_purchases"
where purchase_id is not null
group by purchase_id
having count(*) > 1



  
  
      
    ) dbt_internal_test