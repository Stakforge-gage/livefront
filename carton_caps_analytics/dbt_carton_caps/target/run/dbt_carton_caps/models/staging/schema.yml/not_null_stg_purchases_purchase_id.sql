
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select purchase_id
from "carton_caps_20260203T224751Z"."main"."stg_purchases"
where purchase_id is null



  
  
      
    ) dbt_internal_test