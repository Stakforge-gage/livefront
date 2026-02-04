
  
    
    

    create  table
      "carton_caps_20260203T224751Z"."main"."dim_school__dbt_tmp"
  
    as (
      select
  school_id,
  name,
  address,
  city,
  state,
  zip_code,
  created_at
from "carton_caps_20260203T224751Z"."main"."stg_schools"
    );
  
  