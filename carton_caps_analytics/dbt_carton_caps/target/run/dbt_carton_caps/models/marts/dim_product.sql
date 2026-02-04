
  
    
    

    create  table
      "carton_caps_20260203T224751Z"."main"."dim_product__dbt_tmp"
  
    as (
      select
  product_id,
  name,
  category,
  price,
  points_per_dollar,
  created_at
from "carton_caps_20260203T224751Z"."main"."stg_products"
    );
  
  