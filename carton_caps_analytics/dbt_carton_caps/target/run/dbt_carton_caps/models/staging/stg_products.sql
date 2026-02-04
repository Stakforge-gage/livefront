
  
  create view "carton_caps_20260203T224751Z"."main"."stg_products__dbt_tmp" as (
    with src as (
  select * from "carton_caps_20260203T224751Z"."raw"."products"
)
select
  cast(product_id as integer) as product_id,
  cast(name as varchar) as name,
  cast(category as varchar) as category,
  cast(price as double) as price,
  cast(points_per_dollar as integer) as points_per_dollar,
  cast(created_at as timestamp) as created_at
from src
  );
