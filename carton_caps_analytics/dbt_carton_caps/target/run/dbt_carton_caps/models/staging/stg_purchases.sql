
  
  create view "carton_caps_20260203T224751Z"."main"."stg_purchases__dbt_tmp" as (
    with src as (
  select * from "carton_caps_20260203T224751Z"."raw"."purchases"
)
select
  cast(purchase_id as integer) as purchase_id,
  cast(user_id as integer) as user_id,
  cast(product_id as integer) as product_id,
  cast(quantity as integer) as quantity,
  cast(price_paid as double) as price_paid,
  cast(points_earned as integer) as points_earned,
  cast(purchased_at as timestamp) as purchased_at,
  cast(day_of_week as varchar) as day_of_week,
  cast(hour_of_day as integer) as hour_of_day
from src
  );
