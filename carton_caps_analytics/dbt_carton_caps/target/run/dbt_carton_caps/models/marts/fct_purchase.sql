
  
    
    

    create  table
      "carton_caps_20260203T224751Z"."main"."fct_purchase__dbt_tmp"
  
    as (
      select
  p.purchase_id,
  p.user_id,
  u.school_id,
  p.product_id,
  pr.category as product_category,
  p.quantity,
  p.price_paid,
  p.points_earned,
  p.purchased_at,
  date_trunc('day', p.purchased_at) as purchase_date,
  p.day_of_week,
  p.hour_of_day
from "carton_caps_20260203T224751Z"."main"."stg_purchases" p
left join "carton_caps_20260203T224751Z"."main"."stg_users" u
  on p.user_id = u.user_id
left join "carton_caps_20260203T224751Z"."main"."stg_products" pr
  on p.product_id = pr.product_id
    );
  
  