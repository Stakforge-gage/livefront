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
from {{ ref('stg_purchases') }} p
left join {{ ref('stg_users') }} u
  on p.user_id = u.user_id
left join {{ ref('stg_products') }} pr
  on p.product_id = pr.product_id
