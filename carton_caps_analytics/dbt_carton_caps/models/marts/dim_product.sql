select
  product_id,
  name,
  category,
  price,
  points_per_dollar,
  created_at
from {{ ref('stg_products') }}
