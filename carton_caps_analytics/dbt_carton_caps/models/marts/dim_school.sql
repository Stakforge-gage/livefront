select
  school_id,
  name,
  address,
  city,
  state,
  zip_code,
  created_at
from {{ ref('stg_schools') }}
