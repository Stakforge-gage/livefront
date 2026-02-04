select
  u.user_id,
  u.first_name,
  u.last_name,
  u.email,
  u.school_id,
  s.name as school_name,
  s.state as school_state,
  u.created_at,
  u.user_type,
  u.is_verified,
  u.device_id,
  u.marketing_channel
from {{ ref('stg_users') }} u
left join {{ ref('stg_schools') }} s
  on u.school_id = s.school_id
