with src as (
  select * from {{ source('raw','events') }}
)
select
  cast(event_id as integer) as event_id,
  cast(user_id as integer) as user_id,
  cast(event_type as varchar) as event_type,
  cast(event_at as timestamp) as event_at,
  cast(referral_id as integer) as referral_id,
  cast(metadata_json as varchar) as metadata_json
from src
