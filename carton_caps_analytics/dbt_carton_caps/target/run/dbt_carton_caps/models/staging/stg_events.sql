
  
  create view "carton_caps_20260203T224751Z"."main"."stg_events__dbt_tmp" as (
    with src as (
  select * from "carton_caps_20260203T224751Z"."raw"."events"
)
select
  cast(event_id as integer) as event_id,
  cast(user_id as integer) as user_id,
  cast(event_type as varchar) as event_type,
  cast(event_at as timestamp) as event_at,
  cast(referral_id as integer) as referral_id,
  cast(metadata_json as varchar) as metadata_json
from src
  );
