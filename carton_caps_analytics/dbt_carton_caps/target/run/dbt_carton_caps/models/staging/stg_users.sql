
  
  create view "carton_caps_20260203T224751Z"."main"."stg_users__dbt_tmp" as (
    with src as (
  select * from "carton_caps_20260203T224751Z"."raw"."users"
)
select
  cast(user_id as integer) as user_id,
  cast(first_name as varchar) as first_name,
  cast(last_name as varchar) as last_name,
  lower(cast(email as varchar)) as email,
  cast(school_id as integer) as school_id,
  cast(created_at as timestamp) as created_at,
  cast(user_type as varchar) as user_type,
  cast(is_verified as integer) as is_verified,
  cast(device_id as varchar) as device_id,
  cast(marketing_channel as varchar) as marketing_channel
from src
  );
