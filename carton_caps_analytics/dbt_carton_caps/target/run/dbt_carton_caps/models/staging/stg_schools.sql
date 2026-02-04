
  
  create view "carton_caps_20260203T224751Z"."main"."stg_schools__dbt_tmp" as (
    with src as (
  select * from "carton_caps_20260203T224751Z"."raw"."schools"
)
select
  cast(school_id as integer) as school_id,
  cast(name as varchar) as name,
  cast(address as varchar) as address,
  cast(city as varchar) as city,
  cast(state as varchar) as state,
  cast(zip_code as varchar) as zip_code,
  cast(created_at as timestamp) as created_at
from src
  );
