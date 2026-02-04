
  
  create view "carton_caps_20260203T224751Z"."main"."stg_referrals__dbt_tmp" as (
    with src as (
  select * from "carton_caps_20260203T224751Z"."raw"."referrals"
)
select
  cast(referral_id as integer) as referral_id,
  cast(referrer_user_id as integer) as referrer_user_id,
  lower(cast(referred_email as varchar)) as referred_email,
  cast(referred_user_id as integer) as referred_user_id,
  cast(referral_code as varchar) as referral_code,
  cast(sent_at as timestamp) as sent_at,
  cast(converted_at as timestamp) as converted_at,
  cast(status as varchar) as status
from src
  );
