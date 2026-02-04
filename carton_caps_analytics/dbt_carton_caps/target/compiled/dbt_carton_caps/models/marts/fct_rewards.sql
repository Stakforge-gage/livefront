with e as (
  select *
  from "carton_caps_20260203T224751Z"."main"."stg_events"
),

awards as (
  select
    referral_id,
    user_id,
    event_at as awarded_at,
    -- reward_type is inside metadata_json like {"reward_type":"referrer_bonus"}
    regexp_extract(metadata_json, '"reward_type":"([^"]+)"', 1) as reward_type
  from e
  where event_type = 'reward_awarded'
),

redeems as (
  select
    referral_id,
    user_id,
    event_at as redeemed_at,
    regexp_extract(metadata_json, '"reward_type":"([^"]+)"', 1) as reward_type
  from e
  where event_type = 'reward_redeemed'
)

select
  -- deterministic surrogate key
  md5(cast(a.referral_id as varchar) || '|' || cast(a.user_id as varchar) || '|' || coalesce(a.reward_type,'')) as reward_id,
  a.referral_id,
  a.user_id,
  a.reward_type,
  a.awarded_at,
  min(r.redeemed_at) as redeemed_at
from awards a
left join redeems r
  on a.referral_id = r.referral_id
  and a.user_id = r.user_id
  and coalesce(a.reward_type,'') = coalesce(r.reward_type,'')
group by 1,2,3,4,5