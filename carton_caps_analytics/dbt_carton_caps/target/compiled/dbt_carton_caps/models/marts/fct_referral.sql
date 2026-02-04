with r as (
  select *
  from "carton_caps_20260203T224751Z"."main"."stg_referrals"
),

e as (
  select *
  from "carton_caps_20260203T224751Z"."main"."stg_events"
),

-- Referral-related event timestamps for referred users
ref_events as (
  select
    referral_id,
    user_id,

    min(case when event_type = 'install' then event_at end) as install_at,
    min(case when event_type = 'referral_applied' then event_at end) as referral_applied_at,
    min(case when event_type = 'onboarding_complete' then event_at end) as onboarding_completed_at,
    min(case when event_type = 'school_linked' then event_at end) as school_linked_at

  from e
  where referral_id is not null
  group by 1,2
),

-- First scan/purchase time for referred users (proxy for qualifying action)
first_purchase as (
  select
    user_id,
    min(purchased_at) as first_purchase_at
  from "carton_caps_20260203T224751Z"."main"."stg_purchases"
  group by 1
)

select
  r.referral_id,
  r.referrer_user_id,
  r.referred_email,
  r.referred_user_id,
  r.referral_code,
  r.sent_at,
  r.converted_at,
  r.status,

  re.install_at,
  re.referral_applied_at,
  re.onboarding_completed_at,
  re.school_linked_at,

  fp.first_purchase_at,

  -- qualifying action is: first_purchase OR school_linked (whichever happens first)
  least(fp.first_purchase_at, re.school_linked_at) as qualifying_action_at,

  -- window compliance (48 hours) using install -> referral_applied
  case
    when re.install_at is null or re.referral_applied_at is null then null
    when datediff('hour', re.install_at, re.referral_applied_at) <= 48 then true
    else false
  end as within_48h_window,

  -- simple eligibility (rules-compliant posture)
  case
    when r.status <> 'converted' then false
    when re.onboarding_completed_at is null then false
    when least(fp.first_purchase_at, re.school_linked_at) is null then false
    when re.install_at is null or re.referral_applied_at is null then false
    when datediff('hour', re.install_at, re.referral_applied_at) > 48 then false
    else true
  end as eligible_referral

from r
left join ref_events re
  on r.referral_id = re.referral_id
  and r.referred_user_id = re.user_id
left join first_purchase fp
  on r.referred_user_id = fp.user_id