import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df

st.set_page_config(page_title="Trust & Safety", layout="wide")

def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

st.title("Trust & Safety")
st.caption("Signals for fraud/abuse and operational risk in referral rewards.")

# Device reuse: multiple users with same device_id
device_reuse, db_path = q("""
select
  device_id,
  count(*) as users_on_device
from dim_user
where device_id is not null
group by 1
having count(*) > 1
order by users_on_device desc
limit 50
""")

st.caption(f"Warehouse: {db_path}")

c1, c2 = st.columns(2)
with c1:
    st.subheader("Device Reuse (Possible Multi-accounting)")
    if len(device_reuse) == 0:
        st.success("No device reuse detected (with current synthetic settings).")
    else:
        st.dataframe(device_reuse, use_container_width=True)

# Referral velocity: high invites in short time
velocity, _ = q("""
with sends as (
  select
    referrer_user_id,
    date_trunc('day', sent_at) as d,
    count(*) as invites_sent
  from fct_referral
  group by 1,2
)
select *
from sends
where invites_sent >= 8
order by invites_sent desc
limit 50
""")

with c2:
    st.subheader("High Referral Velocity (≥ 8/day)")
    st.dataframe(velocity, use_container_width=True)

st.divider()

# Rewards outstanding (awarded but not redeemed)
outstanding, _ = q("""
select
  reward_type,
  count(*) as outstanding_rewards
from fct_rewards
where redeemed_at is null
group by 1
order by outstanding_rewards desc
""")

st.subheader("Outstanding Rewards (Liability Proxy)")
st.bar_chart(outstanding.set_index("reward_type"))

st.divider()

# Eligibility gaps: converted but not eligible (missing window, onboarding, or qualifying action)
gaps, _ = q("""
select
  case
    when status <> 'converted' then 'not_converted'
    when within_48h_window is false then 'over_48h'
    when onboarding_completed_at is null then 'missing_onboarding'
    when qualifying_action_at is null then 'missing_qualifying_action'
    when within_48h_window is null then 'missing_events'
    else 'eligible'
  end as bucket,
  count(*) as cnt
from fct_referral
group by 1
order by cnt desc
""")

st.subheader("Referral Eligibility Breakdown")
st.dataframe(gaps, use_container_width=True)

st.divider()

# Suspicious clusters: device_id + conversions
clusters, _ = q("""
with u as (
  select user_id, device_id from dim_user
),
c as (
  select referred_user_id as user_id
  from fct_referral
  where status='converted' and referred_user_id is not null
)
select
  u.device_id,
  count(distinct u.user_id) as users_on_device,
  sum(case when c.user_id is not null then 1 else 0 end) as converted_users_on_device
from u
left join c on u.user_id = c.user_id
where u.device_id is not null
group by 1
having count(distinct u.user_id) > 1
order by converted_users_on_device desc, users_on_device desc
limit 25
""")

st.subheader("Multi-user Devices with Conversions (Higher Risk)")
if len(clusters) == 0:
    st.info("No clusters detected in this run (depends on synthetic collision rate).")
else:
    st.dataframe(clusters, use_container_width=True)
