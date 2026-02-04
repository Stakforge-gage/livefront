import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df

st.set_page_config(page_title="Network Effects", layout="wide")

def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

st.title("Network Effects")
st.caption("How referrals propagate, who drives growth, and which schools have viral momentum.")

# Core network KPIs
kpis_df, db_path = q("""
with base as (
  select
    count(*) as total_referrals,
    sum(case when status='converted' then 1 else 0 end) as total_conversions,
    avg(case when status='converted' then 1 else 0 end) as conversion_rate
  from fct_referral
)
select
  total_referrals,
  total_conversions,
  conversion_rate
from base
""")
kpis = kpis_df.iloc[0]

st.caption(f"Warehouse: {db_path}")

c1, c2, c3 = st.columns(3)
c1.metric("Total referrals", int(kpis["total_referrals"]))
c2.metric("Total conversions", int(kpis["total_conversions"]))
c3.metric("Overall conversion rate", f"{float(kpis['conversion_rate']):.1%}")

st.divider()

# Degree distribution: how many referrals each user sends
deg, _ = q("""
select
  referrer_user_id,
  count(*) as referrals_sent,
  sum(case when status='converted' then 1 else 0 end) as conversions
from fct_referral
group by 1
""")

st.subheader("Referrals Sent Distribution (Super-spreader signal)")
st.write("Most users refer a few friends; a small tail refers many.")
hist = deg["referrals_sent"].value_counts().sort_index().reset_index()
hist.columns = ["referrals_sent", "users"]
st.bar_chart(hist.set_index("referrals_sent"))

st.divider()

# Top referrers table
st.subheader("Top Referrers")
top = deg.sort_values(["conversions", "referrals_sent"], ascending=False).head(25)
st.dataframe(top, use_container_width=True)

st.divider()

# School-level network health
school_net, _ = q("""
with u as (
  select user_id, school_id from dim_user
),
r as (
  select
    fr.referrer_user_id,
    fr.status
  from fct_referral fr
)
select
  u.school_id,
  count(*) as referrals_sent,
  sum(case when r.status='converted' then 1 else 0 end) as conversions,
  avg(case when r.status='converted' then 1 else 0 end) as conversion_rate
from r
join u on r.referrer_user_id = u.user_id
group by 1
order by conversions desc
limit 20
""")

st.subheader("Top Schools by Referral Conversions")
st.dataframe(school_net, use_container_width=True)

st.divider()

# Viral coefficient proxy (K-factor): conversions per active user (rough proxy)
# We'll approximate "active" as having at least 1 app_open in events.
kfactor_df, _ = q("""
with active_users as (
  select distinct user_id
  from stg_events
  where event_type = 'app_open'
),
conv as (
  select referred_user_id as user_id
  from fct_referral
  where status='converted' and referred_user_id is not null
)
select
  (select count(*) from conv) as conversions,
  (select count(*) from active_users) as active_users,
  (select count(*) from conv) * 1.0 / nullif((select count(*) from active_users), 0) as k_factor_proxy
""")
kfactor = kfactor_df.iloc[0]

st.subheader("Viral Coefficient Proxy (K-factor)")
st.write("Simple proxy: conversions / active users (active = had at least one app_open).")
st.metric("K-factor proxy", f"{float(kfactor['k_factor_proxy']):.3f}")
