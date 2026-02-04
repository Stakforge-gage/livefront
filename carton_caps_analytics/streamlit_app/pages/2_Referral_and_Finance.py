import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df

st.set_page_config(page_title="Referral + Finance", layout="wide")

def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

st.title("Referral Program + Finance")
st.caption("Funnel performance, 48h compliance, and reward award/redeem behavior.")

# Funnel
funnel, db_path = q("""
select
  status,
  count(*) as cnt,
  avg(case when within_48h_window then 1 else 0 end) as pct_within_48h,
  avg(case when eligible_referral then 1 else 0 end) as pct_eligible
from fct_referral
group by 1
order by cnt desc
""")

st.caption(f"Warehouse: {db_path}")

st.subheader("Referral Funnel Summary")
st.dataframe(funnel, use_container_width=True)

st.divider()

# 48h compliance breakdown for converted
compliance, _ = q("""
select
  case
    when within_48h_window is true then 'within_48h'
    when within_48h_window is false then 'over_48h'
    else 'missing_events'
  end as bucket,
  count(*) as cnt
from fct_referral
where status = 'converted'
group by 1
order by cnt desc
""")

st.subheader("48-hour Window Compliance (Converted)")
st.bar_chart(compliance.set_index("bucket"))

st.divider()

# Rewards timeline
rewards_daily, _ = q("""
with a as (
  select awarded_at::date as d, count(*) as awards
  from fct_rewards
  group by 1
),
r as (
  select redeemed_at::date as d, count(*) as redeems
  from fct_rewards
  where redeemed_at is not null
  group by 1
)
select
  coalesce(a.d, r.d) as d,
  coalesce(a.awards, 0) as awards,
  coalesce(r.redeems, 0) as redeems,
  sum(coalesce(a.awards,0)) over (order by coalesce(a.d, r.d)) as cum_awards,
  sum(coalesce(r.redeems,0)) over (order by coalesce(a.d, r.d)) as cum_redeems
from a
full outer join r on a.d = r.d
order by 1
""")

st.subheader("Awards vs Redeems Over Time")
st.line_chart(rewards_daily.set_index("d")[["awards", "redeems"]])

st.subheader("Cumulative Liability Proxy")
st.line_chart(rewards_daily.set_index("d")[["cum_awards", "cum_redeems"]])

st.divider()

# Top referrers
top_referrers, _ = q("""
select
  referrer_user_id,
  count(*) as referrals_sent,
  sum(case when status='converted' then 1 else 0 end) as conversions,
  avg(case when status='converted' then 1 else 0 end) as conversion_rate
from fct_referral
group by 1
order by conversions desc
limit 15
""")
st.subheader("Top Referrers (by conversions)")
st.dataframe(top_referrers, use_container_width=True)
