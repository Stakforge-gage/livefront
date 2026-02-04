import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df

st.set_page_config(page_title="Product Insights", layout="wide")

def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

st.title("Product Insights")
st.caption("Funnels and engagement signals derived from events + purchases.")

# Date range filter
bounds_df, db_path = q("""
select min(event_at) as min_dt, max(event_at) as max_dt
from stg_events
""")
bounds = bounds_df.iloc[0]

st.caption(f"Warehouse: {db_path}")

min_dt, max_dt = bounds["min_dt"], bounds["max_dt"]
date_range = st.date_input("Date range", value=(min_dt.date(), max_dt.date()))

start = str(date_range[0])
end = str(date_range[1])

# Funnel
funnel_df, _ = q(f"""
with e as (
  select *
  from stg_events
  where event_at::date between '{start}' and '{end}'
)
select
  sum(case when event_type='app_open' then 1 else 0 end) as app_opens,
  sum(case when event_type='incentive_viewed' then 1 else 0 end) as incentive_views,
  sum(case when event_type='receipt_scan_started' then 1 else 0 end) as scan_started,
  sum(case when event_type='receipt_scan_completed' then 1 else 0 end) as scan_completed
from e
""")
funnel = funnel_df.iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("App opens", int(funnel["app_opens"]))
c2.metric("Incentive views", int(funnel["incentive_views"]))
c3.metric("Scans started", int(funnel["scan_started"]))
c4.metric("Scans completed", int(funnel["scan_completed"]))

st.divider()

# Conversion rates
scan_start = max(1, int(funnel["scan_started"]))
app_open = max(1, int(funnel["app_opens"]))

cr1 = int(funnel["scan_started"]) / app_open
cr2 = int(funnel["scan_completed"]) / scan_start

st.subheader("Funnel Conversion Rates")
colA, colB = st.columns(2)
colA.metric("Open → Scan Start", f"{cr1:.1%}")
colB.metric("Scan Start → Scan Complete", f"{cr2:.1%}")

st.divider()

# Daily trend
daily, _ = q(f"""
with e as (
  select event_at::date as d, event_type
  from stg_events
  where event_at::date between '{start}' and '{end}'
)
select
  d,
  sum(case when event_type='app_open' then 1 else 0 end) as app_opens,
  sum(case when event_type='receipt_scan_started' then 1 else 0 end) as scan_started,
  sum(case when event_type='receipt_scan_completed' then 1 else 0 end) as scan_completed
from e
group by 1
order by 1
""")

st.subheader("Daily Activity Trend")
st.line_chart(daily.set_index("d"))

st.divider()

# Segment: user_type engagement via purchases/day
seg, _ = q(f"""
with p as (
  select
    purchase_date,
    u.user_type,
    count(*) as purchases,
    sum(price_paid) as spend
  from fct_purchase fp
  join dim_user u on fp.user_id = u.user_id
  where purchase_date between '{start}' and '{end}'
  group by 1,2
)
select
  user_type,
  sum(purchases) as purchases,
  sum(spend) as spend
from p
group by 1
order by spend desc
""")

st.subheader("Engagement by User Type")
st.dataframe(seg, use_container_width=True)
