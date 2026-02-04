import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df

st.set_page_config(page_title="Carton Caps Analytics", layout="wide")

def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

st.title("Carton Caps — Analytics MVP")

col1, col2, col3, col4 = st.columns(4)

kpi_df, db_path = q("""
select
  (select count(*) from dim_user) as users,
  (select count(*) from dim_school) as schools,
  (select count(*) from fct_purchase) as purchases,
  (select count(*) from fct_referral) as referrals
""")
kpi = kpi_df.iloc[0]

st.caption(f"Warehouse: {db_path}")

col1.metric("Users", int(kpi["users"]))
col2.metric("Schools", int(kpi["schools"]))
col3.metric("Purchases", int(kpi["purchases"]))
col4.metric("Referrals", int(kpi["referrals"]))

st.divider()

st.subheader("Quick Health Checks")

hc, _ = q("""
select
  status,
  count(*) as cnt
from fct_referral
group by 1
order by cnt desc
""")
st.write("Referral funnel distribution (from fct_referral):")
st.dataframe(hc, use_container_width=True)

st.write("Top 10 schools by purchase $ (proxy for fundraising volume):")
top_schools, _ = q("""
select
  school_id,
  max(school_id) as _,
  sum(price_paid) as total_spend,
  sum(points_earned) as total_points,
  count(*) as purchases
from fct_purchase
group by 1
order by total_spend desc
limit 10
""")
st.dataframe(top_schools, use_container_width=True)
