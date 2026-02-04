import numpy as np
import pandas as pd
import streamlit as st
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from streamlit_app.utils.db import query_df

st.set_page_config(page_title="Finance Forecast", layout="wide")

def q(sql: str, params=None):
    df, db_path = query_df(ROOT, sql, params=params)
    return df, db_path

st.title("Finance Forecast & Reward Liability")
st.caption("Scenario modeling for referral conversion, reward redemption, and cash-out timing.")

# -----------------------------
# Controls
# -----------------------------
st.sidebar.header("Scenario Assumptions")

conv_rate = st.sidebar.slider(
    "Referral conversion rate",
    min_value=0.05, max_value=0.60, value=0.25, step=0.05
)

redeem_rate = st.sidebar.slider(
    "Reward redemption rate",
    min_value=0.30, max_value=0.95, value=0.75, step=0.05
)

avg_days_to_redeem = st.sidebar.slider(
    "Avg days to redeem",
    min_value=3, max_value=60, value=14, step=1
)

reward_amount = st.sidebar.slider(
    "Reward value ($ per award)",
    min_value=1, max_value=20, value=5, step=1
)

forecast_days = st.sidebar.selectbox(
    "Forecast horizon (days)",
    [30, 60, 90],
    index=1
)

# -----------------------------
# Baseline volumes
# -----------------------------
base_df, db_path = q("""
select
  count(*) as total_referrals,
  sum(case when status='converted' then 1 else 0 end) as historical_conversions
from fct_referral
""")
base = base_df.iloc[0]

st.caption(f"Warehouse: {db_path}")

total_referrals = int(base["total_referrals"])

# -----------------------------
# Forecast math
# -----------------------------
expected_conversions = total_referrals * conv_rate
expected_awards = expected_conversions
expected_redeems = expected_awards * redeem_rate

total_liability = expected_awards * reward_amount
expected_cash_out = expected_redeems * reward_amount

# -----------------------------
# KPIs
# -----------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total referrals", f"{total_referrals:,}")
c2.metric("Expected awards", f"{expected_awards:,.0f}")
c3.metric("Expected redeems", f"{expected_redeems:,.0f}")
c4.metric("Total liability ($)", f"${total_liability:,.0f}")

st.divider()

# -----------------------------
# Redemption curve
# -----------------------------
days = np.arange(0, forecast_days + 1)

# Simple exponential decay curve
lambda_ = 1 / avg_days_to_redeem
cdf = 1 - np.exp(-lambda_ * days)

redeemed_curve = expected_redeems * cdf
liability_curve = expected_awards - redeemed_curve

df_curve = pd.DataFrame({
    "day": days,
    "cumulative_redeemed": redeemed_curve * reward_amount,
    "remaining_liability": liability_curve * reward_amount
})

st.subheader("Projected Cash-Out & Remaining Liability")
st.line_chart(
    df_curve.set_index("day")[["cumulative_redeemed", "remaining_liability"]]
)

st.divider()

# -----------------------------
# Weekly view
# -----------------------------
weekly = df_curve.copy()
weekly["week"] = weekly["day"] // 7
weekly = weekly.groupby("week").max().reset_index()

st.subheader("Weekly View (Finance-friendly)")
st.dataframe(
    weekly[["week", "cumulative_redeemed", "remaining_liability"]],
    use_container_width=True
)

st.divider()

st.caption(
    "Model notes: simple exponential redemption curve. "
    "Designed for scenario comparison, not actuarial precision."
)
