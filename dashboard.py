import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import plotly.express as px
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

st.set_page_config(page_title="📈 Stock Dashboard", layout="wide")

# Database connection
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def load_data():
    query = "SELECT * FROM stock_data ORDER BY timestamp DESC LIMIT 1000;"
    with get_connection().cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return pd.DataFrame(rows)

# Load data
df = load_data()

# Clean datetime
df['timestamp'] = pd.to_datetime(df['timestamp'])

st.title("📊 Stock Data Dashboard")
st.markdown("Visualizing stock metrics from PostgreSQL.")

# Sidebar filters
symbol_filter = st.sidebar.selectbox("Select Symbol", sorted(df["symbol"].unique()))
df = df[df["symbol"] == symbol_filter]

# Date range
date_range = st.sidebar.date_input("Select Date Range", [])
if len(date_range) == 2:
    start, end = date_range
    df = df[(df["timestamp"].dt.date >= start) & (df["timestamp"].dt.date <= end)]

# Show filtered table
st.subheader(f"Latest Stock Data for {symbol_filter}")
st.dataframe(df.tail(20), use_container_width=True)

# Price over time
st.subheader("📈 Close Price Trend")
fig = px.line(df, x="timestamp", y="close", title="Close Price Over Time")
st.plotly_chart(fig, use_container_width=True)

# Volatility zone pie
st.subheader("📊 Volatility Zone Distribution")
zone_counts = df["volatility_zone"].value_counts().reset_index()
zone_counts.columns = ['volatility_zone', 'count']  # Rename properly

fig2 = px.pie(zone_counts, names='volatility_zone', values='count', title='Volatility Zone Breakdown')
st.plotly_chart(fig2, use_container_width=True)

# Volume bar chart
st.subheader("📉 Volume over Time")
fig3 = px.bar(df, x="timestamp", y="volume", title="Volume Traded")
st.plotly_chart(fig3, use_container_width=True)

# Trend counts
st.subheader("📌 Trend Summary")
trend_count = df["trend"].value_counts()
st.bar_chart(trend_count)

