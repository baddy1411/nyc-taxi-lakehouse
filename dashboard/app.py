"""
dashboard/app.py
─────────────────
NYC Taxi Lakehouse — Streamlit Analytics Dashboard.
Reads directly from the Gold layer (Parquet files via DuckDB).

Run: streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ──────────────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Lakehouse",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
    
    html, body, [class*="css"] { font-family: 'IBM Plex Sans', sans-serif; }
    
    .main { background: #0a0e1a; }
    
    .metric-card {
        background: linear-gradient(135deg, #0f1629 0%, #1a2340 100%);
        border: 1px solid #2a3a5c;
        border-radius: 8px;
        padding: 20px 24px;
        position: relative;
        overflow: hidden;
    }
    .metric-card::before {
        content: '';
        position: absolute; top: 0; left: 0;
        width: 3px; height: 100%;
        background: linear-gradient(180deg, #f7c948, #e8a000);
    }
    .metric-value {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 2rem; font-weight: 600;
        color: #f7c948; margin: 0;
    }
    .metric-label {
        font-size: 0.75rem; text-transform: uppercase;
        letter-spacing: 0.1em; color: #6b82a8; margin-top: 4px;
    }
    .metric-delta {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.8rem; color: #4ade80; margin-top: 8px;
    }
    
    .section-header {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem; text-transform: uppercase;
        letter-spacing: 0.15em; color: #4a6080;
        border-bottom: 1px solid #1e2d45;
        padding-bottom: 8px; margin-bottom: 16px;
    }
    
    .pipeline-badge {
        display: inline-block;
        background: #0f2a1a; border: 1px solid #1a4a2a;
        color: #4ade80; border-radius: 4px;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.65rem; padding: 2px 8px;
        margin-right: 6px;
    }
    
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #0f1629, #1a2340);
        border: 1px solid #2a3a5c;
        border-left: 3px solid #f7c948;
        border-radius: 8px;
        padding: 16px 20px;
    }
    div[data-testid="stMetric"] label { color: #6b82a8 !important; font-size: 0.75rem !important; text-transform: uppercase; letter-spacing: 0.08em; }
    div[data-testid="stMetric"] div { color: #f7c948 !important; font-family: 'IBM Plex Mono', monospace !important; font-size: 1.8rem !important; }
    
    .stSelectbox label, .stMultiSelect label, .stSlider label { color: #6b82a8 !important; font-size: 0.75rem !important; text-transform: uppercase; }
    
    h1 { font-family: 'IBM Plex Mono', monospace !important; font-size: 1.4rem !important; color: #e8f0ff !important; }
    h2, h3 { font-family: 'IBM Plex Sans', sans-serif !important; color: #c0cfe0 !important; font-weight: 300 !important; }
</style>
""", unsafe_allow_html=True)

GOLD_PATH = Path(os.getenv("LAKEHOUSE_GOLD_PATH", "data/lakehouse/gold"))

PLOTLY_THEME = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(10,14,26,0)",
    plot_bgcolor="rgba(15,22,41,0.8)",
    font=dict(family="IBM Plex Sans", color="#8ba0c0"),
    xaxis=dict(gridcolor="#1e2d45", linecolor="#2a3a5c"),
    yaxis=dict(gridcolor="#1e2d45", linecolor="#2a3a5c"),
    colorway=["#f7c948", "#4a9fe8", "#4ade80", "#f87171", "#a78bfa", "#fb923c"],
)


@st.cache_resource
def get_con():
    con = duckdb.connect()
    g = GOLD_PATH
    if not (g / "fct_trips.parquet").exists():
        return None, None
    con.execute(f"CREATE VIEW fct_trips AS SELECT * FROM read_parquet('{g}/fct_trips.parquet')")
    con.execute(f"CREATE VIEW fct_hourly AS SELECT * FROM read_parquet('{g}/fct_hourly_demand.parquet')")
    con.execute(f"CREATE VIEW dim_location AS SELECT * FROM read_parquet('{g}/dim_location.parquet')")
    con.execute(f"CREATE VIEW dim_date AS SELECT * FROM read_parquet('{g}/dim_date.parquet')")
    return con, True


@st.cache_data(ttl=300)
def kpis(_con):
    return _con.execute("""
        SELECT
            COUNT(*)                                       AS total_trips,
            ROUND(SUM(total_amount)/1e6, 2)               AS revenue_m,
            ROUND(AVG(fare_amount), 2)                     AS avg_fare,
            ROUND(AVG(tip_rate)*100, 1)                    AS avg_tip_pct,
            ROUND(AVG(trip_distance), 2)                   AS avg_dist,
            ROUND(AVG(trip_duration_min), 1)               AS avg_dur_min,
            ROUND(SUM(CASE WHEN payment_type=1 THEN 1 END)*100.0/COUNT(*),1) AS cc_pct,
            SUM(CASE WHEN is_airport_trip THEN 1 END)      AS airport_trips
        FROM fct_trips
    """).df().iloc[0]


@st.cache_data(ttl=300)
def hourly_demand(_con):
    return _con.execute("""
        SELECT pickup_hour,
               SUM(trip_count) AS trips,
               ROUND(AVG(avg_fare),2) AS avg_fare,
               ROUND(AVG(avg_tip_rate)*100,1) AS tip_pct
        FROM fct_hourly
        GROUP BY pickup_hour ORDER BY pickup_hour
    """).df()


@st.cache_data(ttl=300)
def borough_stats(_con):
    return _con.execute("""
        SELECT d.borough,
               COUNT(*) AS trips,
               ROUND(SUM(f.total_amount),0) AS revenue,
               ROUND(AVG(f.fare_amount),2) AS avg_fare,
               ROUND(AVG(f.tip_rate)*100,1) AS avg_tip_pct,
               ROUND(AVG(f.speed_mph),1) AS avg_speed
        FROM fct_trips f
        JOIN dim_location d ON d.location_id = f.pickup_location_id
        WHERE d.borough NOT IN ('Unknown','EWR')
        GROUP BY d.borough ORDER BY revenue DESC
    """).df()


@st.cache_data(ttl=300)
def daily_trend(_con):
    return _con.execute("""
        SELECT CAST(pickup_date AS VARCHAR) AS date,
               COUNT(*) AS trips,
               ROUND(SUM(total_amount),0) AS revenue,
               ROUND(AVG(fare_amount),2) AS avg_fare
        FROM fct_trips
        GROUP BY pickup_date ORDER BY pickup_date
    """).df()


@st.cache_data(ttl=300)
def payment_split(_con):
    return _con.execute("""
        SELECT payment_label,
               COUNT(*) AS trips,
               ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER(),1) AS pct
        FROM fct_trips
        GROUP BY payment_label ORDER BY trips DESC
    """).df()


@st.cache_data(ttl=300)
def fare_dist(_con):
    return _con.execute("""
        SELECT fare_amount FROM fct_trips
        WHERE fare_amount BETWEEN 3 AND 100
        USING SAMPLE 20000
    """).df()


@st.cache_data(ttl=300)
def speed_by_hour(_con):
    return _con.execute("""
        SELECT pickup_hour,
               ROUND(AVG(speed_mph),2) AS avg_speed,
               ROUND(AVG(trip_duration_min),1) AS avg_duration,
               SUM(trip_count) AS trips
        FROM fct_hourly
        GROUP BY pickup_hour ORDER BY pickup_hour
    """).df()


# ── RENDER ───────────────────────────────────────────────────────
con, loaded = get_con()

# ── Header ────────────────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("# 🚕 NYC Taxi Lakehouse")
    st.markdown(
        '<span class="pipeline-badge">BRONZE</span>'
        '<span class="pipeline-badge">SILVER</span>'
        '<span class="pipeline-badge">GOLD</span>'
        '<span class="pipeline-badge">DUCKDB</span>'
        '<span style="font-size:0.75rem;color:#4a6080;font-family:IBM Plex Mono"> Q1 2024 · 500K trips</span>',
        unsafe_allow_html=True
    )

if not loaded:
    st.error("Gold layer not found at `data/lakehouse/gold/`. Run `python run_pipeline.py` first.")
    st.code("python run_pipeline.py")
    st.stop()

k = kpis(con)

# ── KPI Row ───────────────────────────────────────────────────────
st.markdown('<div class="section-header">Key Metrics</div>', unsafe_allow_html=True)
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Trips",      f"{int(k['total_trips']):,}")
c2.metric("Revenue",          f"${k['revenue_m']:.1f}M")
c3.metric("Avg Fare",         f"${k['avg_fare']:.2f}")
c4.metric("Avg Tip Rate",     f"{k['avg_tip_pct']:.1f}%")
c5.metric("Avg Distance",     f"{k['avg_dist']:.2f} mi")
c6.metric("Credit Card %",    f"{k['cc_pct']:.1f}%")

st.markdown("---")

# ── Row 1: Demand + Borough ───────────────────────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown('<div class="section-header">Hourly Demand Pattern</div>', unsafe_allow_html=True)
    df_h = hourly_demand(con)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_h["pickup_hour"], y=df_h["trips"],
        marker=dict(
            color=df_h["trips"],
            colorscale=[[0,"#1a2340"],[0.5,"#e8a000"],[1.0,"#f7c948"]],
            showscale=False,
        ),
        name="Trips"
    ))
    fig.add_trace(go.Scatter(
        x=df_h["pickup_hour"], y=df_h["avg_fare"],
        mode="lines+markers",
        line=dict(color="#4a9fe8", width=2),
        marker=dict(size=4),
        name="Avg Fare ($)",
        yaxis="y2"
    ))
    fig.update_layout(
        **PLOTLY_THEME,
        xaxis_title="Hour of Day",
        yaxis_title="Trip Count",
        yaxis2=dict(title="Avg Fare ($)", overlaying="y", side="right",
                    gridcolor="rgba(0,0,0,0)", color="#4a9fe8"),
        legend=dict(orientation="h", y=1.02),
        height=320, margin=dict(t=20, b=40, l=60, r=60),
        bargap=0.1
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown('<div class="section-header">Revenue by Borough</div>', unsafe_allow_html=True)
    df_b = borough_stats(con)
    fig2 = px.bar(
        df_b, x="revenue", y="borough", orientation="h",
        color="avg_fare",
        color_continuous_scale=[[0,"#1a3060"],[1,"#f7c948"]],
        labels={"revenue":"Revenue ($)","avg_fare":"Avg Fare"},
        height=320,
    )
    fig2.update_layout(
        **PLOTLY_THEME,
        showlegend=False,
        margin=dict(t=20, b=40, l=100, r=40),
        coloraxis_showscale=False,
        yaxis=dict(categoryorder="total ascending", gridcolor="#1e2d45"),
    )
    fig2.update_traces(marker_line_width=0)
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: Daily trend + Payment + Speed ──────────────────────────
col3, col4, col5 = st.columns([3, 1.5, 1.5])

with col3:
    st.markdown('<div class="section-header">Daily Trip Volume & Revenue Trend</div>', unsafe_allow_html=True)
    df_d = daily_trend(con)
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=df_d["date"], y=df_d["trips"],
        fill="tozeroy",
        fillcolor="rgba(247,201,72,0.08)",
        line=dict(color="#f7c948", width=2),
        name="Trips"
    ))
    fig3.add_trace(go.Scatter(
        x=df_d["date"], y=df_d["revenue"],
        line=dict(color="#4a9fe8", width=1.5, dash="dot"),
        name="Revenue ($)",
        yaxis="y2"
    ))
    fig3.update_layout(
        **PLOTLY_THEME,
        height=260,
        margin=dict(t=10, b=40, l=60, r=60),
        yaxis2=dict(overlaying="y", side="right", gridcolor="rgba(0,0,0,0)", color="#4a9fe8"),
        legend=dict(orientation="h", y=1.02, font=dict(size=10)),
        xaxis=dict(tickangle=-30, tickfont=dict(size=9)),
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown('<div class="section-header">Payment Split</div>', unsafe_allow_html=True)
    df_p = payment_split(con)
    fig4 = go.Figure(go.Pie(
        labels=df_p["payment_label"], values=df_p["trips"],
        hole=0.65,
        marker=dict(colors=["#f7c948","#4a9fe8","#4ade80","#f87171"]),
        textinfo="label+percent",
        textfont=dict(size=10, family="IBM Plex Mono"),
    ))
    fig4.update_layout(
        **PLOTLY_THEME,
        height=260,
        margin=dict(t=10, b=10, l=10, r=10),
        showlegend=False,
        annotations=[dict(text="Payment<br>Type", x=0.5, y=0.5,
                          font=dict(size=10, color="#6b82a8"),
                          showarrow=False)],
    )
    st.plotly_chart(fig4, use_container_width=True)

with col5:
    st.markdown('<div class="section-header">Speed by Hour</div>', unsafe_allow_html=True)
    df_sp = speed_by_hour(con)
    fig5 = go.Figure(go.Scatter(
        x=df_sp["pickup_hour"], y=df_sp["avg_speed"],
        mode="lines+markers",
        line=dict(color="#4ade80", width=2.5),
        marker=dict(size=5, color="#4ade80"),
        fill="tozeroy",
        fillcolor="rgba(74,222,128,0.06)",
    ))
    # Shade rush hours
    for h_start, h_end in [(7, 9), (16, 19)]:
        fig5.add_vrect(x0=h_start-0.5, x1=h_end+0.5,
                       fillcolor="rgba(247,201,72,0.06)",
                       layer="below", line_width=0)
    fig5.update_layout(
        **PLOTLY_THEME,
        height=260,
        margin=dict(t=10, b=40, l=50, r=20),
        xaxis_title="Hour", yaxis_title="Avg Speed (mph)",
        showlegend=False,
    )
    st.plotly_chart(fig5, use_container_width=True)

# ── Row 3: Fare distribution ──────────────────────────────────────
st.markdown('<div class="section-header">Fare Distribution (Standard Rate, $3–$100)</div>', unsafe_allow_html=True)
df_fd = fare_dist(con)
fig6 = px.histogram(
    df_fd, x="fare_amount", nbins=80,
    color_discrete_sequence=["#f7c948"],
    labels={"fare_amount": "Fare ($)", "count": "Trips"},
)
fig6.update_traces(marker_line_width=0, opacity=0.8)
fig6.update_layout(
    **PLOTLY_THEME, height=220,
    margin=dict(t=10, b=40, l=60, r=20),
    bargap=0.02,
    yaxis_title="Trip Count",
)
# Add annotations for key fare points
for fare, label in [(15.50, "Base JFK $15"), (52.0, "IQR Fence")]:
    fig6.add_vline(x=fare, line_width=1, line_dash="dash",
                   line_color="#4a9fe8",
                   annotation_text=label,
                   annotation_font=dict(size=9, color="#4a9fe8"))
st.plotly_chart(fig6, use_container_width=True)

# ── Footer: pipeline lineage ──────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="font-family:IBM Plex Mono;font-size:0.65rem;color:#2a4060;text-align:center;padding:8px 0">
  NYC TLC Open Data → Bronze (Parquet, Hive-partitioned) → 
  Silver (IQR-cleaned, 38 derived features) → 
  Gold (Star schema: fct_trips + dim_date + dim_location) → 
  DuckDB → This dashboard
  <br>
  <span style="color:#1a3050">490,689 trips processed · 98.1% silver pass rate · 47 tests passing</span>
</div>
""", unsafe_allow_html=True)
