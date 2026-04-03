"""
Streamlit application for visualizing weather data from DuckDB/MotherDuck.

Environment Variables:
    DB_TYPE: Set to 'motherduck' to connect to MotherDuck, otherwise uses local DuckDB
    MOTHERDUCK_TOKEN: Required if DB_TYPE='motherduck'
"""

import os
import logging

from datetime import datetime, timedelta

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

st.set_page_config(page_title="Weather Data Dashboard", layout="wide")


load_dotenv(".env")

def get_connection():
    """Create database connection based on environment variable."""
    db_type = os.getenv("DB_TYPE", "duckdb").lower()
    logging.info("db_type: %s", db_type)

    if db_type == "motherduck":
        token = os.getenv("MOTHERDUCK_TOKEN")
        if not token:
            st.error("MOTHERDUCK_TOKEN environment variable is required for MotherDuck connection")
            st.stop()
        conn = duckdb.connect(f"md:?token={token}")
    else:
        db_path = os.getenv("DUCKDB_DATABASE")
        logging.info("db_path: %s", db_path)
        conn = duckdb.connect(db_path)

    return conn


def get_date_range(conn):
    """Get min and max dates from the dataset."""
    result = conn.execute("""
        SELECT MIN(date)::date as min_date, MAX(date)::date as max_date
        FROM report.a01_temp_report
    """).fetchone()
    return result[0], result[1]


def get_temperature_data(conn, start_date, end_date):
    """Get average temperature over time."""
    query = """
        SELECT
            date::date as date,
            AVG(TAVG) as avg_temp,
            AVG(TMIN) as min_temp,
            AVG(TMAX) as max_temp
        FROM report.a01_temp_report
        WHERE date >= ? AND date <= ?
        GROUP BY date::date
        ORDER BY date
    """
    return conn.execute(query, [start_date, end_date]).fetchdf()


def get_precipitation_data(conn, start_date, end_date):
    """Get precipitation and snow depth data."""
    query = """
        SELECT
            date::date as date,
            AVG(SNWD) as avg_snow_depth,
            AVG(PRCP) as avg_precip
        FROM report.a01_temp_report
        WHERE date >= ? AND date <= ?
        GROUP BY date::date
        ORDER BY date
    """
    return conn.execute(query, [start_date, end_date]).fetchdf()


def get_latest_stations_data(conn, end_date):
    """Get latest temperature data for each station at the end date."""
    query = """
        WITH latest_data AS (
            SELECT
                station_id,
                latitude,
                longitude,
                altitude,
                TAVG,
                date,
                ROW_NUMBER() OVER (PARTITION BY station_id ORDER BY date DESC) as rn
            FROM report.a01_temp_report
            WHERE date <= ?
        )
        SELECT
            station_id,
            latitude,
            longitude,
            altitude,
            TAVG as temperature
        FROM latest_data
        WHERE rn = 1
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
    """
    return conn.execute(query, [end_date]).fetchdf()


def main():
    st.title("Weather Data Dashboard")
    st.markdown("*NOAA Weather Station Data Visualization*")

    db_type = os.getenv("DB_TYPE", "duckdb").lower()
    if db_type == "motherduck":
        st.sidebar.info("Connected to: **MotherDuck**")
    else:
        st.sidebar.info("Connected to: **Local DuckDB** (noaa.db)")

    conn = None
    try:
        conn = get_connection()
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        st.stop()

    min_date, max_date = get_date_range(conn)

    st.sidebar.header("Time Range Selection")
    start_date = st.sidebar.date_input(
        "Start Date",
        value=min_date,
        min_value=min_date,
        max_value=max_date
    )
    end_date = st.sidebar.date_input(
        "End Date",
        value=max_date,
        min_value=min_date,
        max_value=max_date
    )

    if start_date > end_date:
        st.sidebar.error("Start date must be before end date")
        st.stop()

    with st.spinner("Loading data..."):
        temp_data = get_temperature_data(conn, start_date, end_date)
        precip_data = get_precipitation_data(conn, start_date, end_date)
        map_data = get_latest_stations_data(conn, end_date)

    if temp_data.empty:
        st.warning("No temperature data available for the selected time range.")
    else:
        st.subheader("Temperature Over Time")
        fig_temp = go.Figure()
        fig_temp.add_trace(go.Scatter(
            x=temp_data["date"],
            y=temp_data["min_temp"],
            mode="lines",
            name="Min Temp",
            line=dict(color="blue")
        ))
        fig_temp.add_trace(go.Scatter(
            x=temp_data["date"],
            y=temp_data["avg_temp"],
            mode="lines",
            name="Avg Temp",
            line=dict(color="green")
        ))
        fig_temp.add_trace(go.Scatter(
            x=temp_data["date"],
            y=temp_data["max_temp"],
            mode="lines",
            name="Max Temp",
            line=dict(color="red")
        ))
        fig_temp.update_layout(
            xaxis_title="Date",
            yaxis_title="Temperature",
            hovermode="x unified"
        )
        st.plotly_chart(fig_temp, width='stretch')

    if precip_data.empty:
        st.warning("No precipitation data available for the selected time range.")
    else:
        st.subheader("Precipitation and Snow Depth")
        fig_precip = go.Figure()
        fig_precip.add_trace(go.Bar(
            x=precip_data["date"],
            y=precip_data["avg_precip"],
            name="Precipitation (PRCP)",
            marker_color="blue"
        ))
        fig_precip.add_trace(go.Bar(
            x=precip_data["date"],
            y=precip_data["avg_snow_depth"],
            name="Snow Depth (SNWD)",
            marker_color="white",
            marker_line_color="gray",
            marker_line_width=1
        ))
        fig_precip.update_layout(
            xaxis_title="Date",
            yaxis_title="Value",
            barmode="stack",
            hovermode="x unified"
        )
        st.plotly_chart(fig_precip, width='stretch')

    if map_data.empty:
        st.warning("No station data available for the selected end date.")
    else:
        st.subheader(f"Station Temperatures (as of {end_date})")
        fig_map = px.scatter_map(
            map_data,
            lat="latitude",
            lon="longitude",
            color="temperature",
            size=[10] * len(map_data),
            color_continuous_scale="RdBu_r",
            range_color=[-20, 40],
            hover_data=["station_id", "altitude", "temperature"],
            zoom=3,
            height=500
        )
        fig_map.update_layout(
            map_style="carto-positron",
            margin={"r": 0, "t": 30, "l": 0, "b": 0}
        )
        st.plotly_chart(fig_map, width='stretch')

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Stations", len(map_data))
        with col2:
            avg_temp = map_data["temperature"].mean()
            st.metric("Average Temperature", f"{avg_temp:.1f}" if pd.notna(avg_temp) else "N/A")
        with col3:
            if not map_data["temperature"].isna().all():
                min_temp = map_data["temperature"].min()
                max_temp = map_data["temperature"].max()
                st.metric("Temp Range", f"{min_temp:.1f} to {max_temp:.1f}")
            else:
                st.metric("Temp Range", "N/A")

    conn.close()


if __name__ == "__main__":
    main()
