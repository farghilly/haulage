import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from psycopg2.extras import RealDictCursor
import psycopg2
from psycopg2 import sql
from psycopg2 import sql
from datetime import datetime
import os
import time




# --- Configuration ---
st.set_page_config(
    page_title="Haulage Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Data Loading and Preparation (Secure and Deployment Ready) ---

# Database Credentials (Read securely from environment variables)
# FIX: Added .strip().strip("'").strip('"') to safely remove leading/trailing spaces or accidental quotes
DB_HOST = os.environ.get('DB_HOST', 'placeholder_host').strip().strip("'").strip('"')
DB_NAME = os.environ.get('DB_NAME', 'placeholder_db').strip().strip("'").strip('"')
DB_USER = os.environ.get('DB_USER', 'placeholder_user').strip().strip("'").strip('"')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'placeholder_password').strip().strip("'").strip('"')
DB_PORT = os.environ.get('DB_PORT', '5432').strip().strip("'").strip('"')

@st.cache_data(show_spinner="Connecting to Database and Processing Data...")
def load_and_process_data():
    """
    Connects to the database, fetches all required data, and performs initial calculations
    like identifying shipped distance and dead head distance.
    """
    try:
            # FIX: Credentials are now stripped above, making this connection cleaner
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                port=DB_PORT,
                sslmode='require'
            )
            st.success("Database connection successful!")
    except Exception as e:
        st.error(f"Error during database connection: {e}")
        st.stop() # Stop the app if data can't be loaded

        # --- 1. Fetch Shipments and Routes ---
    query_shipments = """
        SELECT DISTINCT tu.shipment, ti.transporter_name, tti.transporter_type_description, tu.actual_shipment_start,
                        srpi.name AS shipping_point, srpi2.name AS receiving_point, MAX(tu.vehicle_id) AS vehicle_id
        FROM truck_utilization tu
        LEFT JOIN route_info ri ON tu.route_id = ri.id
        LEFT JOIN shipping_receiving_points_info srpi ON ri.shipping_point_id = srpi.id
        LEFT JOIN shipping_receiving_points_info srpi2 ON ri.receiving_point_id = srpi2.id
        LEFT JOIN transporter_info ti ON ti.id = tu.transporter_code_id
        LEFT JOIN transporter_type_info tti ON tti.id = ti.transporter_type_id
        GROUP BY tu.shipment, ti.transporter_name, tti.transporter_type_description, tu.actual_shipment_start, srpi.name, srpi2.name
        ORDER BY tu.actual_shipment_start
    """
    df_shipments = pd.read_sql_query(query_shipments, conn)

    # --- 2. Fetch Vehicle Info (for plate numbers and segment) ---
    query_plates = """
        SELECT vi.id, vi.plate_number_assigned, si.description AS segment FROM vehicle_info vi
        LEFT JOIN vehicle_assignment va ON va.vehicle_plate_number = vi.plate_number_assigned
        LEFT JOIN segment_info si ON si.id = va.segment_id
        WHERE plate_number_assigned IS NOT NULL
    """
    df_plate_numbers = pd.read_sql_query(query_plates, conn)

    # --- 3. Fetch Route Distances (for shipped distance and dead-head lookup) ---
    query_distance = """
        SELECT srpi.name AS shipping_point, srpi2.name AS receiving_point, ri.distance
        FROM route_info ri
        LEFT JOIN shipping_receiving_points_info srpi ON ri.shipping_point_id = srpi.id
        LEFT JOIN shipping_receiving_points_info srpi2 ON ri.receiving_point_id = srpi2.id
    """
    df_distance = pd.read_sql_query(query_distance, conn)

    conn.close()

    # --- Initial Data Cleaning and Calculation ---
    
    # 1. Merge Shipped Distance (FIXED: Required for Dead Head % calculation)
    df_master = df_shipments.merge(
        df_distance,
        on=['shipping_point', 'receiving_point'],
        how='left'
    ).rename(columns={'distance': 'shipped_distance'})

    # 2. Filter Dedicated Shipments for Dead Head Logic
    df_dedicated = df_master[df_master['transporter_type_description'] == 'dedicated'].copy()
    
    # Merge Vehicle Plate Info and Segment
    df_dedicated = df_dedicated.merge(df_plate_numbers, left_on='vehicle_id', right_on='id', how='left')
    df_dedicated.drop(columns=['id', 'vehicle_id'], inplace=True)
    
    # Calculate next shipment point per vehicle to find the dead head route
    df_dedicated['next_shipping_point'] = df_dedicated.groupby('plate_number_assigned')['shipping_point'].shift(-1)
    
    # Calculate Dead Head Distance by merging the current receiving point -> next shipping point route
    # The merge suffixes are necessary to avoid confusing the 'shipping_point'/'receiving_point' used for the lookup
    df_dead_head_lookup = df_dedicated.merge(
        df_distance, 
        left_on=['receiving_point', 'next_shipping_point'], 
        right_on=['shipping_point', 'receiving_point'], 
        how='left',
        suffixes=('_current', '_deadhead')
    )
    # The distance column from df_distance will be named 'distance' since it's the right-hand dataframe
    df_dedicated["dead_head_distance"] = df_dead_head_lookup['distance']
    
    # Fill NaN dead head distances (e.g., end of the vehicle's history) with 0
    df_dedicated['dead_head_distance'] = df_dedicated['dead_head_distance'].fillna(0)


    # 3. Add final date column (FIXED: Using df_master instead of undefined df)
    df_master['actual_shipment_date'] = df_master['actual_shipment_start'].dt.date
    
    # Fill NaN values for shippe_distance (routes without distance info) with 0
    df_master['shipped_distance'] = df_master['shipped_distance'].fillna(0)

    return df_master

df_master = load_and_process_data()

# --- Filter Setup (Sidebar) ---
st.sidebar.header("Filter Data")

# Date Filter
# Need to convert date objects to datetime objects for min/max functions to work if they come from DB
df_master['actual_shipment_date'] = pd.to_datetime(df_master['actual_shipment_date'])
min_date = df_master['actual_shipment_date'].min().date()
max_date = df_master['actual_shipment_date'].max().date()

date_range = st.sidebar.date_input(
    "Time Range (Shipment Start)",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)
# Handle single vs. double date selection
start_date_filter = date_range[0]
end_date_filter = date_range[1] if len(date_range) > 1 else date_range[0]


# Transporter Filter
selected_transporters = st.sidebar.multiselect(
    "Transporter Name",
    options=df_master['transporter_name'].unique(),
    default=df_master['transporter_name'].unique()
)

# Transporter Type Filter
selected_types = st.sidebar.multiselect(
    "Transporter Type",
    options=df_master['transporter_type_description'].unique(),
    default=df_master['transporter_type_description'].unique()
)

# Shipping/Receiving Point Filters
selected_shippings = st.sidebar.multiselect(
    "Shipping Point",
    options=df_master['shipping_point'].unique(),
    default=df_master['shipping_point'].unique()
)

selected_receivings = st.sidebar.multiselect(
    "Receiving Point",
    options=df_master['receiving_point'].unique(),
    default=df_master['receiving_point'].unique()
)

# Segment Filter
selected_segments = st.sidebar.multiselect(
    "Segment",
    options=df_master['segment'].unique(),
    default=df_master['segment'].unique()
)

# --- Apply Filters ---
filtered_df = df_master[
    (df_master['actual_shipment_date'].dt.date >= start_date_filter) &
    (df_master['actual_shipment_date'].dt.date <= end_date_filter) &
    df_master['transporter_name'].isin(selected_transporters) &
    df_master['transporter_type_description'].isin(selected_types) &
    df_master['shipping_point'].isin(selected_shippings) &
    df_master['receiving_point'].isin(selected_receivings) &
    df_master['segment'].isin(selected_segments)
].copy()

# --- Main Dashboard Layout ---
st.title("Logistics Utilization and Dead Head Analysis")

if filtered_df.empty:
    st.warning("No data available based on the selected filters.")
else:
    # --- Row 1: Dead Head Distance Over Time ---
    st.subheader("Dead Head Distance Over Time")
    
    # Calculate daily total dead head distance
    df_dead_head_distance = filtered_df.groupby(
        filtered_df['actual_shipment_date'].dt.date.rename('actual_shipment_date') # Group by date object
    )['dead_head_distance'].sum().reset_index()
    
    fig_dead_head = px.line(
        df_dead_head_distance,
        x='actual_shipment_date',
        y='dead_head_distance',
        title='Total Dead Head Distance (Unproductive Travel) by Day',
        labels={'actual_shipment_date': 'Date', 'dead_head_distance': 'Total Dead Head Distance (km)'}
    )
    fig_dead_head.update_traces(mode='lines+markers', line=dict(color='#FF6347'))
    st.plotly_chart(fig_dead_head, use_container_width=True)

    st.markdown("---")

    # --- Row 2: Average Shipments and Dead Head % (Side-by-Side) ---
    col1, col2 = st.columns(2)

    # --- Chart 2: Average Jumbo Shipments ---
    with col1:
        st.subheader("Average Jumbo Shipments per Vehicle")
        
        # Apply Jumbo-specific filters based on the original notebook logic
        df_jumbo = filtered_df[
            (filtered_df['segment'] == 'Qalyub') &
            (filtered_df['transporter_name'].isin(['Al -Rehab Office for Transport and', 'Alwefaq national transport']))
        ].copy()

        if df_jumbo.empty:
            st.info("No Jumbo Shipments (Qalyub segment, specific transporters) found with current filters.")
        else:
            # 1. Count daily shipments by plate
            df_jumbo_daily = df_jumbo.groupby(['plate_number_assigned', filtered_df['actual_shipment_date'].dt.date.rename('actual_shipment_date')])['shipment'].count().reset_index(name='shipment_count')
            # 2. Average the daily counts by plate
            df_jumbo_avg = df_jumbo_daily.groupby('plate_number_assigned')['shipment_count'].mean().reset_index(name='average_daily_shipments')

            fig_jumbo = px.bar(
                df_jumbo_avg.sort_values('average_daily_shipments', ascending=True),
                x='average_daily_shipments',
                y='plate_number_assigned',
                orientation='h',
                title='Average Daily Shipments by Vehicle Plate (Jumbo Routes)',
                color='average_daily_shipments',
                color_continuous_scale=px.colors.sequential.Plotly3
            )
            fig_jumbo.update_layout(yaxis_title="Vehicle Plate Number", xaxis_title="Avg. Daily Shipments")
            st.plotly_chart(fig_jumbo, use_container_width=True)

    # --- Chart 3: Dead Head Percentage by Transporter ---
    with col2:
        st.subheader("Dead Head Percentage by Transporter")

        # Now safe to use 'shipped_distance' and 'dead_head_distance'
        df_analysis = filtered_df.groupby('transporter_name').agg(
            total_shipped_distance=('shipped_distance', 'sum'),
            total_dead_head_distance=('dead_head_distance', 'sum')
        ).reset_index()

        df_analysis['total_distance'] = df_analysis['total_shipped_distance'] + df_analysis['total_dead_head_distance']
        
        # Calculate Dead Head %: (Dead Head / Total Distance) * 100
        # Use np.divide and fillna to safely handle zero division
        df_analysis['dead_head_percent'] = np.divide(
            df_analysis['total_dead_head_distance'] * 100,
            df_analysis['total_distance'],
            out=np.zeros_like(df_analysis['total_dead_head_distance']),
            where=df_analysis['total_distance']!=0
        )
        
        # Sort for better visualization
        df_analysis_sorted = df_analysis.sort_values('dead_head_percent', ascending=False)

        fig_percent = px.bar(
            df_analysis_sorted,
            x='dead_head_percent',
            y='transporter_name',
            orientation='h',
            title='Dead Head % of Total Distance (Sorted)',
            color='dead_head_percent',
            color_continuous_scale=px.colors.sequential.Reds,
            labels={'dead_head_percent': 'Dead Head (%)', 'transporter_name': 'Transporter'}
        )
        fig_percent.update_layout(yaxis_title="Transporter", xaxis_title="Dead Head Percentage")
        st.plotly_chart(fig_percent, use_container_width=True)
