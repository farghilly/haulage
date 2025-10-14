import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime
import os
import time

# NOTE: If you deploy this to a live environment (like Render), 
# you will need to install the actual database connector:
# import psycopg2 
# The application uses os.environ.get() to securely read credentials.

# --- Configuration ---
st.set_page_config(
    page_title="Logistics Efficiency Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Data Loading and Preparation (Secure and Deployment Ready) ---

# Mock Database Credentials (Read securely from environment variables)
# IMPORTANT: You must set these variables on your hosting platform (e.g., Render)
DB_HOST = os.environ.get('DB_HOST', 'placeholder_host')
DB_NAME = os.environ.get('DB_NAME', 'placeholder_db')
DB_USER = os.environ.get('DB_USER', 'placeholder_user')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'placeholder_password')
DB_PORT = os.environ.get('DB_PORT', '5432')

@st.cache_data(show_spinner="Connecting to Database and Processing Data...")
def load_and_process_data():
    """
    Simulates connecting to the Neon DB, fetching data, and performing initial calculations.
    
    In a real application, replace the 'SIMULATING DATABASE CONNECTION' block 
    with your actual psycopg2 connection and SQL query logic.
    """
    
    # --- SIMULATING DATABASE CONNECTION (Replace this block with your code) ---
    st.write(f"Attempting connection to DB: {DB_NAME} on {DB_HOST}:{DB_PORT}...")
    time.sleep(1) # Simulate connection time

    try:
        # # ACTUAL DB CONNECTION LOGIC (UNCOMMENT AND USE)
        # conn = psycopg2.connect(
        #     host=DB_HOST,
        #     dbname=DB_NAME,
        #     user=DB_USER,
        #     password=DB_PASSWORD,
        #     port=DB_PORT,
        #     sslmode='require'
        # )
        # st.success("Database connection simulated successfully!")
        
        # # Replace this with your actual SQL query:
        # query = "SELECT * FROM your_logistics_table;"
        # df = pd.read_sql(query, conn)
        # conn.close()
        
        # --- MOCK DATA GENERATION (To be replaced by actual data) ---
        np.random.seed(42)
        dates = pd.to_datetime(pd.date_range('2024-01-01', periods=100, freq='D'))
        transporters = ['Al -Rehab Office for Transport and', 'Alwefaq national transport', 'Generic Transporter A', 'Transporter Beta']
        segments = ['Qalyub', 'Giza', 'Alexandria', 'Ismailia']
        locations = ['Port Said', 'Cairo', 'Suez', 'Alexandria', 'Qalyub', 'Aswan']

        N = 1000
        data = {
            'shipment': [f'S{i}' for i in range(N)],
            'actual_shipment_start': np.random.choice(dates, N),
            'transporter_name': np.random.choice(transporters, N, p=[0.25, 0.25, 0.3, 0.2]),
            'transporter_type_description': np.random.choice(['dedicated', 'spot'], N, p=[0.7, 0.3]),
            'shipping_point': np.random.choice(locations, N),
            'receiving_point': np.random.choice(locations, N),
            'plate_number_assigned': np.random.choice([f'PLATE{i:02d}' for i in range(1, 15)], N),
            'segment': np.random.choice(segments, N),
            'dead_head_distance': np.random.uniform(0, 200, N).round(2),
            'shipped_distance': np.random.uniform(100, 1000, N).round(2)
        }
        df = pd.DataFrame(data)
        st.success("Successfully loaded mock data!")
        # --- END MOCK DATA GENERATION ---

    except Exception as e:
        st.error(f"Error during database connection or query: {e}")
        st.stop() # Stop the app if data can't be loaded

    # --- Initial Data Cleaning and Calculation ---
    
    # 1. Convert shipment start to date object
    df['actual_shipment_date'] = df['actual_shipment_start'].dt.date
    
    # 2. Apply dead-head logic (Only dedicated transporters have non-zero dead head)
    # This reflects the logic from the original notebook
    df['dead_head_distance'] = np.where(
        df['transporter_type_description'] == 'spot', 
        0, 
        df['dead_head_distance']
    )
    
    return df

df_rental = load_and_process_data()

# --- Filter Setup (Sidebar) ---
st.sidebar.header("Filter Data")

# Date Filter
min_date = df_rental['actual_shipment_date'].min()
max_date = df_rental['actual_shipment_date'].max()
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
    options=df_rental['transporter_name'].unique(),
    default=df_rental['transporter_name'].unique()
)

# Transporter Type Filter
selected_types = st.sidebar.multiselect(
    "Transporter Type",
    options=df_rental['transporter_type_description'].unique(),
    default=df_rental['transporter_type_description'].unique()
)

# Shipping/Receiving Point Filters
selected_shippings = st.sidebar.multiselect(
    "Shipping Point",
    options=df_rental['shipping_point'].unique(),
    default=df_rental['shipping_point'].unique()
)

selected_receivings = st.sidebar.multiselect(
    "Receiving Point",
    options=df_rental['receiving_point'].unique(),
    default=df_rental['receiving_point'].unique()
)

# Segment Filter
selected_segments = st.sidebar.multiselect(
    "Segment",
    options=df_rental['segment'].unique(),
    default=df_rental['segment'].unique()
)

# --- Apply Filters ---
filtered_df = df_rental[
    (df_rental['actual_shipment_date'] >= start_date_filter) &
    (df_rental['actual_shipment_date'] <= end_date_filter) &
    df_rental['transporter_name'].isin(selected_transporters) &
    df_rental['transporter_type_description'].isin(selected_types) &
    df_rental['shipping_point'].isin(selected_shippings) &
    df_rental['receiving_point'].isin(selected_receivings) &
    df_rental['segment'].isin(selected_segments)
].copy()

# --- Main Dashboard Layout ---
st.title("Logistics Utilization and Dead Head Analysis")

if filtered_df.empty:
    st.warning("No data available based on the selected filters.")
else:
    # --- Row 1: Dead Head Distance Over Time ---
    st.subheader("1. Dead Head Distance Over Time")
    df_dead_head_distance = filtered_df.groupby('actual_shipment_date')['dead_head_distance'].sum().reset_index()
    
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
        st.subheader("2. Average Jumbo Shipments per Vehicle")
        
        # Apply Jumbo-specific filters based on the original notebook logic
        df_jumbo = filtered_df[
            (filtered_df['segment'] == 'Qalyub') &
            (filtered_df['transporter_name'].isin(['Al -Rehab Office for Transport and', 'Alwefaq national transport']))
        ].copy()

        if df_jumbo.empty:
            st.info("No Jumbo Shipments (Qalyub segment, specific transporters) found with current filters.")
        else:
            # 1. Count daily shipments by plate
            df_jumbo_daily = df_jumbo.groupby(['plate_number_assigned', 'actual_shipment_date'])['shipment'].count().reset_index(name='shipment_count')
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
        st.subheader("3. Dead Head Percentage by Transporter")

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
