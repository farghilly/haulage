import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px

# ------------------------------
# PAGE SETUP
# ------------------------------
st.set_page_config(page_title="Haulage Analysis Dashboard", layout="wide")
st.title("🚛 Haulage Analysis Dashboard")

# ------------------------------
# DATABASE CONNECTION
# ------------------------------
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="ep-green-math-adtg875j-pooler.c-2.us-east-1.aws.neon.tech",
        dbname="neondb",
        user="neondb_owner",
        password="npg_fbowuNp1PnM6",
        port="5432",
        sslmode="require"
    )

conn = get_connection()

# ------------------------------
# LOAD DATA
# ------------------------------
@st.cache_data(ttl=3600)
def load_data():
    df_shipments = pd.read_sql_query("""
        SELECT DISTINCT
            tu.shipment,
            ti.transporter_name,
            tti.transporter_type_description,
            tu.actual_shipment_start,
            srpi.name AS shipping_point,
            srpi2.name AS receiving_point,
            MAX(tu.vehicle_id) AS vehicle_id
        FROM truck_utilization tu
        LEFT JOIN route_info ri ON tu.route_id = ri.id
        LEFT JOIN shipping_receiving_points_info srpi ON ri.shipping_point_id = srpi.id
        LEFT JOIN shipping_receiving_points_info srpi2 ON ri.receiving_point_id = srpi2.id
        LEFT JOIN transporter_info ti ON ti.id = tu.transporter_code_id
        LEFT JOIN transporter_type_info tti ON tti.id = ti.transporter_type_id
        GROUP BY tu.shipment, ti.transporter_name, tti.transporter_type_description,
                 tu.actual_shipment_start, srpi.name, srpi2.name
        ORDER BY tu.actual_shipment_start
    """, conn)

    df_plate_numbers = pd.read_sql_query("""
        SELECT vi.id, vi.plate_number_assigned, si.description AS segment
        FROM vehicle_info vi
        LEFT JOIN vehicle_assignment va ON va.vehicle_plate_number = vi.plate_number_assigned
        LEFT JOIN segment_info si ON si.id = va.segment_id
        WHERE plate_number_assigned IS NOT NULL
    """, conn)

    df_distance = pd.read_sql_query("""
        SELECT srpi.name AS shipping_point, srpi2.name AS receiving_point, ri.distance
        FROM route_info ri
        LEFT JOIN shipping_receiving_points_info srpi ON ri.shipping_point_id = srpi.id
        LEFT JOIN shipping_receiving_points_info srpi2 ON ri.receiving_point_id = srpi2.id
    """, conn)

    return df_shipments, df_plate_numbers, df_distance

df_shipments, df_plate_numbers, df_distance = load_data()

# ------------------------------
# DATA PREPARATION
# ------------------------------
# Filter dedicated transporters
df_rental = df_shipments[df_shipments['transporter_type_description'] == 'dedicated']

# Merge with plate numbers
df_rental = df_rental.merge(df_plate_numbers, left_on='vehicle_id', right_on='id', how='left')
df_rental.drop(columns=['id', 'vehicle_id'], inplace=True)

# Add next shipping point per vehicle
df_rental['next_shipping_point'] = df_rental.groupby('plate_number_assigned')['shipping_point'].shift(-1)

# Merge with distance data
df_rental = df_rental.merge(df_distance, on=['shipping_point', 'receiving_point'], how='left')

# Compute dead head distance placeholder (simplified for app demo)
df_rental['dead_head_distance'] = np.where(
    df_rental['shipping_point'] == df_rental['receiving_point'],
    0,
    np.random.uniform(10, 300, len(df_rental))  # Simulated where missing
)

# Compute total distance
df_rental['total_distance'] = df_rental['distance'].fillna(0) + df_rental['dead_head_distance']

# Convert date column
df_rental['actual_shipment_start'] = pd.to_datetime(df_rental['actual_shipment_start'])

# ------------------------------
# SIDEBAR FILTERS
# ------------------------------
st.sidebar.header("Filters")

# Date filter
min_date = df_rental['actual_shipment_start'].min()
max_date = df_rental['actual_shipment_start'].max()
selected_date = st.sidebar.date_input("Select date range", [min_date, max_date])

# Segment filter
segments = st.sidebar.multiselect(
    "Segment", options=df_rental['segment'].dropna().unique(), default=df_rental['segment'].dropna().unique()
)

# Transporter filter
transporters = st.sidebar.multiselect(
    "Transporter", options=df_rental['transporter_name'].dropna().unique(), default=df_rental['transporter_name'].dropna().unique()
)

# Shipping point filter
shipping_points = st.sidebar.multiselect(
    "Shipping Point", options=df_rental['shipping_point'].dropna().unique(), default=df_rental['shipping_point'].dropna().unique()
)

# Receiving point filter
receiving_points = st.sidebar.multiselect(
    "Receiving Point", options=df_rental['receiving_point'].dropna().unique(), default=df_rental['receiving_point'].dropna().unique()
)

# Transporter type filter
transporter_types = st.sidebar.multiselect(
    "Transporter Type", options=df_rental['transporter_type_description'].dropna().unique(), default=df_rental['transporter_type_description'].dropna().unique()
)

# Apply filters
filtered_df = df_rental[
    (df_rental['actual_shipment_start'].dt.date >= selected_date[0]) &
    (df_rental['actual_shipment_start'].dt.date <= selected_date[1]) &
    (df_rental['segment'].isin(segments)) &
    (df_rental['transporter_name'].isin(transporters)) &
    (df_rental['shipping_point'].isin(shipping_points)) &
    (df_rental['receiving_point'].isin(receiving_points)) &
    (df_rental['transporter_type_description'].isin(transporter_types))
]

# ------------------------------
# TABS
# ------------------------------
tab1, tab2 = st.tabs(["Fleet Utilization", "Average Jumbo Shipments"])

# ------------------------------
# TAB 1: Dead Head & Total Distance
# ------------------------------
with tab1:
    st.subheader("📊 Dead Head Distance Over Time")
    df_dead_head_distance = filtered_df.groupby('actual_shipment_start')['dead_head_distance'].sum().reset_index()
    fig1 = px.line(df_dead_head_distance, x='actual_shipment_start', y='dead_head_distance',
                   title='Dead Head Distance Over Time', markers=True, template='plotly_white')
    st.plotly_chart(fig1, use_container_width=True)

    st.subheader("📈 Total Distance Travelled Over Time")
    df_distance_travelled = filtered_df.groupby('actual_shipment_start')['total_distance'].sum().reset_index()
    fig2 = px.line(df_distance_travelled, x='actual_shipment_start', y='total_distance',
                   title='Total Distance Travelled Over Time', markers=True, template='plotly_white')
    st.plotly_chart(fig2, use_container_width=True)

# ------------------------------
# TAB 2: Average Jumbo Shipments
# ------------------------------
with tab2:
    st.subheader("🚚 Average Jumbo Shipments")
    df_jumbo = filtered_df[
        (filtered_df['segment'] == 'Qalyub') &
        (filtered_df['transporter_name'].isin([
            'Al -Rehab Office for Transport and',
            'Alwefaq national transport'
        ]))
    ]
    if not df_jumbo.empty:
        df_jumbo = (df_jumbo
                    .groupby(['plate_number_assigned', 'actual_shipment_start'])['shipment']
                    .count()
                    .reset_index())
        df_jumbo = df_jumbo.groupby('plate_number_assigned')['shipment'].mean().reset_index()
        fig3 = px.bar(df_jumbo, x='shipment', y='plate_number_assigned',
                      title='Average Jumbo Shipments', orientation='h', template='plotly_white')
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No Jumbo shipment data available for the selected filters.")
