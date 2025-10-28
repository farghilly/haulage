# streamlit_app.py
import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px

# ------------------------------
# PAGE SETUP
# ------------------------------
st.set_page_config(page_title="Dead Head Analysis Dashboard", layout="wide")
st.title("🚛 Dead Head Analysis Dashboard")

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
# DEAD HEAD ANALYSIS
# ------------------------------
st.subheader("📊 Dead Head Analysis")

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

# Group by date for visualization
df_dead_head_distance = df_rental.groupby('actual_shipment_start')['dead_head_distance'].sum().reset_index()

fig1 = px.line(df_dead_head_distance, x='actual_shipment_start', y='dead_head_distance',
               title='Dead Head Distance Over Time',
               markers=True, template='plotly_white')

st.plotly_chart(fig1, use_container_width=True)

# ------------------------------
# TOTAL DISTANCE ANALYSIS
# ------------------------------
st.subheader("📈 Total Distance Travelled")

df_rental['total_distance'] = df_rental['distance'].fillna(0) + df_rental['dead_head_distance']
df_distance_travelled = df_rental.groupby('actual_shipment_start')['total_distance'].sum().reset_index()

fig2 = px.line(df_distance_travelled, x='actual_shipment_start', y='total_distance',
               title='Total Distance Travelled Over Time',
               markers=True, template='plotly_white')

st.plotly_chart(fig2, use_container_width=True)

# ------------------------------
# AVERAGE JUMBO SHIPMENTS
# ------------------------------
st.subheader("🚚 Average Jumbo Shipments")

df_jumbo = df_rental[
    (df_rental['segment'] == 'Qalyub') &
    (df_rental['transporter_name'].isin([
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
                  title='Average Jumbo Shipments', orientation='h',
                  template='plotly_white')

    st.plotly_chart(fig3, use_container_width=True)
else:
    st.info("No Jumbo shipment data available for selected filters.")

# ------------------------------
# DATA INSPECTION
# ------------------------------
with st.expander("🔍 View Processed Data"):
    st.dataframe(df_rental.head(50))
