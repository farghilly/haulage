import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import json
import calendar
from datetime import date, datetime, timedelta


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
# --- Functions for The attendance Log---
def load_assignments():
    query = """
    SELECT vehicle_assignment.vehicle_plate_number, transporter_info.transporter_name, segment_info.description AS segment
    FROM vehicle_assignment
    LEFT JOIN transporter_info ON transporter_info.id = vehicle_assignment.transporter_id
    LEFT JOIN segment_info ON segment_info.id = vehicle_assignment.segment_id
    WHERE vehicle_assignment.period_start_date < CURRENT_DATE AND period_end_date > CURRENT_DATE
    """
    return pd.read_sql_query(query, conn)

def load_drivers():
    query = "SELECT id, driver_name FROM driver_info"
    return pd.read_sql_query(query, conn)

def load_attendance_log(month):
    query = """
    SELECT month, vehicle_plate_number, driver_id, total_working_days, daily_log, last_updated
    FROM rental_vehicles_log
    WHERE month = %s
    """
    return pd.read_sql_query(query, conn, params=(month,))

def save_attendance_log(df_log):
    # Insert/update logic in DB
    with conn.cursor() as cur:
        for _, row in df_log.iterrows():
            cur.execute("""
                INSERT INTO rental_vehicles_log (month, vehicle_plate_number, driver_id, total_working_days, daily_log, last_updated)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (month, vehicle_plate_number)
                DO UPDATE SET driver_id = EXCLUDED.driver_id,
                              total_working_days = EXCLUDED.total_working_days,
                              daily_log = EXCLUDED.daily_log,
                              last_updated = CURRENT_TIMESTAMP
            """, (row['month'], row['vehicle_plate_number'], row['driver_id'], row['total_working_days'], row['daily_log']))
        conn.commit()

# --- TAB UI ---
def attendance_tab():
    st.title("📅 Daily Attendance Log")

    assignments = load_assignments()
    drivers = load_drivers()

    # Filters: only one segment can be chosen
    segment_options = assignments['segment'].unique()
    selected_segment = st.selectbox("Select Segment (only one)", options=segment_options)

    filtered_assignments = assignments[assignments['segment'] == selected_segment]

    # Transporter filter - multiple selectable
    transporter_options = filtered_assignments['transporter_name'].unique()
    selected_transporters = st.multiselect("Select Transporter(s)", options=transporter_options, default=transporter_options)

    filtered_assignments = filtered_assignments[filtered_assignments['transporter_name'].isin(selected_transporters)]

    # Plate numbers available based on filters
    plate_numbers = filtered_assignments['vehicle_plate_number'].unique()

    # Select date range for the daily logs (default current month)
    today = date.today()
    first_day = today.replace(day=1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    selected_month = st.date_input("Select month", value=first_day, min_value=first_day, max_value=last_day)
    
    # Load existing attendance log for the selected month
    log_df = load_attendance_log(selected_month.strftime("%Y-%m"))

    # Prepare daily columns: list of dates in the month
    days_in_month = pd.date_range(start=selected_month, end=last_day)

    # Create base DataFrame for attendance input
    base_df = pd.DataFrame({'vehicle_plate_number': plate_numbers})
    # Merge with existing log (driver and daily log stored as JSON string - we can parse that)
    base_df = base_df.merge(log_df[['vehicle_plate_number', 'driver_id', 'daily_log']], on='vehicle_plate_number', how='left')

    # Create a driver selection for each vehicle with dropdown, no duplicate drivers allowed
    driver_choices = list(drivers['driver_name'])
    assigned_drivers = []  # track selected drivers to avoid duplicates

    driver_id_map = dict(zip(drivers['driver_name'], drivers['id']))

    st.write("### Attendance Input Table")

    # Initialize container for inputs
    updated_rows = []

    for idx, row in base_df.iterrows():
        vehicle = row['vehicle_plate_number']

        # Driver selectbox - preselect from log_df if exists
        current_driver_id = row['driver_id'] if pd.notna(row['driver_id']) else None
        current_driver_name = None
        if current_driver_id:
            current_driver_name = drivers.loc[drivers['id'] == current_driver_id, 'driver_name'].values[0]

        # Disable drivers already assigned in other rows
        available_drivers = [d for d in driver_choices if d not in assigned_drivers or d == current_driver_name]

        driver_selected = st.selectbox(f"Driver for {vehicle}", options=["-- Select Driver --"] + available_drivers, index=(available_drivers.index(current_driver_name) + 1) if current_driver_name else 0, key=f"driver_{vehicle}")

        if driver_selected != "-- Select Driver --":
            assigned_drivers.append(driver_selected)

        # Daily log input for the month: create list for daily log entries
        daily_log = []

        for day in days_in_month:
            col_key = f"log_{vehicle}_{day.strftime('%Y-%m-%d')}"
            val = st.selectbox(f"{day.day} (1=Full, 0.5=Half, 0=None) for {vehicle}",
                               options=[1.0, 0.5, 0.0],
                               index=0,
                               key=col_key)
            daily_log.append(val)

        # Compute total working days
        total_working_days = sum(daily_log)

        # Prepare row to save
        updated_rows.append({
            'month': selected_month.strftime("%Y-%m"),
            'vehicle_plate_number': vehicle,
            'driver_id': driver_id_map.get(driver_selected, None) if driver_selected != "-- Select Driver --" else None,
            'total_working_days': total_working_days,
            'daily_log': daily_log
        })

    # Submit button
    if st.button("Submit Attendance Log"):
        # Check no duplicate drivers assigned
        if len(set([r['driver_id'] for r in updated_rows if r['driver_id']])) < len([r['driver_id'] for r in updated_rows if r['driver_id']]):
            st.error("Each driver can only be assigned to one vehicle.")
        else:
            # Save to DB
            df_to_save = pd.DataFrame(updated_rows)
            # Convert daily_log list to JSON string before saving
            df_to_save['daily_log'] = df_to_save['daily_log'].apply(lambda x: str(x))  # You may want to use json.dumps if you want
            save_attendance_log(df_to_save)
            st.success("Attendance log saved successfully.")

    # Show existing attendance logs below
    st.write("### Attendance History")
    st.dataframe(log_df)

# ------------------------------
# TABS
# ------------------------------
tab_log, tab_fleet = st.tabs(["Daily Log", "Fleet Utilization"])

with tab_log:
    attendance_tab()

with tab_fleet:
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

    st.markdown("---")

    st.subheader("🚚 Average Jumbo Shipments")
    df_jumbo = filtered_df[
        (filtered_df['segment'] == 'Qalyub') &
        (filtered_df['transporter_name'].isin(['Al -Rehab Office for Transport and','Alwefaq national  transport']))
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
