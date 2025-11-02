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

    segment_options = assignments['segment'].unique()
    selected_segment = st.selectbox("Select Segment (only one)", options=segment_options)

    filtered_assignments = assignments[assignments['segment'] == selected_segment]

    transporter_options = filtered_assignments['transporter_name'].unique()
    selected_transporters = st.multiselect("Select Transporter(s)", options=transporter_options, default=transporter_options)

    filtered_assignments = filtered_assignments[filtered_assignments['transporter_name'].isin(selected_transporters)]

    plate_numbers = filtered_assignments['vehicle_plate_number'].unique()

    today = date.today()
    first_day = today.replace(day=1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    selected_month = st.date_input("Select month", value=first_day, min_value=first_day, max_value=last_day)

    log_df = load_attendance_log(selected_month.strftime("%Y-%m-01"))

    days_in_month = pd.date_range(start=selected_month, end=last_day)

    base_df = pd.DataFrame({'vehicle_plate_number': plate_numbers})

    # Parse daily_log from JSON string to list
    log_df['daily_log'] = log_df['daily_log'].apply(lambda x: json.loads(x) if pd.notna(x) else [0]*len(days_in_month))

    # Merge
    base_df = base_df.merge(log_df[['vehicle_plate_number', 'driver_id', 'daily_log']], on='vehicle_plate_number', how='left')

    # Add columns for each day
    for i, day in enumerate(days_in_month):
        base_df[day.strftime('%Y-%m-%d')] = base_df['daily_log'].apply(lambda x: x[i] if len(x) > i else 0)

    # Drop daily_log column (no longer needed)
    base_df.drop(columns=['daily_log'], inplace=True)

    # Add driver names to base_df
    driver_map = dict(zip(drivers['id'], drivers['driver_name']))
    base_df['driver_name'] = base_df['driver_id'].map(driver_map)

    # Prepare driver choices
    driver_choices = ["-- Select Driver --"] + list(drivers['driver_name'])

    # Add driver selection column
    if 'driver_name' not in base_df.columns:
        base_df['driver_name'] = "-- Select Driver --"

    # Show editable table with Streamlit experimental_data_editor
    edited_df = st.experimental_data_editor(
        base_df,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            'vehicle_plate_number': st.column_config.TextColumn('Vehicle Plate Number', disabled=True),
            'driver_name': st.column_config.SelectboxColumn('Driver', options=driver_choices),
            # For day columns, you can restrict to numeric inputs 0, 0.5, 1 if you want
            **{day.strftime('%Y-%m-%d'): st.column_config.NumberColumn(
                day.strftime('%d'),
                min_value=0.0,
                max_value=1.0,
                step=0.5
            ) for day in days_in_month}
        }
    )

    if st.button("Submit Attendance Log"):
        # Validate no duplicate drivers except "-- Select Driver --"
        drivers_selected = edited_df['driver_name'].tolist()
        drivers_filtered = [d for d in drivers_selected if d != "-- Select Driver --"]
        if len(set(drivers_filtered)) < len(drivers_filtered):
            st.error("Each driver can only be assigned to one vehicle.")
            return

        # Prepare dataframe to save
        save_rows = []
        for _, row in edited_df.iterrows():
            vehicle = row['vehicle_plate_number']
            driver = row['driver_name']
            driver_id = drivers.loc[drivers['driver_name'] == driver, 'id'].values[0] if driver != "-- Select Driver --" else None

            # Collect daily logs as list in correct order
            daily_logs = [row[day.strftime('%Y-%m-%d')] for day in days_in_month]

            total_working_days = sum(daily_logs)

            save_rows.append({
                'month': selected_month.strftime("%Y-%m-01"),
                'vehicle_plate_number': vehicle,
                'driver_id': driver_id,
                'total_working_days': total_working_days,
                'daily_log': json.dumps(daily_logs)
            })

        df_to_save = pd.DataFrame(save_rows)
        save_attendance_log(df_to_save)
        st.success("Attendance log saved successfully.")

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
