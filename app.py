import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import json
import calendar
from datetime import datetime, timedelta
from streamlit-aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode


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
# --- Helper Functions for The attendance Log---

@st.cache_data(ttl=3600)
def load_vehicle_assignments():
    query = """
    SELECT vehicle_assignment.vehicle_plate_number, transporter_info.transporter_name, segment_info.description AS segment
    FROM vehicle_assignment
    LEFT JOIN transporter_info ON transporter_info.id = vehicle_assignment.transporter_id
    LEFT JOIN segment_info ON segment_info.id = vehicle_assignment.segment_id
    WHERE vehicle_assignment.period_start_date < CURRENT_DATE AND period_end_date > CURRENT_DATE
    """
    return pd.read_sql_query(query, conn)

@st.cache_data(ttl=3600)
def load_drivers():
    query = "SELECT id, driver_name FROM driver_info"
    return pd.read_sql_query(query, conn)

def fetch_attendance_history(month, vehicle_list):
    if not vehicle_list:
        return pd.DataFrame()
    placeholders = ','.join(['%s'] * len(vehicle_list))
    query = f"""
        SELECT vehicle_plate_number, driver_id, daily_log, total_working_days
        FROM rental_vehicles_log
        WHERE month = %s AND vehicle_plate_number IN ({placeholders})
    """
    params = [month] + vehicle_list
    df = pd.read_sql_query(query, conn, params=params)
    return df

def upsert_attendance_log(month, records):
    cur = conn.cursor()
    for rec in records:
        cur.execute("""
            INSERT INTO rental_vehicles_log (month, vehicle_plate_number, driver_id, daily_log, total_working_days, last_updated)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (month, vehicle_plate_number)
            DO UPDATE SET
                driver_id = EXCLUDED.driver_id,
                daily_log = EXCLUDED.daily_log,
                total_working_days = EXCLUDED.total_working_days,
                last_updated = CURRENT_TIMESTAMP
        """, (month, rec['vehicle_plate_number'], rec['driver_id'], rec['daily_log'], rec['total_working_days']))
    conn.commit()
    cur.close()


# ------------------------------
# TABS
# ------------------------------
tab_log, tab_fleet = st.tabs(["Daily Log", "Fleet Utilization"])

with tab_log:
    st.subheader("🕒 Daily Attendance Log")
    # Load data
    df_assignments = load_vehicle_assignments()
    df_drivers = load_drivers()
    
    # Segment filter (single select)
    segments = df_assignments['segment'].dropna().unique()
    selected_segment = st.selectbox("Select Segment", options=segments)
    
    # Filter by segment
    df_seg = df_assignments[df_assignments['segment'] == selected_segment]
    
    # Transporter multi-select dependent on segment
    transporters = df_seg['transporter_name'].dropna().unique()
    selected_transporters = st.multiselect("Select Transporter(s)", options=transporters, default=transporters)
    
    # Filter by transporter
    df_trans = df_seg[df_seg['transporter_name'].isin(selected_transporters)]
    
    # Plate numbers multi-select dependent on above
    plate_numbers = df_trans['vehicle_plate_number'].dropna().unique()
    selected_plates = st.multiselect("Select Plate Number(s)", options=plate_numbers, default=plate_numbers)
    
    if not selected_plates:
        st.warning("Please select at least one plate number.")
        st.stop()
    
    # Current month info
    today = datetime.today()
    year, month = today.year, today.month
    month_start = datetime(year, month, 1)
    num_days = calendar.monthrange(year, month)[1]
    days = [month_start + timedelta(days=i) for i in range(num_days)]
    day_cols = [d.strftime("%Y-%m-%d") for d in days]
    
    month_str = month_start.strftime("%Y-%m-%d")
    
    # Fetch attendance history from DB
    history_df = fetch_attendance_history(month_str, list(selected_plates))
    
    # Prepare base data rows for pivot
    base_rows = []
    for plate in selected_plates:
        hist_rows = history_df[history_df['vehicle_plate_number'] == plate]
        if not hist_rows.empty:
            row = hist_rows.iloc[0]
            driver_id = row['driver_id']
            daily_log = row['daily_log'] if isinstance(row['daily_log'], list) else []
            total_days = row['total_working_days']
        else:
            driver_id = None
            daily_log = [None] * num_days
            total_days = 0
        base_rows.append({
            "vehicle_plate_number": plate,
            "driver_id": driver_id,
            "total_working_days": total_days,
            **{days[i].strftime("%Y-%m-%d"): daily_log[i] if i < len(daily_log) else None for i in range(num_days)}
        })
    
    df_pivot = pd.DataFrame(base_rows)
    
    # Map driver_id to driver_name
    driver_map = dict(zip(df_drivers['id'], df_drivers['driver_name']))
    df_pivot['driver_name'] = df_pivot['driver_id'].map(driver_map)
    
    # Reorder columns: vehicle_plate_number, driver_name, days..., total_working_days
    cols_order = ["vehicle_plate_number", "driver_name"] + day_cols + ["total_working_days"]
    df_pivot = df_pivot[cols_order]
    
    # Prepare driver dropdown options
    driver_names = df_drivers['driver_name'].tolist()
    
    # Setup AgGrid
    gb = GridOptionsBuilder.from_dataframe(df_pivot)
    
    # Plate number readonly
    gb.configure_column("vehicle_plate_number", header_name="Plate Number", editable=False)
    
    # Driver dropdown editable
    gb.configure_column("driver_name", header_name="Driver", editable=True,
                        cellEditor="agSelectCellEditor", cellEditorParams={"values": driver_names})
    
    # Daily log columns dropdown with options: 1, 0.5, 0, empty string
    for day_col in day_cols:
        gb.configure_column(day_col, header_name=day_col, editable=True,
                            cellEditor="agSelectCellEditor", cellEditorParams={"values": ["1", "0.5", "0", ""]})
    
    # Total working days readonly
    gb.configure_column("total_working_days", header_name="Total Working Days", editable=False)
    
    grid_options = gb.build()
    
    st.markdown("### Edit Attendance Daily Logs")
    
    grid_response = AgGrid(
        df_pivot,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.VALUE_CHANGED,
        fit_columns_on_grid_load=True,
        height=450,
        allow_unsafe_jscode=True,
    )
    
    edited_df = pd.DataFrame(grid_response['data'])
    
    # Validate unique driver assignment
    driver_counts = edited_df['driver_name'].value_counts()
    duplicates = driver_counts[driver_counts > 1].index.tolist()
    if duplicates:
        st.error(f"Driver(s) assigned to multiple rows: {', '.join(duplicates)}. Please assign unique drivers.")
    else:
        # Calculate total working days as sum of daily logs
        def parse_val(x):
            try:
                return float(x)
            except:
                return 0.0
    
        edited_df['total_working_days'] = edited_df[day_cols].applymap(parse_val).sum(axis=1)
    
        st.dataframe(edited_df)
    
        if st.button("Submit Attendance Log"):
            # Map driver_name back to driver_id
            driver_name_to_id = {v: k for k, v in driver_map.items()}
            records = []
            for _, row in edited_df.iterrows():
                driver_id = driver_name_to_id.get(row['driver_name'], None)
                if driver_id is None:
                    st.error(f"Driver '{row['driver_name']}' not found in DB. Please fix before submitting.")
                    st.stop()
    
                daily_log_list = []
                for day_col in day_cols:
                    val = row[day_col]
                    if val in ["1", "0.5", "0"]:
                        daily_log_list.append(float(val))
                    else:
                        daily_log_list.append(0.0)
    
                total_working_days = row['total_working_days']
                records.append({
                    'vehicle_plate_number': row['vehicle_plate_number'],
                    'driver_id': driver_id,
                    'daily_log': daily_log_list,
                    'total_working_days': total_working_days
                })
    
            # Upsert into DB
            upsert_attendance_log(month_str, records)
            st.success("Attendance log submitted successfully.")


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
