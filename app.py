import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
import plotly.express as px
import json

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
df_shipments = df_shipments.merge(df_plate_numbers, left_on='vehicle_id', right_on='id', how='left')
df_shipments.drop(columns=['id', 'vehicle_id'], inplace=True)
df_rental = df_shipments[df_shipments['transporter_type_description'] == 'dedicated']

df_rental['next_shipping_point'] = df_rental.groupby('plate_number_assigned')['shipping_point'].shift(-1)
df_rental = df_rental.merge(df_distance, on=['shipping_point', 'receiving_point'], how='left')
df_rental['dead_head_distance'] = np.where(
    df_rental['shipping_point'] == df_rental['receiving_point'],
    0,
    np.random.uniform(10, 300, len(df_rental))
)
df_rental['total_distance'] = df_rental['distance'].fillna(0) + df_rental['dead_head_distance']
df_rental['actual_shipment_start'] = pd.to_datetime(df_rental['actual_shipment_start'])

# ------------------------------
# SIDEBAR FILTERS
# ------------------------------
st.sidebar.header("Filters")

min_date = df_shipments['actual_shipment_start'].min().date()
max_date = df_shipments['actual_shipment_start'].max().date()
selected_date = st.sidebar.date_input("Select date range", [min_date, max_date])

# Guard for single-date selection
if isinstance(selected_date, (tuple, list)) and len(selected_date) == 2:
    start_date, end_date = selected_date
else:
    start_date = end_date = selected_date

segments = st.sidebar.multiselect(
    "Segment", options=df_shipments['segment'].dropna().unique(),
    default=df_shipments['segment'].dropna().unique()
)
transporters = st.sidebar.multiselect(
    "Transporter", options=df_shipments['transporter_name'].dropna().unique(),
    default=df_shipments['transporter_name'].dropna().unique()
)
shipping_points = st.sidebar.multiselect(
    "Shipping Point", options=df_shipments['shipping_point'].dropna().unique(),
    default=df_shipments['shipping_point'].dropna().unique()
)
receiving_points = st.sidebar.multiselect(
    "Receiving Point", options=df_shipments['receiving_point'].dropna().unique(),
    default=df_shipments['receiving_point'].dropna().unique()
)
transporter_types = st.sidebar.multiselect(
    "Transporter Type", options=df_shipments['transporter_type_description'].dropna().unique(),
    default=df_shipments['transporter_type_description'].dropna().unique()
)

main_filtered_df = df_shipments[
    (df_shipments['actual_shipment_start'].dt.date >= start_date) &
    (df_shipments['actual_shipment_start'].dt.date <= end_date) &
    (df_shipments['segment'].isin(segments)) &
    (df_shipments['transporter_name'].isin(transporters)) &
    (df_shipments['shipping_point'].isin(shipping_points)) &
    (df_shipments['receiving_point'].isin(receiving_points)) &
    (df_shipments['transporter_type_description'].isin(transporter_types))
]

filtered_df = df_rental[
    (df_rental['actual_shipment_start'].dt.date >= start_date) &
    (df_rental['actual_shipment_start'].dt.date <= end_date) &
    (df_rental['segment'].isin(segments)) &
    (df_rental['transporter_name'].isin(transporters)) &
    (df_rental['shipping_point'].isin(shipping_points)) &
    (df_rental['receiving_point'].isin(receiving_points)) &
    (df_rental['transporter_type_description'].isin(transporter_types))
]

# ------------------------------
# TABS
# ------------------------------
tab_log, tab_fleet = st.tabs(["Daily Log", "Fleet Utilization"])

# ------------------------------
# TAB 1: DAILY LOG
# ------------------------------
with tab_log:
    st.subheader("🕒 Daily Attendance Log")

    df_assignments = pd.read_sql_query("""
        SELECT vehicle_assignment.vehicle_plate_number,
               transporter_info.transporter_name,
               segment_info.description AS segment
        FROM vehicle_assignment
        LEFT JOIN transporter_info ON transporter_info.id = vehicle_assignment.transporter_id
        LEFT JOIN segment_info ON segment_info.id = vehicle_assignment.segment_id
        WHERE vehicle_assignment.period_start_date < CURRENT_DATE
          AND vehicle_assignment.period_end_date > CURRENT_DATE
    """, conn)

    df_drivers = pd.read_sql_query("SELECT id, driver_name FROM driver_info", conn)
    driver_map = dict(zip(df_drivers["driver_name"], df_drivers["id"]))

    col1, col2, col3 = st.columns(3)
    with col1:
        selected_segment = st.selectbox("Segment", df_assignments["segment"].dropna().unique())
    df_filtered_segment = df_assignments[df_assignments["segment"] == selected_segment]

    with col2:
        selected_transporters = st.multiselect(
            "Transporter", df_filtered_segment["transporter_name"].dropna().unique()
        )
    df_filtered_transporter = df_filtered_segment[
        df_filtered_segment["transporter_name"].isin(selected_transporters)
    ]

    with col3:
        selected_plates = st.multiselect(
            "Plate Number", df_filtered_transporter["vehicle_plate_number"].unique()
        )

    date_range = st.date_input(
        "Select Date Range",
        [pd.Timestamp.today().replace(day=1), pd.Timestamp.today()]
    )

    if len(date_range) == 2:
        start_date, end_date = date_range
        days = pd.date_range(start=start_date, end=end_date)
    else:
        st.warning("Please select a valid date range.")
        st.stop()

    if selected_plates:
        st.write(f"### Attendance for {selected_segment} ({start_date.strftime('%d %b')} - {end_date.strftime('%d %b')})")

        month_start = pd.Timestamp(start_date).replace(day=1).date()
        query_attendance = """
            SELECT vehicle_plate_number, daily_log
            FROM rental_vehicles_log
            WHERE month = %s
              AND vehicle_plate_number = ANY(%s)
        """
        df_attendance = pd.read_sql(query_attendance, conn, params=(month_start, selected_plates))

        data = pd.DataFrame({"Vehicle": selected_plates, "Driver": [None] * len(selected_plates)})
        for day in days:
            data[str(day.date())] = 0

        for _, row in df_attendance.iterrows():
            plate = row["vehicle_plate_number"]
            daily_log = row["daily_log"]
            if isinstance(daily_log, str):
                daily_log = json.loads(daily_log)
            elif daily_log is None:
                daily_log = {}
            idx = data.index[data["Vehicle"] == plate]
            if not idx.empty:
                idx = idx[0]
                for day_str, val in daily_log.items():
                    if day_str in data.columns:
                        data.at[idx, day_str] = val

        col_config = {
            "Vehicle": st.column_config.Column(disabled=True),
            "Driver": st.column_config.SelectboxColumn(
                "Driver",
                options=df_drivers["driver_name"].tolist(),
                required=True,
                help="Assign a driver to this vehicle (unique per table)."
            ),
        }

        for day in days:
            col_config[str(day.date())] = st.column_config.SelectboxColumn(
                label=str(day.date()),
                options=[1, 0.5, 0],
                help="Mark 1 (Present), 0.5 (Half Day), or 0 (Absent).",
                default=0,
            )

        edited_df = st.data_editor(
            data,
            use_container_width=True,
            key="pivot_attendance",
            column_config=col_config,
            num_rows="fixed"
        )

        if edited_df["Driver"].duplicated().any():
            st.error("❌ Each driver can only be assigned to one vehicle.")
            st.stop()

        if st.button("✅ Submit Attendance"):
            cursor = conn.cursor()
            for _, row in edited_df.iterrows():
                plate = row["Vehicle"]
                driver_name = row["Driver"]
                if not driver_name:
                    continue
                driver_id = driver_map.get(driver_name)
                if not driver_id:
                    st.warning(f"Driver '{driver_name}' not found in database.")
                    continue
                daily_log = {
                    col: float(row[col]) for col in edited_df.columns if col not in ["Vehicle", "Driver"]
                }
                total_days = sum(daily_log.values())
                month_start = pd.Timestamp(start_date).replace(day=1).date()
                cursor.execute("""
                    INSERT INTO rental_vehicles_log (month, vehicle_plate_number, driver_id,
                                                     total_working_days, daily_log, last_updated)
                    VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
                    ON CONFLICT (month, vehicle_plate_number, driver_id)
                    DO UPDATE SET
                        total_working_days = EXCLUDED.total_working_days,
                        daily_log = EXCLUDED.daily_log,
                        last_updated = NOW();
                """, (month_start, plate, driver_id, total_days, json.dumps(daily_log)))
            conn.commit()
            st.success("✅ Attendance data submitted successfully!")

        st.markdown("---")
        st.subheader("📜 Attendance History")
        query = """
            SELECT r.month, r.vehicle_plate_number, d.driver_name,
                   r.total_working_days, r.daily_log, r.last_updated
            FROM rental_vehicles_log r
            LEFT JOIN driver_info d ON r.driver_id = d.id
            WHERE r.vehicle_plate_number = ANY(%s)
            ORDER BY r.last_updated DESC;
        """
        df_history = pd.read_sql(query, conn, params=(selected_plates,))
        if not df_history.empty:
            st.dataframe(df_history, use_container_width=True)
        else:
            st.info("No attendance history found for selected vehicles.")

# ------------------------------
# TAB 2: FLEET UTILIZATION
# ------------------------------
with tab_fleet:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Dead Head Distance Over Time")
        df_dead_head_distance = filtered_df.groupby('actual_shipment_start')['dead_head_distance'].sum().reset_index()
        fig1 = px.line(df_dead_head_distance, x='actual_shipment_start', y='dead_head_distance',
                       title='Dead Head Distance Over Time', markers=True, template='plotly_white')
        st.metric(label="Total Dead Head Distance", value=round({filtered_df['dead_head_distance'].sum():,}))
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        st.subheader("📈 Total Distance Travelled Over Time")
        df_distance_travelled = filtered_df.groupby('actual_shipment_start')['total_distance'].sum().reset_index()
        fig2 = px.line(df_distance_travelled, x='actual_shipment_start', y='total_distance',
                       title='Total Distance Travelled Over Time', markers=True, template='plotly_white')
        st.metric(label="Total Distance Travelled", value=round({filtered_df['total_distance'].sum():,}))
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    st.subheader("🚚 Average Jumbo Shipments")

    df_jumbo = filtered_df[
        (filtered_df['segment'] == 'Qalyub') &
        (filtered_df['transporter_name'].isin(['Al -Rehab Office for Transport and', 'Alwefaq national  transport']))
    ]

    if not df_jumbo.empty:
        df_jumbo_count = (
            df_jumbo.groupby(['plate_number_assigned', 'actual_shipment_start'])['shipment']
            .count()
            .reset_index()
        )
        df_jumbo_avg = df_jumbo_count.groupby('plate_number_assigned')['shipment'].mean().reset_index()
        average_jumbo = df_jumbo_avg['shipment'].mean()
        st.metric(label="Average Jumbo Shipments", value=round(average_jumbo, 2))

        fig3 = px.bar(df_jumbo_avg, x='shipment', y='plate_number_assigned',
                      title='Average Jumbo Shipments Per Plate Number', orientation='h', template='plotly_white')
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No Jumbo shipment data available for the selected filters.")
