import os
from datetime import date, datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ============================================================
# PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Global Seismic Trends",
    page_icon="🌎",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🌎 Global Seismic Trends")
st.caption("Data-driven earthquake insights powered by USGS data and MySQL")

st.markdown(
    """
    Explore earthquake activity by time, magnitude, depth, location, and reporting network.
    The dashboard queries your existing `global_seismic_trends.earthquakes` MySQL table.
    """
)


# ============================================================
# DATABASE CONNECTION
# ============================================================

@st.cache_resource(show_spinner=False)
def get_engine(host, port, username, password, database):
    url = URL.create(
        "mysql+pymysql",
        username=username,
        password=password,
        host=host,
        port=int(port),
        database=database,
        query={"charset": "utf8mb4"},
    )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={"connect_timeout": 10},
    )


def read_sql(engine, query, params=None):
    with engine.connect() as connection:
        return pd.read_sql(
            text(query),
            connection,
            params=params or {},
        )


# ============================================================
# DATE FILTER HELPERS
# ============================================================

def normalize_date(value, fallback):
    """Convert Streamlit date input values to a single date."""

    while isinstance(value, (tuple, list)):
        if not value:
            return fallback
        value = value[0]

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    converted = pd.to_datetime(value, errors="coerce")

    if pd.isna(converted):
        return fallback

    return converted.date()


def get_filtered_where(start_date, end_date, min_mag, max_mag):
    """Build parameterized SQL filters with an inclusive end date."""

    clauses = [
        "`time` >= :start_time",
        "`time` < :end_exclusive",
        "(`mag` IS NULL OR `mag` BETWEEN :min_mag AND :max_mag)",
    ]

    params = {
        "start_time": datetime.combine(
            start_date,
            datetime.min.time(),
        ),
        "end_exclusive": datetime.combine(
            end_date + timedelta(days=1),
            datetime.min.time(),
        ),
        "min_mag": min_mag,
        "max_mag": max_mag,
    }

    return " AND ".join(clauses), params


# ============================================================
# DATABASE SETTINGS
# ============================================================

# Uses the environment variable when configured.
# The fallback matches your current local MySQL password.
# Change this password before publishing the project.
db_host = os.getenv("MYSQL_HOST", "localhost")
db_port = int(os.getenv("MYSQL_PORT", "3306"))
db_name = os.getenv("MYSQL_DATABASE", "global_seismic_trends")
db_user = os.getenv("MYSQL_USER", "root")
db_password = os.getenv("MYSQL_PASSWORD", "12345")

if not db_password:
    st.error("MySQL password is not configured.")
    st.stop()


# ============================================================
# CONNECT TO MYSQL
# ============================================================

try:
    engine = get_engine(
        db_host,
        db_port,
        db_user,
        db_password,
        db_name,
    )

    with engine.connect() as conn:
        mysql_version = conn.execute(
            text("SELECT VERSION()")
        ).scalar()

        row_count = conn.execute(
            text("SELECT COUNT(*) FROM `earthquakes`")
        ).scalar()

        date_bounds = conn.execute(
            text(
                """
                SELECT
                    MIN(`time`) AS min_time,
                    MAX(`time`) AS max_time
                FROM `earthquakes`
                """
            )
        ).mappings().one()

        columns = {
            row[0]
            for row in conn.execute(
                text("SHOW COLUMNS FROM `earthquakes`")
            ).fetchall()
        }

    required_columns = {"time", "mag"}

    if not required_columns.issubset(columns):
        st.error(
            "The earthquakes table must contain the `time` and `mag` columns."
        )
        st.stop()

except SQLAlchemyError as exc:
    st.error("Could not connect to MySQL or read the earthquakes table.")
    st.code(str(exc))
    st.info(
        "Check that MySQL is running, the database and table exist, "
        "and the credentials are correct."
    )
    st.stop()


# ============================================================
# AVAILABLE DATE RANGE
# ============================================================

min_db_time = (
    pd.to_datetime(date_bounds["min_time"]).date()
    if date_bounds["min_time"] is not None
    else date.today()
)

max_db_time = (
    pd.to_datetime(date_bounds["max_time"]).date()
    if date_bounds["max_time"] is not None
    else date.today()
)

default_start = max(
    min_db_time,
    max_db_time - timedelta(days=365 * 5),
)


# ============================================================
# SIDEBAR FILTERS
# ============================================================

with st.sidebar:
    st.header("Filters")
    st.caption("Filters apply to the charts and summary metrics.")

    selected_dates = st.date_input(
        "Date range",
        value=(default_start, max_db_time),
        min_value=min_db_time,
        max_value=max_db_time,
    )

    if isinstance(selected_dates, (tuple, list)):
        if len(selected_dates) >= 2:
            start_raw = selected_dates[0]
            end_raw = selected_dates[-1]
        elif len(selected_dates) == 1:
            start_raw = end_raw = selected_dates[0]
        else:
            start_raw = default_start
            end_raw = max_db_time
    else:
        start_raw = end_raw = selected_dates

    start_date = normalize_date(start_raw, default_start)
    end_date = normalize_date(end_raw, max_db_time)

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    min_mag, max_mag = st.slider(
        "Magnitude range",
        min_value=0.0,
        max_value=10.0,
        value=(0.0, 10.0),
        step=0.1,
    )


where_sql, params = get_filtered_where(
    start_date,
    end_date,
    min_mag,
    max_mag,
)

table = "`earthquakes`"


# ============================================================
# LOAD DASHBOARD DATA
# ============================================================

try:

    # --------------------------------------------------------
    # KPI SUMMARY
    # --------------------------------------------------------

    kpis = read_sql(
        engine,
        f"""
        SELECT
            COUNT(*) AS event_count,
            ROUND(AVG(mag), 2) AS average_magnitude,
            MAX(mag) AS strongest_magnitude,
            ROUND(AVG(depth_km), 2) AS average_depth_km,
            SUM(
                CASE
                    WHEN tsunami = 1 THEN 1
                    ELSE 0
                END
            ) AS tsunami_flagged
        FROM {table}
        WHERE {where_sql}
        """,
        params,
    ).iloc[0]

    # --------------------------------------------------------
    # YEARLY EARTHQUAKE ACTIVITY
    # --------------------------------------------------------

    yearly = read_sql(
        engine,
        f"""
        SELECT
            YEAR(`time`) AS year,
            COUNT(*) AS event_count
        FROM {table}
        WHERE {where_sql}
          AND `time` IS NOT NULL
        GROUP BY YEAR(`time`)
        ORDER BY year
        """,
        params,
    )

    # --------------------------------------------------------
    # MONTHLY EARTHQUAKE ACTIVITY
    #
    # FIX:
    # Build the first day of each month using CAST and CONCAT.
    # The exact same expression appears in GROUP BY.
    # No DATE_FORMAT format strings are required.
    # This supports ONLY_FULL_GROUP_BY.
    # --------------------------------------------------------

    month_start_expression = """
        CAST(
            CONCAT(
                YEAR(`time`), '-',
                LPAD(MONTH(`time`), 2, '0'),
                '-01'
            ) AS DATE
        )
    """

    monthly = read_sql(
        engine,
        f"""
        SELECT
            {month_start_expression} AS month_period,
            COUNT(*) AS event_count
        FROM {table}
        WHERE {where_sql}
          AND `time` IS NOT NULL
        GROUP BY {month_start_expression}
        ORDER BY month_period
        """,
        params,
    )

    if not monthly.empty:
        monthly["month_period"] = pd.to_datetime(
            monthly["month_period"],
            errors="coerce",
        )

        monthly["event_count"] = pd.to_numeric(
            monthly["event_count"],
            errors="coerce",
        )

        monthly = (
            monthly.dropna(
                subset=["month_period", "event_count"]
            )
            .sort_values("month_period")
        )

    # --------------------------------------------------------
    # MAGNITUDE DISTRIBUTION
    # --------------------------------------------------------

    magnitude_distribution = read_sql(
        engine,
        f"""
        SELECT
            CASE
                WHEN mag IS NULL THEN 'Missing'
                WHEN mag < 2 THEN 'Below 2.0'
                WHEN mag < 3 THEN '2.0–2.9'
                WHEN mag < 4 THEN '3.0–3.9'
                WHEN mag < 5 THEN '4.0–4.9'
                WHEN mag < 6 THEN '5.0–5.9'
                WHEN mag < 7 THEN '6.0–6.9'
                ELSE '7.0+'
            END AS magnitude_band,
            COUNT(*) AS event_count,
            MIN(mag) AS band_order
        FROM {table}
        WHERE {where_sql}
        GROUP BY
            CASE
                WHEN mag IS NULL THEN 'Missing'
                WHEN mag < 2 THEN 'Below 2.0'
                WHEN mag < 3 THEN '2.0–2.9'
                WHEN mag < 4 THEN '3.0–3.9'
                WHEN mag < 5 THEN '4.0–4.9'
                WHEN mag < 6 THEN '5.0–5.9'
                WHEN mag < 7 THEN '6.0–6.9'
                ELSE '7.0+'
            END
        ORDER BY band_order
        """,
        params,
    )

    # --------------------------------------------------------
    # DEPTH CATEGORIES
    # --------------------------------------------------------

    depth_categories = read_sql(
        engine,
        f"""
        SELECT
            CASE
                WHEN depth_km IS NULL THEN 'Missing'
                WHEN depth_km < 0 THEN 'Review - negative'
                WHEN depth_km < 70 THEN 'Shallow (<70 km)'
                WHEN depth_km <= 300 THEN 'Intermediate (70–300 km)'
                ELSE 'Deep (>300 km)'
            END AS depth_category,
            COUNT(*) AS event_count,
            ROUND(AVG(mag), 2) AS average_magnitude,
            MIN(depth_km) AS depth_order
        FROM {table}
        WHERE {where_sql}
        GROUP BY
            CASE
                WHEN depth_km IS NULL THEN 'Missing'
                WHEN depth_km < 0 THEN 'Review - negative'
                WHEN depth_km < 70 THEN 'Shallow (<70 km)'
                WHEN depth_km <= 300 THEN 'Intermediate (70–300 km)'
                ELSE 'Deep (>300 km)'
            END
        ORDER BY depth_order
        """,
        params,
    )

    # --------------------------------------------------------
    # REPORTING NETWORK ACTIVITY
    # --------------------------------------------------------

    if "net" in columns:
        network_activity = read_sql(
            engine,
            f"""
            SELECT
                COALESCE(NULLIF(net, ''), 'Unknown') AS network,
                COUNT(*) AS event_count,
                ROUND(AVG(mag), 2) AS average_magnitude
            FROM {table}
            WHERE {where_sql}
            GROUP BY COALESCE(NULLIF(net, ''), 'Unknown')
            ORDER BY event_count DESC
            LIMIT 12
            """,
            params,
        )
    else:
        network_activity = pd.DataFrame()

    # --------------------------------------------------------
    # EARTHQUAKE MAP DATA
    # --------------------------------------------------------

    location_fields = {
        "latitude",
        "longitude",
        "depth_km",
        "place",
        "id",
        "mag",
        "time",
    }

    if location_fields.issubset(columns):
        map_events = read_sql(
            engine,
            f"""
            SELECT
                id,
                `time`,
                latitude,
                longitude,
                depth_km,
                mag,
                place
            FROM {table}
            WHERE {where_sql}
              AND latitude BETWEEN -90 AND 90
              AND longitude BETWEEN -180 AND 180
            ORDER BY `time` DESC
            LIMIT 5000
            """,
            params,
        )
    else:
        map_events = pd.DataFrame()

    # --------------------------------------------------------
    # STRONGEST EARTHQUAKES
    # --------------------------------------------------------

    strongest = read_sql(
        engine,
        f"""
        SELECT
            id,
            `time`,
            mag,
            depth_km,
            place,
            latitude,
            longitude
        FROM {table}
        WHERE {where_sql}
          AND mag IS NOT NULL
        ORDER BY mag DESC, `time` DESC
        LIMIT 10
        """,
        params,
    )

except SQLAlchemyError as exc:
    st.error(
        "A dashboard query failed. Check the table schema "
        "and MySQL error below."
    )
    st.code(str(exc))
    st.stop()


# ============================================================
# CONNECTION STATUS
# ============================================================

st.caption(
    f"Connected to MySQL {mysql_version} · "
    f"Database records: {row_count:,} · "
    f"Filtered period: {start_date} to {end_date}"
)


# ============================================================
# KPI CARDS
# ============================================================

m1, m2, m3, m4, m5 = st.columns(5)

m1.metric(
    "Earthquakes",
    f"{int(kpis['event_count'] or 0):,}",
)

m2.metric(
    "Average magnitude",
    "—"
    if pd.isna(kpis["average_magnitude"])
    else f"{kpis['average_magnitude']:.2f}",
)

m3.metric(
    "Strongest magnitude",
    "—"
    if pd.isna(kpis["strongest_magnitude"])
    else f"{kpis['strongest_magnitude']:.1f}",
)

m4.metric(
    "Average depth",
    "—"
    if pd.isna(kpis["average_depth_km"])
    else f"{kpis['average_depth_km']:.1f} km",
)

m5.metric(
    "Tsunami-flagged",
    f"{int(kpis['tsunami_flagged'] or 0):,}",
)

st.divider()


# ============================================================
# DASHBOARD TABS
# ============================================================

tab_overview, tab_map, tab_magnitude, tab_depth, tab_records = st.tabs(
    [
        "Overview",
        "Earthquake map",
        "Magnitude",
        "Depth & networks",
        "Strongest events",
    ]
)


# ============================================================
# TAB 1: OVERVIEW
# ============================================================

with tab_overview:
    left, right = st.columns(2)

    # Yearly activity
    with left:
        st.subheader("Earthquakes by year")

        if not yearly.empty:
            fig = px.bar(
                yearly,
                x="year",
                y="event_count",
                labels={
                    "year": "Year",
                    "event_count": "Earthquakes",
                },
            )

            fig.update_layout(
    margin=dict(l=10, r=10, t=25, b=10),
    xaxis=dict(
        title="Year",
        type="category",
        categoryorder="category ascending",
    ),
    yaxis=dict(
        title="Earthquakes",
        rangemode="tozero",
        tickformat=",d",
    ),
)

            st.plotly_chart(
                fig,
                use_container_width=True,
            )
        else:
            st.info("No records match the selected filters.")

    # Monthly activity
    with right:
        st.subheader("Monthly activity")

        if not monthly.empty:
            fig = px.line(
                monthly,
                x="month_period",
                y="event_count",
                markers=True,
                labels={
                    "month_period": "Month",
                    "event_count": "Earthquakes",
                },
            )

            fig.update_layout(
                margin=dict(l=10, r=10, t=25, b=10),
                xaxis=dict(
                    title="Month",
                    tickformat="%b %Y",
                    type="date",
                ),
                yaxis=dict(
                    title="Earthquakes",
                    rangemode="tozero",
                    tickformat=",d",
                ),
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )
        else:
            st.info(
                "No valid monthly data was returned for the selected filters."
            )

    # Magnitude distribution
    st.subheader("Magnitude distribution")

    if not magnitude_distribution.empty:
        fig = px.bar(
            magnitude_distribution,
            x="magnitude_band",
            y="event_count",
            labels={
                "magnitude_band": "Magnitude band",
                "event_count": "Earthquakes",
            },
        )

        fig.update_layout(
            margin=dict(l=10, r=10, t=25, b=10),
            yaxis=dict(
                rangemode="tozero",
                tickformat=",d",
            ),
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )
    else:
        st.info("No magnitude data for the selected filters.")


# ============================================================
# TAB 2: EARTHQUAKE MAP
# ============================================================

with tab_map:
    st.subheader("Recent earthquake locations")

    st.caption(
        "The map displays up to 5,000 matching events, prioritizing recent events."
    )

    if not map_events.empty:
        map_events["time"] = pd.to_datetime(
            map_events["time"],
            errors="coerce",
        )

        map_events["mag_display"] = (
            pd.to_numeric(
                map_events["mag"],
                errors="coerce",
            )
            .fillna(0.0)
            .clip(lower=0)
        )

        fig = px.scatter_map(
            map_events,
            lat="latitude",
            lon="longitude",
            color="mag_display",
            size="mag_display",
            size_max=13,
            hover_name="place",
            hover_data={
                "time": True,
                "mag": True,
                "depth_km": True,
                "latitude": ":.3f",
                "longitude": ":.3f",
                "mag_display": False,
            },
            zoom=0,
            height=650,
            color_continuous_scale="Turbo",
            labels={
                "mag_display": "Magnitude",
                "depth_km": "Depth (km)",
            },
        )

        fig.update_layout(
            map_style="open-street-map",
            margin=dict(l=0, r=0, t=0, b=0),
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
        )
    else:
        st.info(
            "Location fields are missing or no valid coordinates match these filters."
        )


# ============================================================
# TAB 3: MAGNITUDE
# ============================================================

with tab_magnitude:
    st.subheader("Magnitude bands")

    if not magnitude_distribution.empty:
        st.dataframe(
            magnitude_distribution.drop(
                columns=["band_order"],
                errors="ignore",
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No magnitude data for the selected filters.")

    st.subheader("Top 10 strongest matching events")

    if not strongest.empty:
        st.dataframe(
            strongest,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No events match the selected filters.")


# ============================================================
# TAB 4: DEPTH AND NETWORKS
# ============================================================

with tab_depth:
    left, right = st.columns(2)

    with left:
        st.subheader("Depth categories")

        if not depth_categories.empty:
            fig = px.bar(
                depth_categories,
                x="depth_category",
                y="event_count",
                labels={
                    "depth_category": "Depth category",
                    "event_count": "Earthquakes",
                },
            )

            fig.update_layout(
                margin=dict(l=10, r=10, t=25, b=10),
                yaxis=dict(
                    rangemode="tozero",
                    tickformat=",d",
                ),
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

            st.dataframe(
                depth_categories.drop(
                    columns=["depth_order"],
                    errors="ignore",
                ),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No depth data for the selected filters.")

    with right:
        st.subheader("Reporting network activity")

        if not network_activity.empty:
            fig = px.bar(
                network_activity.sort_values("event_count"),
                x="event_count",
                y="network",
                orientation="h",
                labels={
                    "network": "Network",
                    "event_count": "Earthquakes",
                },
            )

            fig.update_layout(
                margin=dict(l=10, r=10, t=25, b=10),
                xaxis=dict(
                    rangemode="tozero",
                    tickformat=",d",
                ),
            )

            st.plotly_chart(
                fig,
                use_container_width=True,
            )

            st.dataframe(
                network_activity,
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info(
                "The `net` field is unavailable or no matching records were found."
            )


# ============================================================
# TAB 5: STRONGEST EVENTS AND CSV EXPORT
# ============================================================

with tab_records:
    st.subheader("Strongest events in the selected period")

    st.dataframe(
        strongest,
        use_container_width=True,
        hide_index=True,
    )

    csv = strongest.to_csv(index=False).encode("utf-8")

    st.download_button(
        "Download strongest events (CSV)",
        data=csv,
        file_name="strongest_earthquakes.csv",
        mime="text/csv",
    )


# ============================================================
# FOOTER
# ============================================================

st.divider()

st.caption(
    "Data source: USGS earthquake records stored in your local MySQL database. "
    "Negative depths are flagged for review rather than silently removed. "
    "Tsunami flags indicate a USGS event flag and do not by themselves describe impact."
)