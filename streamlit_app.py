
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text


DB_USER = "root"
DB_PASSWORD = "12345"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "earthquake_db"

engine = create_engine(f"mysql+mysqlconnector://root:12345@localhost:3306/earthquake_db")


questions = {
    "1. Top 10 strongest earthquakes (mag)": """
        SELECT place, mag, time
        FROM earthquakes
        ORDER BY mag DESC
        LIMIT 10;
    """,
    "2. Top 10 deepest earthquakes (depth_km)": """
        SELECT place, depth_km, time
        FROM earthquakes
        ORDER BY depth_km DESC
        LIMIT 10;
    """,
    "3. Shallow earthquakes < 50 km and mag > 7.5": """
        SELECT place, depth_km, mag, time
        FROM earthquakes
        WHERE depth_km < 50 AND mag > 7.5
        ORDER BY mag DESC;
    """,
    "4. Average depth per continent": """
        SELECT continent, AVG(depth_km) AS avg_depth
        FROM earthquakes
        GROUP BY continent;
    """,
    "5. Average magnitude per magnitude type (magType)": """
        SELECT magType, AVG(mag) AS avg_mag
        FROM earthquakes
        GROUP BY magType;
    """,
    "6. Year with most earthquakes": """
        SELECT year, COUNT(*) AS quake_count
        FROM earthquakes
        GROUP BY year
        ORDER BY quake_count DESC
        LIMIT 1;
    """,
    "7. Month with highest number of earthquakes": """
        SELECT month, COUNT(*) AS quake_count
        FROM earthquakes
        GROUP BY month
        ORDER BY quake_count DESC
        LIMIT 1;
    """,
    "8. Day of week with most earthquakes": """
        SELECT DAYNAME(time) AS weekday, COUNT(*) AS quake_count
        FROM earthquakes
        GROUP BY weekday
        ORDER BY quake_count DESC;
    """,
    "9. Count of earthquakes per hour of day": """
        SELECT HOUR(time) AS hour_of_day, COUNT(*) AS quake_count
        FROM earthquakes
        GROUP BY hour_of_day
        ORDER BY hour_of_day;
    """,
    "10. Most active reporting network (net)": """
        SELECT net, COUNT(*) AS quake_count
        FROM earthquakes
        GROUP BY net
        ORDER BY quake_count DESC
        LIMIT 1;
    """,
    "11. Top 5 places with highest casualties": """
        SELECT place, casualties
        FROM earthquakes
        ORDER BY casualties DESC
        LIMIT 5;
    """,
    "12. Total estimated economic loss per continent": """
        SELECT continent, SUM(economic_loss) AS total_loss
        FROM earthquakes
        GROUP BY continent
        ORDER BY total_loss DESC;
    """,
    "13. Average economic loss by alert level": """
        SELECT alert_level, AVG(economic_loss) AS avg_loss
        FROM earthquakes
        GROUP BY alert_level;
    """,
    "14. Count of reviewed vs automatic earthquakes (status)": """
        SELECT status, COUNT(*) AS count
        FROM earthquakes
        GROUP BY status;
    """,
    "15. Count by earthquake type (type)": """
        SELECT type, COUNT(*) AS count
        FROM earthquakes
        GROUP BY type;
    """,
    "16. Number of earthquakes by data type (types)": """
        SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(types, ',', n.n), ',', -1) AS data_type,
               COUNT(*) AS count
        FROM earthquakes
        JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 
              UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8) n
          ON LENGTH(types) - LENGTH(REPLACE(types, ',', '')) >= n.n - 1
        GROUP BY data_type
        ORDER BY count DESC;
    """,
    "17. Average RMS and gap per continent": """
        SELECT continent, AVG(rms) AS avg_rms, AVG(gap) AS avg_gap
        FROM earthquakes
        GROUP BY continent;
    """,
    "18. Events with high station coverage (nst > threshold)": """
        SELECT place, nst, time
        FROM earthquakes
        WHERE nst > 50
        ORDER BY nst DESC;
    """,
    "19. Number of tsunamis triggered per year": """
        SELECT year, COUNT(*) AS tsunami_count
        FROM earthquakes
        WHERE tsunami = 1
        GROUP BY year;
    """,
    "20. Count earthquakes by alert levels": """
        SELECT alert_level, COUNT(*) AS count
        FROM earthquakes
        GROUP BY alert_level
        ORDER BY count DESC;
    """,
     "21. Top 5 countries with highest avg magnitude in past 10 years": """
        SELECT country, AVG(mag) AS avg_mag
        FROM earthquakes
        WHERE year >= YEAR(CURDATE()) - 10
        GROUP BY country
        ORDER BY avg_mag DESC
        LIMIT 5;
    """,
    "22. Countries with both shallow and deep earthquakes within same month": """
        SELECT country, year, month
        FROM earthquakes
        GROUP BY country, year, month
        HAVING SUM(depth_km < 70) > 0 AND SUM(depth_km > 300) > 0;
    """,
    "23. Year-over-year growth rate in total number of earthquakes": """
        SELECT year,
               COUNT(*) AS total_quakes,
               LAG(COUNT(*)) OVER (ORDER BY year) AS prev_year,
               (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY year)) / LAG(COUNT(*)) OVER (ORDER BY year) * 100 AS yoy_growth_percent
        FROM earthquakes
        GROUP BY year;
    """,
    "24. 3 most seismically active regions (frequency x magnitude)": """
        SELECT region, COUNT(*) AS count, AVG(mag) AS avg_mag,
               COUNT(*) * AVG(mag) AS activity_score
        FROM earthquakes
        GROUP BY region
        ORDER BY activity_score DESC
        LIMIT 3;
    """,
    "25. Avg depth per country within ±5° latitude of equator": """
        SELECT country, AVG(depth_km) AS avg_depth
        FROM earthquakes
        WHERE latitude BETWEEN -5 AND 5
        GROUP BY country;
    """,
    "26. Countries with highest ratio of shallow to deep earthquakes": """
        SELECT country,
               SUM(depth_km < 70) / NULLIF(SUM(depth_km > 300), 0) AS shallow_deep_ratio
        FROM earthquakes
        GROUP BY country
        ORDER BY shallow_deep_ratio DESC;
    """,
    "27. Avg magnitude difference between tsunami and non-tsunami quakes": """
        SELECT AVG(CASE WHEN tsunami = 1 THEN mag END) -
               AVG(CASE WHEN tsunami = 0 THEN mag END) AS avg_mag_diff
        FROM earthquakes;
    """,
    "28. Events with lowest data reliability (highest avg error margins)": """
        SELECT place, gap, rms, (gap + rms)/2 AS avg_error
        FROM earthquakes
        ORDER BY avg_error DESC
        LIMIT 10;
    """,
    "29. Pairs of consecutive earthquakes within 50 km and 1 hour": """
        SELECT e1.id AS quake1, e2.id AS quake2, e1.time AS time1, e2.time AS time2, 
               ST_DISTANCE(POINT(e1.longitude, e1.latitude), POINT(e2.longitude, e2.latitude)) AS distance_km
        FROM earthquakes e1
        JOIN earthquakes e2
          ON e2.time > e1.time 
         AND TIMESTAMPDIFF(HOUR, e1.time, e2.time) <= 1
        HAVING distance_km <= 50;
    """,
    "30. Regions with highest frequency of deep-focus earthquakes (>300 km)": """
        SELECT region, COUNT(*) AS deep_quakes
        FROM earthquakes
        WHERE depth_km > 300
        GROUP BY region
        ORDER BY deep_quakes DESC;
    """
}


st.title("🌎 Earthquake Data Insights")
st.write("Select a question from the dropdown and click 'Run' to see the results.")

# Dropdown for selecting question
selected_question = st.selectbox("Choose a question:", list(questions.keys()))

# Run button
if st.button("Run"):
    query = questions[selected_question]
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        st.success(f"✅ Query executed successfully: {selected_question}")
        st.dataframe(df)
    except Exception as e:
        st.error(f"❌ Error running query: {e}")
