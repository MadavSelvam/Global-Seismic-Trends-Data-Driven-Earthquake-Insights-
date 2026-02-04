import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_earthquake_data(starttime, endtime, min_magnitude=4.5):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": starttime,
        "endtime": endtime,
        "minmagnitude": min_magnitude
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        return pd.DataFrame()

    data = response.json()
    if 'features' not in data:
        return pd.DataFrame()

    records = []
    for feature in data['features']:
        prop = feature['properties']
        geom = feature['geometry']
        record = {
            "id": feature['id'],
            "time": datetime.utcfromtimestamp(prop['time'] / 1000),
            "updated": datetime.utcfromtimestamp(prop['updated'] / 1000),
            "latitude": geom['coordinates'][1],
            "longitude": geom['coordinates'][0],
            "depth_km": geom['coordinates'][2],
            "mag": prop.get('mag'),
            "magType": prop.get('magType'),
            "place": prop.get('place'),
            "status": prop.get('status'),
            "tsunami": prop.get('tsunami', 0),
            "sig": prop.get('sig'),
            "net": prop.get('net'),
            "nst": prop.get('nst'),
            "dmin": prop.get('dmin'),
            "rms": prop.get('rms'),
            "gap": prop.get('gap'),
            "magError": prop.get('magError'),
            "depthError": prop.get('depthError'),
            "magNst": prop.get('magNst'),
            "locationSource": prop.get('locationSource'),
            "magSource": prop.get('magSource'),
            "types": prop.get('types'),
            "ids": prop.get('ids'),
            "sources": prop.get('sources'),
            "type": prop.get('type')
        }
        records.append(record)

    return pd.DataFrame(records)

all_data = pd.DataFrame()
start_year = 2020
end_year = 2025

for year in range(start_year, end_year + 1):
    for month in range(1, 13):
        start_date = f"{year}-{month:02d}-01"
        
        if month == 12:
            end_date = f"{year}-12-31"
        else:
            end_date = f"{year}-{month + 1:02d}-01"
            end_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"Fetching data: {start_date} to {end_date}")
        df_month = fetch_earthquake_data(start_date, end_date)
        all_data = pd.concat([all_data, df_month], ignore_index=True)


print(f"Total records fetched: {len(all_data)}")
all_data






all_data.drop_duplicates(subset="id", inplace=True)


text_cols = [
    "place", "magType", "status", "net",
    "locationSource", "magSource", "type"
]

for col in text_cols:
    all_data[col] = (
        all_data[col]
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


all_data["distance_km"] = (
    all_data["place"]
    .str.extract(r"(\d+)\s*km", expand=False)
)

all_data["location_clean"] = (
    all_data["place"]
    .str.replace(r"^\d+\s*km\s*[a-z]+\s*of\s*", "", regex=True)
    .str.replace(r"near the\s*", "", regex=True)
    .str.strip()
)


num_cols = [
    "mag", "depth_km", "gap", "rms",
    "dmin", "magError", "depthError", "magNst"
]

for col in num_cols:
    all_data[col] = (
        all_data[col]
        .astype(str)
        .str.replace(r"[^0-9.\-]", "", regex=True)
    )
    all_data[col] = pd.to_numeric(all_data[col], errors="coerce")


for col in ["types", "sources", "ids"]:
    all_data[col] = (
        all_data[col]
        .fillna("")
        .str.strip(",")
        .str.split(",")
    )

#
all_data["tsunami"] = all_data["tsunami"].fillna(0).astype(int)
all_data["status"] = all_data["status"].fillna("unknown")
all_data["mag"] = all_data["mag"].fillna(all_data["mag"].median())


all_data["year"] = all_data["time"].dt.year
all_data["month"] = all_data["time"].dt.month
all_data["strong_quake"] = all_data["mag"] >= 6.0


all_data.to_csv("clean_earthquakes_2020_2025.csv", index=False)

print(f"✅ Done. Total clean records: {len(all_data)}")


import pandas as pd
import json
from sqlalchemy import create_engine, text


DB_USER = "root"
DB_PASSWORD = "12345"
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "earthquake_db"

engine = create_engine(f"mysql+mysqlconnector://root:12345@localhost:3306/earthquake_db")


with engine.connect() as conn:
    conn.execute(text("SELECT 1"))
    print("✅ Connected to MySQL")



datetime_cols = ["time", "updated"]
for col in datetime_cols:
    all_data[col] = pd.to_datetime(all_data[col], errors="coerce")


numeric_cols = [
    "mag", "depth_km", "gap", "rms",
    "dmin", "magError", "depthError", "magNst", "sig", "nst"
]


for col in numeric_cols:
    all_data[col] = pd.to_numeric(all_data[col], errors="coerce")


for col in numeric_cols:
    if all_data[col].isnull().any():
        
        median_val = all_data[col].median()
        fill_val = median_val if not pd.isna(median_val) else 0
        all_data[col] = all_data[col].fillna(fill_val)


all_data["tsunami"] = all_data["tsunami"].fillna(0).astype(int)


all_data["status"] = all_data["status"].fillna("unknown")
all_data["location_clean"] = all_data["location_clean"].fillna("unknown")
all_data["place"] = all_data["place"].fillna("unknown")
all_data["type"] = all_data["type"].fillna("earthquake")


for col in ['types', 'ids', 'sources']:
    all_data[col] = all_data[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]) if x is None else str(x))




print("Rows before pushing:", len(all_data))
print(all_data.head(3))

all_data.to_sql(
    name="earthquakes",
    con=engine,
    if_exists="replace",  
    index=False,
    chunksize=1000
)

print(f"🚀 Successfully pushed {len(all_data)} rows to MySQL table 'earthquakes'")








