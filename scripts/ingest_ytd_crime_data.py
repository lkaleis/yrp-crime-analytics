import os
import requests
import pandas as pd

csv_path = "data/raw/yrp_crime_ytd.csv"

# If CSV already exists, skip fetching from API
if os.path.exists(csv_path):
    print(f"{csv_path} already exists. Loading from CSV...")
    df = pd.read_csv(csv_path)
else:
    url = "https://services8.arcgis.com/lYI034SQcOoxRCR7/arcgis/rest/services/Occurrence/FeatureServer/0/query?where=1%3D1&outFields=*&outSR=4326&f=json"

    all_data = []
    chunk_size = 2000
    offset = 0

    while True:
        params = {
            "outFields": "*",
            "where": "1=1",
            "f": "json",
            "resultRecordCount": chunk_size,
            "resultOffset": offset
        }
        response = requests.get(url, params=params)
        features = response.json()["features"]
        if not features:  # no more data
            break
        all_data.extend(features)
        offset += chunk_size
        print(f"Fetched {offset} records...")

    # Flatten and save
    df = pd.json_normalize(all_data)
    df.to_csv(csv_path, index=False)
    print(f"Total records fetched: {len(df)}")
    print(f"Data saved to {csv_path}")