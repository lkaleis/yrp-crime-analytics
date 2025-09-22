import os
import requests
import pandas as pd
import time

csv_path = "data/raw/yrp_crime_full.csv"

# If CSV already exists, skip fetching from API
if os.path.exists(csv_path):
    print(f"{csv_path} already exists. Loading from CSV...")
    df = pd.read_csv(csv_path)
else:
    url = "https://services8.arcgis.com/lYI034SQcOoxRCR7/arcgis/rest/services/Occurrence_2016_to_2019/FeatureServer/0/query"

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
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            features = response.json().get("features", [])
            if not features:
                break
            all_data.extend(features)
            offset += chunk_size
            print(f"Fetched {offset} records...")
            time.sleep(1)  # Add delay to avoid rate limiting
        except Exception as e:
            print(f"Error at offset {offset}: {e}")
            time.sleep(10)  # Wait longer before retrying

    # Flatten and save
    df = pd.json_normalize(all_data)
    df.to_csv(csv_path, index=False)
    print(f"Total records fetched: {len(df)}")
    #print(f"Data saved to {csv_path}")