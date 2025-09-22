import requests
import pandas as pd

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
    response = requests.get(url, params=params)
    features = response.json()["features"]
    if not features:  # no more data
        break
    all_data.extend(features)
    offset += chunk_size
    print(f"Fetched {offset} records...")

# Flatten and save
df = pd.json_normalize(all_data)
df.to_csv("data/raw/yrp_crime_full.csv", index=False)
print(f"Total records fetched: {len(df)}")
