import requests
import pandas as pd

url = "https://services8.arcgis.com/lYI034SQcOoxRCR7/arcgis/rest/services/Occurrence/FeatureServer/0/query"
params = {
    "outFields": "*",
    "where": "1=1",
    "f": "json",
    "resultRecordCount": 200000
}

response = requests.get(url, params=params)
data = response.json()["features"]

df = pd.json_normalize(data)
df.to_csv("data/raw/yrp_crime.csv", index=False)
print("Data fetched and saved to data/raw/yrp_crime.csv")
