import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# -----------------------------
# DB connection
# -----------------------------
engine = create_engine('postgresql://postgres:postgres@localhost:5432/york_crime')

# -----------------------------
# Historical CSV path
# -----------------------------
historical_csv = "data/raw/yrp_crime_full.csv"

if not os.path.exists(historical_csv):
    raise FileNotFoundError(f"{historical_csv} not found. Run ingest_crime_data.py first.")

# -----------------------------
# Load CSV
# -----------------------------
df_historical = pd.read_csv(historical_csv)

# -----------------------------
# Rename columns
# -----------------------------
df_historical = df_historical.rename(columns={
    'attributes.UniqueIdentifier': 'UniqueIdentifier',
    'attributes.occ_date': 'occ_date',
    'attributes.case_type_pubtrans': 'case_type_pubtrans',
    'attributes.LocationCode': 'LocationCode',
    'attributes.municipality': 'municipality',
    'attributes.Special_grouping': 'Special_grouping',
    'attributes.OBJECTID': 'OBJECTID',
    'attributes.Shooting': 'Shooting',
    'attributes.occ_id': 'occ_id',
    'attributes.hate_crime': 'hate_crime',
    'attributes.case_status': 'case_status',
    'attributes.occ_type': 'occ_type'
})

# -----------------------------
# Assign proper types
# -----------------------------
df_historical['UniqueIdentifier'] = df_historical['UniqueIdentifier'].astype(str)
df_historical['occ_date'] = pd.to_datetime(df_historical['occ_date'], unit='ms', errors='coerce')
df_historical['case_type_pubtrans'] = df_historical['case_type_pubtrans'].astype(str)
df_historical['LocationCode'] = df_historical['LocationCode'].astype(str)
df_historical['municipality'] = df_historical['municipality'].astype(str)
df_historical['Special_grouping'] = df_historical['Special_grouping'].astype(str)
df_historical['OBJECTID'] = pd.to_numeric(df_historical['OBJECTID'], errors='coerce').astype('Int64')
df_historical['Shooting'] = df_historical['Shooting'].astype(str)
df_historical['occ_id'] = df_historical['occ_id'].astype(str)
df_historical['hate_crime'] = df_historical['hate_crime'].astype(str)
df_historical['case_status'] = df_historical['case_status'].astype(str)
df_historical['occ_type'] = df_historical['occ_type'].astype(str)

df_historical.replace("nan", np.nan, inplace=True)

# -----------------------------
# Load into staging table
# -----------------------------
df_historical.to_sql("stg_yrp_crime", engine, if_exists="replace", index=False)
print(f"Historical data loaded into PostgreSQL table: stg_yrp_crime ({len(df_historical)} rows)")
