import os
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import datetime

# -----------------------------
# DB connection
# -----------------------------
engine = create_engine('postgresql://postgres:postgres@localhost:5432/york_crime')

# -----------------------------
# YTD CSV path
# -----------------------------
ytd_csv = "data/raw/yrp_crime_ytd.csv"

if not os.path.exists(ytd_csv):
    raise FileNotFoundError(f"{ytd_csv} not found. Run ingest_ytd_crime_data.py first.")

# -----------------------------
# Load CSV
# -----------------------------
df_ytd = pd.read_csv(ytd_csv)

# -----------------------------
# Rename columns
# -----------------------------
df_ytd = df_ytd.rename(columns={
    'attributes.UniqueIdentifier': 'uniqueidentifier',
    'attributes.occ_date': 'occ_date',
    'attributes.case_type_pubtrans': 'case_type_pubtrans',
    'attributes.LocationCode': 'locationcode',
    'attributes.municipality': 'municipality',
    'attributes.Special_grouping': 'special_grouping',
    'attributes.OBJECTID': 'objectid',
    'attributes.Shooting': 'shooting',
    'attributes.occ_id': 'occ_id',
    'attributes.hate_crime': 'hate_crime',
    'attributes.case_status': 'case_status',
    'attributes.occ_type': 'occ_type',
    'attributes.rep_date': 'rep_date'
})

# -----------------------------
# Assign proper types
# -----------------------------
df_ytd['uniqueidentifier'] = df_ytd['uniqueidentifier'].astype(str)
df_ytd['occ_date'] = pd.to_datetime(df_ytd['occ_date'], unit='ms', errors='coerce')
df_ytd['case_type_pubtrans'] = df_ytd['case_type_pubtrans'].astype(str)
df_ytd['locationCode'] = df_ytd['locationcode'].astype(str)
df_ytd['municipality'] = df_ytd['municipality'].astype(str)
df_ytd['special_grouping'] = df_ytd['special_grouping'].astype(str)
df_ytd['objectid'] = pd.to_numeric(df_ytd['objectid'], errors='coerce').astype('Int64')
df_ytd['shooting'] = df_ytd['shooting'].astype(str)
df_ytd['occ_id'] = df_ytd['occ_id'].astype(str)
df_ytd['hate_crime'] = df_ytd['hate_crime'].astype(str)
df_ytd['case_status'] = df_ytd['case_status'].astype(str)
df_ytd['occ_type'] = df_ytd['occ_type'].astype(str)
df_ytd['rep_date'] = pd.to_datetime(df_ytd['rep_date'], unit='ms', errors='coerce')
df_ytd.replace("nan", np.nan, inplace=True)

# -----------------------------
# Add run date column
# -----------------------------
df_ytd['run_date'] = datetime.today().strftime('%Y-%m-%d')

# -----------------------------
# Load into staging table
# -----------------------------
df_ytd.to_sql("stg_yrp_crime_ytd", engine, if_exists="replace", index=False)
print(f"YTD data loaded into PostgreSQL table: stg_yrp_crime_ytd ({len(df_ytd)} rows)")
