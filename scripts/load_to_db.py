import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Connect to local PostgreSQL
engine = create_engine('postgresql://postgres:postgres@localhost:5432/york_crime')

df = pd.read_csv("data/raw/yrp_crime_full.csv")

# Rename columns
df = df.rename(columns={
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

# Assign proper types based on metadata
df['UniqueIdentifier'] = df['UniqueIdentifier'].astype(str)
df['occ_date'] = pd.to_datetime(df['occ_date'], unit='ms', errors='coerce')
df['case_type_pubtrans'] = df['case_type_pubtrans'].astype(str)
df['LocationCode'] = df['LocationCode'].astype(str)
df['municipality'] = df['municipality'].astype(str)
df['Special_grouping'] = df['Special_grouping'].astype(str)
df['OBJECTID'] = pd.to_numeric(df['OBJECTID'], errors='coerce').astype('Int64')  # nullable int
df['Shooting'] = df['Shooting'].astype(str)
df['occ_id'] = df['occ_id'].astype(str)
df['hate_crime'] = df['hate_crime'].astype(str)
df['case_status'] = df['case_status'].astype(str)
df['occ_type'] = df['occ_type'].astype(str)

# Replace string "nan" with real NaN
df.replace("nan", np.nan, inplace=True)

df.to_sql("stg_yrp_crime", engine, if_exists="replace", index=False)
print("Data loaded into PostgreSQL table: stg_yrp_crime")
