import pandas as pd
from sqlalchemy import create_engine

# Connect to local PostgreSQL
engine = create_engine('postgresql://postgres:postgres@localhost:5432/york_crime')

df = pd.read_csv("data/raw/yrp_crime.csv")
df.to_sql("stg_yrp_crime", engine, if_exists="replace", index=False)
print("Data loaded into PostgreSQL table: stg_yrp_crime")
