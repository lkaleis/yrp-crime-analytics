# York Crime Analytics

Quick data engineering project using York Regional Police historical occurrence data (2020–2024).  
- Fetch data via API (`scripts/ingest_crime_data.py`)  
- Load into local PostgreSQL (`scripts/load_to_db.py`)  
- Transform and analyze with dbt (`dbt_project/`)  
- Optional: schedule with Airflow

