# York Crime Analytics

Quick data engineering project using York Regional Police historical occurrence data (2020–2024).  
- Fetch data via API (`scripts/ingest_crime_data.py`)  
- Load into local PostgreSQL (`scripts/load_to_db.py`)  
- Transform and analyze with dbt (`dbt_project/`)  
- Optional: schedule with Airflow

# 🚔 YRP Crime Analytics Pipeline

This project builds an **end-to-end data pipeline** for analyzing **York Regional Police (YRP) crime data**.  
We use **Python** for ingestion, **PostgreSQL** (via Docker) for storage, and **dbt** for transformations and analytics.

---

## 📑 Table of Contents

1. [Overview](#overview)  
2. [Architecture](#architecture)  
3. [Tech Stack](#tech-stack)  
4. [Environment Setup](#environment-setup)  
   - [Windows (WSL)](#windows-wsl)  
   - [Mac/Linux](#maclinux)  
5. [Installation](#installation)  
   - [Python Virtual Environment](#python-virtual-environment)  
   - [Docker + PostgreSQL](#docker--postgresql)  
   - [dbt Setup](#dbt-setup)  
6. [Data Ingestion](#data-ingestion)  
7. [Loading into PostgreSQL](#loading-into-postgresql)  
8. [dbt Models](#dbt-models)  
9. [Workflow](#workflow)  
10. [Project Structure](#project-structure)  
11. [Troubleshooting](#troubleshooting)  
12. [Next Steps](#next-steps)  

---

## 📌 Overview

- **Historical data** (2016–2019) and **year-to-date (YTD) data** (daily updated) are fetched from the **ArcGIS API**.  
- Data is stored in CSVs, then loaded into PostgreSQL staging tables.  
- **dbt** transforms and unifies the data into analytics-ready fact models.  
- The pipeline can later be automated with **Airflow** or similar orchestrators.  

---

## 🏗 Architecture

ArcGIS API → Python (Ingestion) → PostgreSQL (Docker) → dbt (Transformations) → Analytics


- **Ingestion**: Python scripts fetch data in chunks (2,000 rows at a time).  
- **Storage**: PostgreSQL database (`york_crime`).  
- **Transformations**: dbt staging, union, deduplication, and fact tables.  

---

## ⚙ Tech Stack

- **Python 3.12+** (requests, pandas, sqlalchemy, psycopg2)  
- **PostgreSQL 15** (Docker container)  
- **dbt-postgres** (SQL transformations)  
- **Docker & Docker Compose**  
- **WSL2** (Windows users)  

---

## 🖥 Environment Setup

### Windows (WSL)
1. Install **WSL2** with Ubuntu → [Microsoft Guide](https://learn.microsoft.com/en-us/windows/wsl/install)  
2. Open WSL terminal and continue with Linux instructions below.  

### Mac/Linux
Ensure you have:  
- Python 3.12+  
- Docker + Docker Compose  

---

## 📥 Installation

### Python Virtual Environment

```bash
# Create venv
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

If missing, create a requirements.txt:

```
pandas
requests
sqlalchemy
psycopg2-binary
dbt-postgres
```

### Docker + PostgreSQL

We use a docker-compose.yml file to start the PostgreSQL database. 

Start the database:

```bash
docker-compose up -d
```

Check running containers:

```bash
docker ps
```

Stop the services:

```bash
docker-compose down
```

### dbt Setup

Initialize dbt project (already included under dbt_project/):

```bash
cd dbt_project
```

Configure your ~/.dbt/profiles.yml:

```bash
yrp_crime:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: admin
      password: admin
      port: 5432
      dbname: york_crime
      schema: public
```

## 📡 Data Ingestion

Scripts under scripts/ handle API ingestion.

```bash
python scripts/ingest_crime_data.py        # Historical data (one-time)
python scripts/ingest_ytd_crime_data.py    # Year-to-date (refreshable)
```

These scripts:
- Fetch data in 2,000-row chunks
- Save raw CSVs into data/raw/
- Skip API calls if historical CSV already exists to not reprocess data

## 📤 Loading into PostgreSQL

Load ingested CSVs into PostgreSQL staging tables:

```bash
python scripts/load_to_db.py
```

This creates/updates:
- stg_yrp_crime_historical
- stg_yrp_crime_ytd


## 🔄 dbt Models

Run models

```bash
cd dbt_project
dbt run
```

Run tests

```bash
dbt test
```

Generate docs

```bash
dbt docs generate
```

## 🚀 Workflow

Typical run sequence:

**Start DB**
docker start yrp-postgres

**Activate environment**
source .venv/bin/activate

**Ingest raw data**
python scripts/ingest_crime_data.py
python scripts/ingest_ytd_crime_data.py

**Load into DB**
python scripts/load_to_db.py

**Run dbt transformations**
cd dbt_project
dbt run
dbt test


## 📂 Project Structure

```bash
yrp-crime-analytics
├── data
│   └── raw
├── scripts
│   ├── ingest_crime_data.py
│   ├── ingest_ytd_crime_data.py
│   ├── load_historical_to_db.py
│   └── load_ytd_to_db.py
├── dbt_project
│   ├── models
│   │   ├── staging
│   │   └── core
│   ├── seeds
│   └── dbt_project.yml
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## 🛠 Troubleshooting

dbt can’t find dbt_project.yml
- Ensure you cd into dbt_project/ before running dbt.

Port conflicts
- If 5432 is in use, change the -p flag in Docker run command (e.g. -p 5433:5432).


## 📈 Next Steps

1. Add Airflow DAGs for orchestration.
2. Automate monthly YTD refresh.
3. Deploy dbt to dbt Cloud or CI/CD pipeline.
4. Build dashboards on top of fact tables (Metabase, Looker, etc.)