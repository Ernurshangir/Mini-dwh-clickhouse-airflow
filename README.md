# Mini Data Warehouse on Docker (Postgres → ClickHouse)

End-to-end Data Engineering project demonstrating OLTP → DWH → BI pipeline
using Postgres, ClickHouse, Airflow, dbt, and Superset.

## Architecture
(Postgres) → (Airflow) → (ClickHouse raw → staging → marts via dbt) → (Superset)

## Tech Stack
- PostgreSQL (OLTP source)
- ClickHouse (Data Warehouse)
- Apache Airflow (ETL orchestration)
- dbt (transformations, tests, docs)
- Apache Superset (BI dashboards)
- Docker & Docker Compose

## Features
- Full + Incremental loads with watermark
- Backfill and housekeeping DAGs
- Data Quality checks (row count, thresholds)
- dbt staging & marts layers
- Automated tests and documentation
- Sales Overview BI dashboard

## How to Run
```bash
git clone https://github.com/USERNAME/mini-dwh-clickhouse-airflow
cd mini-dwh-clickhouse-airflow
cp .env.example .env
docker compose up -d

Project Structure:
mini_dwh/
├─ docker-compose.yml
├─ .env.example
├─ Makefile
├─ README.md
├─ sql/
│  ├─ pg_schema.sql
│  └─ pg_seed.sql (опционально)
├─ seed/
│  └─ generator.py
├─ airflow/
│  ├─ dags/
│  │  ├─ load_raw_pg_to_ch.py
│  │  └─ housekeeping_backfill.py
│  └─ requirements.txt
├─ dbt/
│  ├─ dbt_project.yml
│  ├─ profiles.yml.sample
│  ├─ models/
│  │  ├─ sources.yml
│  │  ├─ staging/
│  │  └─ marts/
│  └─ snapshots/
└─ superset/
   ├─ import_export/
   └─ setup.md

<img width="1470" height="956" alt="Снимок экрана 2026-02-04 в 11 53 41 PM" src="https://github.com/user-attachments/assets/03c9db4c-34e8-43e9-bb93-1a78831ac230" />

<img width="1470" height="956" alt="Снимок экрана 2026-02-04 в 11 53 51 PM" src="https://github.com/user-attachments/assets/3d07b425-40af-4953-a78b-53f2eae9619b" />


