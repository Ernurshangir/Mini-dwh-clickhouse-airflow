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

![Sales Overview Dashboard](docs/Снимок экрана 2026-02-04 в 11.53.41 
PM.png)
![Sales Overview Dashboard](docs/Снимок экрана 2026-02-04 в 11.53.51 
PM.png)
