import logging
import time
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
import clickhouse_connect

# --- КОНФИГУРАЦИЯ ---

# 1. Настройки PostgreSQL
# Используем имя контейнера 'pg_demo' из docker-compose
PG_CONFIG = {
    "host": "pg_demo",  
    "port": 5432,       # Внутренний порт Docker (всегда 5432)
    "user": os.getenv("POSTGRES_USER", "demo"),
    "password": os.getenv("POSTGRES_PASSWORD", "demo_pass"),
    "dbname": os.getenv("POSTGRES_DB", "oltp")
}

# 2. Настройки ClickHouse
# Используем имя контейнера 'ch_demo' из docker-compose
CH_CONFIG = {
    "host": "ch_demo",
    "port": 8123,       # Внутренний порт Docker (всегда 8123)
    "username": os.getenv("CH_USER", "default"),
    "password": os.getenv("CH_PASSWORD", "clickhouse_pass"),
    "database": os.getenv("CH_DB", "raw") # База 'raw' из .env
}

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}

dag = DAG(
    'housekeeping_backfill',
    default_args=default_args,
    schedule_interval=None, # Ручной запуск
    tags=['maintenance'],
    params={
        "start_date": "2025-08-03",
        "end_date": "2026-01-31"
    }
)

def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)

def get_ch_client():
    return clickhouse_connect.get_client(**CH_CONFIG)

def run_backfill_day(**kwargs):
    logger = logging.getLogger("airflow.task")
    
    # Получаем даты из параметров запуска
    params = kwargs['params']
    start_str = params.get('start_date')
    end_str = params.get('end_date')
    
    start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
    
    logger.info(f"Connecting to PG: {PG_CONFIG['host']} ({PG_CONFIG['dbname']})")
    logger.info(f"Connecting to CH: {CH_CONFIG['host']} ({CH_CONFIG['database']})")
    
    ch_client = get_ch_client()
    pg_conn = get_pg_connection()
    
    current_date = start_date
    
    try:
        while current_date <= end_date:
            next_date = current_date + timedelta(days=1)
            date_str = current_date.strftime('%Y-%m-%d')
            logger.info(f"=== Processing Date: {date_str} ===")

            # 1. ОЧИСТКА (Идемпотентность)
            # Удаляем данные за этот день, чтобы не было дублей
            # Важно: база 'raw' должна существовать!
            delete_sql = f"ALTER TABLE raw_orders DELETE WHERE order_ts >= '{date_str}' AND order_ts < '{next_date}'"
            try:
                ch_client.command(delete_sql)
                logger.info("Existing data cleared.")
            except Exception as e:
                logger.warning(f"Delete skipped or failed (maybe table is empty): {e}")
            
            # Пауза для применения мутации в CH
            time.sleep(1)

            # 2. ВЫГРУЗКА ИЗ PG
            with pg_conn.cursor() as cur:
                sql_pg = f"""
                    SELECT order_id, customer_id, order_ts, status 
                    FROM orders 
                    WHERE order_ts >= '{date_str}' AND order_ts < '{next_date}'
                """
                cur.execute(sql_pg)
                rows = cur.fetchall()

            # 3. ВСТАВКА В CH
            if rows:
                logger.info(f"Found {len(rows)} rows. Inserting...")
                ch_client.insert('raw_orders', rows, column_names=['order_id', 'customer_id', 'order_ts', 'status'])
            else:
                logger.info("No data found in source.")

            current_date += timedelta(days=1)

    finally:
        pg_conn.close()
        ch_client.close()

def optimize_table(**kwargs):
    # Схлопывание мелких файлов в ClickHouse
    client = get_ch_client()
    client.command("OPTIMIZE TABLE raw_orders FINAL")

# Определение задач
backfill_task = PythonOperator(
    task_id='execute_backfill_logic',
    python_callable=run_backfill_day,
    provide_context=True,
    dag=dag,
)

optimize_task = PythonOperator(
    task_id='optimize_table',
    python_callable=optimize_table,
    dag=dag,
)

backfill_task >> optimize_task