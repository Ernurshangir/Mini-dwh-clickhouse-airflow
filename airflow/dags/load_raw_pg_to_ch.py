from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect
import logging

# Настройки
PG_CONN_STR = "postgresql+psycopg2://demo:demo_pass@pg_demo:5432/oltp"
CH_CONFIG = {
    "host": "ch_demo",
    "port": 8123,
    "username": "default",
    "password": "clickhouse_pass",
    "database": "raw"
}

def get_ch_client():
    return clickhouse_connect.get_client(**CH_CONFIG)

def init_ch_schema():
    client = get_ch_client()
    client.command("CREATE DATABASE IF NOT EXISTS raw")
    
    # Схемы таблиц строго по генератору
    queries = [
        # Customers: customer_id, full_name, email, created_at
        """CREATE TABLE IF NOT EXISTS raw.raw_customers (
            customer_id Int32, full_name String, email String, created_at DateTime
        ) ENGINE = MergeTree() ORDER BY customer_id""",

        # Products: product_id, name, category, price
        """CREATE TABLE IF NOT EXISTS raw.raw_products (
            product_id Int32, name String, category String, price Float64
        ) ENGINE = MergeTree() ORDER BY product_id""",

        # Orders: order_id, customer_id, order_ts, status
        """CREATE TABLE IF NOT EXISTS raw.raw_orders (
            order_id Int32, customer_id Int32, order_ts DateTime, status String
        ) ENGINE = MergeTree() ORDER BY order_ts""",

        # Order Items: id (PK), order_id, product_id, qty, item_price
        """CREATE TABLE IF NOT EXISTS raw.raw_order_items (
            id Int32, order_id Int32, product_id Int32, qty Int32, item_price Float64
        ) ENGINE = MergeTree() ORDER BY id"""
    ]
    
    for q in queries:
        client.command(q)
    logging.info("Схема ClickHouse синхронизирована с генератором.")

def load_table(pg_table, ch_table, incremental=False, watermark_col=None):
    pg_engine = create_engine(PG_CONN_STR)
    ch_client = get_ch_client()
    temp_table = f"{ch_table}_tmp"

    ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")
    ch_client.command(f"CREATE TABLE {temp_table} AS {ch_table}")

    query = f"SELECT * FROM {pg_table}"
    if incremental and watermark_col:
        res = ch_client.query(f"SELECT max({watermark_col}) FROM {ch_table}")
        last_val = res.result_rows[0][0]
        if last_val:
            # Для DateTime в Clickhouse нужна обертка в кавычки
            if isinstance(last_val, datetime):
                last_val = last_val.strftime('%Y-%m-%d %H:%M:%S')
            query += f" WHERE {watermark_col} > '{last_val}'"

    with pg_engine.connect() as conn:
        for chunk in pd.read_sql(query, conn, chunksize=10000):
            if not chunk.empty:
                # Вставляем данные в ClickHouse
                ch_client.insert_df(temp_table, chunk)
                ch_client.command(f"INSERT INTO {ch_table} SELECT * FROM {temp_table}")
                ch_client.command(f"TRUNCATE TABLE {temp_table}")
    
    ch_client.command(f"DROP TABLE IF EXISTS {temp_table}")

with DAG(
    "load_raw_pg_to_ch",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    t_init = PythonOperator(task_id="init_ch", python_callable=init_ch_schema)
    
    t_cust = PythonOperator(task_id="load_customers", 
                            python_callable=lambda: load_table("customers", "raw_customers"))
    
    t_prod = PythonOperator(task_id="load_products", 
                            python_callable=lambda: load_table("products", "raw_products"))
    
    t_ord = PythonOperator(task_id="load_orders", 
                           python_callable=lambda: load_table("orders", "raw_orders", True, "order_ts"))
    
    t_items = PythonOperator(task_id="load_order_items", 
                             python_callable=lambda: load_table("order_items", "raw_order_items", True, "id"))

    t_init >> [t_cust, t_prod] >> t_ord >> t_items