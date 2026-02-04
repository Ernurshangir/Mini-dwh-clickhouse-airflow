from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Настройки DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # Не повторять сразу при ошибке
}

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_PATH = "/home/airflow/.local/bin"  # добавим в PATH

with DAG(
    dag_id='dbt_clickhouse_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'clickhouse']
) as dag:

    # 1. Установка зависимостей
    dbt_deps = BashOperator(
        task_id='dbt_install_deps',
        bash_command=f'export PATH={DBT_PATH}:$PATH && dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 2. Запуск моделей
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command=f'export PATH={DBT_PATH}:$PATH && dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    # 3. Запуск тестов
    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command=f'export PATH={DBT_PATH}:$PATH && dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}'
    )

    # Цепочка выполнения
    dbt_deps >> dbt_run >> dbt_test
