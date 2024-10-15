from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 7),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_postgres_connection',
    default_args=default_args,
    description='A simple DAG to test Postgres connection',
    schedule_interval=timedelta(days=1),
)

def print_postgres_version(**kwargs):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    postgres_hook = PostgresHook(postgres_conn_id='belka_db')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    result = cursor.fetchone()
    print(f"PostgreSQL version: {result[0]}")

test_conn = PythonOperator(
    task_id='test_conn',
    python_callable=print_postgres_version,
    dag=dag,
)

test_conn