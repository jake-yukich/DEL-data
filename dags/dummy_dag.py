from airflow.decorators import dag, task
from pendulum import datetime

@dag(
    dag_id='dummy_dag',
    start_date=datetime(2024, 10, 16),
    schedule="@once",
    catchup=False,
    default_args={"owner": "Astro", "retries": 1},
    tags=["dummy"],
)
def dummy_dag():
    @task
    def dummy_task():
        print("This is a dummy task")
    
    dummy_task()

dummy_dag()