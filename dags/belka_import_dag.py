from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import pandas as pd
import os
from kaggle.api.kaggle_api_extended import KaggleApi

@dag(
    start_date=datetime(2024, 10, 16),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 1},
    tags=["belka", "import"],
)
def belka_import():
    """
    DAG for importing and processing the BELKA dataset.
    """

    @task
    def download_belka_data():
        """
        Download the BELKA dataset from Kaggle.
        """
        # api = KaggleApi()
        # api.authenticate()
        download_path = '/Users/jake/projects/kaggle-BELKA/leash-BELKA'

        file_names = ['train.parquet', 'test.parquet']
    
        # for file_name in file_names:
        #     api.competition_download_file('leash-BELKA', file_name, path=download_path)

        expected_files = [os.path.join(download_path, file_name) for file_name in file_names]

        for file in expected_files:
            if not os.path.exists(file):
                raise FileNotFoundError(f"Expected file {file} not found after download")

        print(f"BELKA datasets successfully downloaded to {download_path}")
        return expected_files

    @task
    def load_data_to_db(file_paths: list):
        """
        Load the processed data into the database.
        """
        postgres_hook = PostgresHook(postgres_conn_id="belka_db")
        engine = postgres_hook.get_sqlalchemy_engine()

        for path in file_paths:
            df = pd.read_parquet(path)
            table_name = 'belka_' + os.path.basename(path).split('.')[0]
            df.to_sql(table_name, engine, if_exists="replace", index=False)
            print(f"Loaded {len(df)} rows into the database")

    @task
    def validate_data():
        """
        Validate the data in the database.
        """
        postgres_hook = PostgresHook(postgres_conn_id="belka_db")
    
        tables = ['belka_train', 'belka_test']
        
        for table in tables:
            row_count = postgres_hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            print(f"Table {table} has {row_count} rows")
            assert row_count > 0, f"No data loaded into the {table} table"

        print("Data validation completed")

    # dependencies
    download_task = download_belka_data()
    load_task = load_data_to_db(download_task)
    validate_task = validate_data()

    download_task >> load_task >> validate_task

belka_import()