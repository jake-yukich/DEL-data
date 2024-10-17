from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from dask.diagnostics import ProgressBar
from dask_sql import Context
from airflow.exceptions import AirflowException
from sqlalchemy.exc import SQLAlchemyError
from datetime import timedelta
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
        download_path = '/usr/local/airflow/data'

        file_names = ['train.parquet', 'test.parquet']
    
        # for file_name in file_names:
        #     api.competition_download_file('leash-BELKA', file_name, path=download_path)

        expected_files = [os.path.join(download_path, file_name) for file_name in file_names]

        for file in expected_files:
            if not os.path.exists(file):
                raise FileNotFoundError(f"Expected file {file} not found after download")

        print(f"BELKA datasets successfully downloaded to {download_path}")
        return expected_files

    
    @task(execution_timeout=timedelta(hours=1))
    def load_data_to_db(file_paths: list):
        """
        Load the processed data into the database using Dask.
        """
        postgres_hook = PostgresHook(postgres_conn_id="belka_db")
        
        try:
            # with Client() as client:
            #     print(f"Dask dashboard available at: {client.dashboard_link}")
                
            for path in file_paths:
                print(f"Processing file: {path}")
                
                ddf = dd.read_parquet(path)
                print(f"Loaded Dask DataFrame with {len(ddf.divisions)-1} partitions")
                
                table_name = 'belka_' + os.path.basename(path).split('.')[0]
                
                c = Context()
                c.create_table(table_name, ddf)
                
                conn = postgres_hook.get_connection(postgres_hook.conn_name_attr)
                db_url = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
                
                with ProgressBar():
                    ddf.to_sql(table_name, db_url, if_exists="replace", index=False)
                
                print(f"Finished loading data into the database for {table_name}")

        except SQLAlchemyError as e:
            print(f"Database error: {str(e)}")
            raise AirflowException(f"Database error: {str(e)}")
        except Exception as e:
            print(f"Error: {str(e)}")
            raise AirflowException(f"Error: {str(e)}")

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