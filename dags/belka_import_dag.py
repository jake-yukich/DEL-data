from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import requests
import pandas as pd
import os
from rdkit import Chem
from rdkit.Chem import AllChem
from kaggle.api.kaggle_api_extended import KaggleApi

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "Astro", "retries": 3},
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
        # Set up Kaggle API client
        api = KaggleApi()
        api.authenticate()

        # Set the path where the dataset will be downloaded
        download_path = '/Users/jake/...'

        # Download the dataset
        api.competition_download_file('leash-BELKA', 'train.parquet')
        api.competition_download_file('leash-BELKA', 'test.parquet')

        # Check if the download was successful
        expected_file = os.path.join(download_path, "belka_data.csv")
        if not os.path.exists(expected_file):
            raise FileNotFoundError(f"Expected file {expected_file} not found after download")

        print(f"BELKA dataset successfully downloaded to {download_path}")
        return expected_file

    @task
    def preprocess_and_fingerprint_data():
        """
        Preprocess and fingerprint the data.
        """
        # Load the data
        df = pd.read_csv("/path/to/belka_data.csv")
        
        # Preprocess the data (example operations)
        df = df.dropna()  # Remove rows with missing values
        
        # Generate molecular fingerprints
        def generate_fingerprint(smiles):
            mol = Chem.MolFromSmiles(smiles)
            if mol is not None:
                return AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=1024).ToBitString()
            return None

        # Assuming 'SMILES' is the column with SMILES strings
        df['Fingerprint'] = df['SMILES'].apply(generate_fingerprint)
        
        # Remove rows where fingerprint generation failed
        df = df.dropna(subset=['Fingerprint'])
        
        # Save the processed data
        df.to_csv("/path/to/processed_belka_data.csv", index=False)

    @task
    def load_data_to_db():
        """
        Load the processed data into the database.
        """
        postgres_hook = PostgresHook(postgres_conn_id="belka_postgres")
        df = pd.read_csv("/path/to/processed_belka_data.csv")
        df.to_sql("belka_molecules", postgres_hook.get_sqlalchemy_engine(), if_exists="replace", index=False)

    @task
    def validate_data():
        """
        Validate the data in the database.
        """
        postgres_hook = PostgresHook(postgres_conn_id="belka_postgres")
        row_count = postgres_hook.get_first("SELECT COUNT(*) FROM belka_molecules")[0]
        assert row_count > 0, "No data loaded into the database"
        
        # Additional validation for fingerprints
        fingerprint_count = postgres_hook.get_first("SELECT COUNT(*) FROM belka_molecules WHERE Fingerprint IS NOT NULL")[0]
        assert fingerprint_count == row_count, "Some molecules are missing fingerprints"

    # Define task dependencies
    download_task = download_belka_data()
    preprocess_task = preprocess_and_fingerprint_data()
    load_task = load_data_to_db()
    validate_task = validate_data()

    download_task >> preprocess_task >> load_task >> validate_task

belka_import()