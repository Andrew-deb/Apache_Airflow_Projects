from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import your custom functions from the modules subpackage
from modules.extract_data_from_github import extract_csv_files, list_csv_files
from modules.load_data_to_blob import transform_and_load

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'Adventure_Works_ETL',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

# Define tasks using PythonOperator
with DAG("github_raw",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    extract_files = PythonOperator(
        task_id="extract_csv_files",
        python_callable=extract_csv_files,
    )
    extract_files

    list_csv = PythonOperator(
        task_id="list_csv_files",
        python_callable=list_csv_files,
    )
    list_csv

    transform_load = PythonOperator(
        task_id="transform_and_load",
        python_callable=transform_and_load,  # cleans and loads files to Azure Blob Storage
    )
    transform_load

    list_csv >> extract_files >> transform_load