import os
import logging
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from azure.storage.blob import BlobServiceClient
from modules.clean_data import handle_missing_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_file_to_blob(file_path, conn_id='azure_blob_conn', container_name='airflow-jobs'):
    """
    Upload a single file to Azure Blob Storage using the connection stored in Airflow.
    """
    try:
        # Retrieve the connection
        azure_conn = BaseHook.get_connection(conn_id)
        connection_string = azure_conn.extra_dejson.get("connection_string", None)
        if not connection_string:
            raise ValueError("No 'connection_string' found in extra. Please configure the Airflow connection properly.")

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=os.path.basename(file_path)
        )
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info("Uploaded %s to container %s", file_path, container_name)
    except Exception as e:
        logger.exception("Error uploading file %s: %s", file_path, str(e))
        raise

def transform_and_load(conn_id='azure_blob_conn'):
    """
    - Reads each CSV in /tmp
    - Cleans/transforms it using the clean_data.py function handle_missing_values
    - Saves the transformed CSV and loads it to Azure Blob using the given connection ID
    """
    try:
        folder = '/tmp'
        for file in os.listdir(folder):
            if not file.endswith('.csv'):
                continue

            file_path = os.path.join(folder, file)
            try:
                # Read the CSV file into a DataFrame; use Python engine to be more flexible with bad lines
                df = pd.read_csv(file_path, sep=None, engine='python', on_bad_lines='skip')
            except Exception as read_err:
                logger.error("Error reading CSV file %s: %s", file, str(read_err))
                continue

            try:
                # Clean the data by handling missing values (now passing a DataFrame)
                cleaned_df = handle_missing_values(df, method='mean')
            except Exception as clean_err:
                logger.error("Error cleaning data in file %s: %s", file, str(clean_err))
                continue

            # Save the cleaned DataFrame to a new CSV file
            transformed_file = file_path
            try:
                cleaned_df.to_csv(transformed_file, index=False)
                logger.info("Transformed file saved: %s", transformed_file)
            except Exception as save_err:
                logger.error("Error saving transformed file for %s: %s", file, str(save_err))
                continue

            # Upload the transformed CSV to Azure Blob Storage
            try:
                load_file_to_blob(transformed_file, conn_id=conn_id)
            except Exception as load_err:
                logger.error("Error uploading transformed file %s: %s", transformed_file, str(load_err))
                continue

    except Exception as e:
        logger.exception("Error in transform_and_load: %s", str(e))
        raise

if __name__ == "__main__":
    transform_and_load()