import requests
import logging
from airflow.hooks.http_hook import HttpHook

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def list_csv_files(repo_owner='microsoft', repo_name='sql-server-samples',
                   folder_path='samples/databases/adventure-works/data-warehouse-install-script'):
    """
    Uses GitHub API to list files in a folder and filters out CSV files.
    Since the repo is public, no token is needed.
    """
    try:
        url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{folder_path}"
        response = requests.get(url)
        response.raise_for_status()
        files = response.json()
        csv_files = [file['name'] for file in files if file['name'].lower().endswith('.csv')]
        logger.info("Found CSV files: %s", csv_files)
        return csv_files
    except Exception as e:
        logger.error("Error retrieving file list from GitHub: %s", str(e))
        raise

def extract_csv_files():
    """
    Extract CSV files from the public GitHub repo using Airflow's HTTP Connection.
    """
    try:
        # Use Airflow's HTTP hook to reuse the connection details for GitHub raw URLs.
        # Ensure the HTTP connection is configured with host: https://raw.githubusercontent.com
        http_hook = HttpHook(method='GET', http_conn_id='github_raw')
        base_path = '/microsoft/sql-server-samples/master/samples/databases/adventure-works/data-warehouse-install-script/'
        csv_files = list_csv_files()
        extracted_data = {}
        
        for file_name in csv_files:
            endpoint = base_path + file_name
            response = http_hook.run(endpoint, extra_options={"verify": False})
            if response.status_code == 200:
                extracted_data[file_name] = response.text
                logger.info("Successfully extracted: %s", file_name)
            else:
                logger.error("Failed to retrieve %s. Status: %s", file_name, response.status_code)
                raise Exception(f"Failed to retrieve {file_name}")
                
        # Optionally, save the extracted CSV contents to temporary files for transformation
        for file_name, content in extracted_data.items():
            file_path = f'/tmp/{file_name}'
            with open(file_path, 'w') as f:
                f.write(content, exist= "replace")
            logger.info("Saved file: %s", file_path)
        return extracted_data
    except Exception as e:
        logger.exception("Error in extract_csv_files: %s", str(e))
        raise

if __name__ == "__main__":
    extract_csv_files()