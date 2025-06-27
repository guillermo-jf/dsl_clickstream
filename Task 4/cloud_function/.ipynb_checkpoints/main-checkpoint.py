# main.py - For Google Cloud Function deployment (Definitive Solution)

import os
import subprocess
import json

# --- CONFIGURATION ---
# These are set as Environment Variables in your Cloud Function
COMPOSER_ENVIRONMENT_NAME = os.environ.get("COMPOSER_ENVIRONMENT_NAME") # e.g., 'dsl-clickstream2'
COMPOSER_LOCATION = os.environ.get("COMPOSER_LOCATION")             # e.g., 'us-central1'
DAG_ID = "event_driven_gcs_trigger_logger"
GCS_FILE_PREFIX = "clickstream_task4/"


def trigger_dag(event, context=None):
    """
    Background Cloud Function to be triggered by a GCS event.
    This function uses the 'gcloud composer' command to trigger a DAG,
    which works for all Composer 2 network configurations.
    """
    data = event.get_json()
    if not data or 'name' not in data or 'bucket' not in data:
        print("Invalid GCS event data received.")
        return ("Bad Request: Invalid GCS event data", 400)

    bucket_name = data["bucket"]
    file_name = data["name"]

    print(f"Processing file: {file_name} in bucket: {bucket_name}.")

    if not file_name.startswith(GCS_FILE_PREFIX) or not file_name.lower().endswith(".jsonl"):
        print(f"File {file_name} is not a target file. Skipping.")
        return ("Skipped file, not a target.", 200)

    print(f"File {file_name} is a target file. Triggering Airflow DAG: {DAG_ID}")

    # For this method, we must pass the configuration as a JSON string.
    dag_conf = json.dumps({"file_name": file_name})

    # The gcloud command to trigger the DAG
    command = [
        "gcloud",
        "composer",
        "environments",
        "run",
        COMPOSER_ENVIRONMENT_NAME,
        "--location",
        COMPOSER_LOCATION,
        "dags",
        "trigger",
        "--",
        DAG_ID,
        "--conf",
        dag_conf,
    ]

    try:
        print(f"Executing command: {' '.join(command)}")
        subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True  # Will raise an error for non-zero exit codes
        )
        success_message = f"Successfully triggered DAG {DAG_ID} for file: {file_name}"
        print(success_message)
        return (success_message, 200)

    except subprocess.CalledProcessError as e:
        error_message = f"Error triggering DAG with gcloud. Stderr: {e.stderr}"
        print(error_message)
        return (error_message, 500)
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        print(error_message)
        return (error_message, 500)

