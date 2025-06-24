# main.py - For Google Cloud Function deployment

import os
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests

# --- CONFIGURATION ---
# Set these as Environment Variables in your Cloud Function
# The IAP Client ID can be found on the OAuth consent screen page in the GCP console.
IAP_CLIENT_ID = os.environ.get("IAP_CLIENT_ID")
# The trigger URL for your DAG in Cloud Composer.
# Format: https://<composer-web-server-url>/api/v1/dags/<dag-id>/dagRuns
COMPOSER_DAG_TRIGGER_URL = os.environ.get("COMPOSER_DAG_TRIGGER_URL")
GCS_FILE_PREFIX = "clickstream_task4/"


def make_iap_request(url, method="POST", json=None):
    """
    Makes a request to a URL protected by Identity-Aware Proxy (IAP).
    """
    # Obtain OpenID Connect token
    auth_req = Request()
    # This service_account_email is only needed if you are running this locally
    # and have authenticated with a service account. On Cloud Functions, the
    # runtime service account is used automatically.
    creds = id_token.fetch_id_token(auth_req, IAP_CLIENT_ID)

    headers = {"Authorization": f"Bearer {creds}"}

    response = requests.request(method, url, headers=headers, json=json)
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()


def trigger_dag(event, context):
    """
    Background Cloud Function to be triggered by a GCS event.
    This function is executed when a file is created in a GCS bucket.
    """
    bucket_name = event["bucket"]
    file_name = event["name"]

    print(f"Processing file: {file_name} in bucket: {bucket_name}.")

    # --- Validation ---
    # Check if the file is in the correct folder and has the right extension.
    if not file_name.startswith(GCS_FILE_PREFIX) or not file_name.lower().endswith(".jsonl"):
        print(f"File {file_name} is not a target file. Skipping.")
        return

    print(f"File {file_name} is a target file. Triggering Airflow DAG.")

    # --- Prepare DAG Trigger ---
    # The configuration payload to send to the DAG. The DAG will access this via `dag_run.conf`.
    dag_conf = {"conf": {"file_name": file_name}}

    try:
        # Trigger the DAG
        response = make_iap_request(
            COMPOSER_DAG_TRIGGER_URL, method="POST", json=dag_conf
        )
        print("Successfully triggered DAG:", response)
    except Exception as e:
        print(f"Error triggering DAG: {e}")
        # Re-raise the exception to signal a failure to Cloud Functions
        # This will cause the function to be retried.
        raise


# requirements.txt
#
# google-auth
# google-auth-oauthlib
# google-auth-httplib2
# google-cloud-storage
# requests
