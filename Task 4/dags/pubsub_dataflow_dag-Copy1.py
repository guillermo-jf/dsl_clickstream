# pubsub_dataflow_dag.py

from __future__ import annotations
import pendulum
import json
import os
import base64
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
from airflow.exceptions import AirflowSkipException

# --- CONFIGURATION ---
GCP_PROJECT_ID = "jellyfish-training-demo-6"
COMPOSER_GCS_BUCKET = os.environ.get("GCS_BUCKET", "us-central1-dsl-clickstream-fd8a664b-bucket")
COMPOSER_REGION = "us-central1"

# Pub/Sub settings
PUBSUB_SUBSCRIPTION = "gcs-triggers-sub"

# BigQuery settings
BQ_DATASET = "dsl_project"
BQ_TABLE = "web_visits_batch"

# Dataflow Flex Template settings
FLEX_TEMPLATE_PATH = f"gs://{COMPOSER_GCS_BUCKET}/dataflow_templates/clickstream_template.json"

def process_gcs_notification(ti):
    """
    Pulls messages from Pub/Sub. If a valid message exists, it extracts the
    file path and pushes it to XCom. If no message exists, it skips the task.
    """
    pulled_messages = ti.xcom_pull(task_ids="pull_gcs_notification", key="return_value")
    
    # If no messages are pulled, skip the task and all downstream tasks.
    if not pulled_messages:
        print("No new messages found in subscription. Skipping.")
        raise AirflowSkipException

    # Process the first message
    message = pulled_messages[0]
    
    if not (message and "message" in message and "data" in message["message"]):
        raise ValueError(f"Malformed message received: {message}")

    # The data is a Base64 encoded byte string. Decode it to a regular string.
    notification_str = base64.b64decode(message["message"]["data"]).decode("utf-8")
    
    if not notification_str:
        raise ValueError("Notification data from Pub/Sub is empty after decoding.")

    notification = json.loads(notification_str)
    file_name = notification.get("name")
    bucket_name = notification.get("bucket")
    
    if not file_name or not bucket_name:
        raise ValueError(f"Incomplete GCS notification received: {notification}")

    full_gcs_path = f"gs://{bucket_name}/{file_name}"
    print(f"Successfully parsed notification for file: {full_gcs_path}")
    ti.xcom_push(key="input_file_path", value=full_gcs_path)


with DAG(
    dag_id="pubsub_to_dataflow_flex_template",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["gcp", "gcs", "pubsub", "dataflow", "flex-template"],
) as dag:
    # Task 1: Pulls ONE message and acknowledges it.
    pull_gcs_notification = PubSubPullOperator(
        task_id="pull_gcs_notification",
        project_id=GCP_PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=1,
    )

    # Task 2: This single task now handles both checking and processing.
    # It will be marked "skipped" if no message is found.
    process_notification_task = PythonOperator(
        task_id="process_notification",
        python_callable=process_gcs_notification,
    )

    # Task 3: Launches the Dataflow job. This will only run if the previous
    # task succeeds (i.e., a message was found and processed).
    launch_dataflow_flex_template = BashOperator(
        task_id="launch_dataflow_flex_template_job",
        bash_command=f"""
            gcloud dataflow flex-template run "gcs-to-bq-$(date +%Y%m%d%H%M%S)" \\
                --project="{GCP_PROJECT_ID}" \\
                --region="{COMPOSER_REGION}" \\
                --template-file-gcs-location="{FLEX_TEMPLATE_PATH}" \\
                --parameters "input_file={{{{ ti.xcom_pull(task_ids='process_notification', key='input_file_path') }}}},output_table={GCP_PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"
        """,
    )

    # Define the final, robust dependency chain
    pull_gcs_notification >> process_notification_task >> launch_dataflow_flex_template
