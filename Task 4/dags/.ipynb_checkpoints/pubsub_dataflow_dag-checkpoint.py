# pubsub_dataflow_dag.py

from __future__ import annotations
import pendulum
import json
import os
import base64
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.trigger_rule import TriggerRule

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
    Pulls the message from the PubSub sensor's XCom, decodes it, and pushes
    the GCS file path to XCom for the next task.
    """
    # The sensor pushes a list of received messages to XCom.
    pulled_messages = ti.xcom_pull(task_ids="pull_gcs_notification", key="return_value")
    
    if not pulled_messages:
        # This should not happen if the trigger rule is set correctly, but it is a good safeguard.
        raise ValueError("No message received from PubSub sensor, but task was triggered.")

    # We only process the first message found by the sensor.
    message = pulled_messages[0]
    
    # The data from the sensor's message is Base64 encoded.
    notification_str = base64.b64decode(message["message"]["data"]).decode("utf-8")
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
    # Task 1: Use the PubSubPullSensor. It will poke the subscription for up to
    # the timeout period. If it finds a message, it succeeds. If not, it is skipped.
    pull_gcs_notification = PubSubPullSensor(
        task_id="pull_gcs_notification",
        project_id=GCP_PROJECT_ID,
        subscription=PUBSUB_SUBSCRIPTION,
        max_messages=1,
        poke_interval=10,  # Check for a message every 10 seconds.
        timeout=60,        # The sensor task will run for up to 60 seconds.
        soft_fail=True,    # If no message is found after 60s, mark task as SKIPPED.
    )

    # Task 2: This task will only run if the sensor task SUCCEEDED.
    process_notification_task = PythonOperator(
        task_id="process_notification",
        python_callable=process_gcs_notification,
        trigger_rule=TriggerRule.ALL_SUCCESS, # This is crucial.
    )

    # Task 3: This will only run if the processing task SUCCEEDED.
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

    pull_gcs_notification >> process_notification_task >> launch_dataflow_flex_template
