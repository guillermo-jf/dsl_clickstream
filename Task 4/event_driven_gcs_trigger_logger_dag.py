from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# --- CONFIGURATION ---
# No BigQuery or specific project details needed for this simple version.

with DAG(
    dag_id="event_driven_gcs_trigger_logger",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    # This DAG is triggered externally by a Cloud Function, not on a schedule.
    schedule=None,
    catchup=False,
    tags=["gcp", "gcs", "cloud-function", "trigger-test"],
    doc_md="""
    ### Event-Driven GCS Trigger Logger

    This DAG is designed for testing the trigger mechanism from a Cloud Function.
    When a new file is uploaded to a GCS path, this DAG is triggered and logs
    the name of the file it received.

    - **Trigger**: A Cloud Function listening for file creation in a GCS bucket.
    - **`log_triggered_file`**: This task receives the filename from the Cloud Function
      trigger and prints it to the task logs for verification.
    """,
) as dag:
    # This task uses the BashOperator to simply 'echo' a message.
    # The message includes the filename that is dynamically passed from the
    # DAG run configuration payload.
    # The configuration is provided by the triggering Cloud Function.
    # The `{{ dag_run.conf.get(...) }}` syntax is Jinja templating.
    log_triggered_file = BashOperator(
        task_id="log_triggered_file",
        bash_command="echo 'Successfully triggered by file: {{ dag_run.conf.get(\"file_name\", \"No file name provided in trigger config.\") }}'",
    )
