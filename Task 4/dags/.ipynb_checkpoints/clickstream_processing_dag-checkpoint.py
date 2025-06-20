# IMPORTANT:
# This Airflow DAG orchestrates a Dataflow job for processing clickstream data.
# The actual Dataflow Python script (defined in DATAFLOW_PYTHON_SCRIPT_GCS_PATH)
# needs to be created and deployed to GCS separately.
#
# The Dataflow script should:
# 1. Accept command-line arguments: --input_file (GCS path to the input data)
#    and --output_table (BigQuery table path, e.g., project.dataset.table).
# 2. Read data from the --input_file.
# 3. Parse the clickstream data.
# 4. Write the processed data to the --output_table in BigQuery.
#
# Ensure all placeholder variables (like GCS_BUCKET_NAME, GCP_PROJECT_ID, etc.)
# in this DAG are updated with actual values for your environment.
#
# The GCSObjectExistenceSensor's 'object' parameter is currently set to a pattern
# that includes execution_date and data_interval_end for daily/hourly files.
# For a setup where any new file triggers the DAG (e.g., via a Cloud Function calling DagRun),
# the 'object' parameter for the sensor might need adjustment or the triggering file
# name would be passed via DagRun conf. The current XCom setup for 'input_file'
# in DataFlowPythonOperator assumes the sensor identifies a single, specific file.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
#from airflow.providers.google.cloud.operators.dataflow import DataFlowPythonOperator
from airflow.providers.google.cloud.operators.dataflow import CloudDataflowStartPythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

# Define these as Airflow variables or environment variables in a real setup
GCS_BUCKET_NAME = "jellyfish-training-demo-6"  
GCS_OBJECT_PREFIX = "clickstream_data/"    
DATAFLOW_PYTHON_SCRIPT_GCS_PATH = "gs://jellyfish-training-demo-6/dataflow/jobs/clickstream_parser.py" # Placeholder
BIGQUERY_OUTPUT_TABLE = "jellyfish-training-demo-6.dsl_project.clickstream_output" # Placeholder
GCP_PROJECT_ID = "jellyfish-training-demo-6" # Placeholder
GCP_REGION = "us-central1" # Placeholder
DATAFLOW_TEMP_LOCATION = "gs://dataflow-temp-bucket-gap/temp/" # Placeholder
PUBSUB_TOPIC_NAME = "dsl-project-notifications" # Placeholder


with DAG(
    dag_id="clickstream_processing",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    schedule_interval=None,
    tags=["data_engineering", "clickstream"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "gcp_conn_id": "google_cloud_default", # Added default gcp_conn_id
    }
) as dag:
    wait_for_clickstream_file = GCSObjectExistenceSensor(
        task_id="wait_for_clickstream_file",
        bucket=GCS_BUCKET_NAME,
        # Assuming a specific file naming convention that includes time for uniqueness if multiple files arrive on the same day.
        # For example: 'clickstream_data/2023-10-27/153000_clickstream.json'
        # If the DAG is triggered for a specific file, this object name could be passed via dag_run.conf.
        # For now, making it more specific than just *.json to ideally get one file.
        object=f"{GCS_OBJECT_PREFIX}{{execution_date.strftime('%Y-%m-%d')}}/{{{{ data_interval_end.strftime('%H%M%S') }}}}_clickstream.json",
        google_cloud_conn_id="google_cloud_default",
        mode="poke",
        poke_interval=60,
        timeout=60 * 60 * 24,
    )

    run_dataflow_job = DataFlowPythonOperator(
        task_id="run_dataflow_job",
        py_file=DATAFLOW_PYTHON_SCRIPT_GCS_PATH,
        job_name=f"clickstream-parser-{{{{ds_nodash}}}}-{{{{ti.try_number}}}}", # Unique job name
        options={
            # GCSObjectExistenceSensor pushes a list of found objects to XComs with key 'return_value'.
            # If one file is expected (due to a specific object name):
            'input_file': f"gs://{GCS_BUCKET_NAME}/{{{{ ti.xcom_pull(task_ids='wait_for_clickstream_file', key='return_value')[0] }}}}",
            'output_table': BIGQUERY_OUTPUT_TABLE,
        },
        dataflow_default_options={
            'project': GCP_PROJECT_ID,
            'region': GCP_REGION,
            'tempLocation': DATAFLOW_TEMP_LOCATION,
        },
    )

    publish_processed_notification = PubSubPublishMessageOperator(
        task_id="publish_processed_notification",
        project_id=GCP_PROJECT_ID, # Use the existing placeholder
        topic=PUBSUB_TOPIC_NAME,
        # The message should ideally include the actual processed file name.
        # Pulling from XComs:
        messages=[
            {
                "data": (
                    f"File {{ti.xcom_pull(task_ids='wait_for_clickstream_file', key='return_value')[0]}} processed successfully by Dataflow."
                ).encode('utf-8'),
                "attributes": {"status": "success", "source": "airflow_clickstream_dag"},
            }
        ],
        # gcp_conn_id="google_cloud_default", # Inherited from default_args
    )

    # Define task dependencies
    wait_for_clickstream_file >> run_dataflow_job >> publish_processed_notification
