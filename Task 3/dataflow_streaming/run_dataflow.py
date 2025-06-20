# run_pipeline.py
# This script defines and runs a streaming Apache Beam pipeline on Google Cloud Dataflow.
# It reads JSON messages from a Pub/Sub subscription, windows them into 1-minute batches,
# and writes each batch to a separate .jsonl file in Google Cloud Storage.

import logging
import time
import json
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions

# --- CUSTOM SINK FOR WRITING TO FILES ---
# This class is a dependency for the pipeline and is packaged by setup.py.
class JsonlSink(fileio.FileSink):
    """
    A sink that writes JSONL records to a file. It handles the encoding and
    line breaks for each record.
    """
    def __init__(self):
        self._coder = beam.coders.StrUtf8Coder()
        self._fh = None

    def open(self, fh):
        """This method is called when a new file is created for writing."""
        self._fh = fh

    def write(self, record):
        """This method writes a single record to the file handle saved by open()."""
        if self._fh is None:
            raise RuntimeError('Sink open() was not called before write()')
        self._fh.write(self._coder.encode(record) + b'\n')

    def flush(self):
        """This method flushes the file handle before it is closed."""
        if self._fh is not None:
            self._fh.flush()

# --- PIPELINE DEFINITION ---
def define_and_run_pipeline(pipeline_options, subscription_path, output_path):
    """
    Defines the Apache Beam pipeline graph and runs it with the provided options.
    
    Args:
        pipeline_options: The configuration options for the pipeline.
        subscription_path: The full path to the Pub/Sub subscription.
        output_path: The GCS path (directory) where output files will be written.
    """
    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=subscription_path,
              ).with_output_types(bytes)
            
            | 'Decode Messages' >> beam.Map(lambda msg_bytes: msg_bytes.decode('utf-8'))
            | 'Parse JSON' >> beam.Map(json.loads)
            | 'Log Payloads' >> beam.Map(lambda data: logging.info(f"Processing session: {data.get('session_id')}") or data)
            | 'Convert to JSONL' >> beam.Map(json.dumps)
            | 'Window into 1 Minute Batches' >> beam.WindowInto(window.FixedWindows(60))
            | 'Write Windowed Files' >> fileio.WriteToFiles(
                path=output_path,
                sink=JsonlSink(),
                file_naming=fileio.default_file_naming(prefix="output", suffix=".jsonl")
            )
        )
    logging.info("Pipeline submitted to the runner.")

# --- MAIN EXECUTION BLOCK ---
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)

    # --- 1. Set up your project and GCS details ---
    PROJECT_ID = "jellyfish-training-demo-6"
    SUBSCRIPTION_NAME = "dsl-clickstream-push-beam"
    GCS_BUCKET = "jellyfish-training-demo-6" 
    GCS_PATH = "dsl-project"
    REGION = 'us-central1'

    # --- 2. Configure Dataflow-specific options ---
    JOB_NAME = f'streaming-clickstream-{int(time.time())}'
    DATAFLOW_OUTPUT_PREFIX = f"gs://{GCS_BUCKET}/{GCS_PATH}/dataflow_run/"

    # --- 3. Create and configure pipeline options for Dataflow ---
    options = PipelineOptions()
    
    # Set standard options
    standard_options = options.view_as(StandardOptions)
    standard_options.runner = 'DataflowRunner'
    standard_options.streaming = True

    # Set Google Cloud and Dataflow specific options
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = f"gs://{GCS_BUCKET}/{GCS_PATH}/staging"
    google_cloud_options.temp_location = f"gs://{GCS_BUCKET}/{GCS_PATH}/temp"
    google_cloud_options.region = REGION

    # Set the setup file for packaging custom code
    options.view_as(SetupOptions).setup_file = './setup.py'

    # --- 4. Run the pipeline on Dataflow ---
    logging.info(f"--- Starting Dataflow Job: {JOB_NAME} ---")
    logging.info(f"Region: {REGION}")
    logging.info(f"Writing to: {DATAFLOW_OUTPUT_PREFIX}*")
    logging.info("You can monitor the job's progress in the Google Cloud Console.")

    define_and_run_pipeline(
        pipeline_options=options,
        subscription_path=f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME}",
        output_path=DATAFLOW_OUTPUT_PREFIX
    )
    
    