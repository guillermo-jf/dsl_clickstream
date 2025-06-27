# dataflow_job.py

import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# This is the BigQuery schema adapted from your notebook.
SCHEMA = {
    "fields": [
        {"name": "session_id", "type": "STRING"},
        {"name": "user_id", "type": "STRING"},
        {"name": "device_type", "type": "STRING"},
        {"name": "geolocation", "type": "STRING"},
        {"name": "user_agent", "type": "STRING"},
        {
            "name": "events",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "event",
                    "type": "RECORD",
                    "fields": [
                        {"name": "event_type", "type": "STRING"},
                        {"name": "timestamp", "type": "TIMESTAMP"},
                        {
                            "name": "details",
                            "type": "RECORD",
                            "fields": [
                                {"name": "page_url", "type": "STRING"},
                                {"name": "referrer_url", "type": "STRING"},
                                {"name": "product_id", "type": "STRING"},
                                {"name": "product_name", "type": "STRING"},
                                {"name": "category", "type": "STRING"},
                                {"name": "price", "type": "FLOAT"},
                                {"name": "quantity", "type": "INTEGER"},
                                {"name": "order_id", "type": "STRING"},
                                {"name": "amount", "type": "FLOAT"},
                                {"name": "currency", "type": "STRING"},
                                {
                                    "name": "items",
                                    "type": "RECORD",
                                    "mode": "REPEATED",
                                    "fields": [
                                        {"name": "product_id", "type": "STRING"},
                                        {"name": "product_name", "type": "STRING"},
                                        {"name": "category", "type": "STRING"},
                                        {"name": "price", "type": "FLOAT"},
                                        {"name": "quantity", "type": "INTEGER"},
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
        },
    ]
}

class ParseJsonlFn(beam.DoFn):
    """A DoFn to parse JSON string records."""
    def process(self, element):
        try:
            record = json.loads(element)
            yield record
        except Exception as e:
            logging.error("Error parsing record '%s': %s", element, e)

def run():
    """Defines and runs the Dataflow pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        required=True,
        help="GCS path for the input JSONL file.",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="BigQuery table to write to, in the format 'PROJECT:DATASET.TABLE'.",
    )
    known_args, pipeline_args = parser.parse_known_args()
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read JSONL File" >> beam.io.ReadFromText(known_args.input_file)
            | "Parse JSON" >> beam.ParDo(ParseJsonlFn())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.output_table,
                schema=SCHEMA,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # Using APPEND is safer for file-by-file processing
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
