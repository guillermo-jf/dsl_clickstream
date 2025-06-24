import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import time
from datetime import datetime, timezone

# =================================================================
# 1. DEFINE PIPELINE LOGIC (DoFns)
# =================================================================

class ExtractPageViews(beam.DoFn):
    """
    Parses incoming JSON and yields the raw dictionary for each 'page_view' event.
    All timestamp and value logic has been removed for this diagnostic test.
    """
    def process(self, element, *args, **kwargs):
        try:
            data_str = element.decode('utf-8')
            data = json.loads(data_str)
            
            events = data.get('events', [])
            for event_data in events:
                event = event_data.get('event', {})
                if event.get('event_type') == 'page_view':
                    # Yield the raw event dictionary to test end-to-end data flow.
                    yield event
                        
        except Exception as e:
            logging.error(f"Error parsing element: {e} - Element: {str(element)[:200]}")


class LogIndividualEvents(beam.DoFn):
    """
    Logs the content of each individual event dictionary that it receives.
    This replaces the aggregation logic for our diagnostic test.
    """
    def process(self, element, *args, **kwargs):
        # This function now logs each raw event dictionary.
        # The 'element' is the dictionary yielded from the previous step.
        
        log_entry = {
            "message": "DIAGNOSTIC PIPELINE: Successfully processed one page_view event.",
            "severity": "INFO",
            "jsonPayload": {
                "processed_event": element,
                "diagnostic_metric": "individual_page_view" # Use this for filtering
            }
        }
        print(json.dumps(log_entry))


# =================================================================
# 2. DEFINE PIPELINE OPTIONS AND EXECUTION
# =================================================================

def run(argv=None):
    """Defines and runs the diagnostic Dataflow pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--subscription',
        dest='subscription',
        required=True,
        help='The Pub/Sub subscription to read from. '
             'Format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    print(f"--- Submitting DIAGNOSTIC Dataflow Pipeline ---")
    print(f"--- Reading from: {known_args.subscription} ---")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
            | 'Extract Page Views' >> beam.ParDo(ExtractPageViews())
            
            # --- DIAGNOSTIC STEP ---
            # All windowing and aggregation transforms have been REMOVED.
            # We are now just logging the raw output of the previous step
            # to confirm that data can flow through the pipeline at all.
            | 'Log Each Individual Event' >> beam.ParDo(LogIndividualEvents())
        )
    print("--- Job submitted successfully! You can monitor it in the Dataflow UI. ---")

# =================================================================
# 3. SCRIPT ENTRY POINT
# =================================================================

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
