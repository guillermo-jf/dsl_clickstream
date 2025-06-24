import logging
import json
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
import time
from datetime import datetime, timezone

# =================================================================
# 1. DEFINE PIPELINE LOGIC (DoFns)
# =================================================================

class ExtractPageViews(beam.DoFn):
    """
    Parses incoming JSON from Pub/Sub and yields a key-value pair ('page_view', 1).
    It IGNORES the timestamp in the data, forcing the pipeline to use processing time.
    """
    def process(self, element, *args, **kwargs):
        try:
            data_str = element.decode('utf-8')
            data = json.loads(data_str)
            
            events = data.get('events', [])
            for event_data in events:
                event = event_data.get('event', {})
                if event.get('event_type') == 'page_view':
                    # --- THE DEFINITIVE FIX ---
                    # By yielding a simple tuple instead of a TimestampedValue, we are
                    # telling Beam to use the current "processing time" for windowing.
                    # This completely bypasses all the complex and problematic event-time
                    # and watermark logic that was causing the pipeline to stall.
                    yield ('page_view', 1)
                        
        except Exception as e:
            logging.error(f"Error parsing element: {e} - Element: {str(element)[:200]}")


class LogAggregation(beam.DoFn):
    """
    Formats the aggregated count and logs it via structured logging.
    """
    def process(self, element, window=beam.DoFn.WindowParam):
        event_type, page_view_count = element
        window_start = window.start.to_rfc3339()
        
        log_entry = {
            "message": (
                f"PIPELINE OUTPUT: In the 1-minute window starting at {window_start}, "
                f"there were {page_view_count} {event_type} events."
            ),
            "severity": "INFO",
            "jsonPayload": {
                "window_start": window_start,
                "page_view_count": page_view_count,
                "metric": "page_views_per_minute"
            }
        }
        print(json.dumps(log_entry))


# =================================================================
# 2. DEFINE PIPELINE OPTIONS AND EXECUTION
# =================================================================

def run(argv=None):
    """Defines and runs the Dataflow pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--subscription',
        dest='subscription',
        required=True,
        help='The Pub/Sub subscription to read from. '
             'Format: projects/<PROJECT_ID>/subscriptions/<SUBSCRIPTION_NAME>')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up the pipeline options for Dataflow.
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    print(f"--- Submitting Dataflow STREAMING Pipeline (Processing Time) ---")
    print(f"--- Reading from: {known_args.subscription} ---")

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
            | 'Extract Page Views' >> beam.ParDo(ExtractPageViews())
            
            # --- THE DEFINITIVE FIX ---
            # This now windows based on the time the data is processed, not the
            # timestamp in the data. All complex triggers are removed as they are
            # no longer needed. This is the most robust way to ensure output.
            | 'Window into One Minute Batches' >> beam.WindowInto(window.FixedWindows(60))
            
            # Use the robust CombinePerKey to count the events.
            | 'Count Events Per Minute' >> beam.CombinePerKey(sum)
            
            | 'Format and Log Count' >> beam.ParDo(LogAggregation())
        )
    print("--- Job submitted successfully! You can monitor it in the Dataflow UI. ---")

# =================================================================
# 3. SCRIPT ENTRY POINT
# =================================================================

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()