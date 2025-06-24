import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import datetime
import json
import logging
import re
import os
import sys

# Attempt to import parse_visit from dsllib.visits
try:
    from dsllib.visits import parse_visit
except ImportError:
    # If dsllib is not found in the current path, add it to sys.path
    # This assumes dsllib is located in the parent directory of Task3
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)
    try:
        from dsllib.visits import parse_visit
    except ImportError:
        logging.error("Failed to import parse_visit from dsllib.visits even after adding parent directory to sys.path.")
        # Handle the error as needed, e.g., exit or raise an exception
        raise

# Placeholder for data processing functions
# def process_data(element):
#     # This function will be implemented in a later task
#     pass


class ParseMessage(beam.DoFn):
    """Parses a Pub/Sub message using parse_visit and handles errors."""
    def process(self, element):
        try:
            # Messages from Pub/Sub are bytes, so decode to UTF-8
            decoded_element = element.decode('utf-8')
            parsed = parse_visit(decoded_element)
            if parsed:
                yield parsed
            else:
                logging.warning(f"parse_visit returned None for message: {decoded_element}")
        except Exception as e:
            logging.error(f"Error parsing message: {element}. Error: {e}")


class ExtractPageViewTimestamps(beam.DoFn):
    """Extracts page view event timestamps from parsed visit data."""
    def process(self, element):
        events = element.get('events', [])
        for event in events:
            if event.get('event_type') == 'page_view':
                timestamp_str = event.get('event_timestamp')
                if timestamp_str:
                    try:
                        # Convert to Unix timestamp (float) for Beam's windowing
                        dt_object = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                        yield dt_object.timestamp()
                    except ValueError as e:
                        logging.warning(f"Invalid timestamp format for page_view event: {timestamp_str}. Error: {e}")


def format_for_logging(window_count_tuple):
    """Formats the windowed count into a JSON string for logging."""
    window, count = window_count_tuple
    # The 'window' object is an IntervalWindow. Its 'end' attribute is a Timestamp object.
    # Convert Timestamp to seconds since epoch, then to ISO format string.
    end_timestamp_seconds = window.end.to_seconds()
    end_timestamp_iso = datetime.datetime.fromtimestamp(end_timestamp_seconds, datetime.timezone.utc).isoformat()

    log_entry = {
        "timestamp": end_timestamp_iso,
        "page_views_per_minute": count,
        "log_type": "page_view_stats"
    }
    return json.dumps(log_entry)


def run(argv=None):
    """Sets up and runs the denial of service detection pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project_id',
        required=True,
        help='Google Cloud project ID'
    )
    parser.add_argument(
        '--topic_id',
        required=True,
        help='Pub/Sub topic ID'
    )
    parser.add_argument(
        '--runner',
        default='DirectRunner',
        help='Beam runner (e.g., DataflowRunner, DirectRunner)'
    )
    parser.add_argument(
        '--region',
        default='us-central1',
        help='Dataflow region'
    )
    parser.add_argument(
        '--temp_location',
        required=True,
        help='GCS path for temporary files'
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up pipeline options
    pipeline_options = PipelineOptions(
        pipeline_args,
        runner=known_args.runner,
        project=known_args.project_id,
        region=known_args.region,
        temp_location=known_args.temp_location,
        job_name='denial-of-service-pipeline-' + datetime.datetime.now().strftime('%Y%m%d-%H%M%S'),
        streaming=True,
        save_main_session=True
    )

    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Read from Pub/Sub
        messages = p | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(
            topic=f'projects/{known_args.project_id}/topics/{known_args.topic_id}'
        ).with_output_types(bytes) # Specify bytes output type

        # 2. Parse Messages (with error handling)
        parsed_visits = messages | 'ParseMessages' >> beam.ParDo(ParseMessage())

        # 3. Extract Page View Event Timestamps
        page_view_timestamps = parsed_visits | 'ExtractPageViewTimestamps' >> beam.ParDo(ExtractPageViewTimestamps())

        # 4. Windowing (Fixed 1-minute windows)
        windowed_page_views = page_view_timestamps | 'WindowIntoFixedWindows' >> beam.WindowInto(
            beam.window.FixedWindows(60) # 60 seconds = 1 minute
        )

        # 5. Count Page Views per Minute
        # Use beam.transforms.combiners.Count.PerElement() to count occurrences of each element (timestamp)
        # then sum these counts globally per window.
        # A more direct way for this specific problem is to count all elements within the window.
        page_view_counts = windowed_page_views | 'CountPageViewsPerWindow' >> beam.combiners.Count.Globally()

        # 6. Format for Logging
        # The output of Count.Globally() when windowed is a PCollection of single values (the count for each window).
        # We need to access the window information as well. We can do this by passing windowed PCollections
        # to a ParDo that also receives the window info.
        # A simpler way is to use CombineGlobally with a function that has access to the window.
        # However, Count.Globally() already does what we need if we can get the window info.
        # The output of Count.Globally() when applied to a windowed PCollection is
        # a PCollection where each element is the count for a window.
        # To get the window information, we can use a ParDo transform that receives the window as a parameter.

        # Let's adjust the formatting step to correctly access window information.
        # beam.Map can take a function that accepts (element, window=beam.DoFn.WindowParam).
        # However, Count.Globally() produces a PCollection of counts, not (element, window) tuples directly.
        # We'll use a ParDo to pair the count with its window.

        class AddWindowInfo(beam.DoFn):
            def process(self, element, window=beam.DoFn.WindowParam):
                yield (window, element) # element here is the count from Count.Globally()

        windowed_counts_with_info = page_view_counts | 'AddWindowInfo' >> beam.ParDo(AddWindowInfo())
        
        formatted_logs = windowed_counts_with_info | 'FormatForLogging' >> beam.Map(format_for_logging)

        # 7. Write to Google Cloud Logging
        formatted_logs | 'WriteToLogging' >> beam.Map(logging.info)

        logging.info(f"Streaming pipeline started with job name: {pipeline_options.view_as(PipelineOptions).job_name}")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
