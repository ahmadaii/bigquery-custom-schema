import os
import argparse
import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from transform_data import extract_timestamp_from_log_entry, filter_messages_for_emotions
from transform_data import combine_messages_for_prediction

def create_request_emotion(data):
    request = {"instances":[{"text": None}]
}

class AddTimestampDoFn(beam.DoFn):
  def process(self, element):
    unix_timestamp = extract_timestamp_from_log_entry(element)
    yield beam.window.TimestampedValue(element, unix_timestamp)


def run(argv=None,save_main_session=True):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input_topic',help=('Input PubSub topic of the form ''"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument('--input_subscription',help=('Input PubSub subscription of the form ''"projects/chatbot-dev-356403/subscriptions/dialogflow-logs."'))

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

# Read from PubSub into a PCollection.
    """If a subscription is not provided, everytime a temporary subscription will be created"""
    if known_args.input_subscription:
        messages = (p
                    | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                    .with_output_types(bytes))
    else:
        messages = (p
                    | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                    .with_output_types(bytes))

    # Decode messages for processing.
    decode_messages = messages | 'DecodePubSubMessages' >> beam.Map(lambda x: x.decode('utf-8'))

    # Parse response body data from pub/sub message and build structure for BigQuery load
    filtered_messages = decode_messages | 'FilterIntentAnswers' >> beam.Filter(filter_messages_for_emotions)

    timestamped_items = filtered_messages | 'timestamp' >> beam.ParDo(AddTimestampDoFn())
    print_messages = timestamped_items | 'Print' >> beam.Map(print)

    # Combine filtered messages globally to make a single request to AI model.
    #combined_messages = filtered_messages | 'CombineFilteredMessages' >> beam.CombineGlobally(combine_messages_for_prediction)

    #print_messages = filtered_messages | 'Print' >> beam.Map(print)

    


    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":

    # Logging option
    logging.getLogger().setLevel(logging.INFO)
    run()

    
