import os
import argparse
import logging
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from transform_data import parse_transform_response



def run(argv=None,save_main_session=True):

    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>".'))
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/chatbot-dev-356403/subscriptions/dialogflow-logs."'))
    parser.add_argument('--output_bigquery', required=True,
                        help='Output BQ table to write results to '
                             '"PROJECT_ID:DATASET.TABLE"')
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
    output = decode_messages | 'ParseTransformResponse' >> beam.Map(parse_transform_response)

    # Write to BigQuery
    schema = open('_bigquery_schemas/analytics_logging_schema.json')
    bigquery_table_schema = json.load(schema)

    output | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            known_args.output_bigquery,
            schema=bigquery_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":

    # Logging option
    logging.getLogger().setLevel(logging.INFO)
    run()
