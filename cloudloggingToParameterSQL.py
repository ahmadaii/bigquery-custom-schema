import os
import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from beam_nuggets.io import relational_db

def filter_messages_for_query(data):
    pub_sub_data = json.loads(data)
    if "queryResult" in pub_sub_data["jsonPayload"]:
        if "match" in pub_sub_data["jsonPayload"]["queryResult"]:
            return True

def filter_messages_for_params(data):
    param_data = json.loads(data)
    match = param_data['jsonPayload']['queryResult']['match']
    if match['matchType']!='NO_MATCH':
        return True

def filter_no_matches(data):
    no_match = json.loads(data)
    match = no_match['jsonPayload']['queryResult']['match']
    if match['matchType']=='NO_MATCH':
        return True

def create_parameter_structure(elements):
    data = json.loads(elements)

    parameter_structure = {
                "user_name" : None,
                "goal_name" : None,
                "activity_name" : None,
                "session_id" : None,
                "agent_id" : None,
                "created_at" : None,
                "page_name" :  None,
                "match_type" : None,
                "intent_name": None,
                "resolved_input" : None,
                "parameter_name" : None,
                "parameter_original_value" : None,
                "parameter_resolved_value" : None,
                "parameter_entity_type" : None
        }

    if 'parameters' in data['jsonPayload']['queryResult']:
        parameters = data['jsonPayload']['queryResult']['parameters']
        if "user_name" in parameters:
            parameter_structure['user_name'] = parameters['user_name']
        if "goal_name" in parameters:
            parameter_structure['goal_name'] = parameters['goal_name']
        if "activity_name" in parameters:
            parameter_structure['activity_name'] = parameters['activity_name']


    match = data['jsonPayload']['queryResult']['match']

    if "parameters" in match:

        pm = match["parameters"]
        param_names = []
        for iterator in pm:
            param_names.append(iterator)

        parameter_info = data['jsonPayload']['queryResult']['diagnosticInfo']['Alternative Matched Intents'][0]

        for p in param_names:

            params = parameter_structure.copy()

            params["session_id"] = data['labels']['session_id']
            params["agent_id"] = data['labels']['agent_id']
            params["created_at"] = data['timestamp']
            params["page_name"] = data['jsonPayload']['queryResult']['currentPage']['displayName']
            params["match_type"] = match["matchType"]
            params["resolved_input"] = match['resolvedInput']
            params["parameter_name"] = p
            params["parameter_original_value"] = parameter_info['Parameters'][p]['original']
            params["parameter_resolved_value"] = parameter_info['Parameters'][p]['resolved']
            params["parameter_entity_type"] = parameter_info['Parameters'][p]['type']

            if match["matchType"]=='INTENT':
                params['intent_name']=match['intent']['displayName']

            yield params
    else:

        intents = parameter_structure.copy()

        intents["session_id"] = data['labels']['session_id']
        intents["agent_id"] = data['labels']['agent_id']
        intents["created_at"] = data['timestamp']
        intents["page_name"] = data['jsonPayload']['queryResult']['currentPage']['displayName']
        intents["match_type"] = match["matchType"]
        intents["resolved_input"] = match['resolvedInput']

        if match["matchType"]=='INTENT':
                intents['intent_name']=match['intent']['displayName']

        yield intents

def create_no_match_structure(elements):

    data = json.loads(elements)

    no_match_structure = {

            "user_name":None,
            "goal_name":None,
            "activity_name":None,
            "session_id":None,
            "agent_id":None,
            "created_at":None,
            "previous_page":None,
            "current_page":None,
            "match_type":None,
            "confidence":None,
            "event":None,
            "text":None,
        }
    if 'parameters' in data['jsonPayload']['queryResult']:
        parameters = data['jsonPayload']['queryResult']['parameters']
        if "user_name" in parameters:
            no_match_structure['user_name'] = parameters['user_name']
        if "goal_name" in parameters:
            no_match_structure['goal_name'] = parameters['goal_name']
        if "activity_name" in parameters:
            no_match_structure['activity_name'] = parameters['activity_name']

    match = data['jsonPayload']['queryResult']['match']

    no_match_structure["session_id"] = data['labels']['session_id']
    no_match_structure["agent_id"] = data['labels']['agent_id']
    no_match_structure["created_at"] = data['timestamp']
    no_match_structure["current_page"] = data['jsonPayload']['queryResult']['currentPage']['displayName']
    no_match_structure["match_type"] = match["matchType"]
    no_match_structure["confidence"] = match["confidence"]
    no_match_structure["event"] = match['event']
    no_match_structure["text"] = data['jsonPayload']['queryResult']['text']
    no_match_structure['previous_page'] = data['jsonPayload']['queryResult']['diagnosticInfo']\
                                            ['Execution Sequence'][0]['Step 1']['InitialState']\
                                                ['FlowState']['PageState']['Name']

    return no_match_structure

def create_conversations_structure(elements):
    data = json.loads(elements)

    conversations = {

        "user_name":None,
        "goal_name":None,
        "activity_name":None,
        "session_id":None,
        "agent_id":None,
        "created_at":None,
        "type":None,
        "page":None,
        "owner":None,
        "value":None,
    }

    if 'parameters' in data['jsonPayload']['queryResult']:
        parameters = data['jsonPayload']['queryResult']['parameters']
        if "user_name" in parameters:
            conversations['user_name'] = parameters['user_name']
        if "goal_name" in parameters:
            conversations['goal_name'] = parameters['goal_name']
        if "activity_name" in parameters:
            conversations['activity_name'] = parameters['activity_name']

    

    user = conversations.copy()

    user["session_id"] = data['labels']['session_id']
    user["agent_id"] = data['labels']['agent_id']
    user["created_at"] = data['timestamp']
    user['value'] = data['jsonPayload']['queryResult']['text']
    user['owner'] = "User"
    user['type'] = "text"
    user['page'] = data['jsonPayload']['queryResult']['diagnosticInfo']\
                                ['Execution Sequence'][0]['Step 1']['InitialState']\
                                ['FlowState']['PageState']['Name']
    
    yield user

    responseMessages = data['jsonPayload']['queryResult']['responseMessages']
    for res in responseMessages:
        agent = conversations.copy()

        agent["session_id"] = data['labels']['session_id']
        agent["agent_id"] = data['labels']['agent_id']
        agent["created_at"] = data['timestamp']
        agent['owner'] = "Agent"

        if "text" in res:
            agent['value'] = res['text']['text'][0]
            agent['type'] = "text"

            if res['responseType']=='HANDLER_PROMPT':
                agent['page'] = data['jsonPayload']['queryResult']['diagnosticInfo']\
                                            ['Execution Sequence'][0]['Step 1']['InitialState']\
                                            ['FlowState']['PageState']['Name']
                yield agent

            elif res['responseType']=='ENTRY_PROMPT':
                agent['page'] = data['jsonPayload']['queryResult']['currentPage']['displayName']

                yield agent

        elif "payload" in res:
            #Currently assuming only one richType in the payload.
            type = res['payload']['richContent'][0][0]['type']
            if type == "image" or type == "video":
                agent['type'] = type
                agent['value'] = res['payload']['richContent'][0][0]['rawUrl']
                agent['page'] = data['jsonPayload']['queryResult']['currentPage']['displayName']

                yield agent




parameter_table_config = relational_db.TableConfiguration(
    name='parameters',
    create_if_missing=True,
)

no_match_table_config = relational_db.TableConfiguration(
    name='no_matches',
    create_if_missing=True,
)
conversation_table_config = relational_db.TableConfiguration(
    name='conversations',
    create_if_missing=True,
)


def run(argv=None,save_main_session=True):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input_topic',help=('projects/<PROJECT>/topics/<TOPIC>'))
    group.add_argument('--input_subscription',help='Subscription id',default=os.environ.get("INPUT_SUBSCRIPTION"))
    parser.add_argument('--db_username', help='DB Username', default=os.environ.get("DB_USERNAME"))
    parser.add_argument('--db_password', help='DB Password', default=os.environ.get("DB_PASSWORD"))
    parser.add_argument('--db_host', help='DB Host', default=os.environ.get("DB_HOST"))
    parser.add_argument('--db_name', help='DB Name', default=os.environ.get("DB_NAME"))
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    p = beam.Pipeline(options=pipeline_options)

    source_config = relational_db.SourceConfiguration(

        drivername='postgresql+pg8000',
        host=known_args.db_host,
        port=5432,
        username=known_args.db_username,
        password=known_args.db_password,
        database=known_args.db_name,
)

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

    #Decode messages for processing.
    decode_messages = messages | 'DecodePubSubMessages' >> beam.Map(lambda x: x.decode('utf-8'))

    # Filter only messages that have valid queryResults from DialogFlow logging.
    filtered_messages = decode_messages | 'FilterQueryResults' >> beam.Filter(
        filter_messages_for_query)

    # Filter for parameters. If a log entry does not have parameters,discard it.
    matches = filtered_messages | 'FilterNoParamsMatches' >> beam.Filter(
        filter_messages_for_params)

    # Filter for no_matches. The answers that doesn't match any intent.            
    no_matches = filtered_messages | 'FilterNoMatches' >> beam.Filter(
        filter_no_matches) 

    # Convert 1-to-1 or Many structures from parameters information for the PostgreSQL entries.
    parameter_records = matches | 'TransformParameterData' >> beam.FlatMap(
        create_parameter_structure) 

    # Convert 1-to-1 structure for no_match data.                        
    no_match_records = no_matches | 'TransformNoMatchData' >> beam.Map(
        create_no_match_structure) 

    #Create 1-t0-many conversation records.
    conversation_records = filtered_messages | 'TransformConversationData' >> beam.FlatMap(
        create_conversations_structure) 


    #Write records to DB.
    parameter_records | 'WriteToParametersTable' >> relational_db.Write(
        source_config=source_config,
        table_config=parameter_table_config
    )
    no_match_records | 'WriteToNoMatchesTable' >> relational_db.Write(
        source_config=source_config,
        table_config=no_match_table_config
    )
    conversation_records | 'WriteToConversationsTable' >> relational_db.Write(
        source_config=source_config,
        table_config=conversation_table_config
    )

    p.run()#.wait_until_finish()


if __name__ == "__main__":

    # Logging option
    logging.getLogger().setLevel(logging.INFO)
    run()


#python3 cloudloggingToParameterSQL.py --input_subscription=projects/chatbot-dev-356403/subscriptions/dialogflow-logs