import json
import apache_beam as beam


# function to get response body data from pub/sub message and build structure for BigQuery load
def parse_transform_response(pub_sub_data):

    data = json.loads(pub_sub_data)
    
    return_dict = {
        "user_id": None, 
        "email": None, 
        "company": None, 
        "agent_id": None,
        "session_id": None, 
        "response_id": None, 
        "responsetype": None,
        "text": None,
        "timestamp": None,
        "logname": None,
        "location_id": None,
        "currentpage": {
            "displayname": None,
            "name": None
        },
        "intent": {
            "displayname": None,
            "name": None
        },
        "responsemessages" : [
        {
            "responsetype": None,
            "source": None,
            "text": {
                    "text": ["None"]
            }
            
        }
        ],
        "match":{
        "resolvedinput": None,
        "matchtype": None,
        "confidence": None,
        "event": None,
        "parameters": [
            {
                "name": None,
                "value": None
            }
        ],
        "intent": {
        "displayname": None,
        "name": None
        }
       },
       "parameters": [
            {
                "name": None,
                "value": None
            }
        ],
    }

    if "queryResult" not in data['jsonPayload']:
        print(return_dict)
        return return_dict

    return_dict['currentpage'] = data['jsonPayload']['queryResult']['currentPage']
    return_dict['text'] = data['jsonPayload']['queryResult']['text']
    return_dict['response_id'] = data['jsonPayload']['responseId']
    return_dict['responsetype'] = data['jsonPayload']['responseType']
    return_dict['session_id'] = data['labels']['session_id']
    return_dict['agent_id'] = data['labels']['agent_id']
    return_dict['location_id'] = data['labels']['location_id']
    return_dict['timestamp'] = data['timestamp']
    return_dict['logname'] = data['logName']

    # Map all the parameters to the format.
    if 'parameters' in data['jsonPayload']['queryResult']:
        parameters = data['jsonPayload']['queryResult']['parameters']
        params = []
        for iterator in parameters:
            params.append({"name":iterator,"value":parameters[iterator]})
            return_dict['parameters']=params
           

    if 'intent' in data['jsonPayload']['queryResult']:
        return_dict['intent'] = data['jsonPayload']['queryResult']['intent']

    # Modify match object from logs and restructure the parameters if exist.    
    if "match" in data['jsonPayload']['queryResult']:
        match = data['jsonPayload']['queryResult']["match"]
        return_dict['match']={
                "confidence":match['confidence'],
                "matchtype":match['matchType']
            }
        if 'parameters' in match:
            parameters = []
            for iterator in match['parameters']:
                parameters.append({"name":iterator,"value":match['parameters'][iterator]})
            return_dict['match']['parameters']=parameters
        if "event" in match:
            return_dict['match']['event']=match['event']
        if "resolvedInput" in match:
            return_dict['match']['resolvedInput']=match['resolvedInput']
        if "intent" in match:
            return_dict['match']['intent']=match['intent']


            
    # Modify custom payloads into simple text input format.        
    if data['jsonPayload']['queryResult']['responseMessages']:
        responsemessages = data['jsonPayload']['queryResult']['responseMessages']
        messages = []
        for msg in responsemessages:
            if 'payload' in msg:
                text_list = msg['payload']['richContent'][0][0]['options']
                text = []
                for iterator in text_list:
                    text.append(iterator['text'])
                msg.pop("payload")
                msg['text']={"text":text}
                messages.append(msg)
            elif "endInteraction" in msg:
                msg.pop("endInteraction")
                messages.append({})
            else:
                messages.append(msg)
        return_dict['responsemessages']=messages

    print(return_dict)
    return return_dict


def filter_messages_for_emotions(data):
    pub_sub_data = json.loads(data)
    if "queryResult" in pub_sub_data["jsonPayload"]:
        if "match" in pub_sub_data["jsonPayload"]["queryResult"]:
            if pub_sub_data["jsonPayload"]["queryResult"]["match"]["matchType"]=="INTENT":
                return True

def create_request_emotion(data):
    request = {"instances":[]}

def combine_messages_for_prediction(data):
    filtered_messages_data = json.loads(data)
    print(filtered_messages_data)
    return filtered_messages_data

def extract_timestamp_from_log_entry(element):
    return element['timestamp']

def filter_messages_for_params(data):
    param_data = json.loads(data)
    match = param_data['jsonPayload']['queryResult']['match']
    if "parameters" in match:
        return True


class TransformParameterData(beam.DoFn):

    def __init__(self):
        self.parameter_structure = {

            "user_id" : None,
            "goal_name" : None,
            "session_name" : None,
            "session_id" : None,
            "agent_id" : None,
            "created_at" : None,
            "page_name" :  None,
            "match_type" : None,
            "resolved_input" : None,
            "parameter_name" : None,
            "parameter_original_value" : None,
            "parameter_resolved_value" : None,
            "parameter_entity_type" : None
        }

    def process(self, element):
        data = json.loads(element)
        
        match = data['jsonPayload']['queryResult']['match']
        pm = match["parameters"]
        param_names = []
        for iterator in pm:
            param_names.append(iterator)
        print(param_names)

        parameter_info = data['jsonPayload']['queryResult']['diagnosticInfo']['Alternative Matched Intents'][0]

        for p in param_names:
            self.parameter_structure["session_id"] = data['labels']['session_id']
            self.parameter_structure["agent_id"] = data['labels']['agent_id']
            self.parameter_structure["created_at"] = data['timestamp']
            self.parameter_structure["page_name"] = data['jsonPayload']['queryResult']['currentPage']['displayName']
            self.parameter_structure["match_type"] = match["matchType"]
            self.parameter_structure["resolved_input"] = match['resolvedInput']
            self.parameter_structure["parameter_name"] = p
            self.parameter_structure["parameter_original_value"] = parameter_info['Parameters'][p]['original']
            self.parameter_structure["parameter_resolved_value"] = parameter_info['Parameters'][p]['resolved']
            self.parameter_structure["parameter_entity_type"] = parameter_info['Parameters'][p]['type']

            yield self.parameter_structure
