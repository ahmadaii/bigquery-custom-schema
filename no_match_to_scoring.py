import os
import argparse
import logging
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def autherize():
    SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly','https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE ='service.json'
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return credentials

def search(nameKey,list):
    for p in list:
        if p['name'] == nameKey:
            return p

def search_phrases(existing_phrases,new_phrase):
    for p in existing_phrases:
        if (p[1]==new_phrase):
            return True

def get_folder_id(session):
    creds = autherize()
    try:
        drive = build('drive', 'v3', credentials=creds)
        # Serch for the specified session folder in drive and get its is.
        results = drive.files().list(
            pageSize=1000, 
            fields="nextPageToken, files(id, name)",
            q = f"mimeType = 'application/vnd.google-apps.folder'").execute()

        folders = results.get('files', [])
        for folder in folders:
            if folder['name'].endswith(session):
                folder_id = folder.get('id')
                return folder_id

    except HttpError as error:
        print(f'An error occurred: {error}')


def list_folder_items(folder_id):
    creds = autherize()
    try:
        drive = build('drive', 'v3', credentials=creds)
        # Open folder and list files inside folder.
        results = drive.files().list(
            q = "'" + folder_id + "' in parents", 
            pageSize=50, 
            fields="nextPageToken, files(id, name)").execute()
        items = results.get('files', [])
        return items
    except HttpError as error:
        print(f'An error occurred: {error}')

def get_intents(session):
    try:
        id = get_folder_id(session)
        items = list_folder_items(id)
        for item in items:
            if item['name'] == "Intents" or item['name'] == "01_Intents":
                intents_id = item['id']
                intents = list_folder_items(intents_id)
                return intents
    except HttpError as error:
        print(f'An error occurred: {error}')

def read_sheet(id,range):
    creds = autherize()
    sheet = build('sheets', 'v4', credentials=creds)
    sheet = sheet.spreadsheets().values().get(spreadsheetId=id,range=range).execute()
    return sheet

def update_sheet(spreadsheet_id,export_list):
    creds = autherize()
    sheet = build('sheets', 'v4', credentials=creds)
    sheet.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="'Scoring'!A:D",
            body={"majorDimension": "ROWS","values": export_list},
                valueInputOption="USER_ENTERED").execute()

def write_google_sheets(data):

    session_name = data['Activity']
    scount = session_name.count("_")
    if scount>1:
        session_name = session_name.replace("_", "",1)
    session_id = get_folder_id(session_name)
    global_id = get_folder_id("00_GLOBAL")

    items = list_folder_items(session_id) + list_folder_items(global_id)

    page_id = search("Pages",items)['id']

    page_data = read_sheet(page_id,"'Routing'!A:D")
    values = page_data['values']
    for val in values:
        if val[0]==data['Page']:
            intent = val[1]
            intent_folder_local = search("Intents" , items)['id']
            intent_folder_global = search("01_Intents", items)['id']
            intent_sheets = list_folder_items(intent_folder_local) + list_folder_items(intent_folder_global)

            if(intent.startswith("MCQS_")):
                pass
            else:
                intent_sheet_id = search(intent,intent_sheets)['id']
                existing_intent_data = read_sheet(intent_sheet_id,"'Scoring'!A:D")
                existing_phrases = existing_intent_data['values']
                if (search_phrases(existing_phrases,data['Answers'])):
                    print("Already exist")
                else:
                    print("New training phrase")
                    #update_sheet(intent_sheet_id,[[intent,data['Answers']]])
                    

def run(argv=None,save_main_session=True):
    """Build and run the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument('--input_bq',help=('[project_id]:[dataset_id].[table_id]'))
    known_args, pipeline_args = parser.parse_known_args(argv)


    # Bigquery no_matches table configurations.
    table_spec = known_args.input_bq #chatbot-dev-356403:analytics.temp

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = False
    p = beam.Pipeline(options=pipeline_options)

    query = '''SELECT
                activity.id as Activity,
                no_matches.previous_page AS Page,
                no_matches.text AS Answers,
                no_matches.is_valid AS Valid,
                no_matches.created_at AS CreatedAT,
                no_matches.updated_at AS UpdatedAT,
                no_matches.confidence AS Score
                FROM analytics.no_matches
                JOIN analytics.activity ON no_matches.agent_id=activity.agentid
                WHERE no_matches.updated_at > timestamp_sub(current_timestamp, INTERVAL 7 DAY);

                '''

    no_matches = ( 
        p   |'ReadTable' >> beam.io.ReadFromBigQuery(project = "chatbot-dev-356403",query=query, use_standard_sql=True)
            | 'writeToSheets' >> beam.Map(print))


    p.run().wait_until_finish()

if __name__ == "__main__":
    # Logging option
    logging.getLogger().setLevel(logging.INFO)
    run()


#python3 no_match_to_scoring.py --input_bq="chatbot-dev-356403:analytics.temp" --temp_location=$TEMP_LOCATION

'''SELECT
  activity.id as Activity,
  no_matches.previous_page AS Page,
  no_matches.text AS Answers,
  no_matches.is_valid AS Valid,
  no_matches.created_at AS CreatedAT,
  no_matches.updated_at AS UpdatedAT,
  no_matches.confidence AS Score
FROM analytics.no_matches
JOIN analytics.activity ON no_matches.agent_id=activity.agentid;'''

