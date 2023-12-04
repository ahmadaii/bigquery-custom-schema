#### BigQuery custom logging schema

##### Setup
1. Install any version of python between 3.7 - 3.9
2. Install Dependencies
```
pip3 install beam_nuggets
```
```
pip3 install python-dotenv
```
```
pip3 install apache-beam[gcp]
```

3. Create a .env file in the project root directory and add the following

```
DB_USERNAME=
DB_PASSWORD=
DB_HOST=
DB_NAME=
PROJECT_ID=
TEMP_LOCATION=
INPUT_SUBSCRIPTION=
```
* Note: 

```
DB_USERNAME= // target DB user name 
DB_PASSWORD=  // target DB password 
DB_HOST=  // target DB host 
DB_NAME=  // target DB name 
PROJECT_ID= // project ID on GCP
TEMP_LOCATION= // A temporatory storage bucket path on the project. SHould be create on GCP storage bucket
INPUT_SUBSCRIPTION= //Name of Pub/Sub subscription from where the logs will be extracted
```

4. Deploy pipeline
```
python3 cloudloggingToParameterSQL.py\
--project=$PROJECT_ID\ 
--input_subscription=$INPUT_SUBSCRIPTION\ 
--runner=DataflowRunner\
--temp_location=$TEMP_LOCATION\ 
--region=europe-west2\ 
--requirements_file=requirements.txt

```
5. To update an existing dataflow pipeline

When you launch your replacement job, the value you pass for the --job_name option must match exactly the name of the job you want to replace.

```
# Get the exact name of the job you want to update

gcloud dataflow jobs list

```
If your replacement pipeline has changed any transform names from those in your prior pipeline, the Dataflow service requires a transform mapping. The transform mapping maps the named transforms in your prior pipeline code to names in your replacement pipeline code. You can pass the mapping by using the --transform_name_mapping command-line option, using the following general format:
```
--transform_name_mapping= .
{"oldTransform1":"newTransform1","oldTransform2":"newTransform2",...}
```
