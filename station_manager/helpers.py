import json
import pandas as pd
import requests
from google.cloud import bigquery
from google.cloud import storage


def get_stations(current_page, headers):
    # Traverse through paginated API, appending to the stations dataframe
    stations_data = pd.DataFrame()
    while current_page is not None: 
        current_data, next_page = get_page_info(current_page, headers)
        stations_data = pd.concat([stations_data, current_data], ignore_index=True)
        current_page = next_page 
    return stations_data 

def get_page_info(current_page, headers):
    # Get the stations data for the current page and the URL to the next page
    r = requests.get(current_page, headers)
    r = r.json()
    next_page = r['links']['next']
    data = r['data']
    n = len(data)
    attributes = [data[i]['attributes'] for i in range(n)]
    current_data = pd.DataFrame(attributes)
    return current_data, next_page 

def write_to_gcs(data, bucket_name, bucket_path):
    #Convert dataframe to csv, write to GCS
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bucket.blob(bucket_path).upload_from_string(data.to_csv(index=False), 'text/csv')

def write_to_bigquery(bucket_name, bucket_path, table_id, project):
    bq_client = bigquery.Client()
    bq_client.project = project 
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        autodetect = True, 
        write_disposition = "WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.CSV
    )
    uri = "gs://{}/{}".format(bucket_name, bucket_path)
    load_job = bq_client.load_table_from_uri(
        uri, table_id, job_config=job_config
    ) 
    load_job.result() 
    destination_table = bq_client.get_table(table_id) 
    print("Destination Table: {}".format(table_id))
    print("Loaded {} rows.".format(destination_table.num_rows))
