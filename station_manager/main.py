import os 
from helpers import get_stations, write_to_bigquery, write_to_gcs


def run_cloud_function(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    
    GCP_PROJECT = os.environ.get('GCP_PROJECT')
    GCS_BUCKET = os.environ.get('GCS_BUCKET')
    GCS_PATH = os.environ.get('GCS_PATH')
    TABLE_ID = os.environ.get('TABLE_ID')  

    base_url = 'https://station.services.pbs.org/api/public/v1'
    endpoint = '/stations'
    headers = {'Content-type': 'application/json'}
    current_page = base_url+endpoint

    stations = get_stations(current_page, headers)
    write_to_gcs(stations, GCS_BUCKET, GCS_PATH)
    write_to_bigquery(GCS_BUCKET, GCS_PATH, TABLE_ID, GCP_PROJECT)
