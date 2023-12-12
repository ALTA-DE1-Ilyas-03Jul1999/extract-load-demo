from google.cloud import storage, bigquery
import os
import pandas as pd
import json

def write_to_gcs(bucket_name, blob_name, data_list):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    json_data = json.dumps(data_list)
    blob.upload_from_string(json_data)

def read_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_text()
    return data

def write_bigquery(table_id, insert_table_json):
    client = bigquery.Client()
    table = client.get_table(table_id)
    insert_table = json.loads(insert_table_json)
    errors = client.insert_rows_json(table, insert_table)
    if errors:
        print('gagal nih: {}'.format(errors))
    else:
        print('yeay')

bucket_name = os.getenv('BUCKET_NAME')
blob_name = 'tugas_1.json'
data_list = [
    {"name": "ilyas", "age": 24, "status": "single"},
    {"name": "asep", "age": 16, "status": "single"},
    {"name": "wasimin", "age": 17, "status": "married"},
    {"name": "prabowo", "age": 32, "status": "widower"},
    {"name": "ganjar", "age": 20, "status": "single"},
    {"name": "anies", "age": 61, "status": "married"}
]
write_to_gcs(bucket_name, blob_name, data_list)

data = read_gcs(bucket_name, blob_name)

insert_table_json = data

project_id = os.getenv('PROJECT_ID')
table_id = f'{project_id}.my_dataset.my_table'
write_bigquery(table_id, insert_table_json)