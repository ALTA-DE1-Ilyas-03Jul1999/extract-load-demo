from google.cloud import storage
import os


def write_to_gcs(bucket_name, blob_name, data):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)


bucket_name = os.getenv('BUCKET_NAME')
blob_name = 'test.json'
write_to_gcs(bucket_name, blob_name, '{"name": "test"}')
