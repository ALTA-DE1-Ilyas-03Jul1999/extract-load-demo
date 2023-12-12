from google.cloud import storage
import os


def read_from_gcs(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_text()


bucket_name = os.getenv('BUCKET_NAME')
blob_name = 'test.json'
data = read_from_gcs(bucket_name, blob_name)
print(data)
