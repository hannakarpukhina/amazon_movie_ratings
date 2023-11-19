from google.cloud import storage


class GCSService:
    def save_content_to_file(content, bucket_name, file_name):
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)
