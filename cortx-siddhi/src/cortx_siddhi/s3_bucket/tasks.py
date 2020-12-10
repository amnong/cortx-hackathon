import boto3

import app
import config

@app.app.task
def delete_idle_bucket(bucket_name):
    client = config.get_client()
    client.delete_bucket(Bucket=bucket_name)
