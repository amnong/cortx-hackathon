import logging
import time

import boto3

from . import config

SCAN_INTERVAL = 1 * 60
SCAN_INTERVAL = 5

def compare_buckets(previous_buckets, buckets):
    return buckets.difference(previous_buckets), previous_buckets.difference(buckets)

def get_buckets(client):
    query_result = client.list_buckets()['Buckets']
    result = set((bucket['Name'], bucket['CreationDate']) for bucket in query_result)
    return result

def monitor_buckets(siddhi_input_handler):
    client = config.get_client()
    previous_buckets = get_buckets(client)

    while True:
        time.sleep(SCAN_INTERVAL)
        try:
            buckets = get_buckets(client)
        except boto3.exceptions.botocore.errorfactory.ClientError:
            buckets = None

        if buckets is not None:
            created_buckets, deleted_buckets = compare_buckets(previous_buckets, buckets)
            if len(created_buckets) > 0: 
                print(created_buckets)
                siddhi_input_handler.send([1, True, "something"])

            if len(deleted_buckets) > 0:
                print(deleted_buckets)

            previous_buckets = buckets


if __name__ == '__main__':
    main()
    
