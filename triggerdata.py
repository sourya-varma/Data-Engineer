import json

def lambda_handler(event, context):
    print(event)
    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        object_size = record['s3']['object']['size']
        event_time = record['eventTime']
        
        
    return {
        "statusCode": 200,
        "body": json.dumps("S3 event processed successfully")
    }
