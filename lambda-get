# import json
# import boto3
# #import requests

# s3 = boto3.client('s3')

# def lambda_handler(event, context):
#     bucket_name = event['Records'][0]['s3']['bucket']['name']
#     object_key = event['Records'][0]['s3']['object']['key']
    
#     # s3_location = event['s3_location']
#     # bucket_name, object_key = s3_location.split('/')[2], '/'.join(s3_location.split('/')[3:])

#     # Fetch data from S3
#     response = s3.get_object(Bucket=bucket_name, Key=object_key)
#     data = response['Body'].read().decode('utf-8')
    

#     # Send data to API
#     api_url = "https://942gfzh2a1.execute-api.us-east-1.amazonaws.com/getlambda"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": "Bearer YOUR_API_TOKEN_HERE"  # If required
#     }
#     payload = json.dumps({"data": data})

#     api_response = requests.post(api_url, data=payload, headers=headers)

#     return {
#         "statusCode": api_response.status_code,
#         "body": api_response.text
#     }
    
import boto3
import json

def lambda_handler(event, context):
    # Extract S3 location from the request payload
    body = json.loads(event['body'])
    s3_location = body.get('s3_location')
    s3_parts = s3_location.split("//")[1].split("/", 1)
    bucket_name = s3_parts[0]
    object_key = s3_parts[1]

    # Read data from the specified S3 location
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket = bucket_name, Key = object_key)
    data = response['Body'].read().decode('utf-8')

    return {
        'statusCode': 200,
        'body': json.dumps({'data': data})
    }
