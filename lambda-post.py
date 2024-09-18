import boto3
import json

def lambda_handler(event, context):
    #body = json.loads(event['body'])
    s3_location = event['s3_location']
    
    # # bucket = event['Records'][0]['s3']['bucket']['name']
    # # object = event['Records'][0]['s3']['object']['key']
    
    # # s3_client = boto3.client('s3')
    glue_client = boto3.client('glue')
    
    # # # Specify source S3 location and Glue job name
    # # bucket = event['Records'][0]['s3']['bucket']['name']
    # # objects = event['Records'][0]['s3']['object']['key']
    glue_job_name = 'teslam'
    
    # # # List objects in the source location
    # # response = s3_client.list_objects_v2(Bucket=bucket, Prefix=objects)
    # # object_paths = [obj['Key'] for obj in response.get('Contents', [])]
    
    
    # # # Start Glue job with object paths as arguments
    # # glue_client.start_job_run(JobName=glue_job_name, Arguments={'--object_paths': ','.join(object_paths)})
    
    
    job_arguments = {
        '--s3_location': s3_location
    }
        
    response = glue_client.start_job_run(JobName=glue_job_name, Arguments=job_arguments)
    
    return {
        'statusCode': 200,
        'body': 'Glue job started successfully'
    }
    
