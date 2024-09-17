import json
import boto3
import yaml


from s3_object_per import S3Resources
from glue_object import GlueJobService
from yaml_parsing_service import yamlParsing

def lambda_handler(event, context):
    s3 = S3Resources("us-east-1")
    #result = s3.get_folders_in_s3key("s3://easewithda/dw-with-pyspark/")
    #bucket = s3.create_s3_bucket("souryafdfdfdf")
    #bucket = s3.s3_list_bucket()
    yaml = yamlParsing()
    glue = GlueJobService()
    
    
    bucket,key = s3.get_s3_path_parts("s3://easewithda/dw-with-pyspark/Test/dynamic_glue.yml")
    
    data = s3.read_data_from_s3(bucket,key)
    
    parsed_data = yaml.parse_yaml_from_s3(data)
    
    glue_job_params = parsed_data.get('Glue_Job_params', {})
    
    create_glue_job = glue.create_glue_job(glue_job_params)
    
    print(create_glue_job)