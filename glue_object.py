import boto3
from botocore.exceptions import ClientError
import logging
import json
import os
import datetime as dt
import yaml


class GlueJobService:
    """
    class to interact with glue jobs
    """
    
    def __init__(self):
        self.s3_glue = boto3.client("glue")
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        
    def create_glue_job(self, glue_job_params):
        # Check if 'Name' and 'Role' are present in job_params
        job_name = glue_job_params.get('Name')
        job_role = glue_job_params.get('Role')
        
        # Check inner Command parameters
        command = glue_job_params.get('Command')
        command_name = command.get('Name')
        script_location = command.get('ScriptLocation')
        python_version = str(command.get('Python'))
        max_capacity = glue_job_params.get('MaxCapacity')
        no_of_workers = glue_job_params.get('NumberOfWorkers')
        glue_version = glue_job_params.get('GlueVersion')
        
        
        if not job_name:
            self.logger.error("Job 'Name' is missing in the parameters.")
            return False
    
        if not job_role:
            self.logger.error("Job 'Role' is missing in the parameters.")
            return False
        
        if not(command_name or script_location or python_version):
            self.logger.error("command_name, script_location, python_version is missing in parameters.")
            return False
        
        try:
            response = self.s3_glue.create_job(
                Name=job_name,
                Description=glue_job_params.get('Description', ''),
                Role=job_role,
                Command={
                    'Name': command_name,
                    'ScriptLocation': script_location,
                    'PythonVersion': str(python_version)
                }
            )
            
            self.logger.info(f"Glue Job '{job_name}' created successfully.")
            return True

        except ClientError as e:
            self.logger.error(f"Failed to create Glue job: {e}")
            return False
        return type(python_version)