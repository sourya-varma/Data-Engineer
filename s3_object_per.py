import boto3
from botocore.exceptions import ClientError
import logging
import json
import os
import datetime as dt
import configparser
import yaml

#define logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("python script for s3 bucket operations.......")


class S3Resources:
    """
    class to interact with lambda jobs
    
    Attributes:
    -- Lambda_handler: lambda handler interact with all this functions
    
    """
    
    def __init__(self, region):
        self.region = region
        self.s3_client = boto3.client("s3")
        self.s3_res_client = boto3.resource("s3")
        
    #staging_s3_path
    def get_folders_in_s3key(self,staging_s3_path):
        """ fetch all the subfolders in a given key"""
        
        s3_folders_list = []
        
        s3_bucket,s3_key = self.get_s3_path_parts(staging_s3_path)
        response = self.s3_client.list_objects(Bucket=s3_bucket, Prefix=s3_key, Delimiter='/')
        
        for obj in response["CommonPrefixes"]:
            full_folder = "s3://{}/{}".format(s3_bucket,obj.get("Prefix"))
            s3_folders_list.append(full_folder)
            
        return s3_folders_list
        
            
    def get_s3_path_parts(self, s3_url):
        
        """ 
            Splits an S3 URL into bucket name and key.

            -- s3_url: The S3 URL in the format s3://bucket-name/path/to/object
            -- return: A tuple containing the bucket name and the key
        """
        
        logger.info("s3_url provided is: {}".format(s3_url))
        
        try:
            if not s3_url.startswith("s3://") or s3_url.startswith("s3a://") :
                logger.error("Invalid S3 URL.")
            
            else:
                # Remove 's3://' from the start
                s3_url = s3_url[5:]

                # Split the URL into bucket and key parts
                bucket, key = s3_url.split("/", 1)

                return bucket, key
        
        except ClientError as e:
            print("they are some issues")
            logging.error(e)
            return False
            
    
    def create_s3_bucket(self,bucket_name):
        """
            Create an S3 bucket in a specified region and handle errors.
         -- param bucket_name: Name of the bucket to create
         -- param region: AWS region where the bucket will be created. Default is None (uses default region).
         -- return: True if the bucket was created successfully, False if there was an error.
        """
        try:
            if self.region is None:
                response = self.s3_client.create_bucket(Bucket=bucket_name)
                print(response)
            else:
                s3_client = boto3.client('s3', region_name=self.region)
                s3_client.create_bucket(
                    Bucket=bucket_name
                )
    
            print(f"Bucket '{bucket_name}' created successfully.")
            Exists = True
        except ClientError as e:
            print("some error was occured")
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                Exists = False
    def s3_list_bucket(self):
        """
            List all S3 buckets in the specified region.
        
            -- param region: The AWS region to filter buckets.
            -- return: A list of bucket names in the specified region.
        """
        try:
        # Get a list of all buckets
            response = self.s3_client.list_buckets()
            buckets = response['Buckets']
            
            # List to store buckets in the specified region
            buckets_in_region = []
            
            for bucket in buckets:
                bucket_name = bucket['Name']
                
                buckets_in_region.append(bucket_name)
            
            return buckets_in_region
            
        except ClientError as e:
            logging.error(e)
            return False
            
    
    def upload_file_to_s3(self,file_name, bucket_name, object_name=None):
        """
            Upload a file to an S3 bucket.
            
            --param file_name: Path to the file to upload
            --param bucket_name: Name of the bucket to upload to
            --param object_name: S3 object name. If not specified, file_name is used.
            --return: True if file was uploaded, else False
        """
        if object_name is None:
            object_name = file_name
                
        try:
            response = s3_client.upload_file(file_name, bucket_name, object_name)
            print(f"File '{file_name}' uploaded successfully to '{bucket_name}/{object_name}'.")
            return True
        except ClientError as e:
            print(f"Failed to upload file '{file_name}' to '{bucket_name}/{object_name}'. Error: {e}")
            return False

    
    def read_data_from_s3(self, bucket_name, key):
        """
            Read data from an S3 bucket given the bucket name and key.
            -- param bucket_name: Name of the S3 bucket
            -- param key: Key (path) of the object in the S3 bucket
            -- return: Content of the file as a string
        """
        try:
            # Fetch the object from the S3 bucket
            response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        
            # Read the contents of the object
            data = response['Body'].read().decode('utf-8')
            return data
    
        except Exception as e:
            print(f"Error reading object {key} from bucket {bucket_name}: {e}")
            return None