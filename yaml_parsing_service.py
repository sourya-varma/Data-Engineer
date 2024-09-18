import yaml
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("python script for yaml parsing")

class yamlParsing:
    """
        Load and parse YAML content from an S3 bucket.
        -- param bucket_name: Name of the S3 bucket
        -- param yaml_file_key: The key (file path) of the YAML file in the S3 bucket
        -- return: Parsed YAML data as a Python dictionary
    """
    def __init__(self):
        pass
    
    def parse_yaml_from_s3(self,yaml_content):
        try:
            #Parse YAML content
            parsed_data = yaml.safe_load(yaml_content)
            
            # Check if parsed_data is a dictionary
            if isinstance(parsed_data, dict):
                #print("Parsed YAML data is a valid dictionary.")
                return parsed_data
            else:
                print("Parsed YAML data is not a dictionary.")
                return None
    
        except Exception as e:
            print(f"Error fetching or parsing YAML from S3: {e}")
            return None