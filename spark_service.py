from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark import SparkContext
import sys
from awsglue.utils import getResolvedOptions

class SparkResources:
    
    def __init__(self,appname):
        self.app_name = appname
        self._spark = None
        self._sc = None
        self._glue_context = None
        self.args = self.get_args_resolved(['glue_job_input_json'])
    
    
    def get_args_resolved(self, arg_keys):
        
        """
        Resolve command-line arguments passed to the job.
        Parameters:
        arg_keys (list): List of keys to resolve.
        Returns:
        dict: Resolved arguments.
        """
    
        try:
            args = getResolvedOptions(sys.argv, arg_keys)
            return args
        except Exception as e:
            print(f"Error resolving arguments: {e}")
            return {}
    
    def set_spark_configuration(spark):
        """
            Function to set specific Spark configurations for Parquet read/write operations.

            Parameters:
            spark (SparkSession): The Spark session on which configurations will be set.
        """
        spark_config = {"spark.sql.parquet.datetimeRebaseModeInRead":"CORRECTED",
                        "spark.sql.parquet.datetimeRebaseModeInWrite":"CORRECTED",
                        "spark.sql.parquet.int96RebaseModeInRead":"CORRECTED",
                        "spark.sql.parquet.int96RebaseModeInWrite":"CORRECTED"}
        
        for key, value in spark_config.items():
            self._spark.conf.set(key, value)
            print("set spark configuration")
        
    def get_spark_context(self):
        pass
    
    def create_glue_context(self):
        pass
    
    