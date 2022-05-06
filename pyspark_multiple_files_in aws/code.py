from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('sparkSesss').getOrCreate()

TABLE_NAMES = ["file_data_object.csv", "file_data_part.csv", "file_data_events.csv"]
source_path = "s3://trial-bucket-11/trial"
