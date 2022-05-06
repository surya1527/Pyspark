from pyspark.sql.functions import input_file_name
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('sparkSesss').getOrCreate()

TABLE_NAMES = ["file_data_object.csv", "file_data_part.csv", "file_data_events.csv"]
source_path = "s3://trial-bucket-11/trial"
dest_path = "s3://trial-output/parquet_output"
input_paths = [f"{source_path}/{t}" for t in TABLE_NAMES]

def writecom(input_paths):
    CSV_READ_OPTIONS = {
                "header": True
                }
    for i in input_paths:
        all_df = spark.read.csv(i,**CSV_READ_OPTIONS).withColumn("file_name", input_file_name())
        xyz = i.split("/")[-1].split(".")[0]
        print("xx",xyz)
        all_df.where(all_df["file_name"].contains(xyz)).write.mode("append").parquet(f"{dest_path}/{xyz}")
    return all_df

k = writecom(input_paths)
print("hello",k)

