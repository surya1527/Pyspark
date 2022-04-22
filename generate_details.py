# This results in the group of inserted updated and deletd items in the dataframe from a glue job
# we should have a key column to depend on.

from pyspark.sql.functions import *
def generate_metrics(spark, source_type, **kwargs):

    # Generates the high level metrics for processing
    # spark: spark session
    # type spark: Spark Session
    # source_type: Glue job source type
    # type source_type: String
    # kwargs: Input mutiple dataframes
    # return: A new Spark DataFrame with the metrics
    # rtype: SparkDataframe
 
    output_dfs = []
    for key, temp_df_table in kwargs.items():
        listColumns = temp_df_table.columns
        temp_df_table.registerTempTable(key)
        if "op" in listColumns:
            sql_df = f"select '{key}' as Dataframe_Type, '{source_type}' as Job_Source, count(*) as Count, SUM(CASE When op='I' Then 1 Else 0 End) as Inserted, SUM(CASE When op='U' Then 1 Else 0 End) as Updated, SUM(CASE When op='D' Then 1 Else 0 End) as Deleted from {key}"
        elif "transaction_flag_column" in listColumns:
            sql_df = f"select '{key}' as Dataframe_Type, '{source_type}' as Job_Source, count(*) as Count, SUM(CASE When transaction_flag_column='I' Then 1 Else 0 End) as Inserted, SUM(CASE When transaction_flag_column='U' Then 1 Else 0 End) as Updated, SUM(CASE When transaction_flag_column='D' Then 1 Else 0 End) as Deleted from {key}"
        else:
            sql_df = f"select '{key}' as Dataframe_Type, '{source_type}' as Job_Source, count(*) as Count, '0' as Inserted, '0' as Updated, '0' as Deleted from {key}"
        res_df = spark.sql(sql_df)
        output_dfs.append(res_df)

    return output_dfs
