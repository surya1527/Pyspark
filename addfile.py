from pyspark.sql.functions import *

def add_file_name(source_df, source_name=None, use_file_name=True):
    """Adds the file name that is being read as a column to the dataframe

    :param source_df: source dataframe
    :type source_df: Spark Dataframe
    :return: A new Spark DataFrame
    :rtype: Spark Dataframe
    """
    filename_to_use = input_file_name()

    if not bool(use_file_name):
        filename_to_use = source_name

    file_name_df = source_df.withColumn(FILENAME, lit(filename_to_use))

    return file_name_df