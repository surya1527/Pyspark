import uuid
from pyspark.sql.types import StringType
x = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()


def GenerateUUID(source_df):

    # Add a UUID for an ID
    source_df = source_df.withColumn("id", x())

    metrics_df = source_df.select( 
        col("id")
    )

    return metrics_df 
