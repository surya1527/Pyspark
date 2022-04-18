def cast_column(source_df, column_name, cast_type):

    cast_df = source_df.withColumn(
        column_name, source_df[column_name].cast(cast_type)
    )
    return cast_df