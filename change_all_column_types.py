def change_column_type_all(source_df, cast_type):

    change_column_type_df = source_df.select(
        [source_df.cast(cast_type).alias(c) for c in source_df.columns]
    )
    return change_column_type_df 
