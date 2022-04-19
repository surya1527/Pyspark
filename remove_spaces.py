def remove_whitespaces_from_columns(source_df):

    strip_df = source_df.toDF(*[c.strip() for c in source_df.columns])
    return strip_df 
