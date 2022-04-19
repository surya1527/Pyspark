def upper_case_columns(source_df):

    upper_case_df = source_df.toDF(*[c.upper() for c in source_df.columns])
    return upper_case_df