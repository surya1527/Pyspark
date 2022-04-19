from pyspark.sql.functions import *
UNDERSCORE = "_"
def clean_column_names(source_df):

    # Clean the source columns and build a dictionary with the results
    clean_columns = {
        c: c.replace(" ", UNDERSCORE)
        .replace("-", UNDERSCORE)
        .replace("(", UNDERSCORE)
        .replace(")", UNDERSCORE)
        .replace(":", UNDERSCORE)
        .replace("#", UNDERSCORE)
        .replace("%", "pct")
        .lower()
        .strip()
        for c in source_df.columns
    }

    # Apply the cleaned column names to the dataframe
    clean_df = source_df.select(
        [col(c).alias(clean_columns.get(c, c)) for c in source_df.columns]
    )

    return clean_df