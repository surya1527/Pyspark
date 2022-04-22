def PrintSchemaAsJSON(source_df):


    data_frame_schema = source_df.schema

    columns = [
        {
            "Name": field.name,
            "DataType": field.dataType.simpleString(),
            "Comment": "OPTIONAL",
        }
        for field in data_frame_schema.fields
    ]

    return columns