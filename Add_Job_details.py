def add_glue_job_details(self, source_df, details):

    job_id = details.get("JOB_ID")
    job_run_id = details.get("JOB_RUN_ID")
    job_name = details.get("JOB_NAME")

    glue_job_details = (
        source_df.withColumn(job_ID, lit(job_id))
        .withColumn(RUN_ID, lit(job_run_id))
        .withColumn(JOB_NAME, lit(job_name))
    )

    return glue_job_details