def add_dl_audit_columns(self, source_df, glue_job_details):

    dl_df = self.add_file_name(source_df)
    dl_df = self.add_glue_job_details(dl_df, glue_job_details)
    return dl_df