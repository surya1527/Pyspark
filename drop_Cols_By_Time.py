from logging import exception


class Spark:


    def __init__(self, logger):

        if not logger:
            raise exception("A Glue Logger is required!")

        self._logger = logger
        self._run_date_str = self._run_date.strftime("%Y-%m-%d %H:%M:%S")    
    
    def drop_columns_for_glue(self, source_df, type=None, **kwargs):


        if type.upper() == "MINUTE":
            cols = (
                RUN_DATE_YEAR,
                RUN_DATE_MONTH,
                RUN_DATE_DAY,
                RUN_DATE_HOUR,
                RUN_DATE_MINUTE,
            )
            self._logger.info(f"Dropping columns [{cols}] from data_frame")
            df = source_df.drop(*cols)
        elif type.upper() == "HOUR":
            cols = (
                RUN_DATE_YEAR,
                RUN_DATE_MONTH,
                RUN_DATE_DAY,
                RUN_DATE_HOUR,
            )
            self._logger.info(f"Dropping columns [{cols}] from data_frame")
            df = source_df.drop(*cols)
        elif type.upper() == "DAY":
            cols = (RUN_DATE_YEAR, RUN_DATE_MONTH, RUN_DATE_DAY)
            self._logger.info(f"Dropping columns [{cols}] from data_frame")
            df = source_df.drop(*cols)
        elif type.upper() == "MONTH":
            cols = (RUN_DATE_YEAR, RUN_DATE_MONTH)
            self._logger.info(f"Dropping columns [{cols}] from data_frame")
            df = source_df.drop(*cols)
        elif type.upper() == "YEAR":
            cols = RUN_DATE_YEAR
            self._logger.info(f"Dropping columns [{cols}] from data_frame")
            df = source_df.drop(*cols)
        else:
            df = source_df

        return df