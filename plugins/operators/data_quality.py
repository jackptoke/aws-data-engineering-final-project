from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

default_qualities_to_check = {
    "songplays": {
        "check_column": "play_id",
        "expected_result": 6837
    },
    "users": {
        "check_column": "user_id",
        "expected_result": 104
    },
    "songs": {
        "check_column": "song_id",
        "expected_result": 5154
    },
    "artists": {
        "check_column": "artist_id",
        "expected_result": 6623
    },
    "time": {
        "check_column": "start_time",
        "expected_result": 6813
    },
}

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
        conn_id = "redshift",
        qualities_to_check = default_qualities_to_check,
        *args, **kwargs):
        """Constructor for DataQualityOperator.
        Args:
            conn_id (str): Redshift connection ID
            qualities_to_check (dict): Dictionary of tables to check and their expected results
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.qualities_to_check = qualities_to_check

    def execute(self, context):
        self.log.info("Setting up PostgresHook for Redshift connection {}".format(
            self.conn_id))
        redshift_hook = PostgresHook(self.conn_id)

        for table in self.qualities_to_check.keys():
            check_column = self.qualities_to_check[table]["check_column"]
            expected_result = self.qualities_to_check[table]["expected_result"]

            # Check if the table has any data
            self.log.info(f"Checking table {table} for column {check_column} with expected result {expected_result}")
            records = redshift_hook.get_records(SqlQueries.COUNT_TABLE_ROWS.format(check_column, table))
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"Data quality check failed. Table {table} returned no results")
                raise ValueError(f"Data quality check failed. Table {table} returned no results")

            # Check if the number of records matches the expected result
            num_records = records[0][0]
            if num_records != expected_result:
                self.log.error(f"Data quality check failed. Table {table} contained {num_records} rows, expected {expected_result}")
                raise ValueError(f"Data quality check failed. Table {table} contained {num_records} rows, expected {expected_result}")

            self.log.info(f"Data quality check passed. Table {table} contained {num_records} rows")

        self.log.info("Data quality check passed for all tables")
