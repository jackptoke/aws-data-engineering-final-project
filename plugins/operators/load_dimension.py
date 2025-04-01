from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):
    """
    Load data into a dimension table in Redshift
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
        conn_id = "",
        table = "",
        truncate = False,
        *args, **kwargs):
        """Constructor for LoadDimensionOperator.
        Args:
            conn_id (str): Redshift connection ID
            table (str): Table name to load data into
            truncate (bool): Whether to truncate the table before loading
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Setting up PostgresHook for Redshift connection {}".format(
            self.conn_id))
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        if self.truncate:
            self.log.info("Truncating table {}".format(self.table))
            # Truncate the table before loading
            redshift.run(SqlQueries.TRUNCATE_TABLE_SQL.format(self.table))

        # Load dimension table
        self.log.info("Loading dimension table")
        redshift.run(SqlQueries.LOAD_TABLE_QUERIES[self.table])
