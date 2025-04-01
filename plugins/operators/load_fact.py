from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):
    """
    Load data into a fact table in Redshift
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
        conn_id = "",
        table = "songplays",
        *args, **kwargs):
        """Constructor for LoadFactOperator.
        Args:
            conn_id (str): Redshift connection ID
            table (str): Table name to load data into
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        self.log.info("Setting up PostgresHook for Redshift connection {}".format(
            self.conn_id))
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        # Load fact table
        self.log.info("Loading fact table")
        redshift.run(SqlQueries.LOAD_TABLE_QUERIES[self.table])
