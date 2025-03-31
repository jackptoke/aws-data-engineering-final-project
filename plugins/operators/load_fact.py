from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
        conn_id = "",
        table = "songplays",
        *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Loading fact table")
        # Load fact table
        redshift.run(SqlQueries.LOAD_TABLE_QUERIES[self.table])
