from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
        conn_id = "",
        table = "",
        *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Loading dimension table")
        redshift.run(SqlQueries.LOAD_TABLE_QUERIES[self.table])
