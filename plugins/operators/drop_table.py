from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers.sql_queries import SqlQueries
from airflow.utils.decorators import apply_defaults


class DropTableOperator(BaseOperator):
    """
    Drop a table in Redshift
    """
    ui_color = '#fbc531'

    @apply_defaults
    def __init__(
        self,
        table: str="",
        conn_id: str = "redshift",
        *args,
        **kwargs):
        super(DropTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info(f"Dropping table {self.table} from Redshift")
        redshift_hook.run(SqlQueries.DROP_TABLE_SQL.format(self.table))
