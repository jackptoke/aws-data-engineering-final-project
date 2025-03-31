from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers.sql_queries import SqlQueries
from airflow.utils.decorators import apply_defaults

class CreateTableOperator(BaseOperator):
    """
    Create a table in Redshift
    """
    ui_color = '#9c88ff'

    @apply_defaults
    def __init__(
        self,
        table: str = "",
        conn_id: str = "redshift",
        *args,
        **kwargs
        ):
        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info(f"Creating table {self.table} in Redshift")
        sql_stmt = SqlQueries.CREATE_TABLE_QUERIES[self.table]
        if not sql_stmt:
            raise ValueError(f"Table {self.table} not found in SQL queries")
        self.log.info(f"SQL statement: {sql_stmt}")
        redshift_hook.run(sql_stmt)
