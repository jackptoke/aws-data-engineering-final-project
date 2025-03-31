from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from helpers.sql_queries import SqlQueries
from airflow.utils.decorators import apply_defaults

class ResetTablesOperator(BaseOperator):
    """
    Reset tables in Redshift
    """
    ui_color = '#e84118'

    @apply_defaults
    def __init__(
        self,
        conn_id: str = "redshift",
        tables: list = ["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"],
        *args,
        **kwargs
        ):
        super(ResetTablesOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        self.log.info("Resetting tables in Redshift")

        # Resetting tables
        for table in self.tables:
            drop_sql_stmt = SqlQueries.DROP_TABLE_SQL.format(table)
            if not drop_sql_stmt:
                raise ValueError(f"Table {table} not found in SQL queries")
            self.log.info(f"SQL statement: {drop_sql_stmt}")
            self.log.info(f"Dropping table {table} from Redshift")
            redshift_hook.run(drop_sql_stmt)

            create_sql_stmt = SqlQueries.CREATE_TABLE_QUERIES[table]
            if not create_sql_stmt:
                raise ValueError(f"Table {table} not found in SQL queries")
            self.log.info(f"SQL statement: {create_sql_stmt}")
            self.log.info(f"Creating table {table} in Redshift")
            redshift_hook.run(create_sql_stmt)
        self.log.info("Tables reset successfully")
