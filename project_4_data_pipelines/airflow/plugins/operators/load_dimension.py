from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Runs query to load data into a dimension table, based on fact table.

    Keyword arguments:
    redshift_conn_id  -- Airflow connection name for Redshift detail
    sql_query         -- Query to run
    do_truncate       -- Should the table be truncated before running query
    table_name        -- Target table name. Used for info purposes only
    """

    ui_color = '#660066'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        sql_query: str,
        do_truncate: bool,
        table_name: str,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name
        self.do_truncate = do_truncate

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.do_truncate:
            self.log.info('Running truncate on table %s', self.table_name)
            redshift_hook.run(f'TRUNCATE {self.table_name}')

        self.log.info(
            'Running query to load data into '
            'dim table %s', self.table_name,
        )

        redshift_hook.run(self.sql_query)
        self.log.info('Dim table %s loaded.', self.table_name)
