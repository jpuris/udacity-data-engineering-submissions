from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    # TODO: docstring
    """
        docstring
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
            self.log.info(
                'do_truncate operation set to true... '
                'Running truncate on table %s', self.table_name,
            )
            redshift_hook.run(f'TRUNCATE {self.table_name}')

        self.log.info(
            'Running query to load data into '
            'dim table %s', self.table_name,
        )
        redshift_hook.run(self.sql_query)
        self.log.info('Dim table %s loaded.', self.table_name)
