from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    """
    TODO: docstring
    """

    ui_color = '#e67e22'

    @apply_defaults
    def __init__(self, redshift_conn_id=None, **kwargs):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Creating Postgres SQL Hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Executing creating tables in Redshift.')
        queries = open(
            '/opt/airflow/plugins/helpers/create_tables.sql',
        ).read()
        redshift.run(queries)

        self.log.info('Tables created ')
