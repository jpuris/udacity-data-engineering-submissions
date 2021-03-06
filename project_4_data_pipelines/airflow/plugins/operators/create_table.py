from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):
    """
    Runs the provided SQL file to create sparkify database tables.
    File is '/opt/airflow/plugins/helpers/create_tables.sql

    Keyword arguments:
    redshift_conn_id  -- Airflow connection name for Redshift detail
    """

    ui_color = '#e67e22'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Creating Postgres SQL Hook')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # TODO: the path to create_tables.sql needs to be dynamic
        self.log.info('Executing creating tables in Redshift.')
        queries = open(
            '/opt/airflow/plugins/helpers/create_tables.sql',
        ).read()
        redshift.run(queries)

        self.log.info('Tables created ')
