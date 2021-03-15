from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    # TODO: docstring
    """
        docstring
    """

    ui_color = '#00aae4'

    copy_query = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS json '{}';
    """

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id=None,
        aws_credential_id=None,
        table_name=None,
        s3_bucket=None,
        s3_key=None,
        file_format=None,
        log_json_file=None,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credential_id = aws_credential_id
        self.table = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format
        self.log_json_file = log_json_file
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):

        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        self.log.info(
            'Picking staging file for table %s from location : %s',
            self.table, self.s3_key,
        )

        if self.log_json_file:
            self.log_json_file = f's3://{self.s3_bucket}/{self.log_json_file}'
        else:
            self.log_json_file = 'auto'

        aws_credentials = BaseHook.get_connection(self.aws_credential_id)

        copy_query = self.copy_query.format(
            self.table,
            s3_path,
            aws_credentials.login,
            aws_credentials.password,
            self.log_json_file,
        )

        self.log.info('Loading data into staging table %s', self.table)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift_hook.run(copy_query)
        self.log.info('Table %s staged successfully', self.table)
