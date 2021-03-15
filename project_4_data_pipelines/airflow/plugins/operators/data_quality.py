from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Runs data quality checks:
    - target table must have row count greater than 0

    Keyword arguments:
    redshift_conn_id  -- Airflow connection name for Redshift detail
    tables            -- A list of tables to run the checks on
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        tables: list,
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info(
                'Starting data quality validation on table : %s', table,
            )

            records = redshift_hook.get_records(
                f'SELECT COUNT(*) FROM {table};',
            )

            if len(records) == 0 or len(records[0]) == 0 or records[0][0] == 0:
                raise ValueError(
                    'Data Quality validation failed for table : {}'.
                    format(table),
                )
            self.log.info(
                'Data Quality Validation Passed on table : %s', table,
            )
