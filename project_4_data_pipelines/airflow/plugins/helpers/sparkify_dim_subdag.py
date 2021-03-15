from airflow import DAG
from operators import LoadDimensionOperator


def load_dim_subdag(
    parent_dag_name: str,
    task_id: str,
    redshift_conn_id: str,
    sql_statement: str,
    do_truncate: bool,
    table_name: str,
    **kwargs,
):
    """
    Airflow's subdag wrapper. Implements LoadDimensionOperator operator.
    Subdag's name will be f'{parent_dag_name}.{task_id}'

    Subdag related keyword arguments:
    - parent_dag_name -- Parent DAG name
    - task_id         -- Task ID for the subdag to use

    Keyword arguments:
    redshift_conn_id  -- Airflow connection name for Redshift detail
    sql_statement     -- SQL statement to run
    do_truncate       -- Does the table need to be truncated before running
                         SQL statement
    table_name        -- Dimension table name

    All keyword arguments will be passed to LoadDimensionOperator
    """

    dag = DAG(f'{parent_dag_name}.{task_id}', **kwargs)

    load_dimension_table = LoadDimensionOperator(
        task_id=task_id,
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query=sql_statement,
        do_truncate=do_truncate,
        table_name=table_name,
    )

    load_dimension_table

    return dag
