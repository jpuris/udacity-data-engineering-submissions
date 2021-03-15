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
    # TODO: docstring
    """
        docstring
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
