from airflow import DAG
from operators import LoadDimensionOperator


def load_dim_subdag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    sql_statement,
    delete_load,
    table_name,
    *args,
    **kwargs,
):
    ''' TODO: docstring'''

    dag = DAG(f'{parent_dag_name}.{task_id}', *args, **kwargs)

    load_dimension_table = LoadDimensionOperator(
        task_id=task_id,
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        sql_query=sql_statement,
        delete_load=delete_load,
        table_name=table_name,
    )

    load_dimension_table

    return dag
