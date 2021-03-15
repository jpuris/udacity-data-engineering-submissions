from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from helpers import load_dim_subdag
from helpers import SqlQueries
from operators import CreateTableOperator
from operators import DataQualityOperator
from operators import LoadFactOperator
from operators import StageToRedshiftOperator

DAG_NAME = 'sparkify_data_pipeline'

default_args = {
    'owner': 'sparkify',
    'depends_on_past': True,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag,
)

create_tables_in_redshift = CreateTableOperator(
    task_id='create_tables_in_redshift',
    redshift_conn_id='redshift',
    dag=dag,
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    table_name='staging_events',
    s3_bucket=Variable.get('sparkify_s3_bucket'),
    s3_key=Variable.get('sparkify_s3_log_key'),
    file_format='JSON',
    log_json_file=Variable.get('sparkify_s3_log_json_path_key'),
    redshift_conn_id='redshift',
    aws_credential_id='aws_credentials',
    dag=dag,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    table_name='staging_songs',
    s3_bucket=Variable.get('sparkify_s3_bucket'),
    s3_key=Variable.get('sparkify_s3_song_key'),
    file_format=Variable.get('sparkify_s3_file_format'),
    redshift_conn_id='redshift',
    aws_credential_id='aws_credentials',
    dag=dag,
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert,
    dag=dag,
)

load_user_dim_table = SubDagOperator(
    subdag=load_dim_subdag(
        parent_dag_name=DAG_NAME,
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.user_table_insert,
        do_truncate=True,
        table_name='users',
    ),
    task_id='Load_user_dim_table',
    dag=dag,
)

load_song_dim_table = SubDagOperator(
    subdag=load_dim_subdag(
        parent_dag_name=DAG_NAME,
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.song_table_insert,
        do_truncate=True,
        table_name='songs',
    ),
    task_id='Load_song_dim_table',
    dag=dag,
)

load_artist_dim_table = SubDagOperator(
    subdag=load_dim_subdag(
        parent_dag_name=DAG_NAME,
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.artist_table_insert,
        do_truncate=True,
        table_name='artists',
    ),
    task_id='Load_artist_dim_table',
    dag=dag,
)

load_time_dim_table = SubDagOperator(
    subdag=load_dim_subdag(
        parent_dag_name=DAG_NAME,
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        start_date=default_args['start_date'],
        sql_statement=SqlQueries.time_table_insert,
        do_truncate=True,
        table_name='time',
    ),
    task_id='Load_time_dim_table',
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['artists', 'songplays', 'songs', 'time', 'users'],

)

end_operator = DummyOperator(
    task_id='Stop_execution',
    dag=dag,
)

start_operator >> create_tables_in_redshift

create_tables_in_redshift >> [
    stage_songs_to_redshift,
    stage_events_to_redshift,
] >> load_songplays_table

load_songplays_table >> [
    load_user_dim_table,
    load_song_dim_table,
    load_artist_dim_table,
    load_time_dim_table,
] >> run_quality_checks >> end_operator
