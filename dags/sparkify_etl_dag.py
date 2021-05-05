from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from subdags.dimension_subdag import load_dimension_tables_dag

default_args = {
    'owner': 'abdelraouf',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 5,
    'email_on_failure': False,
    'catchup': False
}

start_date = datetime(2019, 1, 12)

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=start_date,
          schedule_interval='0 * * * *'  # scheduled hourly using cron_expression
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='sql/create_tables.sql',
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    time_format='epochmillisecs',
    region='us-west-2',
    format_type='s3://udacity-dend/log_json_path.json',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    time_format='epochmillisecs',
    region='us-west-2',
    format_type='auto',
    s3_bucket='udacity-dend',
    s3_key='song_data/A/A/A/',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    append_data='False',
    sql=SqlQueries.songplay_table_insert
)

user_table_task_id = "Load_user_dim_table"
load_user_dim_task = SubDagOperator(
    subdag=load_dimension_tables_dag(
        "sparkify_etl_dag",
        user_table_task_id,
        "redshift",
        "users",
        "False",
        SqlQueries.user_table_insert,
        start_date=start_date
    ),
    task_id=user_table_task_id,
    dag=dag
)


song_table_task_id = "Load_song_dim_table"
load_song_dim_task = SubDagOperator(
    subdag=load_dimension_tables_dag(
        "sparkify_etl_dag",
        song_table_task_id,
        "redshift",
        "songs",
        "False",
        SqlQueries.song_table_insert,
        start_date=start_date
    ),
    task_id=song_table_task_id,
    dag=dag
)


artist_table_task_id = "Load_artist_dim_table"
load_artist_dim_task = SubDagOperator(
    subdag=load_dimension_tables_dag(
        "sparkify_etl_dag",
        artist_table_task_id,
        "redshift",
        "artists",
        "False",
        SqlQueries.artist_table_insert,
        start_date=start_date
    ),
    task_id=artist_table_task_id,
    dag=dag
)


time_table_task_id = "Load_time_dim_table"
load_time_dim_task = SubDagOperator(
    subdag=load_dimension_tables_dag(
        "sparkify_etl_dag",
        time_table_task_id,
        "redshift",
        "time",
        "False",
        SqlQueries.time_table_insert,
        start_date=start_date
    ),
    task_id=time_table_task_id,
    dag=dag
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table_1='staging_events',
    table_2='staging_songs',
    table_3='songplays',
    table_4='users',
    table_5='songs',
    table_6='artists',
    table_7='time'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# tasks dependencies management
start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dim_task
load_songplays_table >> load_user_dim_task
load_songplays_table >> load_artist_dim_task
load_songplays_table >> load_time_dim_task
load_song_dim_task >> run_quality_checks
load_user_dim_task >> run_quality_checks
load_artist_dim_task >> run_quality_checks
load_time_dim_task >> run_quality_checks
run_quality_checks >> end_operator
