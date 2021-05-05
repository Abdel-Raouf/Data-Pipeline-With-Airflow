from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from subdags import load_dimension_tables_dag

default_args = {
    'owner': 'abdelraouf',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 5,
    'email_on_failure': False,
    'catchup': False
}

dag = DAG('airflow_redshift_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
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

# user_table_task_id = "Load_user_dim_table"
# load_user_dim_task = SubDagOperator(
#     subdag=load_dimension_tables_dag(
#         "airflow_redshift_dag",
#         user_table_task_id,
#         "redshift",
#         "users",
#         "False",
#         SqlQueries.user_table_insert
#     ),
#     task_id=user_table_task_id,
#     dag=dag
# )

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    append_data='False',
    sql=SqlQueries.user_table_insert
)

# song_table_task_id = "Load_song_dim_table"
# load_song_dim_task = SubDagOperator(
#     subdag=load_dimension_tables_dag(
#         "airflow_redshift_dag",
#         song_table_task_id,
#         "redshift",
#         "songs",
#         "False",
#         SqlQueries.song_table_insert
#     ),
#     task_id=song_table_task_id,
#     dag=dag
# )

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    append_data='False',
    sql=SqlQueries.song_table_insert
)

# artist_table_task_id = "Load_artist_dim_table"
# load_artist_dim_task = SubDagOperator(
#     subdag=load_dimension_tables_dag(
#         "airflow_redshift_dag",
#         artist_table_task_id,
#         "redshift",
#         "artists",
#         "False",
#         SqlQueries.artist_table_insert
#     ),
#     task_id=artist_table_task_id,
#     dag=dag
# )


load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    append_data='False',
    sql=SqlQueries.artist_table_insert
)

# time_table_task_id = "Load_time_dim_table"
# load_time_dim_task = SubDagOperator(
#     subdag=load_dimension_tables_dag(
#         "airflow_redshift_dag",
#         time_table_task_id,
#         "redshift",
#         "time",
#         "False",
#         SqlQueries.time_table_insert
#     ),
#     task_id=time_table_task_id,
#     dag=dag
# )

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    append_data='False',
    sql=SqlQueries.time_table_insert
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
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
