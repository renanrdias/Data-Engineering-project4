from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
import create_tables




S3_BUCKET='udacity-dend'
S3_LOG_KEY='log_data'
S3_SONG_KEY = 'song_data'
REDSHIFT_CONN_ID='redshift'
POSTGRES_CONN_ID='redshift'
AWS_CREDENTIALS='aws_credentials'


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'catchup' : False,
    'email_on_retry' : False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2019, 1, 11)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',          
          schedule_interval='@hourly',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create_tables = PostgresOperator(
#     task_id='create_stage_tables',
#     dag=dag,
#     postgres_conn_id=POSTGRES_CONN_ID,
#     sql=create_tables.create_tables_sql)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials=AWS_CREDENTIALS,
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id=POSTGRES_CONN_ID,
    table='songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql=SqlQueries.user_table_insert,
    redshift_conn_id=REDSHIFT_CONN_ID,
    table='users',
    table_pk='userid'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.song_table_insert,
    table='songs',
    table_pk='songid'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.artist_table_insert,
    table='artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.time_table_insert,
    table='time',
    table_pk='start_time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    sql_test="SELECT COUNT(*) FROM users WHERE userid IS NULL;",
    expected_result=0
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

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
