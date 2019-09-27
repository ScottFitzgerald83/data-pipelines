from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators import DataQualityOperator, LoadDimensionOperator, LoadFactOperator, StageToRedshiftOperator
from helpers import SqlQueries
from helpers import TestHelpers

IAM_ROLE = BaseHook.get_connection("redshift").extra_dejson.get('iam_role')

S3_BUCKET = 'udacity-dend'
LOG_KEY = 'log_data'
SONG_KEY = 'song_data'
LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'

default_args = {
    'owner': 'scott',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 26),
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'catchup': False,
    'email_on_retry': False,
}

with DAG('sparkify_etl',
         default_args=default_args,
         description='S3 -> Redshift ETL for Sparkify songs and event data',
         schedule_interval='@hourly',
         template_searchpath='/usr/local/airflow',
         max_active_runs=1) as dag:

    start_operator = DummyOperator(
        task_id='start_execution',
    )

    create_tables_task = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='redshift',
        sql='plugins/helpers/create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        conn_id='redshift',
        params={'iam_role': IAM_ROLE,
                's3_bucket': S3_BUCKET,
                's3_key': LOG_KEY,
                'table': "events_stage",
                'json_format': LOG_JSONPATH}
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        conn_id='redshift',
        params={'iam_role': IAM_ROLE,
                's3_bucket': S3_BUCKET,
                's3_key': SONG_KEY,
                'table': "songs_stage"}
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id="redshift",
        sql=SqlQueries.songplay_table_insert,
        params={'table': 'songplays', 'truncate': True}
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id="redshift",
        sql=SqlQueries.user_table_insert,
        params={'table': 'users', 'truncate': True}
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id="redshift",
        sql=SqlQueries.song_table_insert,
        params={'table': 'songs', 'truncate': True}
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id="redshift",
        sql=SqlQueries.artist_table_insert,
        params={'table': 'artists', 'truncate': True}
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id="redshift",
        sql=SqlQueries.time_table_insert,
        params={'table': 'time', 'truncate': True}
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id="redshift",
        params={"tests_to_run": TestHelpers.tests_to_run}
    )

    finish_operator = DummyOperator(
        task_id='end_execution')

start_operator \
    >> create_tables_task \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> finish_operator
