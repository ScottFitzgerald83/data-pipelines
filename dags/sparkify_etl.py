from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.operators import DataQualityOperator, LoadDimensionOperator, LoadFactOperator, StageToRedshiftOperator
from helpers import SqlQueries


IAM_ROLE = BaseHook.get_connection("redshift").extra_dejson.get('iam_role')
S3_BUCKET = 'udacity-dend'
LOG_KEY = 'log_data'
SONG_KEY = 'song_data'
LOG_JSONPATH = 's3://udacity-dend/log_json_path.json'

default_args = {
    'owner': 'scott',
    'start_date': datetime(2019, 9, 23),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'wait_for_downstream': True,
    'max_active_runs': 1
}

with DAG('sparkify_etl_dag',
         default_args=default_args,
         description='S3 -> Redshift ETL for Sparkify songs and event data',
         template_searchpath='/usr/local/airflow',
         schedule_interval='@daily') as dag:

    start_operator = DummyOperator(
        task_id='Begin_execution'
    )

    create_tables_task = PostgresOperator(
        task_id='create_tables',
        params={"conn_id": "redshift"},
        postgres_conn_id='redshift',
        sql='plugins/helpers/create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        conn_id='redshift',
        iam_role=IAM_ROLE,
        s3_bucket=S3_BUCKET,
        s3_key=LOG_KEY,
        table="events_stage",
        json_format=LOG_JSONPATH
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        conn_id='redshift',
        iam_role=IAM_ROLE,
        s3_bucket=S3_BUCKET,
        s3_key=SONG_KEY,
        table="songs_stage"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id="redshift",
        sql=SqlQueries.songplay_table_insert,
        params={'table': 'songplays'}
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id="redshift",
        sql=SqlQueries.user_table_insert,
        params={'table': 'users'}
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id="redshift",
        sql=SqlQueries.song_table_insert,
        params={'table': 'songs'}
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id="redshift",
        sql=SqlQueries.artist_table_insert,
        params={'table': 'artists'}
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id="redshift",
        sql=SqlQueries.time_table_insert,
        params={'table': 'time'}
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id="redshift",
        params={'tables': ['songplays', 'users', 'songs', 'artists', 'time']}
    )

    finish_operator = DummyOperator(
        task_id='End_execution')

start_operator \
    >> create_tables_task \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> finish_operator
