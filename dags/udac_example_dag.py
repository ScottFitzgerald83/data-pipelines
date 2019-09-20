from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)

from plugins.helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'scott',
    'start_date': datetime(2019, 9, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

with DAG('udac_example_dag',
         default_args=default_args,
         description='Load and transform data in Redshift with Airflow',
         schedule_interval='0 * * * *') as dag:

    start_operator = DummyOperator(
        task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events')

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks'
    )

    end_operator = DummyOperator(task_id='Stop_execution')

start_operator \
    >> [stage_events_to_redshift, stage_songs_to_redshift] \
    >> load_songplays_table \
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
    >> run_quality_checks \
    >> end_operator
