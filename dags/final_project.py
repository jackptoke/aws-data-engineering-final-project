from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, DropTableOperator, CreateTableOperator ) # , LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    drop_staging_event_table_task = DropTableOperator(
        task_id='Drop_staging_event_table',
        conn_id='redshift',
        table="staging_events",
    )

    drop_staging_songs_table_task = DropTableOperator(
        task_id='Drop_staging_songs_table',
        conn_id='redshift',
        table="staging_songs",
    )

    create_staging_events_table_task = CreateTableOperator(
        task_id='Create_staging_events_table',
        conn_id='redshift',
        table="staging_events",
    )
    create_staging_songs_table_task = CreateTableOperator(
        task_id='Create_staging_songs_table',
        conn_id='redshift',
        table="staging_songs",
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="airflow-toke-bucket",
        s3_key="event-data",
        template="s3://airflow-toke-bucket/log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="airflow-toke-bucket",
        s3_key="song-data",
    )

    # load_songplays_table = LoadFactOperator(
    #     task_id='Load_songplays_fact_table',
    # )

    # load_user_dimension_table = LoadDimensionOperator(
    #     task_id='Load_user_dim_table',
    # )

    # load_song_dimension_table = LoadDimensionOperator(
    #     task_id='Load_song_dim_table',
    # )

    # load_artist_dimension_table = LoadDimensionOperator(
    #     task_id='Load_artist_dim_table',
    # )

    # load_time_dimension_table = LoadDimensionOperator(
    #     task_id='Load_time_dim_table',
    # )

    # run_quality_checks = DataQualityOperator(
    #     task_id='Run_data_quality_checks',
    # )

    drop_staging_event_table_task >> create_staging_events_table_task
    drop_staging_songs_table_task >> create_staging_songs_table_task
    create_staging_events_table_task >> stage_events_to_redshift
    create_staging_songs_table_task >> stage_songs_to_redshift

final_project_dag = final_project()
