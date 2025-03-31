from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, ResetTablesOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator )

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

    reset_tables_task = ResetTablesOperator(
        task_id='Reset_tables',
        conn_id="redshift",
        tables=["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"])

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

    load_songplays_table_task = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id="redshift",
        table="songplays",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id="redshift",
        table="users",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id="redshift",
        table="songs",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id="redshift",
        table="artists",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id="redshift",
        table="time",
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id = "redshift",
    )

    stop_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> reset_tables_task
    reset_tables_task >> stage_events_to_redshift
    reset_tables_task >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table_task
    stage_songs_to_redshift >> load_songplays_table_task
    load_songplays_table_task >> load_user_dimension_table
    load_songplays_table_task >> load_song_dimension_table
    load_songplays_table_task >> load_artist_dimension_table
    load_songplays_table_task >> load_time_dimension_table
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks
    run_quality_checks >> stop_operator

final_project_dag = final_project()
