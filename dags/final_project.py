from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, ResetTablesOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator )

default_args = {
    'owner': 'Jack Toke',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 1),
    'catchup': False,
    'email_on_retry': False,
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)
def final_project():
    """
    DAG for loading data from an S3 bucket and transforming it in Redshift
    """

    start_operator = DummyOperator(task_id='Begin_execution')

    # Uncomment the following lines if you want to reset the tables before loading data
    # reset_tables_task = ResetTablesOperator(
    #     task_id='Reset_tables',
    #     conn_id="redshift",
    #     tables=["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"])

    # Stage the event data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="airflow-toke-bucket",
        s3_key="event-data",
        template="s3://airflow-toke-bucket/log_json_path.json"
    )

    # Stage the song data from S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="airflow-toke-bucket",
        s3_key="song-data",
    )

    # Load the songplays fact table
    load_songplays_table_task = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id="redshift",
        table="songplays",
    )

    # Load the dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id="redshift",
        table="users",
        truncate=True,
    )

    # Load the dimension tables
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id="redshift",
        table="songs",
        truncate=True,
    )

    # Load the dimension tables
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id="redshift",
        table="artists",
        truncate=True,
    )

    # Load the dimension tables
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id="redshift",
        table="time",
        truncate=True,
    )

    # Define the data quality checks
    # This is a dictionary of tables to check and their expected results
    # The expected result is the number of rows in the table
    # The check_column is the column to check for the number of rows
    data_to_check = {
            "songplays": {
                "check_column": "play_id",
                "expected_result": 6837
            },
            "users": {
                "check_column": "user_id",
                "expected_result": 104
            },
            "songs": {
                "check_column": "song_id",
                "expected_result": 5154
            },
            "artists": {
                "check_column": "artist_id",
                "expected_result": 6623
            },
            "time": {
                "check_column": "start_time",
                "expected_result": 6813
            },
        }

    # Run data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id = "redshift",
        qualities_to_check = data_to_check,
    )

    stop_operator = DummyOperator(task_id='Stop_execution')

    # Uncomment the following 3 lines if you want to reset the tables before loading data
    # start_operator >> reset_tables_task
    # reset_tables_task >> stage_events_to_redshift
    # reset_tables_task >> stage_songs_to_redshift

    # Comment the following 2 lines if you want to reset the tables before loading data
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

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
