# Data Pipeline Orchestration with Airflow, AWS S3, and Redshift

This project demonstrates a robust data pipeline built using Apache Airflow for orchestration, AWS S3 for data lake storage, and Amazon Redshift for data warehousing. The pipeline extracts raw data from an S3 bucket, transforms it, and loads it into a Redshift data warehouse, ready for analysis. Custom Airflow operators are implemented to handle data staging, loading, and quality checks.

## Key Features

* **Data Extraction and Transformation:** Extracts JSON-formatted data from AWS S3.
* **Data Warehousing:** Loads transformed data into Amazon Redshift.
* **Custom Airflow Operators:** Implements custom operators for staging, fact/dimension loading, and data quality checks.
* **Data Quality Checks:** Validates data integrity using SQL-based tests.
* **Dockerized Environment:** Runs Airflow within Docker containers for easy setup and portability.
* **Truncate-Insert and Append-Only Patterns:** Supports both truncate-insert (for dimensions) and append-only (for facts) loading strategies.
* **Hourly Scheduling:** The pipeline is scheduled to run hourly.
* **Backfill Support**: The pipeline supports backfilling data for past dates/times.

## Getting Started

### Prerequisites

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
* An active AWS account with S3 and Redshift resources.

### Setup

1. **Start Airflow:**

```bash
    docker-compose up -d
```

   This command starts the Airflow web server and other necessary services in detached mode.

2.**Access Airflow UI:**

* Open your web browser and go to `http://localhost:8080`.
* Log in with the default credentials: `airflow` / `airflow`.

3.**Configure Connections:**

* Navigate to **Admin > Connections** in the Airflow UI.
* Add the following connections:
  * **`aws_credentials`:** AWS credentials for S3 access.
  * **`redshift`:** Redshift connection details.

   **Alternative: Using the Command Line**

   You can also configure connections and variables using the Airflow CLI within the webserver container:

   1. **Get the container ID:**

```bash
        docker ps
```

   2.**Enter the webserver container:**

```bash
docker exec -it <webserver_container_id> /bin/bash
```

   3.**Run the following commands (replace placeholders):**

```bash
airflow connections add 'aws_credentials' \
   --conn-json '{ "conn_type": "aws", "login": "<YOUR_ACCESS_KEY_ID>", "password": "<YOUR_SECRET_ACCESS_KEY>" }' && \
airflow connections add 'redshift' \
   --conn-json '{ "conn_type": "redshift", "login": "<YOUR_REDSHIFT_USER>", "password": "<YOUR_REDSHIFT_PASSWORD>", "host": "<YOUR_REDSHIFT_ENDPOINT>", "port": "5439", "schema": "dev" }' && \
airflow variables set s3_bucket <YOUR_BUCKET_NAME> && \
airflow variables set s3_source_log_prefix log_data && \
airflow variables set s3_source_song_prefix song-data
```

   4.**Start Redshift Cluster:**

* Ensure your Redshift cluster is running in the AWS console.

  5.**Run the DAG**:

* Unpause the DAG and trigger it.

## Project Structure

* **`dags/final_project.py`:** The main Airflow DAG definition, including task dependencies.
* **`plugins/operators/`:** Custom Airflow operators:
  * `data_quality.py`: `DataQualityOperator` for data validation.
  * `load_fact.py`: `LoadFactOperator` for loading fact tables (append-only).
  * `load_dimension.py`: `LoadDimensionOperator` for loading dimension tables (truncate-insert or append).
  * `stage_redshift.py`: `StageToRedshiftOperator` for staging data from S3 to Redshift.
* **`plugins/helpers/sql_queries.py`:** SQL queries used by the operators.
* **`Dockerfile`**: Dockerfile to build the custom Airflow image.
* **`docker-compose.yaml`**: Docker compose file to run the Airflow environment.

## DAG Configuration

The DAG is configured with the following settings:

* **Schedule:** Hourly (`@hourly`).
* **Start Date:** October 1, 2023.
* **Retries:** 3 retries per task.
* **Retry Delay:** 5 minutes between retries.
* **Catchup:** Disabled (`catchup=False`).
* **No dependencies on past runs**: `depends_on_past=False`.

!Working DAG with correct task dependencies

## Custom Operators

### `StageToRedshiftOperator`

* Copies JSON data from S3 to a specified Redshift table.
* Supports automatic JSON format detection (`FORMAT AS JSON 'auto'`).
* Supports loading timestamped files from S3 based on execution time for backfills.

### `LoadFactOperator`

* Loads data into a fact table in Redshift.
* Implements append-only functionality.

### `LoadDimensionOperator`

* Loads data into a dimension table in Redshift.
* Supports both truncate-insert (default) and append-only modes.

### `DataQualityOperator`

* Performs data quality checks using SQL queries.
* Raises an exception if data quality checks fail.

### `ResetTablesOperator`

* Truncates all the tables in the Redshift database.
* This operator is used for development purposes.

## Conclusion

This project provides a practical example of building a data pipeline using Airflow, AWS S3, and Redshift. It demonstrates the use of custom operators, data quality checks, and best practices for data warehousing.

## License

This project is licensed under the terms of the Udacity License.
