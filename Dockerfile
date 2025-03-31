FROM apache/airflow:2.6.0
USER airflow
RUN pip install psycopg2-binary apache-airflow-providers-common-sql asyncpg apache-airflow-providers-postgres

