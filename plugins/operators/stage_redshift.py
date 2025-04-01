from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    """
    Stage data from S3 to Redshift
    """
    ui_color = '#358140'
    template_fields = ("s3_key",)

    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials_id="",
        table="",
        s3_bucket="",
        s3_key="",
        template="auto",
        *args, **kwargs):
        """Constructor for StageToRedshiftOperator.
        Args:
            redshift_conn_id (str): Redshift connection ID
            aws_credentials_id (str): AWS credentials ID
            table (str): Table name to stage data into
            s3_bucket (str): S3 bucket name
            s3_key (str): S3 key name
            template (str): S3 path of the JSON template file
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.template= template
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        # Set up AWS credentials
        self.log.info("Setting up AwsHook  {}".format(
            self.aws_credentials_id))
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info("Getting credentials for AwsHook {}".format(
            self.aws_credentials_id))
        credentials = aws_hook.get_credentials()

        # Set up Redshift connection
        self.log.info("Setting up PostgresHook for Redshift connection {}".format(
            self.redshift_conn_id))
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        # if self.s3_key == "event-data":
        #     # For live data, we apply logical partitioning
        #     # to avoid copying the entire bucket
        #     # The following code is commented out because we don't have live data.
        #     # year = context['execution_date'].year
        #     # month = context['execution_date'].month
        #     # day = context['execution_date'].day
        #     # s3_path = "s3://{bucket}/{folder}/{year:04d}/{month:02d}/{year:04d}-{month:02d}-{day:02d}-events.json".format(bucket = self.s3_bucket, folder=self.s3_key, year=year, month=month, day=day)
        #     s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        # else:
        #     s3_path = "s3://{}/{}/".format(self.s3_bucket, self.s3_key)
        formatted_sql = SqlQueries.STAGING_TABLE_COPY.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.template
        )
        self.log.info("Copying data from s3://{bucket}/{key} to Redshift".format(
            bucket=self.s3_bucket,
            key=self.s3_key
        ))
        redshift.run(formatted_sql)

        self.log.info("Staging table {} completed".format(self.table))
