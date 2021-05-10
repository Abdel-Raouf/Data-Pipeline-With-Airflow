from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser


class StageToRedshiftOperator(BaseOperator):
    """

        StageToRedshiftOperator is an custom operator that is responsible for copying files from S3 into redshift tables
    """

    ui_color = '#358140'

    template_fields = ("s3_key", "execution_date")

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT AS '{}'
        REGION '{}'
        FORMAT AS JSON '{}' 
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 time_format="",
                 region="",
                 format_type="",
                 use_partitioning="",
                 execution_date="",
                 *args, **kwargs):
        """

         __init__ is an OOP function in python that intialize the object behviour -> (constructor).
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.time_format = time_format
        self.region = region
        self.format_type = format_type
        self.use_partitioning = use_partitioning
        self.execution_date = execution_date

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(
            "Clearing data from {} Redshift table".format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        execution_date = parser.parse(self.execution_date)
        self.log.info(f"Execution Date: {execution_date}")
        self.log.info(f"Execution Year: {execution_date.year}")
        self.log.info(f"Execution Month: {execution_date.month}")
        self.log.info(f"Use Partitioning: {self.use_partitioning}")

        self.log.info(
            "Copying data from S3 to {} table in Redshift".format(table))
        rendered_key = self.s3_key.format(**context)

        if self.use_partitioning == True:
            s3_path = "s3://{}/{}/{}/{}".format(
                self.s3_bucket, rendered_key, execution_date.year, execution_date.month)
        else:
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # create the copy command for the staging events table.
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,  # table name
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.time_format,
            self.region,
            self.format_type
        )
        redshift.run(formatted_sql)
