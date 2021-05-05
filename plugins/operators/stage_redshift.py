# from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """

        StageToRedshiftOperator is an custom operator that is responsible for copying files from S3 into redshift tables
    """

    ui_color = '#358140'

    template_fields = ("s3_key",)
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

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(
            "Copying data from S3 to staging events table in Redshift")
        rendered_key = self.s3_key.format(**context)
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
