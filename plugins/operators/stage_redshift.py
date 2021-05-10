from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser


class StageToRedshiftOperator(BaseOperator):
    """

        StageToRedshiftOperator is a custom operator that is responsible for copying files from Amazon S3 into Amazon Redshift tables

        :param redshift_conn_id: Connection id of the Redshift connection to use
        :type redshift_conn_id: string    
            Default is 'redshift'

        :param aws_credentials_id: Connection id of the AWS credentials to use to access S3 data
        :type  aws_credentials_id: string
            Default is 'aws_credentials'

        :param table: Redshift staging table name
        :type table: string

        :param s3_bucket: Amazon S3 Bucket name where we read the staging data from.
        :type s3_bucket: string

        :param s3_key: Amazon S3 key folder that exist inside the S3 bucket that conatians that staging data we need.
        :type s3_key: string

        :param time_format: format of the time that exists in the staging data (that's crucial to redshift to be able to extract the right time format from the staging data)      
        :type time_format: string

        :param region: the region that our Redshift DB exists in.
        type region: string

        :param format_type: the format type, which define the paths to reach a specfic directory in a complex structure of directories (mapping file).
        :type format_type: string

        :param use_partitioning: If true, S3 data will be loaded as partitioned data based on year and month of execution_date
        :type use_partitioning: boolean
            Default is 'False'

        :param execution_date: Logical execution date of DAG run (templated -> loaded at run time)
        :type execution_date: string
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
        """ __init__ is an OOP function in python that intialize the object behviour -> (constructor). """

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
            "Copying data from S3 to {} table in Redshift".format(self.table))
        rendered_key = self.s3_key.format(**context)

        if self.use_partitioning == True:
            # If we are using partitioning, setup S3 path to use year and month of execution_date
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

        # execute the 'formatted_sql' command on Redshift.
        redshift.run(formatted_sql)
