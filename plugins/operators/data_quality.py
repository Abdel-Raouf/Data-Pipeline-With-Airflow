from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
        DataQualityOperator is a custom operator that Performs Data qaulity checks on tables resides in Amazon Redshift DB

        :param redshift_conn_id: Connection id of the Redshift connection to use
        :type redshift_conn_id: string    
            Default is 'redshift'

        :params table_{i}(table_1 -> table_7): from one to the seventh tables names to apply checks on.
        :type table_{i}: string
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_1="",
                 table_2="",
                 table_3="",
                 table_4="",
                 table_5="",
                 table_6="",
                 table_7="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_1 = table_1
        self.table_2 = table_2
        self.table_3 = table_3
        self.table_4 = table_4
        self.table_5 = table_5
        self.table_6 = table_6
        self.table_7 = table_7

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        tables = [self.table_1, self.table_2, self.table_3,
                  self.table_4, self.table_5, self.table_6, self.table_7]
        for table in tables:
            records = redshift.get_records(
                f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check FAILED. {table} -> contained 0 rows")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check FAILED. {table} -> contained 0 records")
            else:
                self.log.info(
                    f"Data quality on table {table} check PASSED with {num_records} records")
