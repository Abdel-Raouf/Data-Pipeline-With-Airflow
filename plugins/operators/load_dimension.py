from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    """
        LoadDimensionOperator is a custom operator that loads and transforms data from Redshift staging table to dimension table.

        :param redshift_conn_id: Connection id of the Redshift connection to use
        :type redshift_conn_id: string    
            Default is 'redshift'

        :param table: Redshift dimension table name, where data will be inserted.
        :type table: string

        :param append_data: if True, we will Append data to the table.
        :type append_data: Boolean

        :param sql: Query representing data that will be inserted
        type sql: string
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 append_data="",
                 sql="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.append_data = append_data
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_data == True:
            self.log.info(
                "Append Data to {} Dimension table".format(self.table))
            redshift.run(self.sql)
        else:
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info(
                "Insert Data to {} Dimension table".format(self.table))
            redshift.run(self.sql)
