from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    """

        LoadFactOperator is a custom Operator that loads and transforms data from Redshift staging table to fact table.

        :param redshift_conn_id: Connection id of the Redshift connection to use
        :type redshift_conn_id: string    
            Default is 'redshift'

        :param table: Redshift fact table name, where data will be inserted.
        :type table: string

        :param sql: Query representing data that will be inserted
        type sql: string

        :param append_data: if True, we will Append data to the table.
        :type append_data: Boolean
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 append_data="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_data == True:
            self.log.info(
                "Append Data to {} Fact table".format(self.table))
            redshift.run(self.sql)
        else:
            self.log.info(
                "Clearing data from destination Redshift {} table".format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info(
                "Insert Data to {} Fact table".format(self.table))
            redshift.run(self.sql)
