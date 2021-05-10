from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

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

# TODO : completing the work of append_data parameter.
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
