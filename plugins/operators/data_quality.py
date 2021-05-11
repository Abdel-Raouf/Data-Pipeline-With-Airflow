from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
        DataQualityOperator is a custom operator that Performs Data qaulity checks on tables resides in Amazon Redshift DB

        :param redshift_conn_id: Connection id of the Redshift connection to use
        :type redshift_conn_id: string    
            Default is 'redshift'

        :params data_qaulity_checks[]: is a list of dictionaries with the criteria we need to test against.
        :type data_quality_checks: list
        :args data_quality_checks[i]: dictionary 
        :type data_quality_checks[i]: dictionary
        :dictionary args: 'check_sql_query': A query to check against it's condition.
                          'targeted_table': The targeted table name we need to apply a check on. 
                          'test_against': the criteria we need to test against as null.
                          'expected_result': the result we expect from the query we run against Redshift.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        for i, dict_params in enumerate(self.data_quality_checks):

            self.log.info(
                f"Executing Data Quality Check {i}: {dict_params.get('check_sql_query', default = 'This is Not A Valid Query')}")

            test_against_criteria = redshift.get_records(dict_params.get(
                'check_sql_query', default='This is Not A Valid Query'))

            default_count_check_query = redshift.get_records(
                f"SELECT COUNT(*) FROM {dict_params.get('targeted_table')}")

            num_records = default_count_check_query[0][0]

            if len(default_count_check_query) < 1 or len(default_count_check_query[0]) < 1:
                raise ValueError(
                    f"Data quality check FAILED. {dict_params.get('targeted_table')} table -> contains 0 rows")
            elif num_records < 1:
                raise ValueError(
                    f"Data quality check FAILED. {dict_params.get('targeted_table')} -> contains 0 records")
            elif dict_params.get('test_against') == 'null':
                raise ValueError(
                    "Data quality check {}. {} table -> contains {} {} values"
                    .format('Passed' if dict_params.get('expected_result') == len(test_against_criteria) else "FAILED",
                            dict_params.get('targeted_table'),
                            dict_params.get('expected_result'),
                            dict_params.get('test_against'))
                )
            else:
                self.log.info(
                    f"Data quality on table {dict_params.get('targeted_table')} PASSED with {num_records} records")
