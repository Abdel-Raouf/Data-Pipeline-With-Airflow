from airflow import DAG
from operators import LoadDimensionOperator


def load_dimension_tables_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        append_data,
        insert_sql_stmt,
        *args, **kwargs):
    """

        load_dimension_tables_dag is a custom subDAG, to make our code for our custom operator LoadDimensionOperator be reusable accross various DAGs.

        :param parent_dag_name: the name of the parent DAG
        :type parent_dag_name: string

        :param task_id: to give the subDag a unique id or name.
        :type task_id: string

        :param redshift_conn_id: Connection id of the Redshift connection to use
        :type redshift_conn_id: string    
            Default is 'redshift'

        :param table: Redshift dimension table name, where data will be inserted.
        :type table: string

        :param append_data: if True, we will Append data to the table.
        :type append_data: Boolean

        param insert_sql_stmt: Query representing data that will be inserted
        type sql: string

    """

    # A specific convention used to pass the subDAG to the parent DAG.
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    # calling our custom operator and passing to it the parameters needed (that our subDAG make it reusable)
    load_dimension_task = LoadDimensionOperator(
        task_id=f'Load_{table}_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        append_data=append_data,
        sql=insert_sql_stmt
    )

    # return the above DAG to make our subDAG accessible, based on the ocnvention above.
    return dag
