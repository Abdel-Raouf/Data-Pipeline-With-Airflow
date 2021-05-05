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

    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_dimension_task = LoadDimensionOperator(
        task_id=f'Load_{table}_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        append_data=append_data,
        sql=insert_sql_stmt
    )

    return dag
