from airflow import DAG
from plugins.operators import LoadDimensionOperator


def load_dimension_tables_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        append_data,
        create_sql_stmt,
        *args, **kwargs):

    dag = DAG(
        f"{parent_dag_name}.{task_id}", **kwargs
    )

    load_dimension_task = LoadDimensionOperator(
        task_id='Load_{table}_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        append_data=append_data,
        sql=create_sql_stmt
    )
