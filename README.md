## Project Scenario

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Data Pipeline

We will use Apache Airflow to create a pipeline to extract, transform, and load JSON data from Amazon S3 to Amazon Redshift, then insert Data into Fact and Dimentions tables, while apply some Data checks to ensure Data Qaulity.

## Airflow Custom Operators

### Stage to Redshift Operator:

`StageToRedshiftOperator` is a custom operator that is responsible for copying files from Amazon S3 into Amazon Redshift tables.

The staging operation is based on parameters passed to the operator:

- redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'.
- aws_credentials_id - The connection ID of the credentials to connect to Amazon S3 configured in Apache Airflow. Defaults to 'aws_credentials'.
- table - Redshift staging table name where data will be copied.
- s3_bucket - Amazon S3 Bucket name where we read the staging data from.
- s3_key - Amazon S3 key folder that exist inside the S3 bucket that conatians that staging data we need.
- time_format - format of the time that exists in the staging data (that's crucial to redshift to be able to extract the right time format from the staging data)
- region - the region that our Redshift DB exists in.
- format_type - the format type, which define the paths to reach a specfic directory in a complex structure of directories (mapping file).
- use_partitioning - If true, S3 data will be loaded as partitioned data based on year and month of execution_date.
- execution_date - Logical execution date of DAG run (templated -> loaded at run time).

### Load Dimension Table Operator:

`LoadDimensionOperator` is a custom operator that loads and transforms data from Redshift staging table to dimension tables.

This operator has the following parameters:

- redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'.
- table - Redshift dimension table name, where data will be inserted.
- append_data - if True, we will Append data to the table.
- sql - Query representing data that will be inserted.

### Load Fact Table Operator:

`LoadFactOperator` is a custom Operator that loads and transforms data from Redshift staging table to the fact table.

This operator has the following parameters:

- redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'.
- table - Redshift fact table name, where data will be inserted.
- sql - Query representing data that will be inserted
- append_data - if True, we will Append data to the table.

### Data Quality Operator:

`DataQualityOperator` is a custom operator that Performs Data qaulity checks on tables resides in Amazon Redshift DB

This operator has the folloing parameters:

- redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'.
- data_quality_checks - is a list of dictionaries with the criteria we need to test against.

## Airflow subDAG:

`load_dimension_tables_dag` is a custom subDAG, to make our code for our custom operator `LoadDimensionOperator` be reusable accross Multiple DAGs.

This subDAG has the following parameters:

- redshift_conn_id - The connection ID of the Amazon Redshift connection configured in Apache Airflow. Defaults to 'redshift'.
- parent_dag_name - the name of the parent DAG.
- task_id - to give the subDag a unique id or name.
- table - Redshift dimension table name, where data will be inserted.
- append_data - if True, we will Append data to the table.
- insert_sql_stmt - Query representing data that will be inserted.

### Pipeline Tasks Dependencies Management:

![Pipeline Tasks Dependencies Management](https://github.com/Abdel-Raouf/Data-Pipeline-With-Airflow/blob/main/images/Screenshot%20from%202021-05-06%2017-17-29.png)

## Airflow Setup:

### Airflow will need two parameters for the connections to AWS and Redshift:

- aws_credentials - AWS connection with credentials to access S3 (With IAM Role -> `AmazonS3FullAccess` given to user, that we use their key_id and secret_key).
- redshift - A PostgreSQL connection with credentials to Amazon Redshift.

### Placing dags and operators in your airflow local folder:

- Copy dag folder contents into your airflow dags.
- Copy plugins folder contents to your airflow plugins folder for custom operators.

## DAG Execution:

Enable the DAG toggle to be on in `Airflow UI` and it will begin Executing our Data-flow on hourly bases.
