from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator

from plugins.s3_to_redshift_operator import S3ToRedshiftOperator

"""
MySQL -> S3 -> Redshift DAG
- Incremental Update 
  - MySQLToS3Operator -> execution_date에 해당하는 레코드만 조회
  - 커스텀 S3ToRedshiftOperator -> 파티션 후 유일한 값만 입력
"""

schema = "gracia10"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

dag = DAG(
    dag_id="MySQL_to_Redshift_v3",
    start_date=datetime(2022, 10, 15),
    schedule_interval='0 20 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

s3_folder_cleanup = S3DeleteObjectsOperator(
    task_id='s3_folder_cleanup',
    bucket=s3_bucket,
    keys=s3_key,
    aws_conn_id="aws_conn_id",
    dag=dag
)

# FIXME MySQLToS3Operator 를 SqlToS3Operator 로 변경 (apache-airflow-providers-amazon v6.0.0 부터 deprecated)
mysql_to_s3_nps = MySQLToS3Operator(
    task_id='mysql_to_s3_nps',
    query="SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    mysql_conn_id="mysql_conn_id",
    aws_conn_id="aws_conn_id",
    verify=False,
    dag=dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id='s3_to_redshift_nps',
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    schema=schema,
    table=table,
    copy_options=['csv'],
    redshift_conn_id="redshift_dev_db",
    primary_key="id",
    order_key='created_at',
    dag=dag
)

s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps
