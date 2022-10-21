from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

"""
MySQL -> S3 -> Redshift DAG
- S3 적재 task 2번 이상 실행시 에러 발생
  - S3는 특정 s3_bucket , s3_key 에 대한 중복을 허용 못함
  - ValueError: The key gracia10-nps already exists.
"""

schema = "gracia10"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table

dag = DAG(
    dag_id="MySQL_to_Redshift_v1",
    start_date=datetime(2022, 10, 20),
    schedule_interval='0 20 * * *',
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

# FIXME MySQLToS3Operator 를 SqlToS3Operator 로 변경 (apache-airflow-providers-amazon v6.0.0 부터 deprecated)
mysql_to_s3_nps = MySQLToS3Operator(
    task_id='mysql_to_s3_nps',
    query="SELECT * FROM prod.nps",
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
    aws_conn_id="aws_conn_id",
    method='REPLACE',
    dag=dag
)

mysql_to_s3_nps >> s3_to_redshift_nps
