import logging
from datetime import datetime

from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


def execsql(**context):
    schema = context['params']['schema']
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """
    sql += select_sql
    cur.execute(sql)

    cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")

    sql = f"""DROP TABLE IF EXISTS {schema}.{table};ALTER TABLE {schema}.temp_{table} RENAME to {table};"""
    sql += "COMMIT;"
    logging.info(sql)

    try:
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id="Build_Summary_v1",
    start_date=datetime(2022, 10, 20),
    schedule_interval='@once',
    catchup=False
)

execsql = PythonOperator(
    task_id='execsql',
    python_callable=execsql,
    params={
        'schema': 'gracia10',
        'table': 'channel_summary',
        'sql': """SELECT
	      DISTINCT A.userid,
        FIRST_VALUE(A.channel) OVER(PARTITION BY A.userid ORDER BY B.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS First_Channel,
        LAST_VALUE(A.channel) OVER(PARTITION BY A.userid ORDER BY B.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Last_Channel
        FROM raw_data.user_session_channel A
        LEFT JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid;"""
    },
    provide_context=True,  # True 설정시 argument set(Jinja 변수 등)를 추가함
    dag=dag
)

# operator 하나만 있으면 순서 생략 가능
