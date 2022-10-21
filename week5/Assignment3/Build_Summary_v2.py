import logging
from datetime import datetime

from airflow import DAG, AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

"""
일별 nps를 계산하는 써머리 테이블 만들어보기
"""


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()


# FIXME jinja 템플릿을 이용해볼 것
def execsql(**context):
    schema = context['params']['schema']
    table = context['params']['table']
    execution_date = context['execution_date']
    select_sql = f"""
          SELECT DATE(created_at) ymd,
                 COUNT(DISTINCT id) total,
                 COUNT(DISTINCT CASE WHEN score >= 9 THEN id END) promoter,
                 COUNT(DISTINCT CASE WHEN score <= 6 THEN id END) detractor,
                 ROUND((promoter - detractor) * 100.0 / total) nps
            FROM gracia10.nps
           WHERE DATE(created_at) = DATE('{execution_date}')
        GROUP BY 1;"""

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
    dag_id="Build_Summary_v2",
    start_date=datetime(2022, 10, 20),
    schedule_interval='@once',
    catchup=False
)

execsql = PythonOperator(
    task_id='execsql',
    python_callable=execsql,
    params={
        'schema': 'gracia10',
        'table': 'nps_summary'
    },
    provide_context=True,  # True 설정시 argument set(Jinja 변수 등)를 추가함
    dag=dag
)
