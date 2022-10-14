from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta
import requests
import logging
import psycopg2


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.set_session(autocommit=True)
    return conn


def extract(**context):
    logging.info("Extract started")
    logging.info(context['task_instance'])

    link = context["params"]["url"]
    f = requests.get(link)
    logging.info("Extract done")
    return (f.text)


def transform(**context):
    logging.info("transform started")
    logging.info(context['task_instance'])

    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    logging.info("transform done")
    return lines


def load(**context):
    logging.info("load started")
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")

    conn = get_Redshift_connection()
    cur = conn.cursor()
    sql = f"BEGIN;DELETE FROM {schema}.{table};"
    for l in lines:
        if l != '':
            (name, gender) = l.split(",")
            sql += f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
    sql += "END;"
    logging.info(sql)
    try:
        cur.execute(sql)
    except (Exception, psycopg2.DatabaseError) as err:
        logging.warning(err)
        cur.execute("ROLLBACK;")
    finally:
        logging.info("load done")
        conn.close()


dag_second_assignment = DAG(
    dag_id='second_assignment_v4',
    start_date=datetime(2022, 10, 10),
    schedule_interval='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    })

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'url': Variable.get("csv_url")
    },
    dag=dag_second_assignment)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag_second_assignment)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'schema': 'gracia10',
        'table': 'name_gender'
    },
    dag=dag_second_assignment)

extract >> transform >> load
