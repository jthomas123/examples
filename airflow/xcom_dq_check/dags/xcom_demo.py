import json
from datetime import datetime, timedelta
from glob import glob

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.check_operator import CheckOperator
from airflow.operators.python_operator import PythonOperator

from utils.operators import SQLTemplatedPythonOperator


def read_and_insert(**kwargs):
    filenames = glob("dags/source_files/*.json")
    rows_to_insert = []
    ti = kwargs['ti']

    table_name = ti.xcom_pull(task_ids='ddl', key='table_name')

    for fn in filenames:
        with open(fn) as f:
            lines = f.readlines()
            for line in lines:
                line_dict = json.loads(line)
                rows_to_insert.append(line_dict.values())

    pg_hook = PostgresHook('postgres_default')
    pg_hook.insert_rows(table_name, rows_to_insert)
    rowcount = int(pg_hook.get_first("SELECT COUNT(1) FROM {table_name}".format(table_name=table_name))[0])
    kwargs['ti'].xcom_push(key='rowcount', value=rowcount)


def run_and_push(**kwargs):
    conn = PostgresHook('postgres_default').get_conn()
    PostgresHook.set_autocommit(PostgresHook(), conn, True)
    cur = conn.cursor()
    cur.execute(kwargs['templates_dict']['script'])
    result = cur.fetchall()
    row = result[0]
    kwargs['ti'].xcom_push(key=row[0], value=row[1])


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("xcom_demo", default_args=default_args, schedule_interval=None)

ddl_task = SQLTemplatedPythonOperator(
    task_id='ddl',
    python_callable=run_and_push,
    templates_dict={"script": "templates/ddl.sql.jinja2"},
    provide_context=True,
    dag=dag
)

read_and_insert_task = PythonOperator(
    task_id='read_and_insert',
    python_callable=read_and_insert,
    provide_context=True,
    dag=dag
)

with open("dags/templates/dq_check.sql.jinja2") as f:
    check_sql = f.read()

dq_check_task = CheckOperator(
    task_id="dq_check",
    sql=check_sql,
    conn_id='postgres_default',
    provide_context=True,
    dag=dag
)

ddl_task >> read_and_insert_task >> dq_check_task
