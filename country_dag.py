import json
import logging
from datetime import datetime

import psycopg2
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks import postgres


def get_redshift_curr(autocommit=True):
    hook = postgres.PostgresHook(postgres_conn_id="redshift_dev_db")
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def extract(**context):
    logging.info("extract Start Shooooooong")
    req = requests.get(context["params"]["url"])
    logging.info("extract end Shooooooong")
    logging.info(type(req.text))
    return req.text


def transform(**context):
    logging.info("transform start")

    # 문자열
    data = context["task_instance"].xcom_pull(
        key="return_value", task_ids="country_extract"
    )
    logging.info(type(data))
    trans_data = json.loads(data)
    logging.info(type(trans_data))
    # trans_data = json.loads(data)
    records = []
    for i in trans_data:
        name = i["name"]["official"]
        population = i["population"]
        area = i["area"]
        records.append([name, population, area])
    logging.info("transform end")
    return records


def load(**context):
    logging.info("load start")
    curr = get_redshift_curr()
    schema = Variable.get("soomers_schema")
    records = context["task_instance"].xcom_pull(
        key="return_value", task_ids="country_transform"
    )
    try:
        curr.execute(f"DROP TABLE IF EXISTS {schema}.country_info;")
        curr.execute(
            f"""CREATE TABLE {schema}.country_info (
            name VARCHAR(80),
            population INTEGER,
            area FLOAT);"""
        )
        for i in records:
            curr.execute(
                f"INSERT INTO {schema}.country_info VALUES ('{i[0]}','{i[1]}','{i[2]}'"
            )
        curr.execute("COMMIT;")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        curr.execute("ROLLBACK;")
    logging.info("load end")


dag = DAG(
    dag_id="countries_dags",
    start_date=datetime(2023, 6, 7),
    catchup=False,
    schedule=" 30 6 * * *",  # 0이 일요일
    tags=["Countries"],
)

extract = PythonOperator(
    task_id="country_extract",
    python_callable=extract,
    params={"url": Variable.get("country_url")},
    dag=dag,
)

transform = PythonOperator(
    task_id="country_transform", python_callable=transform, dag=dag
)

load = PythonOperator(task_id="country_load", python_callable=load, dag=dag)

extract >> transform >> load
