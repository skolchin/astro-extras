# Astro SDK Extras project
# (c) kol, 2023

""" DAGs for lineage testing based on https://github.com/MarquezProject/marquez/tree/main/examples/airflow """

import random
import pendulum

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id='counter',
    start_date=pendulum.today().add(days=-1),
    schedule='@daily',
    catchup=False,
    tags=['demo', 'lineage'],
) as dag:
    PostgresOperator(
        task_id='make_table',
        postgres_conn_id='source_db',
        sql='''
        CREATE TABLE IF NOT EXISTS counts (
        value INTEGER
        );''',
    ) >> \
    PostgresOperator(
        task_id='inc',
        postgres_conn_id='source_db',
        sql='''
        INSERT INTO counts (value)
            VALUES (%(value)s)
        ''',
        parameters={
            'value': random.randint(1, 10)
        },
    )
