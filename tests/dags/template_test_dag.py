# Astro SDK Extras project
# (c) kol, 2023

import pendulum
from airflow.models import DAG
from astro_extras import run_sql_template, run_sql_templates, ETLSession

with DAG(
    dag_id='test-run-template-list',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'template'],
) as dag:
    run_sql_templates(['test_1', 'test_2'], 'source_db')

with DAG(
    dag_id='test-run-template-session',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'template', 'session'],
) as dag, ETLSession('source_db','target_db'):
    run_sql_template('test', 'source_db')
