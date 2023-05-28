# Astro SDK Extras project
# (c) kol, 2023

""" Test ETL session DAG """

import pendulum
from airflow.models import DAG
from astro_extras import open_session, close_session

with DAG(
    dag_id='test-session',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    session = open_session('source_db', 'target_db', dag=dag)
    close_session(session)
