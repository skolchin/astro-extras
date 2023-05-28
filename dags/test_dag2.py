# Astro SDK Extras project
# (c) kol, 2023

import pendulum
from airflow.models import DAG
from astro.sql.table import Table, Metadata
from astro_extras import open_session, close_session, transfer_table, ETLSession

with DAG(
    dag_id='test2',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule=None,
    catchup=False,
) as dag, ETLSession('source_db', 'target_db') as session:

    @dag.task
    def print_session(session: ETLSession):
        print(session)

    # session = open_session('source_db', 'target_db')
    print_session(session)
    # close_session(session)
