# Astro SDK Extras project
# (c) kol, 2023

import pendulum
from airflow.models import DAG
from astro.sql.table import Table, Metadata

from astro_extras import ETLSession, transfer_table, declare_tables, transfer_tables

with DAG(
    dag_id='transfer_table',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'table']
) as dag, ETLSession('source_db', 'target_db') as session:

    input_table = Table('dict_types', conn_id='source_db', metadata=Metadata(schema='public'))
    output_table = Table('dict_types', conn_id='target_db', metadata=Metadata(schema='stage'))

    transfer_table(input_table, output_table, session=session)

with DAG(
    dag_id='transfer_tables',
    start_date=pendulum.today('Europe/Moscow').add(days=-1),
    schedule=None,
    catchup=False,
    tags=['test', 'table']
) as dag, ETLSession('source_db', 'target_db') as session:

    input_tables = declare_tables(['dict_types', 'table_data'], 'source_db', 'public')
    output_tables = declare_tables(['dict_types', 'table_data'], 'target_db', 'stage')

    transfer_tables(input_tables, output_tables, session=session)

