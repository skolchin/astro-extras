# Astro SDK Extras project
# (c) kol, 2023

import pendulum
import pandas as pd

from airflow.models import DAG
from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro_extras import *
from astro_extras.tests.test_data import TestData

with DAG(
    dag_id='test-table-load_save',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:

    input_table = Table('types', conn_id='source_db',
                        metadata=Metadata(schema='public'))
    output_table = Table('tmp_types', conn_id='target_db',
                         metadata=Metadata(schema='public'))

    @aql.dataframe
    def modify_data(data: pd.DataFrame):
        data['some_column'] = 'new_value'
        return data

    data = load_table(input_table)
    modified_data = modify_data(data)
    save_table(modified_data, output_table, fail_if_not_exist=False)

with DAG(
    dag_id='test-table-save_fail',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:

    data = load_table('public.types', 'source_db')
    save_table(data, 'public.tmp_types2', conn_id='target_db')

with DAG(
    dag_id='test-transfer-tables',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, \
    TestData('source_db', dest_conn_id='target_db', num_cols=20, num_rows=50, 
             name_tables=['test_table', 'test_tables_1', 'test_tables_2', 'test_tables_3']) as data:
    
    @dag.task
    def create_tables(**context):
        # Create source table with data and target table without data
        data.append_tables_to_db()

    create_tables() >> \
    transfer_table(source='test_table', target='test_table', source_conn_id='source_db', destination_conn_id='target_db') >> \
    transfer_tables(source_tables=['test_tables_1', 'test_tables_2', 'test_tables_3'], source_conn_id='source_db', destination_conn_id='target_db')

with DAG(
    dag_id='test-transfer-tables-session',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, \
    ETLSession('source_db', 'target_db') as session, \
    TestData('source_db', dest_conn_id='target_db', num_cols=20, num_rows=50, name_tables=['test_table'], with_session_id=True) as data:
    
    @dag.task
    def create_tables(**context):
        # Create source table with data and target table without data
        data.append_tables_to_db()

    create_tables() >> \
    transfer_table(source='test_table', target='test_table', source_conn_id='source_db', destination_conn_id='target_db', session=session)

with DAG(
    dag_id='test-transfer-changed-tables',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, \
    ETLSession('source_db', 'target_db') as session, \
    TestData('source_db', 'target_db', num_cols=20, num_rows=50, name_tables=['test_table'], with_session_id=True) as data:
    
    @dag.task
    def modify_data(**context):
        # Update only source data (it must be created in test-transfer-tables DAG)
        data.renew_tables_in_source_db()

    modify_data() >> \
    transfer_changed_table('test_table', 'test_table', 'source_db', destination_conn_id='target_db', session=session, 
                           task_id='transfer-changed-test_table')
