# Astro SDK Extras project
# (c) kol, 2023

import pendulum
import pandas as pd

from typing import List
from airflow.models import DAG
from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro_extras import load_table, save_table, transfer_table, transfer_tables, TestData

with DAG(
    dag_id='test-table-load_save',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:

    input_table = Table('dict_types', conn_id='source_db',
                        metadata=Metadata(schema='public'))
    output_table = Table('tmp_dict_types', conn_id='target_db',
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

    data = load_table('public.dict_types', 'source_db')
    save_table(data, 'public.tmp_dict_types2', conn_id='target_db')

with DAG(
    dag_id='test-transfer-table',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    @dag.task
    def create_source_tables_with_data(**context):
        test_data = TestData('source_db')
        tables: List[str] = test_data.generate_tables_and_uppend_to_db(
            20, 50, name_tables=['test_table'])

        serialized_tables = test_data.serialize_tables(tables)

        context['ti'].xcom_push(key='data', value=serialized_tables)

    @dag.task
    def create_target_tables(**context):
        test_data = TestData('target_db')

        serialized_tables = context['ti'].xcom_pull(
            key='data', task_ids='create_source_tables_with_data')

        deserialized_tables = test_data.deserialize_tables(serialized_tables)

        deserialized_tables = [test_data.create_sqla_table_object(
            table['name'], test_data.metadata, table['columns']) for table in deserialized_tables]

        test_data.create_tables(deserialized_tables)

    create_source_tables_with_data() >>\
        create_target_tables() >>\
        transfer_table(source='test_table', target='test_table', source_conn_id='source_db',
                       destination_conn_id='target_db')


with DAG(
    dag_id='test-transfer-tables',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    @dag.task
    def create_source_tables_with_data(**context):
        test_data = TestData('source_db')
        tables: List[str] = test_data.generate_tables_and_uppend_to_db(
            20, 50, name_tables=['test_tables_1', 'test_tables_2', 'test_tables_3'])

        serialized_tables = test_data.serialize_tables(tables)

        context['ti'].xcom_push(key='data', value=serialized_tables)

    @dag.task
    def create_target_tables(**context):
        test_data = TestData('target_db')

        serialized_tables = context['ti'].xcom_pull(
            key='data', task_ids='create_source_tables_with_data')

        deserialized_tables = test_data.deserialize_tables(serialized_tables)

        deserialized_tables = [test_data.create_sqla_table_object(
            table['name'], test_data.metadata, table['columns']) for table in deserialized_tables]

        test_data.create_tables(deserialized_tables)

    create_source_tables_with_data() >>\
        create_target_tables() >>\
        transfer_tables(source_tables=['test_tables_1', 'test_tables_2', 'test_tables_3'],
                        source_conn_id='source_db', destination_conn_id='target_db')
