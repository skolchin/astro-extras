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
    """ Test for load_table() / save_table() functions.

        The DAG loads `test_table_1` from source db data into memory, 
        adds a new column and then saves the data to 
        new `tmp_test_table_1` in target db.
    """

    input_table = Table('test_table_1', conn_id='source_db',
                        metadata=Metadata(schema='public'))
    output_table = Table('tmp_test_table_1', conn_id='target_db',
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
    """ Test that `save_table()` function fails if no target table exists.

    By default, Astro-SDK creates a table upon saving, if needed, but
    this is often undesired. Therefore, Astro-Extras `save_table()` version
    checks whether target table exists and fails if not unless 
    `fail_if_not_exist` option is set to `False`.
    """

    data = load_table('public.test_table_1', 'source_db')
    save_table(data, 'public.tmp_test_table_11', conn_id='target_db')

with DAG(
    dag_id='test-transfer-tables',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    """ Test for transfer_table() / transfer_tables() functions.

        Transfers test tables 1,2,3 from source to target database without ETL session wrapping. 
        Table 2 transfer uses custom data selection template limiting number of records (11)
        to be transferred (see `templates\test-transfer-tables\test_table_2.sql`).
    """

    transfer_table('test_table_1', 'test_table_1', source_conn_id='source_db', destination_conn_id='target_db') >> \
    transfer_tables(['test_table_2', 'test_table_3'], source_conn_id='source_db', destination_conn_id='target_db')

with DAG(
    dag_id='test-transfer-tables-session',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, ETLSession('source_db', 'target_db') as session:
    """ Test for transfer_table() / transfer_tables() functions.

        Transfers test table 4,5,6 from source to target database under ETL session. 
        Table 5 transfer uses custom data selection template limiting number of records (11)
        to be transferred (see `templates\test-transfer-tables-session\test_table_5.sql`).
    """
    
    transfer_table('test_table_4', 'test_table_4', 'source_db', 'target_db', session=session) >> \
    transfer_tables(['test_table_5', 'test_table_6'], source_conn_id='source_db', destination_conn_id='target_db', session=session)

with DAG(
    dag_id='test-transfer-changed-tables',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, ETLSession('source_db', 'target_db') as session:
    """ Test for `transfer_changed_table()` function.

        Assuming that initial transfer has already been performed, 
        modifies data in `test_table_4` table and trigger the transfer. 
        Table is transferred in dictionary mode (copied as a whole upon data change), 
        change timestamps are ignored.
    """
    
    input_table = Table('test_table_4', conn_id='source_db', metadata=Metadata(schema='public'))
    output_table = Table('test_table_4', conn_id='target_db', metadata=Metadata(schema='public'))

    @aql.run_raw_sql
    def modify_data(table: Table):
        return """
            update {{table}}
            set test1 = q.test1
            from (select id, md5(random()::text) as test1 from generate_series(10, 20) id) q
            where {{table}}.id = q.id
        """

    modify_data(input_table) >> \
    transfer_changed_table(input_table, output_table, 'source_db', 'target_db', session=session)

with DAG(
    dag_id='test-transfer-ods-tables-1',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, ETLSession('source_db', 'target_db') as session:
    transfer_ods_table('test_table_7', 'test_table_7', 'source_db', 'target_db', session=session)

with DAG(
    dag_id='test-transfer-ods-tables-2',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag, ETLSession('source_db', 'target_db') as session:
    
    input_table = Table('test_table_7', conn_id='source_db', metadata=Metadata(schema='public'))
    output_table = Table('test_table_7', conn_id='target_db', metadata=Metadata(schema='public'))

    @aql.run_raw_sql
    def modify_data(table: Table):
        return """
            update {{table}}
            set test1 = q.test1
            from (select id, md5(random()::text) as test1 from generate_series(10, 20) id) q
            where {{table}}.id = q.id
        """

    @aql.run_raw_sql
    def remove_data(table: Table):
        return """
            delete from {{table}}
            where id in (select id from generate_series(30, 40) id)
        """

    modify_data(input_table) >> \
    remove_data(input_table) >> \
    transfer_ods_table(input_table, output_table, 'source_db', 'target_db', session=session)
