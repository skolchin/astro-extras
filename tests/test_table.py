# Astro SDK Extras project
# (c) kol, 2023

""" Table unit tests """

import pytest
import sqlalchemy
import pandas as pd
from itertools import zip_longest
from confsupport import run_dag, logger
from astro.sql.table import Table
from sqlalchemy import MetaData, Table as sqlaTable, select, func, inspect

try:
    from astro_extras import declare_tables
except ModuleNotFoundError:
    declare_tables = None

IGNORE_ATTR = set(['session_id', '_deleted', '_modified'])

def compare_table_contents(source_table: sqlalchemy.Table, destination_table: sqlalchemy.Table) -> bool:
    """
    Compare contents of two tables using SQLAlchemy.

    Args:
        source_table (sqlalchemy.Table): Source table object in SQLAlchemy.
        destination_table (sqlalchemy.Table): Destination table object in SQLAlchemy.

    Returns:
        bool: A boolean value indicating whether contents match or not.
    """

    # Get data from source and destination tables.
    with source_table.bind.connect() as src_conn, destination_table.bind.connect() as dest_conn:
        source_data = src_conn.execute(source_table.select())
        dest_data = dest_conn.execute(destination_table.select())

    # Check if data is identical ignoring technical fields
    for (source_row, dest_row) in zip_longest(source_data, dest_data):
        if source_row is None or dest_row is None:
            return False
        
        source_attr = source_row._asdict()
        dest_attr = dest_row._asdict()
        for key in source_attr:
            if key not in IGNORE_ATTR:
                src_val = source_attr[key]
                dst_val = dest_attr[key]
                if src_val != dst_val:
                    return False

    return True

def get_table_row_count(table: sqlalchemy.Table) -> int:
    """
    Returns the number of rows in the given SQLalchemy Table object.

    Args:
        table (sqlalchemy.Table): SQLalchemy Table object to count rows for

    Returns:
        int: The number of rows in the table
    """

    # Connect to the database engine using the table's bind
    with table.bind.connect() as conn:
        # Construct a SQLalchemy select statement that counts the number of rows in the table
        count_query = select([func.count()]).select_from(table)

        # Execute the query and retrieve the count from the result
        row_count = conn.scalar(count_query)

    return row_count

def compare_table_schemas(source_table: sqlalchemy.Table, destination_table: sqlalchemy.Table) -> bool:
    """
    Compare schema of two tables using SQLAlchemy.

    Args:
        source_table (sqlalchemy.Table): Source table object in SQLAlchemy.
        destination_table (sqlalchemy.Table): Destination table object in SQLAlchemy.

    Returns:
        bool: A boolean value indicating whether schemas match or not.
    """

    # Get column names for source and destination tables.
    # Destination might contain some technical fields ('session_id') which are ignored
    source_columns = set([column.name for column in source_table.columns]) - IGNORE_ATTR
    dest_columns = set([column.name for column in destination_table.columns]) - IGNORE_ATTR

    # Check if columns match
    if source_columns != dest_columns:
        return False

    # Get index names for source and destination tables.
    source_indexes = [index.name for index in inspect(source_table).indexes]
    dest_indexes = [index.name for index in inspect(destination_table).indexes]

    # Check if indexes match.
    if sorted(source_indexes) != sorted(dest_indexes):
        return False

    return True

def assert_tables_equal(source_db, target_db, lst_tables, use_actuals_view: bool = False):
    # Create connections to the source and target databases.
    with source_db.connect() as src_conn,\
         target_db.connect() as tgt_conn:

        # Get metadata of tables in source and target databases.
        # Reflect on the metadata of source and target databases to get their table schema.
        meta_src = MetaData(src_conn)
        meta_src.reflect()
        meta_tgt = MetaData(tgt_conn)
        meta_tgt.reflect()

        # Check if required tables exist in source and target databases.
        for table in lst_tables:
            assert table in meta_src.tables
            assert table in meta_tgt.tables

        # Get the tables to be compared in source and target databases.
        src_tables = [sqlaTable(table_name, meta_src) for table_name in lst_tables]
        tgt_tables = [sqlaTable(table_name, meta_tgt) for table_name in lst_tables]

        # Compare schemas
        for src_table, tgt_table in zip(src_tables, tgt_tables):
            assert compare_table_schemas(src_table, tgt_table)

        # Now compare the content using actuals view if requested
        src_tables = [sqlaTable(table_name, meta_src) for table_name in lst_tables]
        tgt_tables = [sqlaTable(f'{table_name}{"_a" if use_actuals_view else ""}', meta_tgt) for table_name in lst_tables]

        # Compare the tables row count and schemas
        for src_table, tgt_table in zip(src_tables, tgt_tables):
            assert compare_table_contents(src_table, tgt_table)

def test_table_load_save(source_db, target_db, docker_ip, docker_services, airflow_credentials):
    """ Test for `load_table` and `save_table` functions """
    logger.info(f'Testing table load and save')
    result = run_dag('test-table-load_save', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

    with target_db.begin() as conn:
        data = pd.read_sql_table('tmp_test_table_1', conn)
        assert data.shape[0] == 3
        assert 'some_column' in data.columns

def test_table_save_fail(docker_ip, docker_services, airflow_credentials):
    """ Test that `save_table()` function fails if no target table exists. """
    logger.info(f'Testing table save to non-existing table')
    result = run_dag('test-table-save_fail', docker_ip, docker_services, airflow_credentials)
    assert result == 'failed'

def test_table_declare_tables():
    if declare_tables is None:
        pytest.skip('Astro-extras not installed')

    # Test function to declare tables using the given table names and source database name.
    logger.info(f'Testing declare_tables function')

    # Declare tables.
    res = declare_tables(['table_name_1', 'table_name_2'], 'source_db')

    # Check if the return type is a list.
    assert isinstance(res, list)

    # Check if each item in the list is an instance of the sqlalchemy.Table class.
    for i in res:
        assert isinstance(i, Table)

def test_transfer_tables(docker_ip, docker_services, airflow_credentials, source_db, target_db):
    """ Test for transfer_table() / transfer_tables() functions. """

    logger.info(f'Testing transfer_tables function')
    result = run_dag('test-transfer-tables', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'


def test_transfer_tables_session(docker_ip, docker_services, airflow_credentials, source_db, target_db):
    """ Test of transfer_tables() operator under ETL session """

    logger.info(f'Testing transfer_tables() function with ETL session provided')
    result = run_dag('test-transfer-tables-session', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

def test_transfer_changed_tables(docker_ip, docker_services, airflow_credentials, source_db, target_db):
    """ Test of transfer_changed_tables() operator """

    logger.info(f'Testing transfer_changed_tables() function')
    result = run_dag('test-transfer-tables-session', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

    result = run_dag('test-transfer-changed-tables', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'


def test_transfer_ods_tables(docker_ip, docker_services, airflow_credentials, source_db, target_db):
    """ Test of transfer_changed_tables() operator """

    logger.info(f'Testing transfer_ods_tables() function')

    result = run_dag('test-transfer-ods-tables-1', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'

    result = run_dag('test-transfer-ods-tables-2', docker_ip, docker_services, airflow_credentials)
    assert result == 'success'
