# Astro SDK Extras project
# (c) kol, 2023

""" Table unit tests """


from datetime import datetime, timedelta

from sqlalchemy import Table
from confsupport import logger

from utils import (
    update_mod_ts_tables,
    get_table_row_count,
    create_tables_tuple
)

from asserts import (
    assert_dag_run,
    assert_table_columns_existence,
    assert_tables_existence,
    assert_views_existence,
    assert_tables_equality,
    assert_compare_table_schemas,
    assert_compare_tables_contents,
    assert_table_row_count,
    assert_tables_data_in_target
)


# Table attributes that will be ignored during tests
IGNORE_ATTR = set(['session_id', '_deleted', '_modified'])
# IDs of records that will be modified for testing
MOD_IDS = {1, 2, 3}
# Get the current date and time
DT_NOW = datetime.now()
# Calculate the date 10 days before today
DATE_10_DAYS_BEFORE = (DT_NOW.date() - timedelta(days=10))
# Dictionary containing modification dates for testing purposes
MOD_DATES = {
    # Target date and time 11 days from now
    'target': (DT_NOW + timedelta(days=11)),
    # Start date 5 days from today, formatted as "YYYY-MM-DD"
    'p_start': (DT_NOW.date() + timedelta(days=5)).strftime("%Y-%m-%d"),
    # End date 15 days from today, formatted as "YYYY-MM-DD"
    'p_end': (DT_NOW.date() + timedelta(days=15)).strftime("%Y-%m-%d")
}


def test_table_load_save(
        target_db,
        docker_ip,
        docker_services,
        airflow_credentials,
        tgt_metadata
    ):
    """ 
    Test for `load_table` and `save_table` functions.

    This test verifies the correct functionality of the `load_table` and `save_table` functions.

    Args:
    -----
        target_db: SQLAlchemy engine for the target database.
        docker_ip: IP address of the Docker host.
        docker_services: Docker services for managing containers.
        airflow_credentials: Credentials for Airflow in the form (username, password).
        tgt_metadata: SQLAlchemy MetaData object for the target database.

    Raises:
    -------
        AssertionError: If any of the assertions fail.
    """
    logger.info(f'Testing table load and save')
    table_name = 'tmp_test_table_1'
    # Ensure the DAG run named 'test-table-load_save' completes successfully
    assert_dag_run('test-table-load_save', docker_ip, docker_services, airflow_credentials)
    # Check if the temporary test table 'tmp_test_table_1' exists in the target database
    assert_tables_existence(target_db, [table_name])
    # Verify that 'tmp_test_table_1' has exactly 100 rows
    assert_table_row_count(target_db, Table(table_name, tgt_metadata, autoload_with=target_db), 100)
    # Check if the column 'some_column' exists in 'tmp_test_table_1'
    assert_table_columns_existence(target_db, table_name, ['some_column'])


def test_table_save_fail(
        docker_ip,
        docker_services,
        airflow_credentials,
        target_db
    ):
    """ 
    Test that `save_table()` function fails if no target table exists.

    This test verifies that the `save_table()` function correctly fails when attempting to save to a non-existing table.

    Args:
    -----
        docker_ip: IP address of the Docker host.
        docker_services: Docker services for managing containers.
        airflow_credentials: Credentials for Airflow in the form (username, password).
        target_db: SQLAlchemy engine for the target database.

    Raises:
    -------
        AssertionError: If any of the assertions fail.
    """
    logger.info(f'Testing table save to non-existing table')
    # Verify that the DAG run failed
    assert_dag_run('test-table-save_fail', docker_ip, docker_services, airflow_credentials, success=False)
    # Check that the non-existing table does not exist in the target database
    assert_tables_existence(target_db, ['tmp_test_table_11'], success=False)


def test_transfer_tables(
        docker_ip,
        docker_services,
        airflow_credentials,
        source_db,
        target_db,
        src_metadata,
        tgt_metadata
    ):
    """
    Test function for transfer_table() / transfer_tables() functions.

    This test verifies the correct functioning of the `transfer_table()` and `transfer_tables()` functions.
    For `test_table_2`, a template is used to select 11 rows, while for other tables, 
    records are completely reloaded. The session is not in use.

    Args:
    -----
        docker_ip (str): IP address of the Docker host.
        docker_services (pytest_docker.plugin.Services): Docker services for managing containers.
        airflow_credentials (tuple): Credentials for Airflow in the form (username, password).
        source_db (Engine): SQLAlchemy engine for the source database.
        target_db (Engine): SQLAlchemy engine for the target database.
        src_metadata (MetaData): SQLAlchemy MetaData object for the source database.
        tgt_metadata (MetaData): SQLAlchemy MetaData object for the target database.

    Returns:
    --------
        None

    Raises:
    -------
        AssertionError: If any of the assertions fail.
    """
    logger.info(f'Testing transfer_tables function')
    
    tables_name = ['test_table_1', 'test_table_2', 'test_table_3']
    
    # Check the existence of the table in the target database
    assert_tables_existence(target_db, tables_name)
    
    # Create SQLAlchemy Table objects (source)
    test_table_1_source, test_table_2_source, test_table_3_source = create_tables_tuple(
        source_db,
        tables_name,
        src_metadata
    )

    # Create SQLAlchemy Table objects (target)
    test_table_1_target, test_table_2_target, test_table_3_target = create_tables_tuple(
        target_db,
        tables_name,
        tgt_metadata
    )

    # Verify the successful execution of the DAG
    assert_dag_run('test-transfer-tables', docker_ip, docker_services, airflow_credentials)

    # Compare table schemas between the source and target databases
    assert_compare_table_schemas(test_table_2_source, test_table_2_target)

    # Verify the row count in the target table
    assert_table_row_count(target_db, test_table_2_target, 11)

    # Compare data in tables between the source and target databases, ignoring certain columns
    assert_tables_equality(
        source_db,
        target_db,
        [(test_table_1_source, test_table_1_target),
         (test_table_3_source, test_table_3_target)],
        ignore_cols=IGNORE_ATTR)


def test_transfer_tables_session(
        docker_ip,
        docker_services,
        airflow_credentials,
        source_db,
        target_db,
        src_metadata,
        tgt_metadata
    ):
    """
    Test function for the transfer_tables() operator within an ETL session.

    This test validates the transfer of data between tables in the source and target databases 
    and checks for data integrity, schema consistency, and row count accuracy. The test runs 
    the DAG twice: first to perform an initial load for a given period, and second to load 
    the updated delta for a new period. For `test_table_5`, a template is used to select a 
    specific number of records by identifiers within the period, while for other tables, 
    data selection is done for the entire period.

    Args:
    -----
        docker_ip (str): IP address of the Docker host.
        docker_services (pytest_docker.plugin.Services): Docker services for managing containers.
        airflow_credentials (tuple): Credentials for Airflow in the form (username, password).
        source_db (Engine): SQLAlchemy engine for the source database.
        target_db (Engine): SQLAlchemy engine for the target database.
        src_metadata (MetaData): SQLAlchemy MetaData object for the source database.
        tgt_metadata (MetaData): SQLAlchemy MetaData object for the target database.

    Returns:
    --------
        None

    Raises:
    -------
        AssertionError: If any of the assertions fail.
    """
    logger.info(f'Testing transfer_tables_session function under ETL session')
    
    tables_name = ['test_table_4', 'test_table_5', 'test_table_6']
    
    # Check the existence of the tables in the target database
    assert_tables_existence(target_db, tables_name)
    
    # Create SQLAlchemy Table objects (source and target)
    test_table_4_source, test_table_5_source, test_table_6_source = create_tables_tuple(
        source_db,
        tables_name,
        src_metadata
    )
    
    test_table_4_target, test_table_5_target, test_table_6_target = create_tables_tuple(
        target_db,
        tables_name,
        tgt_metadata
    )

    assert_views_existence(target_db, [f"{test_table_4_target.name}_a"])
    test_table_4_actuals = Table(f"{test_table_4_target.name}_a", tgt_metadata, autoload_with=target_db)
    
    for table in tables_name:
        assert_table_columns_existence(target_db, table, ['session_id'])
    
    # Initial load: Run the DAG and assert success
    assert_dag_run(
        'test-transfer-tables-session',
        docker_ip,
        docker_services,
        airflow_credentials,
        params = f'{{"period": "[{DATE_10_DAYS_BEFORE}, {DT_NOW.date() + timedelta(days=1)}]"}}'
    )

    # Assert that the data in 'test_table_4' and 'test_table_6' in the source and target databases are equal
    # Assert that the data in 'test_table_4' using the actuals view is equal
    assert_tables_equality(
        source_db,
        target_db,
        [(test_table_4_source, test_table_4_target),
         (test_table_6_source, test_table_6_target),
         (test_table_4_source, test_table_4_actuals)],
        ignore_cols=IGNORE_ATTR
    )
    
    # Assert that the schemas of 'test_table_5' in the source and target databases are equal
    assert_compare_table_schemas(test_table_5_source, test_table_5_target, ignore_cols=IGNORE_ATTR)
    
    # Assert the row count of 'test_table_5' in the target database
    assert_table_row_count(target_db, test_table_5_target, 11)

    # Update the 'mod_ts' field for specific tables and rows in the source database
    update_mod_ts_tables(
        source_db,
        {test_table_4_source: MOD_IDS, test_table_6_source: MOD_IDS},
        MOD_DATES['target']
    )

    # Delta load: Re-run the DAG and assert success
    assert_dag_run(
        'test-transfer-tables-session',
        docker_ip,
        docker_services,
        airflow_credentials,
        params = f'{{"period": "[{MOD_DATES["p_start"]}, {MOD_DATES["p_end"]}]"}}'
    )
    
    # Assert that the data in 'test_table_4' using the actuals view is equal after updates
    assert_compare_tables_contents(
        source_db,
        target_db,
        [(test_table_4_source, test_table_4_actuals)],
        ignore_cols=IGNORE_ATTR
    )
    
    # Assert that the data in 'test_table_4', 'test_table_5', 'test_table_6' in the source and target databases are equal
    assert_compare_tables_contents(
        source_db,
        target_db,
        [
            (test_table_4_source, test_table_4_target),
            (test_table_5_source, test_table_5_target),
            (test_table_6_source, test_table_6_target)
        ],
        ignore_cols=IGNORE_ATTR,
        success=False
    )
    
    # Assert the row count of 'test_table_5' in the target database after updates
    assert_table_row_count(target_db, test_table_5_target, 11)
    
    # Assert that the data in specific tables in the source database are present in the target database
    assert_tables_data_in_target(
        source_db,
        target_db,
        [
            (test_table_4_source, test_table_4_target),
            (test_table_6_source, test_table_6_target)
        ],
        ignore_cols=IGNORE_ATTR
    )


def test_transfer_changed_tables(
        docker_ip,
        docker_services,
        airflow_credentials,
        source_db,
        target_db,
        src_metadata,
        tgt_metadata
    ):
    """
    Test function for the transfer_changed_tables() operator.

    This test verifies the correct functioning of the `transfer_changed_tables()` function,
    ensuring that only changed records are transferred. The test involves running the DAG
    to perform an initial load, updating specific records, and running the DAG again to
    transfer only the changed records.

    Args:
    -----
        docker_ip (str): IP address of the Docker host.
        docker_services (pytest_docker.plugin.Services): Docker services for managing containers.
        airflow_credentials (tuple): Credentials for Airflow in the form (username, password).
        source_db (Engine): SQLAlchemy engine for the source database.
        target_db (Engine): SQLAlchemy engine for the target database.
        src_metadata (MetaData): SQLAlchemy MetaData object for the source database.
        tgt_metadata (MetaData): SQLAlchemy MetaData object for the target database.

    Returns:
    --------
        None

    Raises:
    -------
        AssertionError: If any of the assertions fail.
    """
    logger.info(f'Testing transfer_changed_tables() function')

    table_name = ['test_table_7']

    # Assert the existence of 'test_table_7' in the target database
    assert_tables_existence(target_db, table_name)

    # Create SQLAlchemy Table objects
    test_table_7_source, = create_tables_tuple(source_db, table_name, src_metadata)
    test_table_7_target, = create_tables_tuple(target_db, table_name, tgt_metadata)

    # Check the existence of the views in the target database
    assert_views_existence(target_db, [f"{test_table_7_target.name}_a"])

    # Create actuals view table object for 'test_table_7'
    test_table_7_actuals = Table(f"{test_table_7_target.name}_a", tgt_metadata, autoload_with=target_db)

    # Initial load: Run the DAG and assert success
    assert_dag_run(
        'test-transfer-changed-tables',
        docker_ip,
        docker_services,
        airflow_credentials,
        params=f'{{"period": "[{DATE_10_DAYS_BEFORE}, {DT_NOW.date() + timedelta(days=1)}]"}}'
    )

    # Assert that the data in 'test_table_7' in the source and target databases
    assert_tables_equality(
        source_db,
        target_db,
        [(test_table_7_source, test_table_7_actuals),
         (test_table_7_source, test_table_7_target)],
        ignore_cols=IGNORE_ATTR
    )

    # Update the 'mod_ts' field for specific rows in 'test_table_7' in the source database
    update_mod_ts_tables(
        source_db,
        {test_table_7_source: MOD_IDS},
        datetime.now() + timedelta(days=4)
    )

    # Run the DAG to transfer changed records and assert success
    assert_dag_run(
        'test-transfer-changed-tables',
        docker_ip,
        docker_services,
        airflow_credentials
    )

    # Assert that the data in 'test_table_7' using the actuals view is equal after updates
    assert_compare_tables_contents(
        source_db,
        target_db,
        [(test_table_7_source, test_table_7_actuals)],
        ignore_cols=IGNORE_ATTR
    )

    # Assert that all data from the source table 'test_table_7' is in the destination table
    assert_tables_data_in_target(
        source_db,
        target_db,
        [(test_table_7_source, test_table_7_target)],
        ignore_cols=IGNORE_ATTR
    )

    # Capture the current row count of 'test_table_7' in the target database
    row_count = get_table_row_count(target_db, test_table_7_target)

    # Run the DAG again to ensure no additional records are transferred and assert success
    assert_dag_run(
        'test-transfer-changed-tables',
        docker_ip,
        docker_services,
        airflow_credentials
    )

    # Assert that the row count of 'test_table_7' in the target database has not changed
    assert_table_row_count(target_db, test_table_7_target, row_count)


def test_transfer_ods_tables(
        docker_ip,
        docker_services,
        airflow_credentials,
        source_db,
        target_db,
        src_metadata,
        tgt_metadata
    ):
    """
    Test function for the transfer_ods_tables() operator.

    This test verifies the correct functioning of the `transfer_ods_tables()` function, ensuring that
    records are correctly transferred and updated between source and target tables. The test involves 
    multiple runs of the DAG to validate initial data transfer, data consistency, and additional 
    modifications.

    Args:
    -----
        docker_ip (str): IP address of the Docker host.
        docker_services (pytest_docker.plugin.Services): Docker services for managing containers.
        airflow_credentials (tuple): Credentials for Airflow in the form (username, password).
        source_db (Engine): SQLAlchemy engine for the source database.
        target_db (Engine): SQLAlchemy engine for the target database.
        src_metadata (MetaData): SQLAlchemy MetaData object for the source database.
        tgt_metadata (MetaData): SQLAlchemy MetaData object for the target database.

    Returns:
    --------
        None

    Raises:
    -------
        AssertionError: If any of the assertions fail.
    """
    logger.info(f'Testing transfer_ods_tables() function')

    table_name = ['test_table_8']

    # Assert the existence of 'test_table_8' in the target database
    assert_tables_existence(target_db, table_name)

    # Assert that the columns '_modified' and '_deleted' exist in 'test_table_8' in the target database
    assert_table_columns_existence(target_db, table_name[0], ['_modified', '_deleted'])

    # Create SQLAlchemy Table objects (source and target)
    test_table_8_source, = create_tables_tuple(source_db, table_name, src_metadata)
    test_table_8_target, = create_tables_tuple(target_db, table_name, tgt_metadata)

    # Check the existence of the views in the target database
    assert_views_existence(target_db, [f"{test_table_8_target.name}_a"])

    # Initial load: Run the DAG and assert success
    assert_dag_run('test-transfer-ods-tables-1', docker_ip, docker_services, airflow_credentials)

    # Create actuals view table object for 'test_table_8'
    test_table_8_a = Table(f"{test_table_8_target.name}_a", tgt_metadata, autoload_with=target_db)

    # Assert that the data in 'test_table_8' in the source and target databases are equal using the actuals view
    # Assert that the data in 'test_table_8' in the source and target databases are equal
    assert_tables_equality(
        source_db,
        target_db,
        [(test_table_8_source, test_table_8_a),
         (test_table_8_source, test_table_8_target)],
        ignore_cols=IGNORE_ATTR
    )

    # Run the DAG again and assert success
    assert_dag_run('test-transfer-ods-tables-1', docker_ip, docker_services, airflow_credentials)

    # Assert that the data in 'test_table_8' in the source and target databases are equal using the actuals view
    # Assert that the data in 'test_table_8' in the source and target databases are equal
    assert_compare_tables_contents(
        source_db,
        target_db,
        [(test_table_8_source, test_table_8_a),
         (test_table_8_source, test_table_8_target)],
        ignore_cols=IGNORE_ATTR
    )

    # Capture the current row count of 'test_table_8' in the target database
    row_count = get_table_row_count(target_db, test_table_8_target)
    
    # Run the DAG to ensure additional modifications are transferred and assert success
    assert_dag_run('test-transfer-ods-tables-2', docker_ip, docker_services, airflow_credentials)
    
    # Assert that the data in 'test_table_8' in the source and target databases are equal using the actuals view
    assert_compare_tables_contents(
        source_db,
        target_db,
        [(test_table_8_source, test_table_8_a)],
        ignore_cols=IGNORE_ATTR
    )
    
    # Assert that the row count of 'test_table_8' in the target database has increased by 22
    assert_table_row_count(target_db, test_table_8_target, row_count + 22)
    
    # Assert that all data from the source table 'test_table_8' is in the destination table
    assert_tables_data_in_target(
        source_db,
        target_db,
        [(test_table_8_source, test_table_8_target)],
        ignore_cols=IGNORE_ATTR
    )
