# Astro SDK Extras project
# (c) kol, 2024


from typing import List, Any, Set, Tuple

from sqlalchemy import Table, inspect
from sqlalchemy.engine import Engine

from confsupport import run_dag

from utils import (
    get_table_data,
    get_table_columns,
    get_table_row_count
)

def assert_dag_run(
        dag_id: str,
        docker_ip: str | Any,
        docker_services: Any,
        airflow_credentials: tuple,
        params: str = None,
        timeout: int = 30,
        success: bool = True
    ) -> None:
    """
    Assert that the DAG run finishes with the expected state.

    Args:
    -----
        `dag_id` (str): ID of the DAG to run.
        `docker_ip` (str | Any): IP address of the Docker host.
        `docker_services` (Any): Docker services for managing containers.
        `airflow_credentials` (tuple): Credentials for Airflow in the form (username, password).
        `params` (str, optional): Parameters to pass to the DAG run.
        `timeout` (int, optional): Timeout for the DAG run.
        `success` (bool, optional): Flag to indicate the expected state of the DAG run. Defaults to True.

    Returns:
    --------
        `None`
    """
    expected_state = 'success' if success else 'failed'
    actual_state = run_dag(dag_id, docker_ip, docker_services, airflow_credentials, params, timeout)
    assert actual_state == expected_state, f"Expected DAG state '{expected_state}' but got '{actual_state}'"


def assert_table_row_count(
        engine: Engine,
        table: Table,
        count: int,
        success: bool = True
    ) -> None:
    """
    Assert the row count of the specified table.

    Args:
    -----
        `engine` (Engine): SQLAlchemy engine to use for the query.
        `table` (Table): SQLAlchemy Table object.
        `count` (int): The row count to match or mismatch.
        `success` (bool, optional): Flag to indicate whether to match (True) or mismatch (False) the row count. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the row count does not match the expectation.
    """
    actual_count = get_table_row_count(engine, table)
    if success:
        assert actual_count == count, f"Expected {count} rows, but found {actual_count}."
    else:
        assert actual_count != count, f"Did not expect {count} rows, but found {actual_count}."


def assert_compare_table_contents(
        src_engine: Engine,
        tgt_engine: Engine,
        src_table: Table,
        tgt_table: Table,
        ignore_cols: Set[str] = None,
        success: bool = True
    ) -> None:
    """
    Assert that the contents of the source and target tables match or mismatch.

    Args:
    -----
        `src_engine` (Engine): SQLAlchemy engine for the source database.
        `tgt_engine` (Engine): SQLAlchemy engine for the target database.
        `src_table` (Table): SQLAlchemy Table object for the source table.
        `tgt_table` (Table): SQLAlchemy Table object for the target table.
        `ignore_cols` (Set[str], optional): Set of column names to ignore in the comparison.
        `success` (bool, optional): Flag to indicate whether to match (True) or mismatch (False) the table contents. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the table contents do not match the expectation.
    """
    src_data = get_table_data(src_engine, src_table, ignore_cols)
    tgt_data = get_table_data(tgt_engine, tgt_table, ignore_cols)
    
    if success:
        assert src_data.equals(tgt_data), f"The contents of the source: '{src_table.name}' and target: '{tgt_table.name}' tables do not match."
    else:
        assert not src_data.equals(tgt_data), f"The contents of the source: '{src_table.name}' and target: '{tgt_table.name}' tables should not match."


def assert_compare_tables_contents(
        src_engine: Engine,
        tgt_engine: Engine,
        table_pairs: List[Tuple[Table, Table]],
        ignore_cols: Set[str] = None,
        success: bool = True
    ) -> None:
    """
    Assert that the contents of the source tables match or do not match the target tables.

    This function checks that all records present in the source tables are also present in the target tables
    or that there are differences between the records present in the source tables and the target tables
    based on the success flag.

    Args:
    -----
        `src_engine` (Engine): Source database engine.
        `tgt_engine` (Engine): Target database engine.
        `table_pairs` (List[Tuple[Table, Table]]): List of tuples containing source and target SQLAlchemy Table objects.
        `ignore_cols` (Set[str], optional): Set of column names to ignore. Defaults to None.
        `success` (bool, optional): Flag to indicate whether to match (True) or mismatch (False) the table contents. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the table contents do not match the expectation.
    """
    for src_table, tgt_table in table_pairs:
        src_data = get_table_data(src_engine, src_table, ignore_cols)
        tgt_data = get_table_data(tgt_engine, tgt_table, ignore_cols)
        
        if success:
            assert src_data.equals(tgt_data), f"The contents of the source table '{src_table.name}' and target table '{tgt_table.name}' do not match."
        else:
            assert not src_data.equals(tgt_data), f"The contents of the source table '{src_table.name}' and target table '{tgt_table.name}' should not match."


def assert_compare_table_schemas(
        src_table: Table,
        tgt_table: Table,
        ignore_cols: Set[str] = None,
        success: bool = True
    ) -> None:
    """
    Assert that the schemas of the source and target tables match or do not match.

    This function checks that the columns and indexes of the source table match or do not match
    the columns and indexes of the target table based on the success flag.

    Args:
    -----
        `src_table` (Table): SQLAlchemy Table object for the source table.
        `tgt_table` (Table): SQLAlchemy Table object for the target table.
        `ignore_cols` (Set[str], optional): Set of column names to ignore. Defaults to None.
        `success` (bool, optional): Flag to indicate whether to match (True) or mismatch (False) the table schemas. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the table schemas do not match the expectation.
    """
    ignore_cols = ignore_cols if ignore_cols else set()

    src_columns = set(column.name for column in src_table.columns) - ignore_cols
    tgt_columns = set(column.name for column in tgt_table.columns) - ignore_cols

    src_indexes = sorted(index.name for index in inspect(src_table).indexes)
    tgt_indexes = sorted(index.name for index in inspect(tgt_table).indexes)

    if success:
        assert src_columns == tgt_columns, f"Columns do not match. Source columns: {src_columns}. Target columns: {tgt_columns}."
        assert src_indexes == tgt_indexes, f"Indexes do not match. Source indexes: {src_indexes}. Target indexes: {tgt_indexes}."
    else:
        assert src_columns != tgt_columns, f"Columns should not match. Source columns: {src_columns}. Target columns: {tgt_columns}."
        assert src_indexes != tgt_indexes, f"Indexes should not match. Source indexes: {src_indexes}. Target indexes: {tgt_indexes}."


def assert_tables_existence(
        engine: Engine,
        tables: Set[str],
        schema: str = 'public',
        success: bool = True
    ) -> None:
    """
    Assert the existence or non-existence of tables in the specified schema.

    This function checks whether the specified tables exist or do not exist in the given schema
    based on the success flag.

    Args:
    -----
        `engine` (Engine): SQLAlchemy engine to use for the query.
        `tables` (Set[str]): Set of table names to check.
        `schema` (str, optional): Schema in which to check for table existence. Defaults to 'public'.
        `success` (bool, optional): Flag to indicate whether to check for existence (True) or non-existence (False) of the tables. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the tables do not match the expectation.
    """
    db_tables = inspect(engine).get_table_names(schema=schema)
    for table in tables:
        if success:
            assert table in db_tables, f"{table} is not found in schema '{schema}' of database '{engine.url.database}'"
        else:
            assert table not in db_tables, f"{table} should not be found in schema '{schema}' of database '{engine.url.database}'"


def assert_views_existence(
        engine: Engine,
        views: Set[str],
        schema: str = 'public',
        success: bool = True
    ) -> None:
    """
    Assert the existence or non-existence of views in the specified schema.

    This function checks whether the specified views exist or do not exist in the given schema
    based on the success flag.

    Args:
    -----
        `engine` (Engine): SQLAlchemy engine to use for the query.
        `views` (Set[str]): Set of view names to check.
        `schema` (str, optional): Schema in which to check for view existence. Defaults to 'public'.
        `success` (bool, optional): Flag to indicate whether to check for existence (True) or non-existence (False) of the views. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the views do not match the expectation.
    """
    db_views = inspect(engine).get_view_names(schema=schema)
    for view in views:
        if success:
            assert view in db_views, f"{view} is not found in schema '{schema}' of database '{engine.url.database}'"
        else:
            assert view not in db_views, f"{view} should not be found in schema '{schema}' of database '{engine.url.database}'"


def assert_table_columns_existence(
        engine: Engine,
        table_name: str,
        columns: Set[str],
        schema: str = 'public',
        success: bool = True
    ) -> None:
    """
    Assert the existence or non-existence of columns in the specified table.

    This function checks whether the specified columns exist or do not exist in the given table
    based on the success flag.

    Args:
    -----
        `engine` (Engine): SQLAlchemy engine to use for the query.
        `table_name` (str): Name of the table to check.
        `columns` (Set[str]): Set of column names to check.
        `schema` (str, optional): Schema in which the table is located. Defaults to 'public'.
        `success` (bool, optional): Flag to indicate whether to check for existence (True) or non-existence (False) of the columns. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the columns do not match the expectation.
    """
    table_columns = get_table_columns(engine, table_name, schema)
    for column in columns:
        if success:
            assert column in table_columns, f"Column '{column}' does not exist in the table '{table_name}' in schema '{schema}'."
        else:
            assert column not in table_columns, f"Column '{column}' should not exist in the table '{table_name}' in schema '{schema}'."


def assert_tables_equality(
        src_engine: Engine,
        tgt_engine: Engine,
        table_pairs: List[Tuple[Table, Table]],
        ignore_cols: Set[str] = None,
        success: bool = True
    ) -> None:
    """
    Assert that the tables in the source and target databases are equal or not equal.

    This function checks that the schemas and contents of the tables in the source database
    match or do not match the schemas and contents of the tables in the target database
    based on the success flag.

    Args:
    -----
        `src_engine` (Engine): Source database engine.
        `tgt_engine` (Engine): Target database engine.
        `table_pairs` (List[Tuple[Table, Table]]): List of tuples containing source and target SQLAlchemy Table objects.
        `ignore_cols` (Set[str], optional): Set of column names to ignore. Defaults to None.
        `success` (bool, optional): Flag to indicate whether to check for equality (True) or inequality (False) of the tables. Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the tables do not match the expectation.
    """
    for src_table, tgt_table in table_pairs:
        assert_compare_table_schemas(src_table, tgt_table, ignore_cols, success=success)
        assert_compare_table_contents(src_engine, tgt_engine, src_table, tgt_table, ignore_cols, success=success)


def assert_table_data_in_target(
        src_engine: Engine,
        tgt_engine: Engine,
        src_table: Table,
        tgt_table: Table,
        ignore_cols: Set[str] = None,
        success: bool = True
    ) -> None:
    """
    Assert that data from the source table exists or does not exist in the target table.

    This function checks that all records present in the source table are also present in the target table
    or that there are records present in the source table that are not present in the target table
    based on the success flag.

    Args:
    -----
        `src_engine` (Engine): SQLAlchemy engine for the source database.
        `tgt_engine` (Engine): SQLAlchemy engine for the target database.
        `src_table` (Table): SQLAlchemy Table object for the source table.
        `tgt_table` (Table): SQLAlchemy Table object for the target table.
        `ignore_cols` (Set[str], optional): Set of column names to ignore. Defaults to None.
        `success` (bool, optional): Flag to indicate whether to check for data presence (True) or absence (False). Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the data presence does not match the expectation.
    """
    src_data = get_table_data(src_engine, src_table, ignore_cols)
    tgt_data = get_table_data(tgt_engine, tgt_table, ignore_cols)
    
    missing_data = src_data[~src_data.isin(tgt_data.to_dict(orient='list')).all(axis=1)]
    
    if success:
        assert missing_data.empty, f"The following data from the source table '{src_table.name}' is missing in the target table '{tgt_table.name}':\n{missing_data}"
    else:
        assert not missing_data.empty, f"All data from the source table '{src_table.name}' is present in the target table '{tgt_table.name}', expected some data to be missing."


def assert_tables_data_in_target(
        src_engine: Engine,
        tgt_engine: Engine,
        table_pairs: List[Tuple[Table, Table]],
        ignore_cols: Set[str] = None,
        success: bool = True
    ) -> None:
    """
    Assert that data from the source tables exists or does not exist in the target tables.

    This function checks that all records present in the source tables are also present in the target tables
    or that there are records present in the source tables that are not present in the target tables
    based on the success flag.

    Args:
    -----
        `src_engine` (Engine): SQLAlchemy engine for the source database.
        `tgt_engine` (Engine): SQLAlchemy engine for the target database.
        `table_pairs` (List[Tuple[Table, Table]]): List of tuples containing source and target SQLAlchemy Table objects.
        `ignore_cols` (Set[str], optional): Set of column names to ignore. Defaults to None.
        `success` (bool, optional): Flag to indicate whether to check for data presence (True) or absence (False). Defaults to True.

    Returns:
    --------
        `None`

    Raises:
    -------
        `AssertionError`: If the data presence does not match the expectation.
    """
    for src_table, tgt_table in table_pairs:
        src_data = get_table_data(src_engine, src_table, ignore_cols)
        tgt_data = get_table_data(tgt_engine, tgt_table, ignore_cols)
        
        missing_data = src_data[~src_data.isin(tgt_data.to_dict(orient='list')).all(axis=1)]
        
        if success:
            assert missing_data.empty, f"The following data from the source table '{src_table.name}' is missing in the target table '{tgt_table.name}':\n{missing_data}"
        else:
            assert not missing_data.empty, f"All data from the source table '{src_table.name}' is present in the target table '{tgt_table.name}', expected some data to be missing."
