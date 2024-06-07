# Astro SDK Extras project
# (c) kol, 2024


import pandas as pd

from datetime import datetime
from typing import Dict, List, Set, Tuple

from sqlalchemy.engine import Engine
from sqlalchemy import (
    Table,
    MetaData,
    inspect,
    select,
    func,
    update,
    delete,
    insert
)


def get_table_row_count(
        engine: Engine,
        table: Table
    ) -> int:
    """
    Returns the number of rows in the specified table.

    Args:
    -----
        engine (Engine): SQLAlchemy engine to use for the query.
        table (Table): SQLAlchemy Table object.

    Returns:
    --------
        int: Number of rows in the table.
    """
    count_query = select(func.count()).select_from(table)

    with engine.begin() as conn:
        return conn.execute(count_query).scalar()


def get_table_data(
        engine: Engine,
        table: Table,
        ignore_cols: Set[str] = None
    ) -> pd.DataFrame:
    """
    Returns the data from the specified table as a pandas DataFrame.

    Args:
    -----
        `engine` (Engine): Engine to use for the query.
        `table` (Table): Table to retrieve data from.
        `ignore_cols` (Set[str], optional): Columns to ignore.

    Returns:
    --------
        pd.DataFrame: DataFrame containing the table data.
    """
    with engine.begin() as conn:
        result = conn.execute(select(table))
        data = pd.DataFrame(result.fetchall(), columns=result.keys())
    
    data.columns = data.columns.str.lower()

    if ignore_cols:
        data.drop(columns=ignore_cols, inplace=True, errors='ignore')

    return data


def get_table_columns(
        engine: Engine,
        table_name: str,
        schema: str = 'public'
    ) -> set:
    """
    Returns the column names of the specified table.

    Args:
    -----
        `engine` (Engine): Engine to use for the query.
        `table_name` (str): Name of the table.
        `schema` (str, optional): Schema of the table.

    Returns:
    --------
        set: Set of column names.
    """
    inspector = inspect(engine)
    return set(column['name'] for column in inspector.get_columns(table_name, schema=schema))


def update_mod_ts_table(
        engine: Engine,
        table: Table,
        ids: Set[int],
        new_mod_ts: datetime
    ) -> None:
    """
    Update the `mod_ts` field for given IDs in the specified table.

    Args:
    -----
        `engine` (Engine): Database engine.
        `table` (Table): SQLAlchemy Table object.
        `ids` (Set[int]): Set of IDs for which to update the `mod_ts` field.
        `new_mod_ts` (datetime): The new value for the `mod_ts` field.

    Returns:
    --------
        None
    """
    stmt = update(table).where(table.c.id.in_(ids)).values(mod_ts=new_mod_ts)

    with engine.begin() as conn:
        conn.execute(stmt)


def update_mod_ts_tables(
        engine: Engine,
        table_ids_map: Dict[Table, Set[int]],
        new_mod_ts: datetime
    ) -> None:
    """
    Update the `mod_ts` field for given IDs in the specified tables.

    Args:
    -----
        `engine` (Engine): Database engine.
        `table_ids_map` (Dict[Table, Set[int]]): Dictionary where keys are SQLAlchemy Table objects and values are sets of IDs for which to update the `mod_ts` field.
        `new_mod_ts` (datetime): The new value for the `mod_ts` field.

    Returns:
    --------
        None
    """
    with engine.begin() as conn:
        for table, ids in table_ids_map.items():
            stmt = update(table).where(table.c.id.in_(ids)).values(mod_ts=new_mod_ts)
            conn.execute(stmt)


def insert_records(
        engine: Engine,
        table: Table,
        records: List[dict]
    ) -> None:
    """
    Insert records into the specified table.

    Args:
    -----
        `engine` (Engine): Database engine.
        `table` (Table): SQLAlchemy Table object.
        `records` (List[dict]): List of dictionaries representing the records to insert.

    Returns:
    --------
        None
    """
    stmt = insert(table).values([dict(record) for record in records])

    with engine.begin() as conn:
        conn.execute(stmt)


def delete_records(
        engine: Engine,
        table: Table,
        ids: Set[int]
    ) -> None:
    """
    Delete records with given IDs from the specified table.

    Args:
    -----
        `engine` (Engine): Database engine.
        `table` (Table): SQLAlchemy Table object.
        `ids` (Set[int]): Set of IDs of the records to delete.

    Returns:
    --------
        None
    """
    stmt = delete(table).where(table.c.id.in_(ids))

    with engine.begin() as conn:
        conn.execute(stmt)
        

def clear_tables(
        engine: Engine,
        tables: List[Table]
    ) -> None:
    """
    Clear the contents of the specified tables.

    Args:
    -----
        engine (Engine): Database engine.
        tables (List[Table]): List of SQLAlchemy Table objects to clear.

    Returns:
    --------
        None
    """
    with engine.begin() as conn:
        for table in tables:
            stmt = delete(table)
            conn.execute(stmt)


def create_tables_tuple(
        engine: Engine,
        table_names: List[str],
        metadata: MetaData
    ) -> Tuple[Table, ...]:
    """
    Create a tuple of SQLAlchemy Table objects from a list of table names.

    Args:
    -----
        engine (Engine): SQLAlchemy engine to use for loading table metadata.
        table_names (List[str]): List of table names to create Table objects for.
        metadata (MetaData): SQLAlchemy MetaData object to use for table reflection.

    Returns:
    --------
        Tuple[Table, ...]: Tuple of SQLAlchemy Table objects.
    """
    return tuple(Table(table_name, metadata, autoload_with=engine) for table_name in table_names)
