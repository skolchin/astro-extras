# Astro SDK Extras project
# (c) kol, 2023-2024

""" Custom PostgreSQL table merge operator. Based on Astro-SDK PostgresDatabase.merge_table() """

import logging
from psycopg2 import sql as postgres_sql
from sqlalchemy import text
from sqlalchemy.engine.base import Connection as SqlaConnection
from astro.constants import MergeConflictStrategy
from astro.table import BaseTable

_logger = logging.getLogger('airflow.task')

def postgres_merge_tables(
        conn: SqlaConnection,
        source_table: BaseTable | None,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        source_sql: str | None = None,
        if_conflicts: MergeConflictStrategy = "update",
) -> int:
    """
    Merge the source table rows into a destination table.
    The argument `if_conflicts` allows the user to define how to handle conflicts.

    Differs from Astro-SDK `PostgresDatabase.merge_table()` as it supports pre-established
    connections (probably within transaction), custom `select` as data source
    and in that it will return number of rows affected.

    :param conn: Connection to execute merge on.
    :param source_table: Contains the rows to be merged to the target_table. Can be `None`.
    :param target_table: Contains the destination table in which the rows will be merged
    :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param source_sql:   Query to get rows to be merged. Either this or `source_table` has to be provided.
    :param if_conflicts: The strategy to be applied if there are conflicts.
    """

    def identifier_args(table: BaseTable):
        schema = table.metadata.schema
        return (schema, table.name) if schema else (table.name,)

    if source_table is None and not source_sql:
        raise ValueError('Either `source_table` or `source_sql` must be provided')

    statement = (
        "INSERT INTO {target_table} ({target_columns}) SELECT {source_columns} FROM {source_table}"
    )

    source_columns = list(source_to_target_columns_map.keys())
    target_columns = list(source_to_target_columns_map.values())

    if if_conflicts == "ignore":
        statement += " ON CONFLICT ({target_conflict_columns}) DO NOTHING"
    elif if_conflicts == "update":
        statement += " ON CONFLICT ({target_conflict_columns}) DO UPDATE SET {update_statements}"

    source_column_names = [postgres_sql.Identifier(col) for col in source_columns]
    target_column_names = [postgres_sql.Identifier(col) for col in target_columns]
    update_statements = [
        postgres_sql.SQL("{col_name}=EXCLUDED.{col_name}").format(col_name=col_name) \
            for col_name in target_column_names
    ]

    query = postgres_sql.SQL(statement).format(
        target_columns=postgres_sql.SQL(",").join(target_column_names),
        target_table=postgres_sql.Identifier(*identifier_args(target_table)),
        source_columns=postgres_sql.SQL(",").join(source_column_names),
        source_table=postgres_sql.Identifier(*identifier_args(source_table)) if source_table \
                    else postgres_sql.SQL("(" + source_sql + ") q"),
        update_statements=postgres_sql.SQL(",").join(update_statements),
        target_conflict_columns=postgres_sql.SQL(",").join(
            [postgres_sql.Identifier(x) for x in target_conflict_columns]
        ),
    )

    sql = query.as_string(conn.connection.dbapi_connection)
    _logger.info(f'Executing {sql}')
    return conn.execute(text(sql).execution_options(autocommit=True)).rowcount