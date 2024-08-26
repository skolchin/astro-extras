# Astro SDK Extras project
# (c) kol, 2023-2024

""" Custom PostgreSQL table operations """

import logging
from sqlalchemy import text, Column
from psycopg2 import sql as postgres_sql
from astro.table import BaseTable, TempTable
from astro.constants import MergeConflictStrategy
from sqlalchemy.engine.base import Connection as SqlaConnection
from sqlalchemy.dialects.postgresql.base import PGDialect
from typing import Literal, Tuple

_logger = logging.getLogger('airflow.task')

def assert_is_postgres(conn: SqlaConnection):
    """ Check whether given connection is to a Postgres database. Raises `AssertException` if not. """
    assert isinstance(conn.dialect, PGDialect), f'{conn.engine.url} is not a Postgres connection'

MergeDeleteStrategy = Literal['ignore', 'logical', 'physical']
""" Deletion strategy """

def postgres_merge_tables(
        conn: SqlaConnection,
        source_table: BaseTable | None,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        source_sql: str | None = None,
        if_conflicts: MergeConflictStrategy = 'update',
        delete_strategy: MergeDeleteStrategy = 'ignore',
) -> int | Tuple[int]:
    """
    Merge the source table rows into a destination table.
    The argument `if_conflicts` allows the user to define how to handle conflicts.

    Differs from Astro-SDK `PostgresDatabase.merge_table()` as it supports pre-established
    connections (probably within transaction), custom `select` as data source and logical/physical delete.
    Also, the function will return number of rows affected.

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

    assert_is_postgres(conn)
    assert (source_table or source_sql), f'Either `source_table` or `source_sql` must be provided'

    def construct_upsert_query() -> str:
        statement = (
            "INSERT INTO {target_table} ({target_columns}) SELECT {source_columns} FROM {source_table}"
        )

        source_columns = list(source_to_target_columns_map.keys())
        target_columns = list(source_to_target_columns_map.values())

        match if_conflicts:
            case "ignore":
                statement += " ON CONFLICT ({target_conflict_columns}) DO NOTHING"
            case "update":
                statement += " ON CONFLICT ({target_conflict_columns}) DO UPDATE SET {update_statements}"

        source_column_names = [postgres_sql.Identifier(col) for col in source_columns]
        target_column_names = [postgres_sql.Identifier(col) for col in target_columns]
        update_statements = [
            postgres_sql.SQL("{col_name}=EXCLUDED.{col_name}").format(col_name=col_name) \
                for col_name in target_column_names
        ]

        query = postgres_sql.SQL(statement).format(
            target_table=postgres_sql.Identifier(*identifier_args(target_table)),
            source_table=postgres_sql.Identifier(*identifier_args(source_table)) if source_table \
                        else postgres_sql.SQL("(" + source_sql.strip(' \n\r;') + ") q"),
            target_columns=postgres_sql.SQL(",").join(target_column_names),
            source_columns=postgres_sql.SQL(",").join(source_column_names),
            update_statements=postgres_sql.SQL(",").join(update_statements),
            target_conflict_columns=postgres_sql.SQL(",").join(
                [postgres_sql.Identifier(x) for x in target_conflict_columns]
            ),
        )
        return query

    def construct_delete_query() -> str:
        statement = (
            "DELETE FROM {target_table} WHERE {target_join_columns} NOT IN (SELECT {source_join_columns} FROM {source_table})"
        )
        join_columns = postgres_sql.SQL(",").join([postgres_sql.Identifier(x) for x in target_conflict_columns])
        if len(target_conflict_columns) > 1:
            join_columns = "(" + join_columns + ")"

        query = postgres_sql.SQL(statement).format(
            target_table=postgres_sql.Identifier(*identifier_args(target_table)),
            source_table=postgres_sql.Identifier(*identifier_args(source_table)) if source_table \
                        else postgres_sql.SQL("(" + source_sql.strip(' \n\r;') + ") q"),
            target_join_columns=join_columns,
            source_join_columns=join_columns,
        )
        return query

    def construct_logical_delete_query() -> str:
        statement = (
            'UPDATE {target_table} SET "_deleted" = current_timestamp ' \
            'WHERE "_deleted" IS NULL AND {target_join_columns} NOT IN (SELECT {source_join_columns} FROM {source_table})'
        )
        join_columns = postgres_sql.SQL(",").join([postgres_sql.Identifier(x) for x in target_conflict_columns])
        if len(target_conflict_columns) > 1:
            join_columns = "(" + join_columns + ")"

        query = postgres_sql.SQL(statement).format(
            target_table=postgres_sql.Identifier(*identifier_args(target_table)),
            source_table=postgres_sql.Identifier(*identifier_args(source_table)) if source_table \
                        else postgres_sql.SQL("(" + source_sql.strip(' \n\r;') + ") q"),
            target_join_columns=join_columns,
            source_join_columns=join_columns,
        )
        return query

    sql_list = []
    match delete_strategy:
        case 'ignore':
            sql_list.append(construct_upsert_query())
        case 'physical':
            sql_list.append(construct_upsert_query())
            sql_list.append(construct_delete_query())
        case 'logical':
            sql_list.append(construct_upsert_query())
            sql_list.append(construct_logical_delete_query())

    rowcounts = []
    for sql in sql_list:
        sql = sql.as_string(conn.connection.dbapi_connection)
        _logger.info(f'Executing {sql}')
        rowcounts.append(conn.execute(text(sql)).rowcount)

    return tuple(rowcounts) if len(rowcounts) > 1 else rowcounts[0]

def _get_pgtype_name(oid: int, conn: SqlaConnection) -> str:
    """ Returns name of Postgres type identified by OID """

    # TODO: add type cache
    return conn.execute(text(f'select pg_catalog.format_type({oid}, NULL)')).one()[0]

def postgres_infer_query_structure(sql: str, conn: SqlaConnection, infer_pk: bool = False) -> BaseTable:
    """ Reconstructs an SELECT query column structure.

    Internally, the query is executed with `LIMIT 0` addition and transform resulting cursor to SQLA-column set.

    :param sql: SELECT query
    :param conn: Connection to execute query on
    :param infer_pk: If `True`, then if the 1st table field is of `integer` type,
        it will be marked as primary key
    :returns: Astro-SDK `TempTable` with `columns` attribute assigned with inferred query structure.
    """

    assert_is_postgres(conn)

    sql = sql.strip(' \n\r;') + ' LIMIT 0'
    _logger.info(f'Executing {sql}')
    cur = conn.execute(text(sql))

    native_columns = [(x.name, x.type_code, _get_pgtype_name(x.type_code, conn)) for x in cur.cursor.description]
    _logger.debug(f'Native columns retrieved from query: {native_columns}')

    defaults = {
        'default': None,
        'notnull': None,
        'domains': None,
        'enums': None,
        'schema': None,
        'comment': None,
        'generated': None,
        'identity': None,
    }
    def get_column(pos, name, code, tp_name) -> Column:
        is_pk = False
        if infer_pk and code in (20, 21, 23) and pos == 0:
            # If 1st table field is integer, consider it as PK
            _logger.info(f'Column {name} is considered to be a primary key')
            is_pk = True

        info = conn.dialect._get_column_info(name, tp_name, **defaults)
        return Column(name=info['name'], type_=info['type'], primary_key=is_pk)

    columns = [get_column(n, name, code, tp_name) for n, (name, code, tp_name) in enumerate(native_columns)]
    table = TempTable(None, columns=columns)
    _logger.info(f'Query columns: {table.columns}')

    return table
