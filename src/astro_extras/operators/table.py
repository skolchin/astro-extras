# Astro SDK Extras project
# (c) kol, 2023-2024

""" Table operations """

import logging
import warnings
import pandas as pd
from functools import cached_property
from sqlalchemy import Table as SqlaTable, MetaData as SqlaMetadata
from sqlalchemy import text, Integer, BigInteger, SmallInteger
from sqlalchemy.engine.base import Connection as SqlaConnection
from sqlalchemy.exc import InvalidRequestError as SqlaInvalidRequestError

from airflow.models.dag import DagContext
from airflow.utils.context import Context
from airflow.models.xcom_arg import XComArg
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException
from airflow.operators.generic_transfer import GenericTransfer

from astro import sql as aql
from astro.databases import create_database
from astro.sql.table import BaseTable, Table
from astro.databases.base import BaseDatabase
from astro.airflow.datasets import kwargs_with_datasets

from .session import ETLSession, ensure_session
from ..utils.utils import ensure_table, schedule_ops, is_same_database_uri
from ..utils.template import get_template, get_template_file, get_predefined_template
from ..utils.data_compare import compare_datasets, compare_timed_dict
from ..utils.postgres_sql import postgres_merge_tables

from typing import Iterable, Type, Tuple

DEFAULT_CHUNK_SIZE: int = 10000
""" Chunk size for `pd.to_sql()` calls. Set to value lesser than `DEFAULT_CHUNK_SIZE` in Astro SDK to avoid memory overload """

class TableTransfer(GenericTransfer):
    """
    Table transfer operator to be used within `transfer_table` function.
    Implements 'bulk' data transfer without any extra conditioning.
    """

    template_fields = ("sql", "preoperator", "source_table", "destination_table")
    template_ext = (".sql", ".hql" )
    template_fields_renderers = {"sql": "sql", "preoperator": "sql"}
    ui_color = "#b4f07c"

    # Public attributes
    source_db: BaseDatabase
    """ Source database object """

    dest_db: BaseDatabase
    """ Destination database object """

    source: BaseTable
    """ Source table object as it was passed in. Property `source_table_def` reflects the same table but filled with columns structure """

    source_table: str
    """ Source table fully-qualified name """

    destination: BaseTable
    """ Destination table object as it was passed in. Property `dest_table_def` reflects the same table but filled with columns structure """

    destination_table: str
    """ Destination table fully-qualified name """

    session: ETLSession | XComArg | None
    """ ETL Session. Use `ensure_session` to cast to proper session type """

    in_memory_transfer: bool
    """ Flag to use in-memory transfer (othewise it would be Airflow's cursor-based, which is several times slower) """

    chunk_size: int
    """ Chunk size for `pd.to_sql() calls """

    def __init__(
        self,
        *,
        source_table: BaseTable,
        destination_table: BaseTable,
        session: ETLSession | None = None,
        in_memory_transfer: bool = False,
        chunk_size: int | None = DEFAULT_CHUNK_SIZE,
        **kwargs,
    ) -> None:

        # source and target databases
        self.source_db = create_database(source_table.conn_id)
        self.dest_db = create_database(destination_table.conn_id)

        # source and target AstroSDK tables
        self.source = source_table
        self.destination = destination_table

        # source and target fully-qualified table names
        # self.destination_table will be set by `super().__init__()`
        self.source_table = self.source_db.get_table_qualified_name(self.source)

        # task_id would be 'transfer-<schema>_<table>'
        task_id = kwargs.pop('task_id', f'transfer-{self.source_table.replace(".", "_")}')

        # sql is either passed in by the caller or rendered from template
        # here, original `source_table` is used to match filename case
        sql = kwargs.pop('sql', self._get_sql(source_table, self.source_db, session))

        super().__init__(task_id=task_id,
                         sql=sql,
                         destination_table=self.dest_db.get_table_qualified_name(self.destination),
                         source_conn_id=self.source.conn_id,
                         destination_conn_id=self.destination.conn_id,
                         **kwargs)

        self.session = session
        self.in_memory_transfer = in_memory_transfer
        self.chunk_size = chunk_size

        # flag to prevent multiple _pre_execute() calls
        self._pre_execute_called: bool = False

        # statistics for lineage
        self._row_count: int = 0

    def _get_sql(self, table: BaseTable, db: BaseDatabase, session: ETLSession | None = None, suffix: str | None = None) -> str:
        """ Internal - get a sql statement or template for given table """

        # Check whether a template SQL exists for given table under dags\templates\<dag_id>
        # Actual query will be loaded by Airflow templating itself
        full_name = db.get_table_qualified_name(table)
        self.log.info(f'Looking up a template file for table {full_name}')
        if (sql_file := get_template_file(full_name, '.sql')) or (sql_file := get_template_file(table.name, '.sql')) :
            self.log.info(f'Using template file {sql_file}')
            return sql_file

        # Nope, load an SQL from package resources substituting template fields manually
        # SQL file names are fixed according to whether we do run under ETL session or not
        template_name = 'table_transfer_nosess.sql' if not session else 'table_transfer_sess.sql'
        template = get_predefined_template(template_name)
        sql = template.render(source_table=full_name + (suffix or ''))
        self.log.info(f'Using predefined SQL template {template_name} rendered as {sql}')
        return sql

    def _pre_execute(self, context: Context):
        """ Internal - run before execution """

        if self._pre_execute_called:
            return

        # Set debug level logging if requested upon DAG start
        # Config is: {..., "debug": true}
        assert 'dag_run' in context
        if context['dag_run'].conf.get('debug'):
            self.log.setLevel(logging.DEBUG)
            logging.getLogger('airflow.task').setLevel(logging.DEBUG)
            self.log.debug('Log level set to DEBUG')

        # Transform XComArg session into ETLSession
        self.session = ensure_session(self.session, context)
        if self.session:
            if not self.source_conn_id:
                self.source_conn_id = self.session.source_conn_id
            if not self.destination_conn_id:
                self.destination_conn_id = self.session.destination_conn_id

        if not self.source_conn_id:
            raise AirflowFailException('source connection not specified')
        if not self.destination_conn_id:
            raise AirflowFailException('destination connection not specified')
        if self.source_conn_id == self.destination_conn_id and self.source_table == self.destination_table:
            raise AirflowFailException('Source and destination must not be the same')

        self._pre_execute_called = True

    def _load_table_def(self, base_table: BaseTable, db: BaseDatabase) -> BaseTable:
        """ Internal - load single table definition """
        # Use base_table's SQLA metadata to retrieve table columns from database
        # There's something that looks like a bug in SQLA - 
        # only lower-case schema/table names could be used with reflect
        try:
            full_name = db.get_table_qualified_name(base_table).lower()
            meta = SqlaMetadata(schema=base_table.metadata.schema.lower() \
                                if base_table.metadata and base_table.metadata.schema else None)
            meta.reflect(bind=db.connection, only=[base_table.name.lower()])
        except SqlaInvalidRequestError as ex:
            self.log.error(f'Could not load table {full_name} structure on {db.conn_id}')
            return base_table

        if (sqla_table := meta.tables.get(full_name, None)) is None:
            self.log.error(f'Could not find table {full_name} on {db.conn_id}')
            return base_table

        # Construct new table object containing all props of `base_table` and additional columns metadata
        result_table = Table(
            name=base_table.name,
            conn_id=db.conn_id,
            metadata=base_table.metadata,
            temp=base_table.temp,
            columns=sqla_table.columns)
        
        self.log.debug(f'Table {full_name} columns: {",".join([f"{c.name} {c.type}" for c in result_table.columns])}')
        return result_table

    @cached_property
    def source_table_def(self) -> BaseTable:
        """ Source table object populated with metadata """
        return self._load_table_def(self.source, self.source_db)

    @cached_property
    def dest_table_def(self) -> BaseTable:
        """ Destination table object populated with metadata """
        return self._load_table_def(self.destination, self.dest_db)

    def _adjust_dtypes(self, data: pd.DataFrame, table: BaseTable) -> pd.DataFrame:
        """ Checks whether `data` column dtypes match given `table` ones and reset them to proper type.
        This could happen if, for example, source has nulls in `int` columns which would have `float` type in dataframe.
        """
        for col in table.columns:
            if col.name not in data.columns:
                self.log.warning(f'Column {col.name} of table {table.name} was not found in dataset')
                continue

            match col.type:
                case Integer() | SmallInteger() | BigInteger() if data.dtypes[col.name] == 'float':
                    self.log.info(f'Coersing column {col.name} dtype from `float` to `int`')
                    data[col.name] = data[col.name].astype('Int64')

                case _:
                    pass

        return data

    def execute(self, context: Context):
        """ Execute operator """
        self._pre_execute(context)
        if not self.in_memory_transfer:
            return super().execute(context)
        
        # upload source data to memory
        self.log.info(f'In-memory transfer using {self.sql}')
        data = pd.read_sql(self.sql, self.source_db.connection)
        if data is None or data.empty:
            self.log.info('No data to transfer')
            return

        # Adjust dtypes
        data = self._adjust_dtypes(data, self.dest_table_def)

        with self.dest_db.connection as conn, conn.begin():
            # run preoperator
            if self.preoperator:
                self.log.info(f'Executing: {self.preoperator}')
                conn.execute(text(self.preoperator))

            # save to target table
            self._row_count = len(data)
            self.log.info(f'{self._row_count} records to be transferred (chunk size is {self.chunk_size})')
            data.to_sql(
                self.destination.name,
                con=conn,
                schema=self.destination.metadata.schema if self.destination.metadata else None,
                if_exists='append',
                method='multi',
                chunksize=self.chunk_size,
                index=False,
            )

    def _make_openlineage_facets(self, task_instance):
        """ Make OpenLineage facets and dataset info """

        from openlineage.client.run import Dataset
        from openlineage.client.facet import (
            BaseFacet,
            DataSourceDatasetFacet,
            OutputStatisticsOutputDatasetFacet,
            SchemaDatasetFacet,
            SchemaField,
            SqlJobFacet,
        )

        def _make_dataset(table_def: BaseTable, db: BaseDatabase) -> Dataset:
            ns = db.openlineage_dataset_namespace()
            full_name = db.get_table_qualified_name(table_def)
            if (dbname := table_def.metadata.database or db.default_metadata.database):
                full_name = f'{dbname}.{full_name}'

            return Dataset(
                namespace=ns,
                name=f'{full_name}',
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[SchemaField(name=col.name, type=str(col.type)) for col in table_def.columns],
                    ),
                    "dataSource": DataSourceDatasetFacet(
                        name=full_name, uri=f"{ns}/{full_name}"
                    ),
                })

        # Construct input dataset...
        if self.source_table_def.openlineage_emit_temp_table_event():
            source_datasets = [_make_dataset(self.source_table_def, self.source_db)]
        else:
            source_datasets = []

        # ... output dataset ...
        if self.dest_table_def.openlineage_emit_temp_table_event():
            dest_datasets = [_make_dataset(self.dest_table_def, self.dest_db)]
        else:
            dest_datasets = []

        # ... runtime facets ...
        run_facets: dict[str, BaseFacet] = {
            "outputStatistics": OutputStatisticsOutputDatasetFacet(
                rowCount=self._row_count,
            ),
        }
        job_facets: dict[str, BaseFacet] = {
            "sql": SqlJobFacet(query=str(self.sql)),
        }
        return source_datasets, dest_datasets, run_facets, job_facets

    def get_openlineage_facets_on_complete(self, task_instance):  # skipcq: PYL-W0613
        """ Provide OpenLineage information """
        from airflow.providers.openlineage.extractors.base import OperatorLineage

        inputs, outputs, run_facets, job_facets = self._make_openlineage_facets(task_instance)
        return OperatorLineage(
            inputs=inputs, outputs=outputs, run_facets=run_facets, job_facets=job_facets
        )

class ChangedTableTransfer(TableTransfer):
    """
    Table transfer operator to be used within `transfer_changed_table` function.
    Compares source and target data and transfers only if any changes detected.
    Requiures `xxx_a` view to exist on target.
    """

    template_fields = ("sql", "preoperator", "source_table", "destination_table", "destination_sql")
    template_fields_renderers = {"sql": "sql", "preoperator": "sql", "destination_sql": "sql"}
    ui_color = "#95f07c"

    def __init__(
        self,
        *,
        source_table: BaseTable,
        destination_table: BaseTable,
        session: ETLSession | None = None,
        **kwargs,
    ) -> None:

        dest_db = create_database(destination_table.conn_id, destination_table)
        dest_sql = kwargs.pop('destination_sql', self._get_sql(destination_table, dest_db, suffix='_a'))

        super().__init__(source_table=source_table,
                         destination_table=destination_table,
                         session=session,
                         **kwargs)

        self.destination_sql: str = dest_sql

    def _compare_datasets(self, src_conn: SqlaConnection, dest_conn: SqlaConnection, stop_on_first_diff: bool, logger: logging.Logger | None = None):
        """ Internal - compare source and target dictionaries """

        logger = logger or self.log

        logger.info(f'Executing: {self.sql}')
        df_src = pd.read_sql(self.sql, src_conn)
        logger.info(f'{len(df_src)} records selected on source')

        logger.info(f'Executing: {self.destination_sql}')
        df_trg  = pd.read_sql(self.destination_sql, dest_conn)
        logger.info(f'{len(df_trg)} records selected on target')

        return compare_datasets(df_src, df_trg, stop_on_first_diff=stop_on_first_diff, logger=logger)

    def execute(self, context: Context):
        """ Execute operator """
        self._pre_execute(context)
        if not self._compare_datasets(self.source_db.connection, self.dest_db.connection, stop_on_first_diff=True):
            return super().execute(context)

class OdsTableTransfer(ChangedTableTransfer):
    """
    Table transfer operator to be used within `transfer_ods_table` function
    to transfer data from source to ODS-style target.
    
    Requiures `xxx_a` view to exist on target. Target table must have
    `_modified' and `_deleted` attributes of `timestamp` or `timestamptz` type.
    """
    ui_color = "#78f07c"

    def _save_data(self, data: pd.DataFrame, conn: SqlaConnection, modified: pd.Timestamp, deleted: pd.Timestamp | None, category: str) -> None:
        if data is not None and not data.empty:
            if self.session is not None:
                data.drop(columns=set(['session_id', '_modified', '_deleted']) & set(data.columns), inplace=True)
                data.insert(0, '_deleted', deleted)
                data.insert(0, '_modified', modified)
                data.insert(0, 'session_id', self.session.session_id)
            else:
                data['_modified'] = modified
                data['_deleted'] = deleted

            self.log.info(f'Saving {data.shape[0]} {category} records to {self.destination_table} (chunk size is {self.chunk_size})')
            data.to_sql(
                self.destination.name,
                con=conn,
                schema=self.destination.metadata.schema if self.destination.metadata else None,
                if_exists='append',
                method='multi',
                chunksize=self.chunk_size,
                index=False,
            )
            self._row_count += len(data)

    def execute(self, context: Context):
        """ Execute operator """

        # All the checks and strcture load
        self._pre_execute(context)

        # Verify target table structure
        n_col = 0
        if self.session is not None:
            if self.dest_table_def.columns[n_col].name.lower() != 'session_id':
                raise AirflowFailException(f'Invalid ODS table {self.destination_table} structure: `session_id` missing or at improper place')
            n_col += 1
        if self.dest_table_def.columns[n_col].name.lower() != '_modified':
            raise AirflowFailException(f'Invalid ODS table {self.destination_table} structure: `_modified` column missing or at improper place')
        if self.dest_table_def.columns[n_col+1].name.lower() != '_deleted':
            raise AirflowFailException(f'Invalid ODS table {self.destination_table} structure: `_deleted` column missing or at improper place')

        # compare datasets and save delta frames to target (new/modified/deleted)
        self._row_count = 0
        with self.source_db.connection as src_conn, self.dest_db.connection as dest_conn:
            dfn, dfm, dfd = self._compare_datasets(src_conn, dest_conn, stop_on_first_diff=False)
            with dest_conn.begin():
                self._save_data(dfn, dest_conn, pd.Timestamp.utcnow(), pd.NaT, 'new')
                self._save_data(dfm, dest_conn, pd.Timestamp.utcnow(), pd.NaT, 'modified')
                self._save_data(dfd, dest_conn, pd.Timestamp.utcnow(), pd.Timestamp.utcnow(), 'deleted')

class ActualsTableTransfer(TableTransfer):
    """
    Table transfer operator to be used within `transfer_actuals_table` function.
    Source and target table must have `_deleted` attribute of `timestamp` or `timestamptz` type.
    """

    ui_color = "#5af07d"

    def __init__(
        self,
        *,
        source_table: BaseTable,
        destination_table: BaseTable,
        session: ETLSession | None = None,
        transfer_delta: bool = True,
        as_ods: bool = False,
        keep_temp_table: bool = False,
        replace_data: bool = False,
        **kwargs,
    ) -> None:
        self.transfer_delta = transfer_delta
        self.as_ods = as_ods
        self.keep_temp_table = keep_temp_table
        self.replace_data = replace_data
        
        super().__init__(source_table=source_table, destination_table=destination_table, session=session, **kwargs)

    def _get_sql(self, table: BaseTable, db: BaseDatabase, session: ETLSession | None = None, suffix: str | None = None) -> str:
        """ Internal - get a sql statement or template for given table """

        # Find table-specific template
        full_name = db.get_table_qualified_name(table)
        if (sql := get_template(full_name, '.sql', fail_if_not_found=False)):
            # If such template exist, replace table name with subquery
            # The subquery should contain the same subset of columns with target table, 
            # otherwise it might be problems with dataset comparsion
            full_name = f'({sql})'

        # Get template SQL from package resources
        # The template defines `select` query which takes data from source
        template = get_predefined_template('table_actuals_select_delta.sql' if self.transfer_delta else 'table_actuals_select_all.sql')
        return template.render(source_table=full_name)

    def execute(self, context: Context):
        """ Execute operator """

        # All the checks
        self._pre_execute(context)

        # Verify structure
        if self.as_ods:
            # Check source and target meet the ODS requirements
            if not [c for c in self.source_table_def.columns if c.name == '_deleted']:
                raise AirflowFailException(f'Source table {self.source_table} does not have `_deleted` column required for ODS-style transfer')
            if not [c for c in self.dest_table_def.columns if c.name == '_deleted']:
                raise AirflowFailException(f'Target table {self.destination_table} does not have `_deleted` column required for ODS-style transfer')

        # Build a source-to-target column mapping
        col_map = {}
        id_cols = []
        for src_col in self.source_table_def.columns:
            dest_cols = [c for c in self.dest_table_def.columns if c.name.lower() == src_col.name.lower()]
            if not dest_cols:
                self.log.warning(f'Column {src_col.name} was not found in {self.destination_table} table, skipping')
            else:
                # If a column is PK, store it to use in `on conflict` statement part
                col_map[src_col.name] = dest_cols[0].name
                if dest_cols[0].primary_key:
                    id_cols.append(dest_cols[0].name)

        # Check there are any ID columns on target
        if not id_cols:
            raise AirflowFailException(f'Could not detect primary key on {self.destination_table}')

        # Replace `t.id` with ids and `t.*` with selection columns list and proper session_id
        id_cols_str = ", ".join(id_cols)
        all_cols_str = f'{self.session.session_id} as session_id, ' + ", ".join([col for col in col_map if col != 'session_id'])
        sql = self.sql.replace('t.id', id_cols_str).replace('t.*', all_cols_str)
        self.log.info(f'Source extraction SQL:\n{sql}')

        # Check whether the hooks point to the same database
        same_db = is_same_database_uri(self.source_db.hook.get_uri(), self.dest_db.hook.get_uri())

        # Update/merge
        with self.source_db.connection as src_conn, self.dest_db.connection as dest_conn:
            if same_db:
                # Source and destination tables are in the same database, 
                self.log.info(f'Source and destination tables are in the same database')
                with dest_conn.begin():
                    # If `replace_data` is set, purge target table
                    if self.replace_data:
                        dest_conn.execute(text(f'delete from {self.destination_table}'))

                    # Merge into target table directly using source sql
                    self._row_count = postgres_merge_tables(
                        conn=dest_conn,
                        source_table=None,
                        target_table=self.destination,
                        source_to_target_columns_map=col_map,
                        target_conflict_columns=id_cols,
                        source_sql=sql,
                        if_conflicts='update'
                    )
                    self.log.info(f'{self._row_count} records transferred')

            else:
                # Source and destination tables are in different databases
                temp_table = Table(metadata=self.destination.metadata, temp=True)
                self.log.info(f'Source and destination tables are in different databases, transferring via temporary table')

                # Load data from source table
                data = pd.read_sql(sql, src_conn)
                if data is None or data.empty:
                    self.log.info('No data to transfer')
                    return
                self.log.info(f'{len(data)} records to be transferred (chunk size is {self.chunk_size})')
                
                data = self._adjust_dtypes(data, self.dest_table_def)
                if self.session:
                    data['session_id'] = self.session.session_id
                    col_map['session_id'] = 'session_id'

                # Temp table has to be created 1st, otherwise it might be column types mismatch
                temp_sqla_table = SqlaTable(temp_table.name, temp_table.sqlalchemy_metadata, *([c.copy() for c in self.source_table_def.columns]))
                self.destination.sqlalchemy_metadata.create_all(bind=dest_conn, tables=[temp_sqla_table], checkfirst=False)
                self.log.info(f'{temp_table.name} temporary table created')

                try:
                    # Save data to temporary table
                    data.to_sql(
                        temp_table.name,
                        con=dest_conn,
                        schema=temp_table.metadata.schema if temp_table.metadata else None,
                        if_exists='append',
                        method='multi',
                        chunksize=self.chunk_size,
                        index=False,
                    )
                    self.log.info(f'Source data transferred to {temp_table.name}')

                    with dest_conn.begin():
                        # If `replace_data` is set, purge target table
                        if self.replace_data:
                            self.log.info(f'Cleaning up destination table {self.destination_table}')
                            dest_conn.execute(text(f'delete from {self.destination_table}'))

                        # Merge temporary and target tables
                        self.log.info(f'Merging from {self.dest_db.get_table_qualified_name(temp_table)} to {self.destination_table}')
                        self._row_count = postgres_merge_tables(
                            conn=dest_conn,
                            source_table=temp_table,
                            target_table=self.destination,
                            source_to_target_columns_map=col_map,
                            target_conflict_columns=id_cols,
                            if_conflicts='update'
                        )
                        self.log.info(f'{self._row_count} records transferred')

                finally:
                    if not self.keep_temp_table:
                        self.dest_db.drop_table(temp_table)

class TimedTableTransfer(ChangedTableTransfer):
    """
    Table transfer operator to be used within `update_timed_dict` function.
    Implements timed dictionary update behaviour.
    Requiures `xxx_a` view to exist both on source and target.
    """
    ui_color = "#3cf07e"

    def execute(self, context: Context):
        """ Execute operator """

        # All the checks
        self._pre_execute(context)

        # Get source and target data
        with self.source_db.connection as src_conn, self.dest_db.connection as dest_conn, dest_conn.begin():
            self.log.info(f'Executing: {self.sql}')
            df_src = pd.read_sql(self.sql, src_conn)
            self.log.info(f'Executing: {self.destination_sql}')
            df_trg = pd.read_sql(self.destination_sql, dest_conn)

            # Get the deltas
            df_opening, df_closing = compare_timed_dict(df_src, df_trg)
            if (df_opening is None or df_opening.empty) and (df_closing is None or df_closing.empty):
                self.log.info('No changes detected, nothing to do')
                return

            # Update records to be closed
            if df_closing is not None and not df_closing.empty:
                update_sql_template = get_predefined_template('table_timed_update.sql')
                update_sql = update_sql_template.render(destination_table=self.destination_table)
                update_params = df_closing.to_dict(orient='list')
                dest_conn.execute(text(update_sql).execution_options(autocommit=True), update_params)

            # Insert new records
            if df_opening is not None and not df_opening.empty:
                df_opening.to_sql(
                    self.destination.name,
                    con=dest_conn,
                    schema=self.destination.metadata.schema if self.destination.metadata else None,
                    if_exists='append',
                    method='multi',
                    chunksize=self.chunk_size,
                    index=False,
                )


class CompareTableOperator(ChangedTableTransfer):
    """
    Table comparsion operator to be used within `compare_table` function.
    Compares source and target data and prints results to log.
    """
    ui_color = "#64bf62"

    def execute(self, context: Context):
        """ Execute operator """
        logger = logging.getLogger(f'compare_tables_logger')
        logger.setLevel(logging.DEBUG)
        if not self._compare_datasets(self.source_db.connection, self.dest_db.connection, stop_on_first_diff=True, logger=logger):
            raise AirflowFailException(f'Differences detected')

def load_table(
        table: str | BaseTable,
        conn_id: str | None = None,
        session: ETLSession | None = None,
        sql: str | None = None) -> XComArg:
    """ Loads table into memory.

    This is a wrapper over Astro-SDK `run_raw_sql` to
    load data from given database table into XCom and make it available for 
    further processing.

    SQL templating is supported, e.g. if a template for given table was found, it
    will be executed to get the data (see `astro_extras.utils.template.get_template_file`).

    Please note that in order to operate even on modest volumes of data,
    intermediate XCom storage might be required. Easiest way to set it up is to use
    local Parquet file storage by setting
    `AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID=local` environment
    variable. However, this will add extra serialization/deserialization
    operation to every task thus increasing overall DAG execution time.

    See https://astro-sdk-python.readthedocs.io/en/1.2.0/guides/xcom_backend.html
    for details.

    Args:
        table:  Either a table name or Astro-SDK `Table` object to load data from
        conn_id:    Airflow connection ID to underlying database. If not specified,
            and `Table` object is passed it, its `conn_id` attribute will be used.
        session:    `astro_extras.operators.session.ETLSession` object. 
            Used only to link up to the `open_SESSIONion` operator.
        sql:    Custom SQL to load data, used only if no SQL template found.
            If neither SQL nor template is given, all table data will be loaded.

    Results:
        `XComArg` object suitable for further manipulations with Astro-SDK functions

    Examples:
        >>> @aql.dataframe
        >>> def modify_data(data: pd.DataFrame):
        >>>     data['some_column'] = 'new_value'
        >>>     return data
        >>> data = load_table('test_table', conn_id='source_db')
        >>> modified_data = modify_data(data)
        >>> save_table(modified_data, conn_id='target_db')
    """

    if not isinstance(table, BaseTable):
        dag = DagContext.get_current_dag()
        @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                        conn_id=conn_id,
                        task_id=f'load-{table}',
                        results_format='pandas_dataframe')
        def _load_table_by_name(table: str, session: ETLSession | None):
            sql_file = get_template_file(table, '.sql', dag=dag)
            return sql or sql_file or f'select * from {table}'

        return _load_table_by_name(table, session)
    else:
        dag = DagContext.get_current_dag()
        @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                        conn_id=conn_id,
                        task_id=f'load-{table.name}',
                        results_format='pandas_dataframe')
        def _load_table(table: BaseTable, session: ETLSession | None):
            sql_file = get_template_file(table.name, '.sql', dag=dag)
            return sql or sql_file or '''select * from {{table}}'''

        return _load_table(table, session)

def save_table(
        data: XComArg,
        table: str | BaseTable,
        conn_id: str | None = None,
        session: ETLSession | None = None,
        fail_if_not_exist: bool | None = True) -> XComArg:
    """ Saves a table into database """

    table = ensure_table(table, conn_id)
    task_id = f'save-{table.name}'
    conn_id = conn_id or table.conn_id

    @aql.dataframe(if_exists='append', conn_id=conn_id, task_id=task_id)
    def _save_data(data: pd.DataFrame, session: ETLSession):
        if fail_if_not_exist:
            db = create_database(conn_id, table)
            if not db.table_exists(table):
                raise AirflowFailException(f'Table {table.name} was not found under {conn_id} connection')
        session = ensure_session(session)
        if session and 'session_id' not in data.columns:
            data.insert(0, 'session_id', session.session_id)
        return data

    return _save_data(data, session, output_table=table)

def declare_tables(
        table_names: Iterable[str],
        conn_id: str | None = None,
        schema: str | None = None,
        database: str | None = None,
        lowercase: bool = False
) -> Iterable[BaseTable]:
    """ Convert list of string table names to list of `Table` objects """
    if not lowercase:
        return [ensure_table(t, conn_id, schema, database) for t in table_names]
    else:
        return [ensure_table(t.lower(), conn_id, schema, database) for t in table_names]

def _do_transfer_table(
        op_cls: Type[GenericTransfer],
        source: str | BaseTable,
        target: str | BaseTable | None,
        source_conn_id: str | None,
        destination_conn_id: str | None,
        session: XComArg | ETLSession | None,
        **kwargs) -> XComArg:
    """ Internal - table transfer implementation """

    source_table = ensure_table(source, source_conn_id)
    dest_table = ensure_table(target or source_table, destination_conn_id)

    op = op_cls(
        source_table=source_table,
        destination_table=dest_table,
        session=session,
        **kwargs_with_datasets(kwargs=kwargs, 
                               input_datasets=source_table, 
                               output_datasets=dest_table)
    )
    return XComArg(op)

def _do_transfer_tables(
        op_cls: Type[GenericTransfer],
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None,
        source_conn_id: str | None,
        destination_conn_id: str | None,
        group_id: str | None,
        num_parallel: int,
        session: XComArg | ETLSession | None,
        **kwargs) -> TaskGroup:
                 
    """ Internal - transfer multiple tables """

    if target_tables and len(target_tables) != len(source_tables):
        raise AirflowFailException(f'Source and target tables list size must be equal')

    target_tables = target_tables or source_tables

    with TaskGroup(group_id, add_suffix_on_collision=True, prefix_group_id=False) as tg:
        ops_list = []
        for (source, target) in zip(source_tables, target_tables):
            source_table = ensure_table(source, source_conn_id)
            dest_table = ensure_table(target, destination_conn_id) or source_table
            op = op_cls(
                source_table=source_table,
                destination_table=dest_table,
                session=session,
                **kwargs_with_datasets(
                    kwargs=kwargs, 
                    input_datasets=source_table, 
                    output_datasets=dest_table))
            ops_list.append(op)

        schedule_ops(ops_list, num_parallel)

    return tg

def transfer_table(
        source: str | BaseTable,
        target: str | BaseTable | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        session: XComArg | ETLSession | None = None,
        changes_only: bool | None = None,
        **kwargs) -> XComArg:
    
    """ Cross-database data transfer.

    This function implements cross-database geterogenous data transfer.

    It reads data from source table into memory and then sequentaly inserts 
    each record into the target table. Fields order in the source and target tables 
    must be identical and field types must be compatible, or transfer will fail or produce 
    undesirable results.

    To limit data selection or customize fields, a SQL template could be 
    created for the source table (see `astro_extras.utils.template.get_template_file`).
    The template must ensure fields order and type compatibility with the target table.
    If transfer is running under `astro_extras.operators.session.ETLSession` context, 
    a `session_id` field must also be manually added at proper place.

    For example, if these tables are to participate in transfer:

    ``` sql
    create table source_data ( a int, b text );
    create table target_data ( session_id int, b text, a int );
    ```

    then, this SQL template might be created as `source_data.sql` file:

    ``` sql
    select {{ti.xcom_pull(key="session").session_id}} as session_id, b, a 
    from source_data;
    ```

    Args:
        source: Either a table name or a `Table` object which would be a data source.
            If a string name is provided, it may contain schema definition denoted by `.`. 
            For `Table` objects, schema must be defined in `Metadata` field,
            otherwise Astro SDK might fall to use its default schema.
            If a SQL template exists for this table name, it will be executed,
            otherwise all table data will be selected.

        target: Either a table name or a `Table` object where data will be saved into.
            If a name is provided, it may contain schema definition denoted by `.`. 
            For `Table` objects, schema must be defined in `Metadata` field,
            otherwise Astro SDK might fall to use its default schema.
            If omitted, `source` argument value is used (this makes sense only
            with string table name and different connections).

        source_conn_id: Source database Airflow connection.
            Used only with string source table name; for `Table` objects, `conn_id` field is used.
            If omitted and `session` argument is provided, `session.source_conn_id` will be used.

        destination_conn_id: Destination database Airflow connection.
            Used only with string target table name; for `Table` objects, `conn_id` field is used.
            If omitted and `session` argument is provided, `session.destination_conn_id` will be used.

        session:    `ETLSession` object. If set and no SQL template is defined,
            a `session_id` field will be automatically added to selection.

        changes_only:   If set to `True`, the operator will compare source and target
            tables and transfer data only when they are different. Target data are obtained
            by runnning `destination_sql`. By default, this query will be built using 
            `<destination_table>_a` view to get actual data.

            Deprecated since 0.1.1, use `transfer_changed_table` instead.

        kwargs:     Any parameters passed to underlying operator (e.g. `preoperator`, ...)

    Returns:
        `XComArg` object

    Examples:
        Using `Table` objects (note use of `Metadata` object to specify schemas):

        >>> with DAG(...) as dag:
        >>>     input_table = Table('table_data', conn_id='source_db', 
        >>>                          metadata=Metadata(schema='public'))
        >>>     output_table = Table('table_data', conn_id='target_db', 
        >>>                          metadata=Metadata(schema='stage'))
        >>>     transfer_table(input_table, output_table)

        Using string table name:

        >>> with DAG(...) as dag, ETLSession('source_db', 'target_db') as sess:
        >>>     transfer_table('public.table_data', session=sess)

    """
    if changes_only is not None:
        warnings.warn('`changes_only` is deprecated since 0.0.13, use `transfer_changed_table()` instead')

    op_cls = TableTransfer if not changes_only else ChangedTableTransfer
    return _do_transfer_table(
        op_cls=op_cls,
        source=source,
        target=target,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        session=session,
        **kwargs)

def transfer_tables(
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        group_id: str | None = None,
        num_parallel: int = 1,
        session: XComArg | ETLSession | None = None,
        changes_only: bool | None = None,
        **kwargs) -> TaskGroup:

    """ Transfer multiple tables.

    Creates an Airflow task group with transfer tasks for each table pair 
    from `source_tables` and `target_tables` lists.

    See `transfer_table` for more information.
    """
    if changes_only is not None:
        warnings.warn('`changes_only` is deprecated since 0.1.1, use `transfer_changed_tables()` instead')

    op_cls = TableTransfer if not changes_only else ChangedTableTransfer
    return _do_transfer_tables(
        op_cls=op_cls,
        source_tables=source_tables,
        target_tables=target_tables,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        group_id=group_id or 'transfer-tables',
        num_parallel=num_parallel,
        session=session,
        **kwargs
    )

def transfer_changed_table(
        source: str | BaseTable,
        target: str | BaseTable | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        session: XComArg | ETLSession | None = None,
        **kwargs) -> XComArg:
    
    """ Cross-database changed data transfer.

    This function implements cross-database geterogenous data transfer using snap-shotting
    mechanism, which is then any change is detected on source, whole source data is transferred
    to target making a new snapshot there.

    In order to detect these changes, it will load both source and target data and compare
    row count, structure and each and every record.

    This mode could be used to transfer small-sized tables (dictionaries), especially if
    the table does not contain any change mark.

    The operator uses templated SQLs to retrieve data from source and target tables
    (`sql` and `destination_sql` respectively). By default, it expects that an
    "actual data view" named `<destination_table>_a` to exist on target. This view
    has to return latest target data snapshot (usually related to last successfull ETL session),
    see example DAGs for more details.

    See `transfer_table` for details on arguments.

    Returns:
        `XComArg` object

    """
    return _do_transfer_table(
        op_cls=ChangedTableTransfer,
        source=source,
        target=target,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        session=session,
        **kwargs)

def transfer_changed_tables(
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        group_id: str | None = None,
        num_parallel: int = 1,
        session: XComArg | ETLSession | None = None,
        **kwargs) -> TaskGroup:

    """ Transfer multiple changed tables.

    Creates an Airflow task group with transfer tasks for each table pair 
    from `source_tables` and `target_tables` lists.

    See `transfer_changed_table` for more information.
    """
    return _do_transfer_tables(
        op_cls=ChangedTableTransfer,
        source_tables=source_tables,
        target_tables=target_tables,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        group_id=group_id or 'transfer-dict',
        num_parallel=num_parallel,
        session=session,
        **kwargs
    )

def transfer_ods_table(
        source: str | BaseTable,
        target: str | BaseTable | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        session: XComArg | ETLSession | None = None,
        **kwargs) -> XComArg:
    
    """ Cross-database ODS-like data transfer.

    Returns:
        `XComArg` object

    """
    return _do_transfer_table(
        op_cls=OdsTableTransfer,
        source=source,
        target=target,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        session=session,
        **kwargs)

def transfer_ods_tables(
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        group_id: str | None = None,
        num_parallel: int = 1,
        session: XComArg | ETLSession | None = None,
        **kwargs) -> TaskGroup:

    """ Transfer multiple ODS tables.

    Creates an Airflow task group with transfer tasks for each table pair 
    from `source_tables` and `target_tables` lists.

    See `transfer_ods_table` for more information.
    """
    return _do_transfer_tables(
        op_cls=OdsTableTransfer,
        source_tables=source_tables,
        target_tables=target_tables,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        group_id=group_id or 'transfer-ods-tables',
        num_parallel=num_parallel,
        session=session,
        **kwargs
    )

def transfer_actuals_table(
        source: str | BaseTable,
        target: str | BaseTable | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        session: XComArg | ETLSession = None,
        transfer_delta: bool = True,
        as_ods: bool = False,
        keep_temp_table: bool = False,
        replace_data: bool = False,
        **kwargs) -> XComArg:
    
    """ Transfer table from stage to actuals.

    Returns:
        `XComArg` object

    """
    assert session is not None, 'Transfer to actuals requires ETL session'

    kwargs['transfer_delta'] = transfer_delta
    kwargs['as_ods'] = as_ods
    kwargs['keep_temp_table'] = keep_temp_table
    kwargs['replace_data'] = replace_data

    return _do_transfer_table(
        op_cls=ActualsTableTransfer,
        source=source,
        target=target,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        session=session,
        **kwargs)

def transfer_actuals_tables(
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        group_id: str | None = None,
        num_parallel: int = 1,
        session: XComArg | ETLSession | None = None,
        transfer_delta: bool = True,
        as_ods: bool = False,
        keep_temp_table: bool = False,
        replace_data: bool = False,
        **kwargs) -> TaskGroup:

    """ Transfer multiple tables from stage to actuals.
    """
    assert session is not None, 'Transfer to actuals requires ETL session'

    kwargs['transfer_delta'] = transfer_delta
    kwargs['as_ods'] = as_ods
    kwargs['keep_temp_table'] = keep_temp_table
    kwargs['replace_data'] = replace_data

    return _do_transfer_tables(
        op_cls=ActualsTableTransfer,
        source_tables=source_tables,
        target_tables=target_tables,
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        group_id=group_id or 'transfer-actuals',
        num_parallel=num_parallel,
        session=session,
        **kwargs
    )

def compare_table(
        source: str | BaseTable,
        target: str | BaseTable | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        **kwargs) -> TaskGroup:

    """ Compare two tables.

    Creates a task of `CompareTableTransfer` operator to compare
    given tables, which would fail if any differences
    are detected among them.

    Args:
        source: Either a table name or a `Table` object which would be a data source.
            If a string name is provided, it may contain schema definition denoted by `.`. 
            For `Table` objects, schema must be defined in `Metadata` field,
            otherwise Astro SDK might fall to use its default schema.
            If a SQL template exists for this table name, it will be executed,
            otherwise all table data will be selected.

        target: Either a table name or a `Table` object where data will be saved into.
            If a name is provided, it may contain schema definition denoted by `.`. 
            For `Table` objects, schema must be defined in `Metadata` field,
            otherwise Astro SDK might fall to use its default schema.
            If omitted, `source` argument value is used (this makes sense only
            with string table name and different connections).

        source_conn_id: Source database Airflow connection.
            Used only with string source table name; for `Table` objects, `conn_id` field is used.
            If omitted and `session` argument is provided, `session.source_conn_id` will be used.

        destination_conn_id: Destination database Airflow connection.
            Used only with string target table name; for `Table` objects, `conn_id` field is used.
            If omitted and `session` argument is provided, `session.destination_conn_id` will be used.

    """
    source_table = ensure_table(source, source_conn_id)
    dest_table = ensure_table(target or source_table, destination_conn_id)

    op = CompareTableOperator(
        source_table=source_table, 
        destination_table=dest_table, 
        task_id=f'compare-{source_table.name}')
    return XComArg(op)

def compare_tables(
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        group_id: str | None = None,
        num_parallel: int = 1,
        **kwargs) -> TaskGroup:

    """ Compares multiple tables.

    Creates an Airflow task group consisting of `CompareTableTransfer` operators 
    for each table pair from `source_tables` and `target_tables` lists.
    Each operator will compare source and target table and fails if any differences
    are detected among them.

    See `compare_table` for details.
    """
    if not target_tables or len(target_tables) != len(source_tables):
        raise AirflowFailException(f'Source and target tables list size must be equal')

    with TaskGroup(group_id or 'compare-tables', add_suffix_on_collision=True) as tg:
        ops_list = []
        for (source, target) in zip(source_tables, target_tables):
            source_table = ensure_table(source, source_conn_id)
            dest_table = ensure_table(target, destination_conn_id) or source_table
            op = CompareTableOperator(
                source_table=source_table, 
                destination_table=dest_table, 
                task_id=f'compare-{source_table.name}')
            ops_list.append(op)
        schedule_ops(ops_list, num_parallel)
    return tg

def update_timed_table(
        source: str | BaseTable,
        target: str | BaseTable | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        session: XComArg | ETLSession | None = None,
        **kwargs) -> XComArg:
    
    """ Updates timed dictionary table (experimental) """

    source_table = ensure_table(source, source_conn_id)
    dest_table = ensure_table(target, destination_conn_id) or source_table
    op = TimedTableTransfer(
        source_table=source_table,
        destination_table=dest_table,
        session=session,
        **kwargs_with_datasets(kwargs=kwargs, 
                               input_datasets=source_table, 
                               output_datasets=dest_table)
    )
    return XComArg(op)

def update_timed_tables(
        source_tables: Iterable[str | Table],
        target_tables: Iterable[str | Table] | None = None,
        source_conn_id: str | None = None,
        destination_conn_id: str | None = None,
        group_id: str | None = None,
        num_parallel: int = 1,
        session: XComArg | ETLSession | None = None,
        **kwargs) -> TaskGroup:

    """ Updates multiple timed dictionary tables (experimental )"""

    if target_tables and len(target_tables) != len(source_tables):
        raise AirflowFailException(f'Source and target tables list size must be equal')

    target_tables = target_tables or source_tables

    with TaskGroup(group_id or 'transfer-tables', add_suffix_on_collision=True) as tg:
        ops_list = []
        for (source, target) in zip(source_tables, target_tables):
            source_table = ensure_table(source, source_conn_id)
            dest_table = ensure_table(target, destination_conn_id) or source_table
            op = TimedTableTransfer(
                source_table=source_table,
                destination_table=dest_table,
                session=session,
                **kwargs_with_datasets(
                    kwargs=kwargs, 
                    input_datasets=source_table, 
                    output_datasets=dest_table))
            ops_list.append(op)
        schedule_ops(ops_list, num_parallel)
    return tg

