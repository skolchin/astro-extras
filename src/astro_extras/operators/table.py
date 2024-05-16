# Astro SDK Extras project
# (c) kol, 2023-2024

""" Table operations """

import logging
import pandas as pd
import warnings
from sqlalchemy import MetaData as SqlaMetadata, Table as SqlaTable

from airflow.models.dag import DagContext
from airflow.utils.context import Context
from airflow.models.xcom_arg import XComArg
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.operators.generic_transfer import GenericTransfer

from astro import sql as aql
from astro.databases import create_database
from astro.sql.table import BaseTable, Table
from astro.databases.base import BaseDatabase
from astro.query_modifier import QueryModifier
from astro.airflow.datasets import kwargs_with_datasets

from .session import ETLSession, ensure_session
from ..utils.utils import ensure_table, schedule_ops, is_same_database_uri
from ..utils.template import get_template, get_template_file, get_predefined_template
from ..utils.data_compare import compare_datasets, compare_timed_dict

from typing import Iterable, Type

class TableTransfer(GenericTransfer):
    """
    Table transfer operator to be used within `transfer_table` function.
    Implements 'bulk' data transfer without any extra conditioning.
    """

    template_fields = ("sql", "preoperator", "source_table", "destination_table")
    template_ext = (".sql", ".hql" )
    template_fields_renderers = {"sql": "sql", "preoperator": "sql"}
    ui_color = "#b4f07c"

    def __init__(
        self,
        *,
        source_table: BaseTable,
        destination_table: BaseTable,
        session: ETLSession | None = None,
        in_memory_transfer: bool = False,
        **kwargs,
    ) -> None:

        self.source_db: BaseDatabase = create_database(source_table.conn_id)
        self.dest_db: BaseDatabase = create_database(destination_table.conn_id)
        sql: str = kwargs.pop('sql', self._get_sql(source_table, self.source_db, session))

        task_id = kwargs.pop('task_id', f'transfer-{source_table.name}')
        super().__init__(task_id=task_id,
                         sql=sql,
                         destination_table=self.dest_db.get_table_qualified_name(destination_table),
                         source_conn_id=source_table.conn_id,
                         destination_conn_id=destination_table.conn_id,
                         **kwargs)

        self.source: BaseTable = source_table
        self.source_table: str = self.source_db.get_table_qualified_name(self.source)
        self.destination: BaseTable = destination_table
        # self.destination_table is set by super().__init__()
        self.session: ETLSession = session
        self.in_memory_transfer: bool = in_memory_transfer
        self._pre_execute_called = False

    def _get_sql(self, table: BaseTable, db: BaseDatabase, session: ETLSession | None = None, suffix: str | None = None) -> str:
        """ Internal - get a sql statement or template for given table """

        # Check whether a template SQL exists for given table under dags\templates\<dag_id>
        # Actual query will be loaded by Airflow templating itself
        if (sql_file := get_template_file(table.name, '.sql')):
            self.log.info(f'Using template file {sql_file}')
            return sql_file

        # Nope, load an SQL from package resources substituting template fields manually
        # SQL file names are fixed according to whether we do run under ETL session or not
        full_name = db.get_table_qualified_name(table) + (suffix or '')
        template_name = 'table_transfer_nosess.sql' if not session else 'table_transfer_sess.sql'
        template = get_predefined_template(template_name)
        return template.render(source_table=full_name)

    def _pre_execute(self, context: Context):
        """ Do all checks before execution """
        if self._pre_execute_called:
            return
        
        assert 'dag_run' in context
        if context['dag_run'].conf.get('debug'):
            self.log.setLevel(logging.DEBUG)
            logging.getLogger('airflow.task').setLevel(logging.DEBUG)

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

    def execute(self, context: Context):
        """ Execute operator """
        self._pre_execute(context)
        if not self.in_memory_transfer:
            return super().execute(context)
        
        # upload source data to memory
        src: DbApiHook = DbApiHook.get_hook(self.source_conn_id)
        data = src.get_pandas_df(self.sql)
        if data is None or data.empty:
            self.log.info('No data to transfer')
            return
        
        # Save to target table
        self.log.info(f'{len(data)} records to be transferred')
        self.dest_db.load_pandas_dataframe_to_table(data, self.destination, if_exists='append')

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

    def _compare_datasets(self, stop_on_first_diff: bool, logger: logging.Logger | None = None):
        """ Internal - compare source and target dictionaries """

        logger = logger or self.log
        src = DbApiHook.get_hook(self.source_conn_id)
        dest = DbApiHook.get_hook(self.destination_conn_id)

        logger.info(f'Executing: {self.sql}')
        df_src = src.get_pandas_df(self.sql)
        logger.info(f'Executing: {self.destination_sql}')
        df_trg = dest.get_pandas_df(self.destination_sql)

        return compare_datasets(df_src, df_trg, stop_on_first_diff=stop_on_first_diff, logger=logger)

    def execute(self, context: Context):
        """ Execute operator """
        self._pre_execute(context)
        if not self._compare_datasets(stop_on_first_diff=True):
            return super().execute(context)

class OdsTableTransfer(ChangedTableTransfer):
    """
    Table transfer operator to be used within `transfer_ods_table` function
    to transfer data from source to ODS-style target.
    
    Requiures `xxx_a` view to exist on target. Target table must have
    `_modified' and `_deleted` attributes of `timestamp` or `timestamptz` type.
    """
    ui_color = "#78f07c"

    def _save_data(self, df: pd.DataFrame, modified: pd.Timestamp, deleted: pd.Timestamp | None, category: str) -> None:
        if df is not None and not df.empty:
            if self.session is not None:
                df.drop(columns=set(['session_id', '_modified', '_deleted']) & set(df.columns), inplace=True)
                df.insert(0, '_deleted', deleted)
                df.insert(0, '_modified', modified)
                df.insert(0, 'session_id', self.session.session_id)
            else:
                df['_modified'] = modified
                df['_deleted'] = deleted

            self.log.info(f'Saving {df.shape[0]} {category} records to {self.destination_table}')
            self.dest_db.load_pandas_dataframe_to_table(df, self.destination, if_exists='append')

    def execute(self, context: Context):
        """ Execute operator """

        # All the checks
        self._pre_execute(context)

        # compare datasets and save delta frames to target (new/modified/deleted)
        dfn, dfm, dfd = self._compare_datasets(stop_on_first_diff=False)
        self._save_data(dfn, pd.Timestamp.utcnow(), pd.NaT, 'new')
        self._save_data(dfm, pd.Timestamp.utcnow(), pd.NaT, 'modified')
        self._save_data(dfd, pd.Timestamp.utcnow(), pd.Timestamp.utcnow(), 'deleted')

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
        as_ods: bool = False,
        keep_temp_table: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(source_table=source_table, destination_table=destination_table, session=session, **kwargs)
        self.as_ods = as_ods
        self.keep_temp_table = keep_temp_table

    def _get_sql(self, table: BaseTable, db: BaseDatabase, session: ETLSession | None = None, suffix: str | None = None) -> str:
        """ Internal - get a sql statement or template for given table """

        # If an SQL template is defined for given table, wrap it as a subquery,
        # so resulting query would be like `select * from (select * from ...)`
        full_name = db.get_table_qualified_name(table)
        if (sql := get_template(table.name, '.sql', fail_if_not_found=False)):
            full_name = f'({sql})'

        # Get template SQL from package resources
        # The template defines `select` query which takes data from 
        # source actual data view (<source_table>_a) limited 
        # for successfull sessions with the same period as this operator has
        template = get_predefined_template('table_actuals_select.sql')
        return template.render(source_table=full_name)

    def execute(self, context: Context):
        """ Execute operator """

        # All the checks
        self._pre_execute(context)

        # Get the hooks to setup the transfer
        src: DbApiHook = DbApiHook.get_hook(self.source_conn_id)
        dest: DbApiHook = DbApiHook.get_hook(self.destination_conn_id)

        with src.get_sqlalchemy_engine().connect() as src_conn, dest.get_sqlalchemy_engine().connect() as dest_conn:
            # Load source and target table structures using SQL alchemy
            src_meta = SqlaMetadata(src_conn, schema=self.source.metadata.schema if self.source.metadata else None)
            src_meta.reflect(only=[self.source.name])
            src_sqla_table = src_meta.tables[self.source_table]

            dest_meta = SqlaMetadata(dest_conn, schema=self.destination.metadata.schema if self.destination.metadata else None)
            dest_meta.reflect(only=[self.destination.name])
            dest_sqla_table = dest_meta.tables[self.destination_table]

            if self.as_ods:
                # Check source and target meet the ODS requirements
                if '_deleted' not in src_sqla_table.columns:
                    raise AirflowFailException(f'Source table {self.source_table} does not have `_deleted` column required for ODS-style transfer')
                if '_deleted' not in dest_sqla_table.columns:
                    raise AirflowFailException(f'Target table {self.destination_table} does not have `_deleted` column required for ODS-style transfer')

            # Build a source-to-target column mapping
            col_map = {}
            id_cols = []
            for src_col in src_sqla_table.columns:
                if (dest_col := dest_sqla_table.columns.get(src_col.name)) is None:
                    self.log.warning(f'Column {src_col.name} does not exist in {self.destination_table} table, skipping')
                else:
                    # If a column is PK, store it to use in `on conflict` statement part
                    col_map[src_col.name] = dest_col.name
                    if dest_col.primary_key:
                        id_cols.append(dest_col.name)

            # Check there are any ID columns on target
            if not id_cols:
                raise AirflowFailException(f'Could not detect primary key on {self.destination_table}')

            # Wrap data selection query into `select distinct on` in order to avoid having multiple IDs error
            id_cols_str = ",".join(id_cols)
            sql = f'select distinct on ({id_cols_str}) * from ({self.sql}) q order by {id_cols_str}, session_id desc'
            self.log.info(f'Source extraction SQL:\n{sql}')

            # Check whether the hooks point to the same database
            same_db = is_same_database_uri(src.get_uri(), dest.get_uri())

            # Upload source data to temporary table in destination database
            temp_table = Table(metadata=self.destination.metadata, temp=True)
            if same_db:
                # Source and destination tables are in the same database, use `insert` query
                # Query modifier is required to set proper session_id (source table already contains this column)
                self.log.info(f'Source and destination tables are in the same database')
                qm = None if not self.session else \
                    QueryModifier(post_queries=[f'update {self.dest_db.get_table_qualified_name(temp_table)} set session_id={self.session.session_id}'])
                self.dest_db.create_table_from_select_statement(sql, temp_table, query_modifier=qm)
            else:
                # Source and destination tables are in different databases
                self.log.info(f'Source and destination tables are in different databases, using in-memory transfer')
                df = src.get_pandas_df(sql)
                if self.session:
                    df['session_id'] = self.session.session_id
                    col_map['session_id'] = 'session_id'

                # Temp table has to be created 1st, otherwise it might be column types mismatch
                temp_sqla_table = SqlaTable(temp_table.name, dest_meta, *([c.copy() for c in src_sqla_table.columns]))
                dest_meta.create_all(dest_conn, tables=[temp_sqla_table], checkfirst=False)
                self.dest_db.load_pandas_dataframe_to_table(df, temp_table, if_exists='append')

        # Count rows
        row_count = self.dest_db.row_count(temp_table)
        self.log.info(f'Number of rows selected: {row_count}')

        # Merge temporary and target tables
        try:
            self.log.info(f'Merging from {temp_table.name} to {self.destination_table}')
            self.dest_db.merge_table(
                source_table=temp_table,
                target_table=self.destination,
                source_to_target_columns_map=col_map,
                target_conflict_columns=id_cols,
                if_conflicts='update',
            )
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
        src: DbApiHook = DbApiHook.get_hook(self.source_conn_id)
        dest: DbApiHook = DbApiHook.get_hook(self.destination_conn_id)

        self.log.info(f'Executing: {self.sql}')
        df_src = src.get_pandas_df(self.sql)
        self.log.info(f'Executing: {self.destination_sql}')
        df_trg = dest.get_pandas_df(self.destination_sql)

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
            self.dest_db.run_single_sql_query(update_sql, parameters=update_params)

        # Insert new records
        if df_opening is not None and not df_opening.empty:
            self.dest_db.load_pandas_dataframe_to_table(df_opening, self.destination, if_exists='append')

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
        if not self._compare_datasets(stop_on_first_diff=True, logger=logger):
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
) -> Iterable[BaseTable]:
    """ Convert list of string table names to list of `Table` objects """

    return [ensure_table(t, conn_id, schema, database) for t in table_names]

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
        op_cls=OdsTableTransfer,
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
        as_ods: bool = False,
        **kwargs) -> XComArg:
    
    """ Transfer table from stage to actuals.

    Returns:
        `XComArg` object

    """
    assert session is not None, 'Transfer to actuals requires ETL session'
    kwargs['as_ods'] = as_ods

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
        as_ods: bool = False,
        **kwargs) -> TaskGroup:

    """ Transfer multiple tables from stage to actuals.
    """
    assert session is not None, 'Transfer to actuals requires ETL session'
    kwargs['as_ods'] = as_ods

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

