# Astro SDK Extras project
# (c) kol, 2023

""" Table operations """

import pandas as pd
import logging
from airflow.hooks.base import BaseHook
from airflow.models.xcom_arg import XComArg
from airflow.models.dag import DagContext
from airflow.operators.generic_transfer import GenericTransfer
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException

from astro import sql as aql
from astro.databases import create_database
from astro.sql.table import BaseTable, Table
from astro.databases.base import BaseDatabase
from astro.airflow.datasets import kwargs_with_datasets

from typing import Union, Literal, Optional, Iterable, List

from .session import ETLSession, ensure_session
from ..utils.utils import ensure_table, schedule_ops
from ..utils.template import get_template_file
from ..utils.data_compare import compare_datasets

class TableTransfer(GenericTransfer):
    """ Customized table transfer operator. Usually is used within `transfer_table` function """

    template_fields = ("sql", "preoperator", "source_table", "destination_table", "destination_sql")
    template_ext = (".sql", ".hql" )
    template_fields_renderers = {"sql": "sql", "preoperator": "sql", "destination_sql": "sql"}
    ui_color = "#b0f07c"

    def __init__(
        self,
        *,
        source_table: BaseTable,
        destination_table: BaseTable,
        mode: Optional[Literal['default', 'dict', 'sync']] = 'default',
        session: Optional[ETLSession] = None,
        **kwargs,
    ) -> None:

        source_db = create_database(source_table.conn_id, source_table)
        dest_db = create_database(destination_table.conn_id, destination_table)

        task_id = kwargs.pop('task_id', f'transfer-{source_table.name}')
        sql = kwargs.pop('sql', self._get_sql(source_table, source_db, session))
        dest_sql = kwargs.pop('destination_sql', self._get_sql(destination_table, dest_db, suffix='_a'))

        super().__init__(task_id=task_id,
                         sql=sql,
                         destination_table=dest_db.get_table_qualified_name(destination_table),
                         source_conn_id=source_table.conn_id,
                         destination_conn_id=destination_table.conn_id,
                         **kwargs)

        self.source: BaseTable = source_table
        self.source_table: str = source_db.get_table_qualified_name(self.source)
        self.destination: BaseTable = destination_table
        # self.destination_table is set by super().__init__()
        self.destination_sql: str = dest_sql
        self.mode: str = mode
        self.session: ETLSession = session

    def _get_sql(self, table: Table, db: BaseDatabase, session: ETLSession = None, suffix: str = None) -> str:
        """ Internal - get a sql statement or template for given table """

        if (sql_file := get_template_file(table.name, '.sql')):
            self.log.info(f'Using template file {sql_file}')
            return sql_file

        full_name = db.get_table_qualified_name(table) + (suffix or '')
        if session:
            return 'select {{ti.xcom_pull(key="session").session_id}} as session_id, * from ' + full_name

        return 'select * from ' + full_name

    def _compare_datasets(self, stop_on_first_diff: bool) -> bool:

        self.log.info(f'Comparing source {self.source_table} with target {self.destination_table}')
        src = BaseHook.get_hook(self.source_conn_id)
        dest = BaseHook.get_hook(self.destination_conn_id)

        self.log.info(f'Executing: {self.sql}')
        df_src = src.get_pandas_df(self.sql)

        self.log.info(f'Executing: {self.destination_sql}')
        df_trg = dest.get_pandas_df(self.destination_sql)
        self.log.info(f'Number of records: {df_src.shape[0]} / {df_trg.shape[0]}')

        return compare_datasets(df_src, df_trg, stop_on_first_diff=stop_on_first_diff)

    def execute(self, context: Context):

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

        match self.mode:
            case 'default':
                return super().execute(context)
            case 'dict':
                if not self._compare_datasets(stop_on_first_diff=True):
                    return super().execute(context)
            case _:
                raise AirflowFailException(f'Invalid or unsupported transfer mode: {self.mode}')

def load_table(
        table: Union[str, BaseTable],
        conn_id: Optional[str] = None,
        session: Optional[ETLSession] = None,
        sql: Optional[str] = None) -> XComArg:
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
            Used only to link up to the `open_session` operator.
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
        def _load_table_by_name(table: str, session: ETLSession):
            sql_file = get_template_file(table, '.sql', dag=dag)
            return sql or sql_file or f'select * from {table}'

        return _load_table_by_name(table, session)
    else:
        dag = DagContext.get_current_dag()
        @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                        conn_id=conn_id,
                        task_id=f'load-{table.name}',
                        results_format='pandas_dataframe')
        def _load_table(table: Table, session: ETLSession):
            sql_file = get_template_file(table.name, '.sql', dag=dag)
            return sql or sql_file or '''select * from {{table}}'''

        return _load_table(table, session)

def save_table(
        data: XComArg,
        table: Union[str, BaseTable],
        conn_id: Optional[str] = None,
        session: Optional[ETLSession] = None,
        fail_if_not_exist: Optional[bool] = True) -> XComArg:
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

def transfer_table(
        source: Union[str, Table],
        target: Union[str, Table, None] = None,
        mode: Optional[Literal['default', 'dict', 'sync']] = 'default',
        source_conn_id: Optional[str] = None,
        destination_conn_id: Optional[str] = None,
        session: Union[XComArg, ETLSession, None] = None,
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

        create table source_data ( a int, b text );
        create table target_data ( session_id int, b text, a int );

    then, this SQL template might be created as `source_data.sql` file:

        select {{ti.xcom_pull(key="session").session_id}} as session_id, b, a 
        from source_data;

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

        mode: Reserved for furter use

        source_conn_id: Source database Airflow connection.
            Used only with string source table name; for `Table` objects, `conn_id` field is used.
            If omitted and `session` argument is provided, `session.source_conn_id` will be used.

        destination_conn_id: Destination database Airflow connection.
            Used only with string target table name; for `Table` objects, `conn_id` field is used.
            If omitted and `session` argument is provided, `session.destination_conn_id` will be used.

        session:    `ETLSession` object. If set and no SQL template is defined,
            a `session_id` field will be automatically added to selection.

        kwargs:     Any parameters passed to underlying `TableTransfer` operator (e.g. `preoperator`, ...)

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

    source_table = ensure_table(source, source_conn_id)
    dest_table = ensure_table(target, destination_conn_id) or source_table
    op = TableTransfer(
        source_table=source_table,
        destination_table=dest_table,
        mode=mode,
        session=session,
        **kwargs_with_datasets(kwargs=kwargs, 
                               input_datasets=source_table, 
                               output_datasets=dest_table)
    )
    return XComArg(op)

def declare_tables(
        table_names: Iterable[str],
        conn_id: Optional[str] = None
) -> List[Table]:
    """ Convert list of string table names to list of `Table` objects """

    return [ensure_table(t, conn_id) for t in table_names]

def transfer_tables(
        source_tables: List[Union[str, Table]],
        target_tables: Optional[List[Union[str, Table]]] = None,
        mode: Optional[Literal['default', 'dict', 'full']] = 'default',
        source_conn_id: Optional[str] = None,
        destination_conn_id: Optional[str] = None,
        group_id: Optional[str] = None,
        num_parallel: Optional[int] = 1,
        session: Union[XComArg, ETLSession, None] = None,
        **kwargs) -> TaskGroup:
    """ Transfer multiple tables """

    if target_tables and len(target_tables) != len(source_tables):
        raise AirflowFailException(f'Source and target tables list size must be equal')

    target_tables = target_tables or source_tables

    with TaskGroup(group_id or 'transfer-tables', add_suffix_on_collision=True) as tg:
        ops_list = []
        for (source, target) in zip(source_tables, target_tables):
            source_table = ensure_table(source, source_conn_id)
            dest_table = ensure_table(target, destination_conn_id) or source_table
            op = TableTransfer(
                source_table=source_table,
                destination_table=dest_table,
                mode=mode,
                session=session,
                **kwargs_with_datasets(
                    kwargs=kwargs, 
                    input_datasets=source_table, 
                    output_datasets=dest_table))
            ops_list.append(op)
        schedule_ops(ops_list, num_parallel)
    return tg
