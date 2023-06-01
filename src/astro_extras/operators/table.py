# Astro SDK Extras project
# (c) kol, 2023

""" Table operations """

import pandas as pd
from airflow.models.xcom_arg import XComArg
from airflow.models.dag import DagContext
from airflow.operators.generic_transfer import GenericTransfer
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException

from astro import sql as aql
from astro.databases import create_database
from astro.sql.table import Table
from astro.databases.base import BaseDatabase
from astro.airflow.datasets import kwargs_with_datasets

from typing import Union, Literal, Optional, Iterable, List

from .session import ETLSession, ensure_session
from ..utils.utils import get_table_template, ensure_table, schedule_ops

class TableTransfer(GenericTransfer):
    """ Customized table transfer operator. Usually is used within `transfer_table` function """

    template_fields = ("sql", "preoperator", "source_table", "destination_table")
    template_ext = (".sql", ".hql" )
    template_fields_renderers = {"sql": "sql", "preoperator": "sql"}
    ui_color = "#b0f07c"

    def __init__(
        self,
        *,
        source_table: Table,
        destination_table: Table,
        mode: Optional[Literal['default', 'delta', 'full']] = 'default',
        session: Optional[ETLSession] = None,
        **kwargs,
    ) -> None:

        source_db = create_database(source_table.conn_id, source_table)
        dest_db = create_database(destination_table.conn_id, destination_table)

        task_id = kwargs.pop('task_id', f'transfer-{source_table.name}')
        sql = kwargs.pop('sql', self._get_sql(source_table, source_db, session))

        super().__init__(task_id=task_id,
                         sql=sql,
                         destination_table=dest_db.get_table_qualified_name(destination_table),
                         source_conn_id=source_table.conn_id,
                         destination_conn_id=destination_table.conn_id,
                         **kwargs)

        self.source = source_table
        self.source_table = source_db.get_table_qualified_name(source_table)
        self.destination = destination_table
        self.mode = mode
        self.session = session

    def _get_sql(self, source_table: Table, source_db: BaseDatabase, session: ETLSession) -> str:
        """ Internal - get a sql statement or template for given table """
        if (sql_file := get_table_template(source_table.name, '.sql')):
            self.log.info(f'Using template file {sql_file}')
            return sql_file
        
        full_name = source_db.get_table_qualified_name(source_table)
        if session:
            return 'select {{ti.xcom_pull(key="session").session_id}} as session_id, * from ' + full_name
        return 'select * from ' + full_name

    def execute(self, context: Context):
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

        # TODO: more transfer modes
        match self.mode:
            case 'default':
                return super().execute(context)
            case _:
                raise AirflowFailException(f'Invalid or unsupported transfer mode: {self.mode}')

def load_table(
        table: Union[str, Table],
        conn_id: Optional[str] = None,
        session: Optional[ETLSession] = None,
        sql: Optional[str] = None) -> XComArg:
    """ Loads table into memory.

    This is a wrapper over Astro-SDK `run_raw_sql` to
    load data from given database table into XCom and make it available for 
    further processing.

    SQL templating is supported, e.g. if a template for given table was found, it
    will be executed to get the data (see `astro_extras.utils.utils.get_table_template`).

    Please note that in order to operate even on modest volumes of data,
    intermediate XCom storage might be required. Easiest way to set it up is to use
    local Parquet file storage by setting
    `AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID=local` environment
    variable. However, this will add extra serialization/deserialization
    operation to every task thus increasing overall DAG execution time.

    See https://astro-sdk-python.readthedocs.io/en/1.2.0/guides/xcom_backend.html
    for details.

    Args:
        table:  Either a table name or Astro-SDK `Table` object
            to load data from
        conn_id:    Airflow connection ID to underlying database. If not specified,
            and `Table` object is passed it, its connection will be used.
        session:    `astro_extras.operators.session.ETLSession` object. 
            Used only to link up to the `open_session` operator.
        sql:    Custom SQL to load data, used only if no SQL template was found.
            If empty, all table data will be loaded.

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

    if not isinstance(table, Table):
        dag = DagContext.get_current_dag()
        @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                        conn_id=conn_id,
                        task_id=f'load-{table}',
                        results_format='pandas_dataframe')
        def _load_table_by_name(table: str, session: ETLSession):
            sql_file = get_table_template(table, '.sql', dag=dag)
            return sql or sql_file or f'select * from {table}'

        return _load_table_by_name(table, session)
    else:
        dag = DagContext.get_current_dag()
        @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                        conn_id=conn_id,
                        task_id=f'load-{table.name}',
                        results_format='pandas_dataframe')
        def _load_table(table: Table, session: ETLSession):
            sql_file = get_table_template(table.name, '.sql', dag=dag)
            return sql or sql_file or '''select * from {{table}}'''

        return _load_table(table, session)

def save_table(
        data: XComArg,
        table: Union[str, Table],
        conn_id: Optional[str] = None,
        session: Optional[ETLSession] = None) -> XComArg:
    """ Saves a table into database """

    if isinstance(table, Table):
        task_id = f'save-{table.name}'
    else:
        task_id = f'save-{table}'
        table = ensure_table(table, conn_id)

    @aql.dataframe(if_exists='append', conn_id=conn_id, task_id=task_id)
    def _save_data(data: pd.DataFrame, session: ETLSession):
        session = ensure_session(session)
        if session and 'session_id' not in data.columns:
            data.insert(0, 'session_id', session.session_id)
        return data

    return _save_data(data, session, output_table=table)

def transfer_table(
        source: Union[str, Table],
        target: Union[str, Table, None] = None,
        mode: Optional[Literal['default', 'delta', 'full']] = 'default',
        source_conn_id: Optional[str] = None,
        destination_conn_id: Optional[str] = None,
        session: Union[XComArg, ETLSession, None] = None,
        **kwargs) -> XComArg:
    
    """ Transfer a table from source to destination database """

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
    """ Define list of `Table` objects """
    return [ensure_table(t, conn_id) for t in table_names]

def transfer_tables(
        source_tables: Iterable[Union[str, Table]],
        target_tables: Optional[Iterable[Union[str, Table]]] = None,
        mode: Optional[Literal['default', 'delta', 'full']] = 'default',
        source_conn_id: Optional[str] = None,
        destination_conn_id: Optional[str] = None,
        group_id: Optional[str] = None,
        num_parallel: Optional[int] = 1,
        session: Union[XComArg, ETLSession, None] = None,
        **kwargs) -> TaskGroup:

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
