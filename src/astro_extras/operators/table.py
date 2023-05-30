# Astro SDK Extras project
# (c) kol, 2023

""" Table operations """

import pandas as pd
from airflow.models.xcom_arg import XComArg
from airflow.operators.generic_transfer import GenericTransfer
from airflow.utils.context import Context
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException

from astro import sql as aql
from astro.sql.table import Table, Metadata
from astro.airflow.datasets import kwargs_with_datasets

from typing import Union, Literal, Optional, Iterable, List

from .session import ETLSession, ensure_session
from ..utils.utils import (
    get_table_template, split_table_name, full_table_name, ensure_table,
    schedule_ops
)

class TableTransfer(GenericTransfer):
    template_fields = ("sql", "source_table", "destination_table", "preoperator")
    template_ext = (".sql", ".hql" )
    template_fields_renderers = {"preoperator": "sql"}
    ui_color = "#b0f07c"

    def __init__(
        self,
        *,
        source_table: str,
        destination_table: str,
        source_conn_id: Optional[str] = None,
        destination_conn_id: Optional[str] = None,
        mode: Optional[Literal['default', 'delta', 'full']] = 'default',
        session: Optional[ETLSession] = None,
        **kwargs,
    ) -> None:

        task_id = kwargs.pop('task_id', f'transfer-{split_table_name(source_table)[1]}')
        sql = kwargs.pop('sql', self._get_sql(source_table, session))

        super().__init__(task_id=task_id,
                         sql=sql,
                         destination_table=destination_table,
                         source_conn_id=source_conn_id,
                         destination_conn_id=destination_conn_id,
                         **kwargs)

        self.source_table = source_table
        self.mode = mode
        self.session = session

    def _get_sql(self, source_table: str, session: ETLSession) -> str:
        if (sql_file := get_table_template(source_table, '.sql')):
            self.log.info(f'Using template file {sql_file}')
            return sql_file
        if session:
            return 'select {{ti.xcom_pull(key="session").session_id}} as session_id, * from ' + source_table
        return 'select * from ' + source_table

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

        match self.mode:
            case 'default':
                return super().execute(context)
            case _:
                raise AirflowFailException(f'Invalid or unsupported transfer mode: {self.mode}')

def load_table(
        table: Union[str, Table],
        conn_id: Optional[str] = None,
        session: Optional[ETLSession] = None,
        sql: Optional[str] = None) -> Table:

        if not isinstance(table, Table):
            @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                            conn_id=conn_id,
                            task_id=f'load-{table}',
                            results_format='pandas_dataframe')
            def _load_table_by_name(table: str, session: ETLSession):
                sql_file = get_table_template(table, '.sql')
                return sql or sql_file or f'select * from {table}'

            return _load_table_by_name(table, session)
        else:
            @aql.run_raw_sql(handler=lambda result: result.fetchall(),
                            conn_id=conn_id,
                            task_id=f'load-{table.name}',
                            results_format='pandas_dataframe')
            def _load_table(table: Table, session: ETLSession):
                sql_file = get_table_template(table.name, '.sql')
                return sql or sql_file or '''select * from {{table}}'''

            return _load_table(table, session)

def save_table(
        data: pd.DataFrame,
        table: Union[str, Table],
        conn_id: Optional[str] = None,
        session: Optional[ETLSession] = None):

    if isinstance(table, Table):
        task_id = f'save-{table.name}'
    else:
        schema, table = split_table_name(table)
        task_id = f'save-{table}'
        table = Table(table, conn_id=conn_id, metadata=Metadata(schema=schema))

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

    target = target or source
    op = TableTransfer(
        source_table=full_table_name(source),
        destination_table=full_table_name(target),
        source_conn_id=source_conn_id,
        destination_conn_id=destination_conn_id,
        mode=mode,
        session=session,
        **kwargs_with_datasets(kwargs=kwargs, 
                               input_datasets=ensure_table(source, source_conn_id), 
                               output_datasets=ensure_table(target, destination_conn_id))
    )
    return XComArg(op)

def declare_tables(
        table_names: Iterable[str],
        conn_id: Optional[str] = None,
        schema: Optional[str] = None,
) -> List[Table]:
    return [Table(t, conn_id=conn_id, metadata=Metadata(schema=schema)) for t in table_names]

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
            op = TableTransfer(
                source_table=full_table_name(source),
                destination_table=full_table_name(target),
                source_conn_id=source_conn_id,
                destination_conn_id=destination_conn_id,
                mode=mode,
                session=session,
                **kwargs_with_datasets(kwargs=kwargs, 
                                    input_datasets=ensure_table(source, source_conn_id), 
                                    output_datasets=ensure_table(target, destination_conn_id)))
            ops_list.append(op)
        schedule_ops(ops_list, num_parallel)
    return tg
