# Astro SDK Extras project
# (c) kol, 2023

""" Misc utility functions """

import os
from itertools import pairwise, groupby
from airflow.models import DAG, BaseOperator
from airflow.models.dag import DagContext
from airflow.configuration import conf
from astro.sql.table import Table, Metadata

from typing import Union, Tuple, Optional, List

def get_table_template(table: str, ext: Optional[str] = '.sql', dag: Optional[DAG] = None) -> Optional[str]:
    """ Returns name of template file for given table """

    _, table = split_table_name(table)
    dag = dag or DagContext.get_current_dag()
    ext = '.' + ext if not ext.startswith('.') else ext
    rel_file_name = os.path.join('templates', dag.dag_id, table + ext)
    full_file_name = os.path.join(conf.get('core','dags_folder'), rel_file_name)
    return rel_file_name if os.path.exists(full_file_name) else None

def split_table_name(table: str) -> Tuple[Optional[str], str]:
    """ Splits table name to schema and table name """
    if not table:
        raise ValueError('Table name not specified')
    parts = table.split('.', 1)
    return tuple(parts) if len(parts) > 1 else (None, parts[0])

def full_table_name(table: Union[str, Table]) -> str:
    """ Construct fully-qualified name containing schema (if set) and actual table name """
    if table is None:
        return None
    if isinstance(table, Table):
        return table.metadata.schema + '.' + table.name if table.metadata.schema else table.name
    if isinstance(table, str):
        return table
    raise TypeError(f'Either str or Table expected, {table.__class__.__name__} found')

def ensure_table(table: Union[str, Table], conn_id: Optional[str] = None, schema: Optional[str] = None) -> Table:
    """ Ensure an object passed in is a table"""
    if table is None:
        return None
    if isinstance(table, Table):
        return table
    if isinstance(table, str):
        return Table(table, conn_id=conn_id, metadata=Metadata(schema=schema))
    raise TypeError(f'Either str or Table expected, {table.__class__.__name__} found')

def schedule_ops(ops_list: List[BaseOperator], num_parallel: int = 1) -> BaseOperator:
    """ Build a linked operators list """

    if not ops_list:
        raise ValueError('Empty list')
    if len(ops_list) == 1:
        return ops_list[0]
    
    if num_parallel <= 1 or len(ops_list) <= num_parallel:
        _ = [a.set_downstream(b) for a, b in pairwise(ops_list)]
        return [ops_list[-1]]

    splits = list({k: [y[1] for y in g] \
        for k, g in groupby(enumerate(ops_list), lambda v: v[0] // (len(ops_list) // num_parallel))}.values())
    _ = [a.set_downstream(b) for g in splits for a, b in pairwise(g)]
    return [x[-1] for x in splits]
