# Astro SDK Extras project
# (c) kol, 2023

""" Misc utility functions """

from itertools import pairwise, groupby
from airflow.models import BaseOperator

from astro.sql.table import Table, Metadata

from typing import Union, Tuple, Optional, List

def split_table_name(table: str) -> Tuple[Optional[str], str]:
    """ Splits table name to schema and table name """
    if not table:
        raise ValueError('Table name not specified')
    parts = table.split('.', 1)
    return tuple(parts) if len(parts) > 1 else (None, parts[0])

def ensure_table(table: Union[str, Table], conn_id: Optional[str] = None) -> Table:
    """ Ensure an object passed in is a table"""
    if table is None:
        return None
    if isinstance(table, Table):
        return table
    if isinstance(table, str):
        schema, table = split_table_name(table)
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
