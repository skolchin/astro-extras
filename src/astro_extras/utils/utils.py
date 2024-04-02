# Astro SDK Extras project
# (c) kol, 2023

""" Misc utility functions """

from itertools import pairwise, groupby
from airflow.models import BaseOperator
from astro.sql.table import BaseTable, Table, Metadata
from typing import Union, Tuple, Optional, List

def split_table_name(table: str) -> Tuple[Optional[str], str]:
    """ Splits table name to schema and table name """
    if not table:
        raise ValueError('Table name not specified')
    parts = table.split('.', 1)
    return tuple(parts) if len(parts) > 1 else (None, parts[0])

def ensure_table(
        table: Union[str, BaseTable], 
        conn_id: Optional[str] = None, 
        schema: Optional[str] = None,
        database: Optional[str] = None) -> BaseTable:
    
    """ Ensure an object is a table """
    if table is None:
        return None
    if isinstance(table, BaseTable):
        return table
    if isinstance(table, str):
        schema_from_name, table = split_table_name(table)
        return Table(table, conn_id=conn_id, metadata=Metadata(schema=schema_from_name or schema, database=database))
    
    raise TypeError(f'Either str or BaseTable expected, {table.__class__.__name__} found')

def schedule_ops(ops_list: List[BaseOperator], num_parallel: int = 1) -> List[BaseOperator]:
    """ Build parallel operator chains. Returns list of last operators in each chain. """

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
