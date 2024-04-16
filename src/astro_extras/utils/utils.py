# Astro SDK Extras project
# (c) kol, 2023

""" Misc utility functions """

from itertools import pairwise, groupby
from airflow.models import BaseOperator
from astro.sql.table import BaseTable, Table, Metadata
from typing import Union, Tuple, Optional, List, Iterable

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
    elif isinstance(table, str):
        schema_from_name, table = split_table_name(table)
        return Table(table, conn_id=conn_id, metadata=Metadata(schema=schema_from_name or schema, database=database))
    else:
        raise TypeError(f'Either str or BaseTable expected, {table.__class__.__name__} found')

def split_iterable(it: Iterable, num_splits: int = 1) -> Iterable:
    splits = [None] * num_splits
    op_iter = iter(it)
    try:
        while True:
            for n_split in range(num_splits):
                op = next(op_iter)
                if splits[n_split] is None:
                    splits[n_split] = [op]
                else:
                    splits[n_split].append(op)
    except StopIteration:
        pass
    return splits

def schedule_ops(ops_list: List[BaseOperator], num_parallel: int = 1) -> List[BaseOperator]:
    """ Build parallel operator chains. Returns list of last operators in each chain. """

    if not ops_list:
        raise ValueError('Empty list')
    if len(ops_list) == 1:
        return ops_list[0]
    
    splits = split_iterable(ops_list, num_parallel)
    for g in splits: 
        for a, b in pairwise(g):
            a.set_downstream(b)

    return [x[-1] for x in splits]
