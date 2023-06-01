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
    """ Returns name of template file for given table
    
    This function looks up for a file `<table><ext>` in `./templates/<dag_id>` folder
    under directory set as DAGS_HOME in Airflow config (usually `./dags`).

    Such files are used as templates for SQL operations, html reporting and so on.
    They consists of some plain-text content mixed with Jinja macros denoted by `{{}}` brackets,
    which are automatically substituted by Airflow upon template execution.

    Args:
        table:  A table or any other object name
        ext:    Template file extension, `.sql` by default
        dag:    Optional DAG context

    Returns:
        Name of template file relative to Airflow's home folder or `None` if no file was found

    Examples:
        If a `test_table` is to be processed in a `test-transfer` DAG, 
        this SQL template could be used in order to add required `session_id` field 
        at the beginning of a target table:

            select {{ti.xcom_pull(key="session").session_id}} as session_id, * from test_table

        The template must be created as `./dags/templates/test-transfer/test_table.sql` file.

        Then, during this DAG execution, the template will automatically be picked up and executed,
        substituting macros with actual `session_id` value:

        >>> with DAG(dag_id='test-transfer', ...) as dag:
        >>>     session = open_session('source', 'target')
        >>>     transfer_table('test_table', session=session)
        >>>     close_session(session)

    """

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
