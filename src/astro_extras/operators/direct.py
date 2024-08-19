# Astro SDK Extras project
# (c) kol, 2023

""" Templated SQL execution """

import logging
from airflow.models.xcom_arg import XComArg
from airflow.models.dag import DagContext
from airflow.utils.task_group import TaskGroup

from astro import sql as aql
from astro.sql.table import Table
from astro.airflow.datasets import kwargs_with_datasets

from typing import Optional, Iterable

from ..utils.utils import schedule_ops
from ..utils.template import get_template

_logger = logging.getLogger('airflow.task')

def run_sql_template(
    template: str,
    conn_id: str,
    input_tables: Iterable[Table] | None = None,
    affected_tables: Iterable[Table] | None = None,
    **kwargs
) -> XComArg:
    """ Runs an SQL script from template file.
    
    SQL script is a single SQL statement or multiple statements 
    to do something good within a single database.

    This function renders given template and executes resulting SQL. Templates are
    text files with optional macros (see `astro_extras.utils.template.get_template_file`).
    Templates are DAG specific.

    Normally, only `INSERT`, `UPDATE` and DDL constructs are used,
    but scope of statements is not limited. However, no data is ever returned to the caller,
    use Astro SDK `run_raw_sql` or `astro_extras.operators.table.load_table` instead if needed.

    Args:
        template: Template name. This should be a base name of `.sql` file
            located at <dags_home>/templates/<dag.dag_id> folder
        conn_id:    Connection to database where to execute templated SQL
        kwargs:     Any extra keyword arguments passed to Astro SDK's `run_raw_sql` function

    Returns:
        Task wrapped in `XComArg`

    Examples:
        This will execute file `./dags/templates/test_dag/test_template.sql` 
        on `target_db` connection:

        >>> with DAG(dag_id='test_dag', ...) as dag:
        >>>     run_sql_template('test_template', 'target_db')
    """

    dag = DagContext.get_current_dag()
    @aql.run_raw_sql(conn_id=conn_id,
                    task_id=f'run-{template}',
                    response_size=0,
                    inlets=input_tables,
                    outlets=affected_tables)
                    # **kwargs_with_datasets(kwargs=kwargs, 
                    #            input_datasets=input_tables,
                    #            output_datasets=affected_tables))
    def _run_sql(template: str):
        sql = get_template(template, '.sql', dag=dag, fail_if_not_found=True)
        _logger.info(f'Executing: {sql}')
        return sql

    return _run_sql(template)

def run_sql_templates(
    templates: Iterable[str],
    conn_id: str,
    group_id: str | None = None,
    num_parallel: int = 1,
    **kwargs,
) -> TaskGroup:
    """ Runs SQL scripts from multiple template files wrapping them up
    in Airflow's `TaskGroup`. See `run_sql_template` for details.

    Args:
        templates: List of template names, which are to be base names of `.sql` files
            located at <dags_home>/templates/<dag.dag_id> folder
        conn_id:    Connection to database where to execute templated SQL
        group_id:   Optional `TaskGroup` id
        num_parallel:   Expected number of parallel task sequences within a group.
            For example, if 4 templates are to be processed, and `num_parallel=2`,
            then 4 resulting tasks will be linked pairwise forming 2 separate chains.
        kwargs:     Any extra keyword arguments passed to Astro SDK's `run_raw_sql` function

    Returns:
        Airflow `TaskGroup`
    """

    with TaskGroup(group_id or 'run-sql', add_suffix_on_collision=True) as tg:
        ops_list = []
        for templ in templates:
            op = run_sql_template(templ, conn_id, **kwargs)
            ops_list.append(op)
        schedule_ops(ops_list, num_parallel)
    return tg
