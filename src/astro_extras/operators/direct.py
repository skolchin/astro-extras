# Astro SDK Extras project
# (c) kol, 2023

""" Direct SQL execution """

import logging
from airflow.models.xcom_arg import XComArg
from airflow.models.dag import DagContext
from airflow.utils.task_group import TaskGroup

from astro import sql as aql

from typing import Optional, Iterable

from ..utils.utils import schedule_ops
from ..utils.template import get_template

_logger = logging.getLogger('airflow.task')

def run_sql_template(
    template: str,
    conn_id: str,
    **kwargs,
) -> XComArg:
    """ Runs an SQL script from template file.
    
    SQL script is a single SQL statement or multiple statements separated by ';',
    which do not return anything (e.g. `INSERT`, `UPDATE` and so on).

    This function renders given template and executes resulting SQL,
    which might be used for in-database data transfers, modificiations and so on.

    Args:
        template: Template name. This should be a base name of `.sql` file
            located at <dags_home>/templates/<dag.dag_id> folder.
        conn_id:    Connection to database where to execute templated SQL
        kwargs:     Any extra keyword arguments passed to Astro SDK's `run_raw_sql` function

    Returns:
        Task wrapped in `XComArg`

    See Also:
        `astro_extras.utils.utils.get_template_file`

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
                    **kwargs)
    def _run_sql(template: str):
        sql = get_template(template, '.sql', dag=dag, fail_if_not_found=True)
        _logger.info(f'Executing: {sql}')
        return sql

    return _run_sql(template)

def run_sql_templates(
    templates: Iterable[str],
    conn_id: str,
    group_id: Optional[str] = None,
    num_parallel: Optional[int] = 1,
    **kwargs,
) -> TaskGroup:
    """ Runs SQL scripts from multiple template files """

    with TaskGroup(group_id or 'run-sql', add_suffix_on_collision=True) as tg:
        ops_list = []
        for templ in templates:
            op = run_sql_template(templ, conn_id, **kwargs)
            ops_list.append(op)
        schedule_ops(ops_list, num_parallel)
    return tg
