# Astro SDK Extras project
# (c) kol, 2023

""" Direct SQL execution """

from airflow.models.xcom_arg import XComArg
from airflow.models.dag import DagContext
from airflow.utils.task_group import TaskGroup

from astro import sql as aql

from typing import Optional, Iterable

from ..utils.utils import get_template_file, schedule_ops

def run_sql_template(
    template: str,
    conn_id: str,
    **kwargs,
) -> XComArg:
    """ Runs an SQL script from template file """

    dag = DagContext.get_current_dag()
    @aql.run_raw_sql(conn_id=conn_id,
                    task_id=f'run-{template}',
                    response_size=0,
                    **kwargs)
    def _run_sql(template: str):
        sql_file = get_template_file(template, '.sql', dag=dag)
        return sql_file

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
