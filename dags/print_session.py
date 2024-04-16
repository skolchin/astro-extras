# Astro SDK Extras project
# (c) kol, 2023

""" Print session info demo DAG """

import pendulum
import astro_extras as asx
from airflow.models import DAG, BaseOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='print-session-1',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['demo', 'session'],
) as dag:

    @dag.task
    def print_session(ti):
        session = ti.xcom_pull(task_ids='open-session', key='session')
        print(session)

    with TaskGroup('session-group') as tg:
        print_session() >> print_session()
        print_session()

    session = asx.open_session('source_db', 'target_db')
    session >> tg
    asx.close_session(session, tg)

with DAG(
    dag_id='print-session-2',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['demo', 'session'],
) as dag, asx.ETLSession('source_db', 'target_db') as session:

    @dag.task
    def print_session(session):
        print(session)

    print_session(session)

class PrintSessionOperator(BaseOperator):
    def execute(self, context):
        session = context['ti'].xcom_pull(key='session')
        print(session)

with DAG(
    dag_id='print-session-3',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
    tags=['demo', 'session'],
) as dag, asx.ETLSession('source_db', 'target_db') as session:

    with TaskGroup('session-group') as tg:
        ops = [PrintSessionOperator(task_id=f'print-session-{n+1}') for n in range(3)]
        asx.schedule_ops(ops, num_parallel=2)
