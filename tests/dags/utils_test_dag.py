# Astro SDK Extras project
# (c) kol, 2023

import pendulum

from airflow.models import DAG
from astro_extras import schedule_ops
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id='test-schedule-ops',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    # Create a list of DummyOperator instances with task_ids "op1" and "op2"
    ops_list = [DummyOperator(task_id=f"op{i}") for i in range(1, 3)]

    # Call the schedule_ops function with ops_list as input
    op_last = schedule_ops(ops_list)


with DAG(
    dag_id='test-schedule-ops-parallel',
    start_date=pendulum.today().add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    # Create a list of DummyOperator instances with task_ids "op1" to "op4"
    ops_list = [DummyOperator(task_id=f"op{i}") for i in range(1, 5)]

    # Call the schedule_ops function with ops_list and num_parallel=2
    op_last = schedule_ops(ops_list, num_parallel=2)
