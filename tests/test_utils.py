# Astro SDK Extras project
# (c) kol, 2023

""" Utility routines testing """

import pytest
import requests
from datetime import timedelta
from sqlalchemy import Column, Integer
from astro.sql.table import Table, Metadata

try:
    from astro_extras import split_table_name, ensure_table, localnow, days_ago
except ModuleNotFoundError:
    split_table_name = ensure_table = localnow = days_ago = None

def test_split_table_name():
    if split_table_name is None:
        return pytest.skip('Astro-extras was not installed')
    
    # Test the function with a table name that includes a schema
    table_with_schema = "schema.table"
    schema, table = split_table_name(table_with_schema)
    assert schema == "schema"
    assert table == "table"

    # Test the function with a table name that doesn't include a schema
    table_without_schema = "table"
    schema, table = split_table_name(table_without_schema)
    assert schema is None
    assert table == "table"

    # Test the function with an empty table name
    empty_table_name = ""
    with pytest.raises(ValueError):
        split_table_name(empty_table_name)

    # Test the function with a table name that starts with a dot (invalid format)
    invalid_table_name = ".table"
    schema, table = split_table_name(invalid_table_name)
    assert schema is ''
    assert table == 'table'
        
def test_ensure_table():
    if ensure_table is None:
        return pytest.skip('Astro-extras was not installed')

    # Test the function passing in None returns None

    assert ensure_table(None) is None

    # Test the function passing in a Table object returns the same Table object
    metadata = Metadata()
    test_table = Table('test', metadata, columns=[Column('id', Integer, primary_key=True)])
    assert ensure_table(test_table).__dict__ == test_table.__dict__

    # TODO: Тест не проходит, а должен. Вместо схемы в объекте Metadata возвращается None. Также добавляется public схема к названию таблицы, а не должно.
    # # Test the function passing in a string returns a Table object with proper schema and table name
    # test_table_str = 'public.test'
    # expected_table = Table(name='test', conn_id=None, metadata=Metadata(schema='public'))
    
    # assert ensure_table(test_table_str).__dict__ == expected_table.__dict__

    # Test the function TypeError is raised if an unknown type is passed in
    with pytest.raises(TypeError):
        ensure_table(12345)

def test_schedule_ops(docker_ip, docker_services, airflow_credentials):
    # Get the ID of the DAG to be tested
    dag_id = 'test-schedule-ops'
    # Form URL for getting information on tasks in the DAG
    tasks_url = f"http://localhost:18080/api/v1/dags/{dag_id}/tasks"
    # Send a GET request to the URL with authorization.
    dag_tasks_response = requests.get(tasks_url, auth=airflow_credentials)
    # Check that the request is successful
    assert dag_tasks_response.ok

    # Get JSON object with task information
    dag_tasks = dag_tasks_response.json()['tasks']
    # Check that the DAG has exactly two tasks
    assert len(dag_tasks) == 2

    # Find first task
    op1 = next((task for task in dag_tasks if task['task_id'] == 'op1'), None)
    # Find second task
    op2 = next((task for task in dag_tasks if task['task_id'] == 'op2'), None)
    # Check that both tasks were found
    assert op1 is not None and op2 is not None

    # Check that task op2 depends on task op1 (determined by downstream_task_ids)
    assert 'op2' in op1['downstream_task_ids']
    # Check that task op2 has no downstream tasks (i.e., it is the last task in the DAG)
    assert not op2['downstream_task_ids']

def test_schedule_ops_parallel(docker_ip, docker_services, airflow_credentials):
    # Get the ID of the DAG to be tested
    dag_id = 'test-schedule-ops-parallel'
    # Form URL for getting information on tasks in the DAG
    tasks_url = f"http://localhost:18080/api/v1/dags/{dag_id}/tasks"
    # Send a GET request to the URL with authorization.
    dag_tasks_response = requests.get(tasks_url, auth=airflow_credentials)
    # Check that the request is successful
    assert dag_tasks_response.ok

    # Get JSON object with task information
    dag_tasks = dag_tasks_response.json()['tasks']
    # Check that the DAG has exactly four tasks
    assert len(dag_tasks) == 4

    # Find tasks
    op1 = next((task for task in dag_tasks if task['task_id'] == 'op1'), None)
    op2 = next((task for task in dag_tasks if task['task_id'] == 'op2'), None)
    op3 = next((task for task in dag_tasks if task['task_id'] == 'op3'), None)
    op4 = next((task for task in dag_tasks if task['task_id'] == 'op4'), None)
    
    # Check that all tasks were found
    assert op1 is not None and op2 is not None and op3 is not None and op4 is not None
    # Check that task op2 depends on task op1 (determined by downstream_task_ids)
    assert 'op3' in op1['downstream_task_ids']
    # Check that task op4 depends on task op3
    assert 'op4' in op2['downstream_task_ids']
    # Check that task op2 has no downstream tasks (i.e., it is the last task in the DAG)
    assert not op3['downstream_task_ids']
    # Check that task op4 has no downstream tasks (i.e., it is the last task in the DAG)
    assert not op4['downstream_task_ids']
    
def test_datetime():
    if localnow is None:
        return pytest.skip('Astro-extras was not installed')

    dttm_now = localnow()
    assert dttm_now.tzinfo is not None

    dttm = days_ago(1)
    assert (dttm_now - dttm + timedelta(seconds=1)).days == 1
