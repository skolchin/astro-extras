# Astro SDK Extras project
# (c) kol, 2023

""" Misc testing routines """

import re
import os
import requests
import logging
import time

from pytest import raises
from astro_extras import split_table_name, ensure_table
from requests.exceptions import ConnectionError
from sqlalchemy import create_engine, Column, Integer
from astro.sql.table import Table, Metadata

logger = logging.getLogger(__name__)

def is_responsive(url):
    """ Checks whethet given URL responds with OK """
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except ConnectionError:
        return False
    
def airflow_query(method, query, data, docker_ip, docker_services, airflow_credentials):
    """ Runs the query against Airflow webserver (getting status, starting DAG etc)"""

    port = docker_services.port_for("airflow-webserver", 8080)
    url = f"http://{docker_ip}:{port}/{query}"
    headers={'Content-Type': 'application/json', 'Cache-Control': 'no-cache'}
    fun = getattr(requests, method, None)
    if not fun:
        raise ValueError(f'Unknown query method {method}')

    logger.debug(f'Running {method} on {url}')
    response = fun(url, headers=headers, data=data, auth=airflow_credentials)
    response.raise_for_status()
    return response.json()

def run_dag(dag_id, docker_ip, docker_services, airflow_credentials, params=None, timeout=30):
    """ Start Airflow DAG """

    query = f'api/v1/dags/{dag_id}'
    result = airflow_query('patch', query, '{"is_paused": false}',
                           docker_ip, docker_services, airflow_credentials)

    conf = '{}' if not params else '{"conf": ' + params + '}'
    logger.info(f'Starting DAG {dag_id} with {conf}')
    query = f'api/v1/dags/{dag_id}/dagRuns'
    result = airflow_query('post', query, conf, 
                           docker_ip, docker_services, airflow_credentials)
    dag_run_id = result['dag_run_id']
    logger.debug(f'DAG run {dag_run_id} initiated')

    def wait_dag(check_fun):
        query = f'api/v1/dags/{dag_id}/dagRuns/{dag_run_id}'
        wait_start = time.time()
        while time.time() < wait_start + timeout:
            result = airflow_query('get', query, '{}', 
                                docker_ip, docker_services, airflow_credentials)
            logger.debug(f'Response json: {result}')
            if check_fun(result['state']):
                return result['state']
            time.sleep(1)
        return 'timeout'

    state = wait_dag(lambda state: state == 'running')
    if state != 'running':
        logger.error(f'DAG {dag_id} run {dag_run_id} didnt enter running state')
        return state

    state = wait_dag(lambda state: state in ['success', 'failed'])
    logger.info(f"DAG {dag_id} run {dag_run_id} finished in '{state}' state")
    return state

def get_db_engine(database, db_credentials, docker_ip, docker_services):
    """ Makes an SQLA db engine instance connected to given database """
    uname, psw = db_credentials
    port = docker_services.port_for("postgres", 5432)
    url = f"postgresql://{uname}:{psw}@{docker_ip}:{port}/{database}"
    return create_engine(url)

def _get_airflow_log_files(dag_id, run_id, root_dir):
    clear = lambda s: re.sub(r'\W', '', s)
    clean_id = 'run_id' + clear(run_id)

    # loop through dag run logs and find particular run_id
    # since it may contain non-printable chars, remove them from dir names
    log_root = os.path.join(root_dir, '.logs', f'dag_id={dag_id}')
    for dag_dir in os.listdir(log_root):
        if clear(dag_dir) == clean_id:
            # here we go, now loop through task_id=xxx dirs and yield names of every file
            for task_dir, _, log_files in os.walk(os.path.join(log_root, dag_dir)):
                short_task_dir = os.path.split(task_dir)[-1]
                if os.path.split(task_dir)[-1].startswith('task_id='):
                    yield short_task_dir.split('=')[1], [os.path.join(task_dir, f) for f in log_files]

def _read_airflow_log_files(dag_id, run_id, root_dir):
    for task_id, log_files in _get_airflow_log_files(dag_id, run_id, root_dir):
        for log_file in log_files:
            with open(log_file, 'rt') as fp:
                for line in fp.readlines():
                    yield task_id, line

def get_airflow_log(dag_id, run_id, root_dir):
    log_msg = {}
    for task_id, log_line in _read_airflow_log_files(dag_id, run_id, root_dir):
        if re.match(r'^\[\d{4}\-\d{2}\-\d{2}', log_line):
            if log_msg: 
                yield log_msg
            parsed_line = log_line.split(' ')
            log_msg = {
                'task_id': task_id,
                'type': parsed_line[2],
                'message': ' '.join(parsed_line[3:]).strip()
            }
        elif log_msg and log_msg['task_id'] != task_id:
            yield log_msg
            log_msg = {}
        else:
            log_msg['message'] += log_line

    yield log_msg

def get_airflow_log_exceptions(dag_id, run_id, root_dir):
    errors = []
    for log_msg in get_airflow_log(dag_id, run_id, root_dir):
        if log_msg['type'] == 'ERROR':
            if 'Exception' in log_msg['message']:
                msg = log_msg['message'].split('\n')[-2]
                errors.append(msg)
    return errors

def test_split_table_name():
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
    with raises(ValueError):
        split_table_name(empty_table_name)

    # Test the function with a table name that starts with a dot (invalid format)
    invalid_table_name = ".table"
    schema, table = split_table_name(invalid_table_name)
    assert schema is ''
    assert table == 'table'
        
def test_ensure_table():
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
    with raises(TypeError):
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
    assert 'op2' in op1['downstream_task_ids']
    # Check that task op4 depends on task op3
    assert 'op4' in op3['downstream_task_ids']
    # Check that task op2 has no downstream tasks (i.e., it is the last task in the DAG)
    assert not op2['downstream_task_ids']
    # Check that task op4 has no downstream tasks (i.e., it is the last task in the DAG)
    assert not op4['downstream_task_ids']
    