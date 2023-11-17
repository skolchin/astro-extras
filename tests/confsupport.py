# Astro SDK Extras project
# (c) kol, 2023

""" PyTest support functions """

import re
import os
import time
import requests
import logging

from requests.exceptions import ConnectionError
from sqlalchemy import create_engine

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

