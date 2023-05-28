# Astro SDK Extras project
# (c) kol, 2023

""" Misc testing routines """

import os
import requests
import logging
import time
from requests.exceptions import ConnectionError
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except ConnectionError:
        return False
    
def get_credentials(rootdir):
    file_name = os.path.join(rootdir, '.env')
    result = {}
    with open(file_name, 'rt') as fp:
        for line in fp.readlines():
            var, val = line.split('=')
            result[var.strip()] = val.strip()
    return result

def airflow_query(method, query, data, docker_ip, docker_services, airflow_credentials):
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
    uname, psw = db_credentials
    port = docker_services.port_for("postgres", 5432)
    url = f"postgresql://{uname}:{psw}@{docker_ip}:{port}/{database}"
    return create_engine(url)
