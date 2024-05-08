# Astro SDK Extras project
# (c) kol, 2023

""" PyTest fixtures """

import os
import pytest
from pathlib import Path
from pytest_docker.plugin import get_cleanup_command
from confsupport import *

def pytest_addoption(parser):
    parser.addoption('--keep', action='store_true', default=False, 
                     help='Keep Airflow docker running after test')

@pytest.fixture(scope="session")
def keep_docker(pytestconfig):
    return pytestconfig.getoption('keep')

@pytest.fixture(scope="session")
def docker_compose_command():
    return "docker compose"

@pytest.fixture(scope="session")
def docker_cleanup(keep_docker, docker_compose_file):
    if not keep_docker:
        return get_cleanup_command() 
    
    logger.warning(f'To stop test dockers use: docker compose -f {docker_compose_file} -p astro-extras-test down -v')
    return None

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose-tests.yml")

@pytest.fixture(scope="session")
def docker_compose_project_name():
    return 'astro-extras-test'

@pytest.fixture(scope="session")
def env(pytestconfig):
    """ Reads an .env file from root directory and parses it to {<var>:<val>} dict """
    file_name = os.path.join(str(pytestconfig.rootdir), '.env')
    result = {}
    with open(file_name, 'rt') as fp:
        for line in fp.readlines():
            var, val = line.split('=')
            result[var.strip()] = val.strip()
    return result

@pytest.fixture(scope='session')
def airflow_credentials(airflow, env):
    """ Airflow login credentials from .env file. Also, starts Airflow docker """
    return (env.get('_AIRFLOW_WWW_USER_USERNAME', 'airflow'),
           env.get('_AIRFLOW_WWW_USER_PASSWORD', ''))

@pytest.fixture(scope='session')
def db_credentials(env):
    """ Database login credentials from .env file """
    return (env.get('POSTGRES_USER', 'postgres'), env.get('POSTGRES_PASSWORD', ''))

@pytest.fixture(scope="session")
def airflow(docker_ip, docker_services):
    """ Starts airflow docker stack and waits for webserver to be ready """
    logger.info('Waiting for Airflow docker to start')
    port = docker_services.port_for("airflow-webserver", 8080)
    url = f"http://{docker_ip}:{port}/health"
    docker_services.wait_until_responsive(
        timeout=60.0, pause=1.0, check=lambda: is_responsive(url)
    )
    return url

@pytest.fixture(scope='session')
def source_db(db_credentials, docker_ip, docker_services, pytestconfig):
    """ Makes a source database connection as SQLA engine """
    dir = pytestconfig.invocation_params.dir / 'init_source_db'
    logger.info(f'Initalizing source database using scripts from {dir} directory')

    engine = get_db_engine('source_db', db_credentials, docker_ip, docker_services)
    init_database(engine, dir)
    return engine

@pytest.fixture(scope='session')
def target_db(db_credentials, docker_ip, docker_services, pytestconfig):
    """ Makes a target database connection as SQLA engine """
    dir = pytestconfig.invocation_params.dir / 'init_target_db'
    logger.info(f'Initalizing target database using scripts from {dir} directory')

    engine = get_db_engine('target_db', db_credentials, docker_ip, docker_services)
    init_database(engine, dir)
    return engine
