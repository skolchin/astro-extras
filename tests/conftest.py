# Astro SDK Extras project
# (c) kol, 2023

""" PyTest fixtures """


import os
import pytest

from pytest_docker.plugin import get_cleanup_command
from confsupport import *

from sqlalchemy import MetaData

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
def source_db(request, db_credentials, docker_ip, docker_services, pytestconfig):
    """Creates a source database connection as SQLA engine"""
    conn_id = getattr(request, 'param', 'source_db')
    return setup_db(conn_id, db_credentials, docker_ip, docker_services, pytestconfig)


@pytest.fixture(scope='session')
def target_db(request, db_credentials, docker_ip, docker_services, pytestconfig):
    """Creates a target database connection as SQLA engine"""
    conn_id = getattr(request, 'param', 'target_db')
    return setup_db(conn_id, db_credentials, docker_ip, docker_services, pytestconfig)


@pytest.fixture(scope='function')
def src_metadata(request, source_db):
    """Creates a SQLAlchemy MetaData object for the source database with a specified schema"""
    schema = getattr(request, 'param', 'public')
    metadata = MetaData(schema=schema)
    metadata.reflect(bind=source_db)
    return metadata


@pytest.fixture(scope='function')
def tgt_metadata(request, target_db):
    """Creates a SQLAlchemy MetaData object for the target database with a specified schema"""
    schema = getattr(request, 'param', 'public')
    metadata = MetaData(schema=schema)
    metadata.reflect(bind=target_db)
    return metadata
