# Astro SDK Extras project
# (c) kol, 2023

""" PyTest fixtures """

import os
import pytest
from test_utils import *
# from pytest_docker import docker_cleanup

@pytest.fixture(scope="session")
def docker_compose_command():
    return "docker compose"

# @pytest.fixture(scope="session")
# def docker_cleanup():
#     return None

@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir), "docker-compose-tests.yml")

@pytest.fixture(scope="session")
def docker_compose_project_name():
    return 'astro-extras'

@pytest.fixture(scope='session')
def airflow_credentials(pytestconfig):
    credentials = get_credentials(str(pytestconfig.rootdir))
    return (credentials.get('_AIRFLOW_WWW_USER_USERNAME', 'airflow'),
           credentials.get('_AIRFLOW_WWW_USER_PASSWORD', ''))

@pytest.fixture(scope='session')
def db_credentials(pytestconfig):
    credentials = get_credentials(str(pytestconfig.rootdir))
    return (credentials.get('POSTGRES_USER', 'postgres'),
           credentials.get('POSTGRES_PASSWORD', ''))

@pytest.fixture(scope="session")
def airflow(docker_ip, docker_services):
    logger.info('Waiting for Airflow docker to start')
    port = docker_services.port_for("airflow-webserver", 8080)
    url = f"http://{docker_ip}:{port}/health"
    docker_services.wait_until_responsive(
        timeout=60.0, pause=1.0, check=lambda: is_responsive(url)
    )
    return url

@pytest.fixture(scope='session')
def source_db(db_credentials, docker_ip, docker_services):
    return get_db_engine('source_db', db_credentials, docker_ip, docker_services)

@pytest.fixture(scope='session')
def target_db(db_credentials, docker_ip, docker_services):
    return get_db_engine('target_db', db_credentials, docker_ip, docker_services)
